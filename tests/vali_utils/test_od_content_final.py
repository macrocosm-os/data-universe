"""Tests for OD Phase A: content-final timing guard + shadow upload-lag +
URL-normalizer hardening.

Covers:
- normalize_url_for_dedup: X keys on tweet ID only; crafted/encoded variants cannot mint keys
- _od_content_final_violation: overwrite-past-expiry detection (with grace), None-safety
- _evaluate_od: listing-level timing screen penalizes violators without sampling them
- _validate_od_submission: fails fast on violation; ETag pins content to the listing
- _log_od_coverage_shadow: upload_lag distribution incl. missing-metadata counter
- od_sampled_rows carries unique_rows/lag_s; speed scored from s3_last_modified
"""

import asyncio
import datetime as dt
import json
import threading
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from common.api_client import (
    MinerJobForValidation,
    OnDemandJob,
    OnDemandJobsStatsResponse,
    OnDemandJobSubmission,
    PlatformJobStats,
)
from vali_utils.miner_evaluator import MinerEvaluator
from vali_utils.url_normalizer import normalize_url_for_dedup


def _stub_evaluator():
    """MinerEvaluator instance without running __init__ (heavy deps)."""
    ev = MinerEvaluator.__new__(MinerEvaluator)
    ev._od_stats_cache = None
    ev._od_stats_lock = threading.Lock()
    ev._last_od_eval_at = {}
    ev.on_demand_validator = MagicMock()
    return ev


def _job(
    platform: str,
    expire_at: dt.datetime,
    content_length: int = 100,
    submitted_at: dt.datetime = None,
    s3_last_modified: dt.datetime = None,
    s3_etag: str = None,
) -> MinerJobForValidation:
    payload = {"platform": platform, "keywords": ["k"]}
    return MinerJobForValidation(
        job=OnDemandJob(id="j", expire_at=expire_at, job=payload),
        submission=OnDemandJobSubmission(
            job_id="j",
            s3_content_length=content_length,
            submitted_at=submitted_at,
            s3_last_modified=s3_last_modified,
            s3_etag=s3_etag,
            s3_presigned_url="https://example.com/presigned",
        ),
    )


def _stats(reddit=(90, 85), x=(60, 40)) -> OnDemandJobsStatsResponse:
    return OnDemandJobsStatsResponse(
        platforms={
            "reddit": PlatformJobStats(total_jobs=reddit[0], doable_jobs=reddit[1]),
            "x": PlatformJobStats(total_jobs=x[0], doable_jobs=x[1]),
        }
    )


class TestUrlNormalizerX(unittest.TestCase):
    """X must key on the tweet ID alone — no URL variant may mint a distinct key."""

    def test_username_variants_collapse(self):
        urls = [
            "https://x.com/alice/status/123456",
            "https://x.com/BOB/status/123456",
            "https://twitter.com/carol/status/123456",
            "https://mobile.twitter.com/dave/status/123456",
        ]
        keys = {normalize_url_for_dedup(u) for u in urls}
        self.assertEqual(keys, {"x:123456"})

    def test_userless_legacy_and_suffix_paths(self):
        self.assertEqual(
            normalize_url_for_dedup("https://x.com/i/web/status/123456"), "x:123456"
        )
        self.assertEqual(
            normalize_url_for_dedup("https://twitter.com/statuses/123456"), "x:123456"
        )
        self.assertEqual(
            normalize_url_for_dedup("https://x.com/alice/status/55/photo/2"), "x:55"
        )

    def test_tracking_params_and_fragments_ignored(self):
        self.assertEqual(
            normalize_url_for_dedup("https://x.com/a/status/99?utm_source=z#top"),
            "x:99",
        )

    def test_leading_zero_ids_collapse(self):
        self.assertEqual(
            normalize_url_for_dedup("https://x.com/a/status/00123"),
            normalize_url_for_dedup("https://x.com/b/status/123"),
        )

    def test_status_segment_hijack_cannot_mint_keys(self):
        """A crafted leading /status/N/ segment must not vary the key."""
        a = normalize_url_for_dedup("https://x.com/status/1/status/123456")
        b = normalize_url_for_dedup("https://x.com/status/2/status/123456")
        self.assertEqual(a, b)
        self.assertEqual(a, "x:unparseable")

    def test_percent_encoded_and_double_encoded_collapse(self):
        single = normalize_url_for_dedup("https://x.com/alice%2Fstatus%2F123456")
        double = normalize_url_for_dedup("https://x.com/alice%252Fstatus%252F123456")
        self.assertEqual(single, "x:123456")
        self.assertEqual(double, "x:123456")

    def test_nfkc_fullwidth_digits_collapse(self):
        self.assertEqual(
            normalize_url_for_dedup("https://x.com/alice/status/１２３"),
            "x:123",
        )

    def test_host_dot_and_port_variants_collapse(self):
        self.assertEqual(
            normalize_url_for_dedup("https://x.com./alice/status/55"), "x:55"
        )
        self.assertEqual(
            normalize_url_for_dedup("https://x.com:443/alice/status/55"), "x:55"
        )

    def test_fallback_host_dot_port_order(self):
        """Port must strip before the trailing dot ('host.:443' collapses too)."""
        self.assertEqual(
            normalize_url_for_dedup("https://example.com.:443/watch"),
            normalize_url_for_dedup("https://example.com/watch"),
        )

    def test_non_status_x_paths_collapse_to_sentinel(self):
        """Non-tweet x.com URLs cannot each mint their own key."""
        keys = {
            normalize_url_for_dedup("https://x.com/pad1"),
            normalize_url_for_dedup("https://x.com/pad2"),
            normalize_url_for_dedup("https://x.com/alice"),
        }
        self.assertEqual(keys, {"x:unparseable"})

    def test_reddit_forms_unchanged(self):
        self.assertEqual(
            normalize_url_for_dedup(
                "https://www.reddit.com/r/Sub/comments/abc123/some_slug/"
            ),
            "reddit:abc123",
        )
        self.assertEqual(
            normalize_url_for_dedup(
                "https://old.reddit.com/r/Sub/comments/abc123/slug/def456/"
            ),
            "reddit:abc123:def456",
        )

    def test_reddit_encoded_slug_keeps_comment_id(self):
        """Decoded '#' in a slug must not gain structural meaning (fragment)."""
        self.assertEqual(
            normalize_url_for_dedup(
                "https://www.reddit.com/r/sub/comments/abc123/a%23b/def456"
            ),
            "reddit:abc123:def456",
        )


class TestContentFinalGuard(unittest.TestCase):
    def setUp(self):
        self.ev = _stub_evaluator()
        self.now = dt.datetime.now(dt.timezone.utc)
        self.guard = MinerEvaluator.OD_CONTENT_FINAL_GUARD_SECS

    def _violation(self, lm_after_expiry_s, submitted_at="default"):
        expire_at = self.now + dt.timedelta(seconds=120)
        sub = self.now if submitted_at == "default" else submitted_at
        j = _job(
            "reddit",
            expire_at,
            submitted_at=sub,
            s3_last_modified=expire_at + dt.timedelta(seconds=lm_after_expiry_s),
        )
        return self.ev._od_content_final_violation(j.job, j.submission)

    def test_write_before_expiry_ok(self):
        self.assertIsNone(self._violation(lm_after_expiry_s=-30))

    def test_write_within_grace_ok(self):
        """Honest uploads finishing just past the deadline are spared."""
        self.assertIsNone(self._violation(lm_after_expiry_s=self.guard - 5))

    def test_write_past_expiry_grace_flagged(self):
        v = self._violation(lm_after_expiry_s=self.guard + 60)
        self.assertIsNotNone(v)
        self.assertIn("expiry", v)

    def test_missing_submitted_at_does_not_disable_guard(self):
        """The expiry check needs only s3_last_modified — a missing
        submitted_at must not fail open."""
        v = self._violation(lm_after_expiry_s=1800, submitted_at=None)
        self.assertIsNotNone(v)

    def test_missing_head_metadata_is_not_a_violation(self):
        j = _job("reddit", self.now, submitted_at=self.now, s3_last_modified=None)
        self.assertIsNone(self.ev._od_content_final_violation(j.job, j.submission))

    def test_naive_datetimes_handled(self):
        naive = dt.datetime.utcnow()
        j = _job(
            "reddit",
            naive + dt.timedelta(seconds=120),
            submitted_at=naive,
            s3_last_modified=naive + dt.timedelta(seconds=999),
        )
        self.assertIsNotNone(self.ev._od_content_final_violation(j.job, j.submission))


class TestValidateSubmissionGates(unittest.TestCase):
    def setUp(self):
        self.now = dt.datetime.now(dt.timezone.utc)

    def test_timing_violation_fails_without_download(self):
        ev = _stub_evaluator()
        expire = self.now - dt.timedelta(minutes=10)
        j = _job(
            "reddit",
            expire,
            submitted_at=expire - dt.timedelta(seconds=30),
            s3_last_modified=expire + dt.timedelta(seconds=999),
        )
        with patch("vali_utils.miner_evaluator.httpx.AsyncClient") as mock_http:
            result = asyncio.run(
                ev._validate_od_submission(1, "hk", j.job, j.submission, "j")
            )
        self.assertEqual(result, (False, 0, 0))
        mock_http.assert_not_called()

    def test_etag_mismatch_fails(self):
        """Content overwritten between the API listing and our GET must fail."""
        ev = _stub_evaluator()
        j = _job(
            "reddit",
            self.now + dt.timedelta(seconds=120),
            submitted_at=self.now,
            s3_last_modified=self.now + dt.timedelta(seconds=2),
            s3_etag="aaa111",
        )
        dl_resp = MagicMock(status_code=200, headers={"etag": '"bbb222"'})
        with patch("vali_utils.miner_evaluator.httpx.AsyncClient") as mock_http:
            client = mock_http.return_value.__aenter__.return_value
            client.get = AsyncMock(return_value=dl_resp)
            result = asyncio.run(
                ev._validate_od_submission(1, "hk", j.job, j.submission, "j")
            )
        self.assertEqual(result, (False, 0, 0))


class TestListingLevelTimingScreen(unittest.TestCase):
    def test_violating_submission_penalized_not_sampled(self):
        ev = _stub_evaluator()
        ev.scorer = MagicMock()
        now = dt.datetime.now(dt.timezone.utc)
        expire = now - dt.timedelta(minutes=30)
        violator = _job(
            "reddit",
            expire,
            submitted_at=expire - dt.timedelta(seconds=10),
            s3_last_modified=expire + dt.timedelta(seconds=999),
        )

        resp = MagicMock()
        resp.jobs = [violator]
        client = MagicMock()
        client.validator_list_miner_jobs = AsyncMock(return_value=resp)
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        ev._on_demand_client = MagicMock(return_value=client)
        ev._get_od_jobs_stats = AsyncMock(return_value=_stats())
        ev._log_od_coverage_shadow = MagicMock()
        ev._validate_od_submission = AsyncMock()

        asyncio.run(ev._evaluate_od(1, "hk"))

        ev.scorer.apply_ondemand_penalty.assert_called_once_with(uid=1, mult_factor=1.0)
        ev._validate_od_submission.assert_not_awaited()
        ev.scorer.apply_ondemand_credibility_bump.assert_not_called()


class TestShadowUploadLag(unittest.TestCase):
    def setUp(self):
        self.ev = _stub_evaluator()
        self.now = dt.datetime.now(dt.timezone.utc)
        self.since = self.now - dt.timedelta(hours=3)

    def _capture_event(self, jobs, stats):
        with patch("vali_utils.miner_evaluator.bt.logging.info") as mock_log:
            self.ev._log_od_coverage_shadow(1, "hk", jobs, stats, self.since)
            if not mock_log.call_args_list:
                return None
            return json.loads(mock_log.call_args_list[-1].args[0])

    def test_lag_distribution_reported(self):
        in_window = self.now - dt.timedelta(hours=1)
        jobs = [
            _job(
                "reddit",
                in_window,
                submitted_at=in_window - dt.timedelta(seconds=100),
                s3_last_modified=in_window - dt.timedelta(seconds=98),  # lag 2s
            ),
            _job(
                "reddit",
                in_window,
                submitted_at=in_window - dt.timedelta(seconds=100),
                s3_last_modified=in_window + dt.timedelta(seconds=200),  # lag 300s, past expiry
            ),
        ]
        event = self._capture_event(jobs, _stats())
        lag = event["upload_lag"]
        self.assertEqual(lag["n"], 2)
        self.assertEqual(lag["p50"], 151.0)  # median of [2, 300]
        self.assertEqual(lag["max"], 300.0)
        self.assertEqual(lag["over_guard"], 1)
        self.assertEqual(lag["over_expiry"], 1)
        self.assertEqual(lag["missing"], 0)

    def test_missing_metadata_counted(self):
        """Non-empty submissions without HEAD metadata must be visible —
        their growth silently degrades the content-final protection."""
        jobs = [_job("reddit", self.now - dt.timedelta(hours=1))]
        event = self._capture_event(jobs, _stats())
        self.assertEqual(event["upload_lag"]["n"], 0)
        self.assertEqual(event["upload_lag"]["missing"], 1)
        self.assertIsNone(event["upload_lag"]["p50"])


class TestSampledRowsAndSpeedSwap(unittest.TestCase):
    def test_event_fields_and_content_final_speed(self):
        ev = _stub_evaluator()
        ev.scorer = MagicMock()
        now = dt.datetime.now(dt.timezone.utc)
        lm = now - dt.timedelta(minutes=5)
        job = _job(
            "reddit",
            now - dt.timedelta(minutes=4),  # lm precedes expiry — no violation
            submitted_at=lm - dt.timedelta(seconds=3),
            s3_last_modified=lm,
        )

        resp = MagicMock()
        resp.jobs = [job]
        client = MagicMock()
        client.validator_list_miner_jobs = AsyncMock(return_value=resp)
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        ev._on_demand_client = MagicMock(return_value=client)
        ev._get_od_jobs_stats = AsyncMock(return_value=_stats())
        ev._log_od_coverage_shadow = MagicMock()
        ev._validate_od_submission = AsyncMock(return_value=(True, 10, 7))
        ev.on_demand_validator.calculate_ondemand_reward_multipliers = MagicMock(
            return_value=(1.0, 1.0)
        )

        with patch("vali_utils.miner_evaluator.bt.logging.info") as mock_log:
            asyncio.run(ev._evaluate_od(1, "hk"))

        sampled_events = [
            json.loads(c.args[0])
            for c in mock_log.call_args_list
            if c.args and isinstance(c.args[0], str) and "od_sampled_rows" in c.args[0]
        ]
        self.assertEqual(len(sampled_events), 1)
        sample = sampled_events[0]["sampled"][0]
        self.assertEqual(sample["rows"], 10)
        self.assertEqual(sample["unique_rows"], 7)
        self.assertAlmostEqual(sample["lag_s"], 3.0, places=1)

        # Speed must be scored from the S3 write time, not the pre-upload stamp.
        call_kwargs = (
            ev.on_demand_validator.calculate_ondemand_reward_multipliers.call_args.kwargs
        )
        self.assertEqual(call_kwargs["submission_timestamp"], lm)


if __name__ == "__main__":
    unittest.main()
