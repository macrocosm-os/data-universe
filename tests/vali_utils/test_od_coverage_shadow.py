"""Tests for OD coverage measurement and the coverage multiplier.

Covers:
- _log_od_coverage_shadow: per-platform numerators, thin-window skip, clamping
- _get_od_jobs_stats: caching across calls, failure tolerance
- _evaluate_od: abstention penalty, coverage-ratio multiplier (skipping
  penalty), full-page truncation failsafe, reward path still restricted to
  the since-last-eval window
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
    OnDemandJobSubmission,
    OnDemandJobsStatsResponse,
    PlatformJobStats,
)
from vali_utils.miner_evaluator import MinerEvaluator


def _stub_evaluator():
    """MinerEvaluator instance without running __init__ (heavy deps)."""
    ev = MinerEvaluator.__new__(MinerEvaluator)
    ev._od_stats_cache = None
    ev._od_stats_lock = threading.Lock()
    ev._last_od_eval_at = {}
    ev.on_demand_validator = MagicMock()
    return ev


def _job(platform: str, expire_at: dt.datetime, content_length: int = 100) -> MinerJobForValidation:
    payload = {"platform": platform, "keywords": ["k"]}
    return MinerJobForValidation(
        job=OnDemandJob(id="j", expire_at=expire_at, job=payload),
        submission=OnDemandJobSubmission(job_id="j", s3_content_length=content_length),
    )


def _stats(reddit=(90, 85), x=(60, 4)) -> OnDemandJobsStatsResponse:
    return OnDemandJobsStatsResponse(
        platforms={
            "reddit": PlatformJobStats(total_jobs=reddit[0], doable_jobs=reddit[1]),
            "x": PlatformJobStats(total_jobs=x[0], doable_jobs=x[1]),
        }
    )


class TestLogCoverageShadow(unittest.TestCase):
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

    def test_counts_per_platform_within_window(self):
        in_window = self.now - dt.timedelta(hours=1)
        out_of_window = self.now - dt.timedelta(hours=5)
        jobs = [
            _job("reddit", in_window, content_length=9_000),
            _job("reddit", in_window, content_length=11_000),
            _job("reddit", out_of_window, content_length=999_999),  # excluded
            _job("x", in_window, content_length=500),
        ]
        event = self._capture_event(jobs, _stats(reddit=(90, 85), x=(60, 40)))
        self.assertEqual(event["reddit"]["submitted"], 2)
        self.assertEqual(event["x"]["submitted"], 1)
        self.assertAlmostEqual(event["reddit"]["coverage"], 2 / 85, places=4)
        self.assertAlmostEqual(event["x"]["coverage"], 1 / 40, places=4)
        self.assertEqual(event["reddit"]["bytes"], 20_000)
        self.assertEqual(event["reddit"]["avg_bytes"], 10_000)
        self.assertEqual(event["x"]["bytes"], 500)

    def test_thin_platform_reports_null_coverage(self):
        """Below OD_COVERAGE_MIN_PLATFORM_JOBS doable jobs -> coverage is None."""
        jobs = [_job("x", self.now - dt.timedelta(hours=1))]
        event = self._capture_event(jobs, _stats(x=(60, 4)))
        self.assertIsNone(event["x"]["coverage"])
        self.assertEqual(event["x"]["submitted"], 1)  # still reported raw

    def test_coverage_clamped_to_one(self):
        """Submitting to more jobs than doable (non-doable jobs) clamps at 1.0."""
        in_window = self.now - dt.timedelta(hours=1)
        jobs = [_job("reddit", in_window) for _ in range(50)]
        event = self._capture_event(jobs, _stats(reddit=(45, 40)))
        self.assertEqual(event["reddit"]["coverage"], 1.0)

    def test_no_stats_logs_nothing(self):
        event = self._capture_event([_job("reddit", self.now)], None)
        self.assertIsNone(event)


class TestStatsCache(unittest.TestCase):
    def setUp(self):
        self.ev = _stub_evaluator()

    def test_second_call_uses_cache(self):
        client = MagicMock()
        client.validator_get_jobs_stats = AsyncMock(return_value=_stats())

        first = asyncio.run(self.ev._get_od_jobs_stats(client))
        second = asyncio.run(self.ev._get_od_jobs_stats(client))

        self.assertIs(first, second)
        client.validator_get_jobs_stats.assert_awaited_once()

    def test_fetch_failure_returns_none_and_does_not_cache(self):
        client = MagicMock()
        client.validator_get_jobs_stats = AsyncMock(side_effect=RuntimeError("boom"))

        self.assertIsNone(asyncio.run(self.ev._get_od_jobs_stats(client)))
        self.assertIsNone(self.ev._od_stats_cache)

        # A later successful fetch works.
        client.validator_get_jobs_stats = AsyncMock(return_value=_stats())
        self.assertIsNotNone(asyncio.run(self.ev._get_od_jobs_stats(client)))


class _EvaluateOdTestBase(unittest.TestCase):
    def _run_eval(self, jobs, stats, fetch_error=None):
        ev = _stub_evaluator()
        ev.scorer = MagicMock()
        resp = MagicMock()
        resp.jobs = jobs
        client = MagicMock()
        if fetch_error is not None:
            client.validator_list_miner_jobs = AsyncMock(side_effect=fetch_error)
        else:
            client.validator_list_miner_jobs = AsyncMock(return_value=resp)
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        ev._on_demand_client = MagicMock(return_value=client)
        ev._get_od_jobs_stats = AsyncMock(return_value=stats)
        ev._log_od_coverage_shadow = MagicMock()
        ev._validate_od_submission = AsyncMock(return_value=(True, 10))
        ev.on_demand_validator.calculate_ondemand_reward_multipliers = MagicMock(
            return_value=(1.0, 1.0)
        )
        asyncio.run(ev._evaluate_od(1, "hk"))
        return ev


class TestAbstentionPenalty(_EvaluateOdTestBase):
    def test_x_abstainer_penalized(self):
        """Full-coverage reddit miner gets OD_ABSTAIN_MULT when doable X jobs existed."""
        from rewards.miner_scorer import MinerScorer

        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1)) for _ in range(20)]
        ev = self._run_eval(jobs, _stats(reddit=(90, 85), x=(60, 4)))
        ev.scorer.set_od_coverage_mult.assert_called_once_with(
            1, MinerScorer.OD_ABSTAIN_MULT
        )

    def test_fully_absent_miner_penalized_on_both(self):
        """Zero submissions anywhere -> penalty applied per platform, despite early return."""
        from rewards.miner_scorer import MinerScorer

        ev = self._run_eval([], _stats(reddit=(90, 85), x=(60, 4)))
        ev.scorer.set_od_coverage_mult.assert_called_once_with(
            1, MinerScorer.OD_ABSTAIN_MULT ** 2
        )

    def test_thin_platform_not_judged_for_abstention(self):
        """X with fewer doable jobs than OD_ABSTAIN_MIN_PLATFORM_JOBS is skipped."""
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1)) for _ in range(20)]
        ev = self._run_eval(jobs, _stats(reddit=(90, 85), x=(60, 2)))
        ev.scorer.set_od_coverage_mult.assert_called_once_with(1, 1.0)

    def test_participant_restored_to_full_mult(self):
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1)) for _ in range(20)] + [
            _job("x", now - dt.timedelta(hours=1)),
        ]
        ev = self._run_eval(jobs, _stats(reddit=(90, 85), x=(60, 4)))
        ev.scorer.set_od_coverage_mult.assert_called_once_with(1, 1.0)


class TestCoverageRatioMult(_EvaluateOdTestBase):
    """Low coverage of doable jobs scales the OD multiplier (skipping penalty)."""

    def _mult(self, ev):
        return ev.scorer.set_od_coverage_mult.call_args.args[1]

    def _reddit_jobs(self, n):
        now = dt.datetime.now(dt.timezone.utc)
        return [_job("reddit", now - dt.timedelta(hours=1)) for _ in range(n)] + [
            _job("x", now - dt.timedelta(hours=1))
        ]

    def test_coverage_at_threshold_gets_full_mult(self):
        """coverage == OD_COVERAGE_FULL_MULT_AT -> 1.0 (15/100 doable)."""
        ev = self._run_eval(self._reddit_jobs(15), _stats(reddit=(110, 100), x=(60, 4)))
        self.assertAlmostEqual(self._mult(ev), 1.0, places=4)

    def test_coverage_below_threshold_scales_linearly(self):
        """10/100 doable -> 0.10 coverage -> 0.10/0.15 mult."""
        ev = self._run_eval(self._reddit_jobs(10), _stats(reddit=(110, 100), x=(60, 4)))
        self.assertAlmostEqual(self._mult(ev), 0.10 / 0.15, places=4)

    def test_minimal_complier_floored_at_abstain_mult(self):
        """1/100 doable -> ratio would be 0.067 but floors at OD_ABSTAIN_MULT."""
        from rewards.miner_scorer import MinerScorer

        ev = self._run_eval(self._reddit_jobs(1), _stats(reddit=(110, 100), x=(60, 4)))
        self.assertAlmostEqual(self._mult(ev), MinerScorer.OD_ABSTAIN_MULT, places=4)

    def test_thin_platform_participant_not_ratio_judged(self):
        """X participant with doable < OD_COVERAGE_MIN_PLATFORM_JOBS: no ratio penalty."""
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1)) for _ in range(20)] + [
            _job("x", now - dt.timedelta(hours=1))
        ]
        ev = self._run_eval(jobs, _stats(reddit=(90, 85), x=(60, 19)))
        self.assertAlmostEqual(self._mult(ev), 1.0, places=4)

    def test_full_fetch_page_resolves_in_miners_favor(self):
        """A full page (possible truncation) -> mult 1.0, no coverage/abstention judged."""
        now = dt.datetime.now(dt.timezone.utc)
        # 1000 reddit jobs, zero x jobs: without the failsafe this would be
        # ratio-judged on a truncated numerator AND x-abstention-penalized.
        jobs = [
            _job("reddit", now - dt.timedelta(hours=1))
            for _ in range(MinerEvaluator.OD_JOBS_FETCH_LIMIT)
        ]
        ev = self._run_eval(jobs, _stats(reddit=(9000, 8500), x=(60, 4)))
        ev.scorer.set_od_coverage_mult.assert_called_once_with(1, 1.0)

    def test_out_of_window_jobs_do_not_count_toward_coverage(self):
        """Jobs older than the coverage window are excluded from the numerator."""
        from rewards.miner_scorer import MinerScorer

        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=5)) for _ in range(50)] + [
            _job("reddit", now - dt.timedelta(hours=1)),
            _job("x", now - dt.timedelta(hours=1)),
        ]
        ev = self._run_eval(jobs, _stats(reddit=(110, 100), x=(60, 4)))
        self.assertAlmostEqual(self._mult(ev), MinerScorer.OD_ABSTAIN_MULT, places=4)

    def test_empty_submissions_dodge_abstention_but_not_ratio(self):
        """0-byte submissions count against abstention but earn no coverage."""
        from rewards.miner_scorer import MinerScorer

        now = dt.datetime.now(dt.timezone.utc)
        jobs = [
            _job("reddit", now - dt.timedelta(hours=1), content_length=0)
            for _ in range(30)
        ] + [_job("x", now - dt.timedelta(hours=1))]
        ev = self._run_eval(jobs, _stats(reddit=(110, 100), x=(60, 4)))
        # No abstention (they did submit), but ratio coverage is 0 -> floor.
        self.assertAlmostEqual(self._mult(ev), MinerScorer.OD_ABSTAIN_MULT, places=4)

    def test_full_page_of_old_jobs_still_resolves_in_miners_favor(self):
        """The failsafe keys off the RAW page length, not the window-filtered count."""
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [
            _job("reddit", now - dt.timedelta(hours=8))
            for _ in range(MinerEvaluator.OD_JOBS_FETCH_LIMIT)
        ]
        ev = self._run_eval(jobs, _stats(reddit=(9000, 8500), x=(60, 4)))
        ev.scorer.set_od_coverage_mult.assert_called_once_with(1, 1.0)

    def test_zero_doable_platform_is_ignored(self):
        """A platform with zero doable jobs neither penalizes nor divides by zero."""
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1)) for _ in range(20)]
        ev = self._run_eval(jobs, _stats(reddit=(90, 85), x=(60, 0)))
        self.assertAlmostEqual(self._mult(ev), 1.0, places=4)

    def test_threshold_adapts_when_doable_exceeds_fetch_cap(self):
        """With doable >> fetch limit, the full-mult bar caps at 0.5*limit/doable."""
        now = dt.datetime.now(dt.timezone.utc)
        # doable=8000 -> full_at = min(0.15, 500/8000) = 0.0625.
        jobs = [_job("reddit", now - dt.timedelta(hours=1)) for _ in range(600)] + [
            _job("x", now - dt.timedelta(hours=1))
        ]
        ev = self._run_eval(jobs, _stats(reddit=(9000, 8000), x=(60, 4)))
        # 600/8000 = 0.075 >= 0.0625 -> full mult despite being far below 0.15.
        self.assertAlmostEqual(self._mult(ev), 1.0, places=4)

    def test_stats_none_relaxes_instead_of_judging(self):
        """No stats -> relax toward 1.0; never judge, never hold a stale penalty."""
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1))]
        ev = self._run_eval(jobs, None)
        ev.scorer.set_od_coverage_mult.assert_not_called()
        ev.scorer.relax_od_coverage_mult.assert_called_once_with(1)

    def test_fetch_failure_relaxes_instead_of_holding_penalty(self):
        """Jobs-fetch failure -> relax toward 1.0 and skip the eval."""
        ev = self._run_eval([], _stats(), fetch_error=RuntimeError("boom"))
        ev.scorer.set_od_coverage_mult.assert_not_called()
        ev.scorer.relax_od_coverage_mult.assert_called_once_with(1)


class TestScorerCoverageMult(unittest.TestCase):
    def _scorer(self, n=2):
        from rewards.data_value_calculator import DataValueCalculator
        from rewards.miner_scorer import MinerScorer

        return MinerScorer(n, DataValueCalculator())

    def test_mult_scales_od_and_drags_caps(self):
        from rewards.miner_scorer import MinerScorer

        scorer = self._scorer()
        for uid in (0, 1):
            scorer.ondemand_boosts[uid] = 100.0
            scorer.ondemand_credibility[uid] = 1.0
            scorer.s3_boosts[uid] = 1000.0
            scorer.s3_credibility[uid] = 1.0

        # First drop smooths halfway: 1.0 -> 0.65; converge with repeats.
        scorer.set_od_coverage_mult(1, MinerScorer.OD_ABSTAIN_MULT)
        self.assertAlmostEqual(float(scorer.od_coverage_mult[1]), 0.65, places=4)
        scores = scorer.get_scores_for_weights()

        # uid0: od=100, s3 capped at 200 -> 300. uid1: od=65, s3 capped at 130 -> 195.
        self.assertAlmostEqual(float(scores[0]), 300.0, places=2)
        self.assertAlmostEqual(float(scores[1]), 195.0, places=2)

    def test_penalty_phases_in_recovery_is_instant(self):
        scorer = self._scorer()
        scorer.set_od_coverage_mult(0, 0.3)
        self.assertAlmostEqual(float(scorer.od_coverage_mult[0]), 0.65, places=4)
        scorer.set_od_coverage_mult(0, 0.3)
        self.assertAlmostEqual(float(scorer.od_coverage_mult[0]), 0.475, places=4)
        # Improvement applies instantly.
        scorer.set_od_coverage_mult(0, 1.0)
        self.assertAlmostEqual(float(scorer.od_coverage_mult[0]), 1.0, places=4)

    def test_relax_moves_halfway_to_full(self):
        scorer = self._scorer()
        scorer.set_od_coverage_mult(0, 0.0)  # -> 0.5 (smoothed)
        scorer.relax_od_coverage_mult(0)
        self.assertAlmostEqual(float(scorer.od_coverage_mult[0]), 0.75, places=4)
        scorer.relax_od_coverage_mult(1)  # already 1.0 -> no-op
        self.assertAlmostEqual(float(scorer.od_coverage_mult[1]), 1.0, places=4)

    def test_state_roundtrip_preserves_mult(self):
        import os
        import tempfile

        scorer = self._scorer(4)
        scorer.set_od_coverage_mult(2, 0.3)  # smoothed: 1.0 -> 0.65
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "scorer.pickle")
            scorer.save_state(path)
            loaded = self._scorer(4)
            loaded.load_state(path)
        self.assertAlmostEqual(float(loaded.od_coverage_mult[2]), 0.65, places=4)
        self.assertAlmostEqual(float(loaded.od_coverage_mult[0]), 1.0, places=4)


class TestEvaluateOdRewardWindow(unittest.TestCase):
    def test_reward_path_only_sees_since_last_eval_jobs(self):
        """Coverage fetch widens the window; rewards must not resample old jobs."""
        ev = _stub_evaluator()
        ev.scorer = MagicMock()

        now = dt.datetime.now(dt.timezone.utc)
        last_eval = now - dt.timedelta(minutes=30)
        ev._last_od_eval_at = {"hk": last_eval}

        new_job = _job("reddit", now - dt.timedelta(minutes=10))
        old_job = _job("reddit", now - dt.timedelta(hours=2))  # in 3h coverage window only

        resp = MagicMock()
        resp.jobs = [new_job, old_job]
        client = MagicMock()
        client.validator_list_miner_jobs = AsyncMock(return_value=resp)
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        ev._on_demand_client = MagicMock(return_value=client)
        ev._get_od_jobs_stats = AsyncMock(return_value=_stats())
        ev._log_od_coverage_shadow = MagicMock()
        ev._validate_od_submission = AsyncMock(return_value=(True, 10))
        ev.on_demand_validator.calculate_ondemand_reward_multipliers = MagicMock(
            return_value=(1.0, 1.0)
        )

        asyncio.run(ev._evaluate_od(1, "hk"))

        # The wide fetch went out, but only the new job reached rewards.
        fetch_req = client.validator_list_miner_jobs.await_args.args[0]
        self.assertLessEqual(
            fetch_req.expired_since, now - dt.timedelta(hours=3) + dt.timedelta(seconds=5)
        )
        ev._validate_od_submission.assert_awaited_once()
        validated_job = ev._validate_od_submission.await_args.args[2]
        self.assertIs(validated_job, new_job.job)
        # Coverage shadow saw BOTH jobs.
        shadow_jobs = ev._log_od_coverage_shadow.call_args.args[2]
        self.assertEqual(len(shadow_jobs), 2)


if __name__ == "__main__":
    unittest.main()
