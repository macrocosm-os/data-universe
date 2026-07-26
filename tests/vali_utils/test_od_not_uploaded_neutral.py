"""A submission whose body never reached S3 must not be penalized.

`s3_content_length == 0` is unreachable from a completed upload — the smallest
well-formed payload a miner can PUT is `{"data_entities": []}` (21 bytes). Zero
therefore means the submit leg created the record but the upload leg failed, which
is infrastructure, not the miner's answer.

The penalty is multiplicative and compounds (`od_boost *= 0.7` per occurrence), so
one API incident is geometric in the number of jobs in flight.
"""

import asyncio
import datetime as dt
import threading
import unittest
from unittest.mock import AsyncMock, MagicMock

from common.api_client import (
    MinerJobForValidation,
    OnDemandJob,
    OnDemandJobSubmission,
    OnDemandJobsStatsResponse,
    PlatformJobStats,
)
from vali_utils.miner_evaluator import MinerEvaluator


def _stub_evaluator():
    ev = MinerEvaluator.__new__(MinerEvaluator)
    ev._od_stats_cache = None
    ev._od_stats_lock = threading.Lock()
    ev._last_od_eval_at = {}
    ev.on_demand_validator = MagicMock()
    return ev


def _job(platform: str, expire_at: dt.datetime, content_length: int) -> MinerJobForValidation:
    return MinerJobForValidation(
        job=OnDemandJob(
            id="j", expire_at=expire_at, job={"platform": platform, "keywords": ["k"]}
        ),
        submission=OnDemandJobSubmission(job_id="j", s3_content_length=content_length),
    )


def _stats() -> OnDemandJobsStatsResponse:
    return OnDemandJobsStatsResponse(
        platforms={
            "reddit": PlatformJobStats(total_jobs=90, doable_jobs=85),
            "x": PlatformJobStats(total_jobs=60, doable_jobs=4),
        }
    )


class TestNotUploadedIsNeutral(unittest.TestCase):
    def _run(self, jobs):
        ev = _stub_evaluator()
        ev.scorer = MagicMock()
        resp = MagicMock()
        resp.jobs = jobs
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
        return ev

    def test_zero_byte_submissions_are_not_penalized(self):
        """The regression: 9 failed uploads must not fire 9 penalties."""
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1), 0) for _ in range(9)]
        jobs += [_job("x", now - dt.timedelta(hours=1), 500)]

        ev = self._run(jobs)

        ev.scorer.apply_ondemand_penalty.assert_not_called()

    def test_all_uploads_failed_is_a_no_op(self):
        """Nothing landed: no reward, no penalty — same as never answering."""
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1), 0) for _ in range(5)]

        ev = self._run(jobs)

        ev.scorer.apply_ondemand_penalty.assert_not_called()
        ev.scorer.apply_ondemand_reward.assert_not_called()

    def test_uploaded_submissions_still_scored(self):
        """A body that DID land is still validated and rewarded — no regression."""
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1), 500) for _ in range(3)]
        jobs += [_job("x", now - dt.timedelta(hours=1), 500)]

        ev = self._run(jobs)

        self.assertTrue(ev._validate_od_submission.await_count >= 1)
        ev.scorer.apply_ondemand_penalty.assert_not_called()

    def test_deliberate_empty_answer_still_reaches_validation(self):
        """21-byte `{"data_entities": []}` is a real answer, not a failed upload.

        It must land in the validated path, where check_data_exists() decides —
        not be silently reclassified as an infrastructure failure.
        """
        now = dt.datetime.now(dt.timezone.utc)
        jobs = [_job("reddit", now - dt.timedelta(hours=1), 21) for _ in range(2)]

        ev = self._run(jobs)

        self.assertTrue(ev._validate_od_submission.await_count >= 1)


if __name__ == "__main__":
    unittest.main()
