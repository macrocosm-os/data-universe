"""X username coverage: skipping username jobs that had data cuts the OD multiplier."""

import asyncio
import datetime as dt
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from common.api_client import (
    JobWithSubmissions,
    ListJobsWithSubmissionForValidationResponse,
    OnDemandJob,
    OnDemandJobSubmission,
)
from vali_utils.miner_evaluator import MinerEvaluator, _UsernameJob, _UsernameJobStore

NOW = dt.datetime.now(dt.timezone.utc)
ME = "5ME"
RULES_DATE = "vali_utils.on_demand.on_demand_validation.OD_ENGAGEMENT_FIX_DATE"
_rules_patch = patch(RULES_DATE, dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc))


def setUpModule():
    _rules_patch.start()  # rules active (date in the past)


def tearDownModule():
    _rules_patch.stop()


def _evaluator():
    ev = MinerEvaluator.__new__(MinerEvaluator)
    ev.on_demand_validator = MagicMock()
    ev.on_demand_validator.check_data_exists = AsyncMock(return_value=True)
    return ev


def _job(job_id, usernames=("const_reborn",), keywords=None, minutes_ago=30):
    payload = {"platform": "x", "usernames": list(usernames) if usernames else None, "keywords": keywords}
    return OnDemandJob(id=job_id, created_at=NOW - dt.timedelta(minutes=minutes_ago + 2),
                       expire_at=NOW - dt.timedelta(minutes=minutes_ago), job=payload)


def _page(*jobs_with_subs):
    return ListJobsWithSubmissionForValidationResponse(
        jobs_with_submissions=[JobWithSubmissions(job=j, submissions=s) for j, s in jobs_with_subs]
    )


def _sub(job_id, hotkey, size=500):
    return OnDemandJobSubmission(job_id=job_id, miner_hotkey=hotkey, s3_content_length=size)


class TestUsernameMult(unittest.TestCase):
    def mult(self, n_jobs, answered_by_me, has_data=True):
        ev = _evaluator()
        store = ev._od_username = _UsernameJobStore()
        for i in range(n_jobs):
            store.jobs[f"j{i}"] = _UsernameJob(job=_job(f"j{i}"), answered={ME} if i < answered_by_me else set(),
                                               has_data=has_data)
        return ev._od_username_mult(uid=1, hotkey=ME)

    def test_formula(self):
        f = MinerEvaluator.OD_USERNAME_FLOOR
        self.assertEqual(self.mult(0, 0), 1.0)
        self.assertEqual(self.mult(100, 95), 1.0)
        self.assertAlmostEqual(self.mult(100, 80), f + (1 - f) * 0.81 / 0.9, places=6)
        self.assertEqual(self.mult(1, 0), 1.0)  # one free miss
        self.assertAlmostEqual(self.mult(3, 0), f + (1 - f) * (1 / 3) / 0.9, places=6)
        self.assertAlmostEqual(self.mult(100, 0), f + (1 - f) * 0.01 / 0.9, places=6)

    def test_jobs_without_data_never_count(self):
        self.assertEqual(self.mult(5, 0, has_data=False), 1.0)
        self.assertEqual(self.mult(5, 0, has_data=None), 1.0)


class TestRefreshUsernameJobs(unittest.TestCase):
    def setUp(self):
        self.ev = _evaluator()
        self.client = MagicMock()

    def refresh(self):
        asyncio.run(self.ev._refresh_od_username_jobs(self.client))
        return self.ev._od_username_store()

    def test_answers_and_validator_check(self):
        self.client.validator_list_jobs_with_submissions = AsyncMock(return_value=_page(
            (_job("a"), [_sub("a", "5BIG"), _sub("a", "5EMPTY", 21)]),
            (_job("b", minutes_ago=40), [_sub("b", "5JUNK", 65)]),
        ))
        self.ev.on_demand_validator.check_data_exists = AsyncMock(side_effect=[True, False])
        store = self.refresh()
        req = self.client.validator_list_jobs_with_submissions.await_args.args[0]
        self.assertEqual(req.job_type, "x_username")
        self.assertEqual(store.jobs["a"].answered, {"5BIG"})
        # "Had data" comes from the validator's check, not from uploads (b had a junk upload).
        self.assertEqual({j: uj.has_data for j, uj in store.jobs.items()}, {"a": True, "b": False})

    def test_cached_and_backs_off_on_failure(self):
        self.client.validator_list_jobs_with_submissions = AsyncMock(side_effect=RuntimeError("down"))
        self.refresh()
        self.refresh()
        self.assertEqual(self.client.validator_list_jobs_with_submissions.await_count, 1)

    def test_api_without_filter_disables_and_clears(self):
        self.client.validator_list_jobs_with_submissions = AsyncMock(return_value=_page((_job("a"), [])))
        store = self.refresh()
        store.refreshed_at = NOW - dt.timedelta(hours=1)
        keyword_job = _job("k", usernames=None, keywords=["bitcoin"])
        self.client.validator_list_jobs_with_submissions = AsyncMock(return_value=_page((keyword_job, [])))
        store = self.refresh()
        self.assertFalse(store.supported)
        self.assertEqual(store.jobs, {})


class TestEnforcementDate(unittest.TestCase):
    """Before OD_ENGAGEMENT_FIX_DATE the username rule and strict engagement are off."""

    def test_before_date_nothing_happens(self):
        ev = _evaluator()
        store = ev._od_username = _UsernameJobStore()
        for jid in ("a", "b", "c"):  # 3 skipped jobs: more than the one free miss
            store.jobs[jid] = _UsernameJob(job=_job(jid), answered=set(), has_data=True)
        client = MagicMock()
        client.validator_list_jobs_with_submissions = AsyncMock(return_value=_page())
        with patch(RULES_DATE, NOW + dt.timedelta(days=1)):
            self.assertEqual(ev._od_username_mult(uid=1, hotkey=ME), 1.0)
            asyncio.run(ev._refresh_od_username_jobs(client))
            client.validator_list_jobs_with_submissions.assert_not_called()
        self.assertLess(ev._od_username_mult(uid=1, hotkey=ME), 1.0)  # after the date it applies

    def test_od_rules_active(self):
        from vali_utils.on_demand.on_demand_validation import od_rules_active
        with patch(RULES_DATE, NOW + dt.timedelta(days=1)):
            self.assertFalse(od_rules_active())
        with patch(RULES_DATE, NOW - dt.timedelta(days=1)):
            self.assertTrue(od_rules_active())


if __name__ == "__main__":
    unittest.main()
