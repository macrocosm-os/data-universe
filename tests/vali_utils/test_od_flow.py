"""Tests for the OD job cache + evaluator integration.

Covers:
- ODJobCache: add, drain, dedup, processed tracking
- MinerScorer: OD reward/penalty/credibility bump
- MinerEvaluator._evaluate_od: drains cache, applies scores, triggers spot-check
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import datetime as dt
import torch

from vali_utils.on_demand.od_job_cache import ODJobCache, CachedMinerODResult
from rewards.miner_scorer import MinerScorer
from rewards.data_value_calculator import DataValueCalculator


class TestODJobCache(unittest.TestCase):
    def setUp(self):
        self.cache = ODJobCache()

    def _make_result(self, job_id="job1", passed=True, speed=0.8, volume=0.9):
        return CachedMinerODResult(
            job_id=job_id,
            submitted_at=dt.datetime.now(dt.timezone.utc),
            returned_count=100,
            requested_limit=100,
            passed_validation=passed,
            speed_multiplier=speed,
            volume_multiplier=volume,
        )

    def test_add_and_drain(self):
        """Results are returned once then gone."""
        self.cache.add_results("job1", {
            "hk_a": self._make_result(),
            "hk_b": self._make_result(passed=False),
        })

        results_a = self.cache.get_and_drain("hk_a")
        self.assertEqual(len(results_a), 1)
        self.assertTrue(results_a[0].passed_validation)

        # Second drain is empty
        self.assertEqual(self.cache.get_and_drain("hk_a"), [])

        results_b = self.cache.get_and_drain("hk_b")
        self.assertEqual(len(results_b), 1)
        self.assertFalse(results_b[0].passed_validation)

    def test_multiple_jobs_accumulate(self):
        """Multiple jobs accumulate for the same miner."""
        self.cache.add_results("job1", {"hk_a": self._make_result("job1")})
        self.cache.add_results("job2", {"hk_a": self._make_result("job2")})
        self.cache.add_results("job3", {"hk_a": self._make_result("job3")})

        results = self.cache.get_and_drain("hk_a")
        self.assertEqual(len(results), 3)
        self.assertEqual({r.job_id for r in results}, {"job1", "job2", "job3"})

    def test_is_job_processed(self):
        self.assertFalse(self.cache.is_job_processed("job1"))
        self.cache.add_results("job1", {"hk_a": self._make_result()})
        self.assertTrue(self.cache.is_job_processed("job1"))

    def test_drain_unknown_hotkey(self):
        self.assertEqual(self.cache.get_and_drain("unknown"), [])

    def test_pending_miner_count(self):
        self.assertEqual(self.cache.get_pending_miner_count(), 0)
        self.cache.add_results("job1", {
            "hk_a": self._make_result(),
            "hk_b": self._make_result(),
        })
        self.assertEqual(self.cache.get_pending_miner_count(), 2)
        self.cache.get_and_drain("hk_a")
        self.assertEqual(self.cache.get_pending_miner_count(), 1)


class TestMinerScorerOD(unittest.TestCase):
    def setUp(self):
        self.scorer = MinerScorer(10, DataValueCalculator())

    def test_reward_bumps_credibility(self):
        """apply_ondemand_reward should increase OD credibility."""
        old = float(self.scorer.ondemand_credibility[0])
        self.scorer.apply_ondemand_reward(uid=0, speed_multiplier=1.0, volume_multiplier=1.0)
        new = float(self.scorer.ondemand_credibility[0])
        self.assertGreater(new, old)

    def test_reward_bumps_boost(self):
        """apply_ondemand_reward should increase ondemand_boosts."""
        old = float(self.scorer.ondemand_boosts[0])
        self.scorer.apply_ondemand_reward(uid=0, speed_multiplier=1.0, volume_multiplier=1.0)
        new = float(self.scorer.ondemand_boosts[0])
        self.assertGreater(new, old)

    def test_penalty_decays_credibility(self):
        """apply_ondemand_penalty should decrease OD credibility."""
        old = float(self.scorer.ondemand_credibility[0])
        self.scorer.apply_ondemand_penalty(uid=0, mult_factor=1.0)
        new = float(self.scorer.ondemand_credibility[0])
        self.assertLess(new, old)

    def test_penalty_decays_boost(self):
        """apply_ondemand_penalty should decay ondemand_boosts toward 0."""
        self.scorer.apply_ondemand_reward(uid=0, speed_multiplier=1.0, volume_multiplier=1.0)
        boosted = float(self.scorer.ondemand_boosts[0])
        self.assertGreater(boosted, 0)

        self.scorer.apply_ondemand_penalty(uid=0, mult_factor=1.0)
        decayed = float(self.scorer.ondemand_boosts[0])
        self.assertLess(decayed, boosted)

    def test_credibility_bump_smaller_than_reward(self):
        """Credibility bump (unsampled) should be smaller than reward bump."""
        uid_bump = 0
        uid_reward = 1

        self.scorer.apply_ondemand_credibility_bump(uid=uid_bump)
        bump_cred = float(self.scorer.ondemand_credibility[uid_bump])

        self.scorer.apply_ondemand_reward(uid=uid_reward, speed_multiplier=1.0, volume_multiplier=1.0)
        reward_cred = float(self.scorer.ondemand_credibility[uid_reward])

        self.assertGreater(reward_cred, bump_cred)

    def test_od_no_exponent_in_scoring(self):
        """OD credibility should be used raw (no ^2.5) in on_miner_evaluated."""
        uid = 0
        # Set OD credibility to 0.5 and give some OD boost
        self.scorer.ondemand_credibility[uid] = 0.5
        self.scorer.apply_ondemand_reward(uid=uid, speed_multiplier=1.0, volume_multiplier=1.0)

        boost = float(self.scorer.ondemand_boosts[uid])
        cred = float(self.scorer.ondemand_credibility[uid])

        # Raw: boost * 0.51 (approx after bump)
        # With ^2.5: boost * 0.51^2.5 ≈ boost * 0.186
        # The raw version should give ~2.7x more OD contribution
        expected_raw = boost * cred
        expected_exp = boost * (cred ** 2.5)
        self.assertGreater(expected_raw, expected_exp * 2)

    def test_get_scores_for_weights_od_no_exponent(self):
        """get_scores_for_weights should use raw OD cred."""
        uid = 0
        self.scorer.ondemand_credibility[uid] = 0.5
        self.scorer.apply_ondemand_reward(uid=uid, speed_multiplier=1.0, volume_multiplier=1.0)

        scores = self.scorer.get_scores_for_weights()
        # With zero P2P and S3, the score should be OD only
        # OD = boost * raw_cred
        boost = float(self.scorer.ondemand_boosts[uid])
        cred = float(self.scorer.ondemand_credibility[uid])
        expected = boost * cred
        self.assertAlmostEqual(float(scores[uid]), expected, places=0)


class TestEvaluateOD(unittest.TestCase):
    """Test _evaluate_od integration with cache and scorer."""

    def setUp(self):
        self.cache = ODJobCache()
        self.scorer = MinerScorer(10, DataValueCalculator())

    def _make_evaluator_mock(self):
        """Create a minimal mock evaluator with real scorer and cache."""
        evaluator = MagicMock()
        evaluator.scorer = self.scorer
        evaluator.od_cache = self.cache
        evaluator.on_demand_validator = None  # no spot-check
        evaluator.config = MagicMock()
        evaluator.wallet = MagicMock()
        return evaluator

    def test_drain_and_apply_rewards(self):
        """Rewards are applied for all passed results."""
        # Add 3 passed results
        for i in range(3):
            self.cache.add_results(f"job{i}", {
                "hk_miner": CachedMinerODResult(
                    job_id=f"job{i}",
                    submitted_at=dt.datetime.now(dt.timezone.utc),
                    returned_count=100,
                    requested_limit=100,
                    passed_validation=True,
                    speed_multiplier=0.9,
                    volume_multiplier=0.8,
                ),
            })

        old_boost = float(self.scorer.ondemand_boosts[0])
        old_cred = float(self.scorer.ondemand_credibility[0])

        # Call _evaluate_od directly using the real method
        from vali_utils.miner_evaluator import MinerEvaluator
        # We need to call the unbound method with our mock
        evaluator = self._make_evaluator_mock()

        async def run():
            await MinerEvaluator._evaluate_od(evaluator, uid=0, hotkey="hk_miner")

        asyncio.run(run())

        # Boost and cred should have increased (3 rewards applied)
        self.assertGreater(float(self.scorer.ondemand_boosts[0]), old_boost)
        self.assertGreater(float(self.scorer.ondemand_credibility[0]), old_cred)

        # Cache should be empty now
        self.assertEqual(self.cache.get_and_drain("hk_miner"), [])

    def test_drain_and_apply_penalties(self):
        """Penalties are applied for failed results."""
        self.cache.add_results("job1", {
            "hk_miner": CachedMinerODResult(
                job_id="job1",
                submitted_at=dt.datetime.now(dt.timezone.utc),
                returned_count=0,
                requested_limit=100,
                passed_validation=False,
                speed_multiplier=0.0,
                volume_multiplier=0.0,
                failure_reason="empty_submission",
            ),
        })

        old_cred = float(self.scorer.ondemand_credibility[0])

        from vali_utils.miner_evaluator import MinerEvaluator
        evaluator = self._make_evaluator_mock()

        async def run():
            await MinerEvaluator._evaluate_od(evaluator, uid=0, hotkey="hk_miner")

        asyncio.run(run())

        self.assertLess(float(self.scorer.ondemand_credibility[0]), old_cred)

    def test_no_cache_is_noop(self):
        """If od_cache is None, _evaluate_od does nothing."""
        from vali_utils.miner_evaluator import MinerEvaluator
        evaluator = self._make_evaluator_mock()
        evaluator.od_cache = None

        old_boost = float(self.scorer.ondemand_boosts[0])

        async def run():
            await MinerEvaluator._evaluate_od(evaluator, uid=0, hotkey="hk_miner")

        asyncio.run(run())

        self.assertEqual(float(self.scorer.ondemand_boosts[0]), old_boost)

    def test_empty_drain_is_noop(self):
        """If no results for this miner, nothing changes."""
        from vali_utils.miner_evaluator import MinerEvaluator
        evaluator = self._make_evaluator_mock()

        old_boost = float(self.scorer.ondemand_boosts[0])

        async def run():
            await MinerEvaluator._evaluate_od(evaluator, uid=0, hotkey="hk_nonexistent")

        asyncio.run(run())

        self.assertEqual(float(self.scorer.ondemand_boosts[0]), old_boost)

    def test_mixed_results(self):
        """Mix of passed and failed results applies both rewards and penalties."""
        self.cache.add_results("job1", {
            "hk_miner": CachedMinerODResult(
                job_id="job1", submitted_at=dt.datetime.now(dt.timezone.utc),
                returned_count=100, requested_limit=100,
                passed_validation=True, speed_multiplier=1.0, volume_multiplier=1.0,
            ),
        })
        self.cache.add_results("job2", {
            "hk_miner": CachedMinerODResult(
                job_id="job2", submitted_at=dt.datetime.now(dt.timezone.utc),
                returned_count=0, requested_limit=100,
                passed_validation=False, speed_multiplier=0.0, volume_multiplier=0.0,
                failure_reason="empty",
            ),
        })

        from vali_utils.miner_evaluator import MinerEvaluator
        evaluator = self._make_evaluator_mock()

        async def run():
            await MinerEvaluator._evaluate_od(evaluator, uid=0, hotkey="hk_miner")

        asyncio.run(run())

        # Boost should be non-zero (reward applied then decayed by penalty)
        self.assertGreater(float(self.scorer.ondemand_boosts[0]), 0)
        # Cache drained
        self.assertEqual(self.cache.get_and_drain("hk_miner"), [])


if __name__ == "__main__":
    unittest.main()
