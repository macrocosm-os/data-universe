"""Tests for the frozen-score-on-index-fail fix.

A miner that refuses/fails the GetMinerIndex query must not keep stale scores:
- eval_miner runs S3 validation and applies its result BEFORE the index fetch,
  so a broken index can't skip update_s3_effective_size (same exploit as #870,
  one early-return earlier).
- on_miner_evaluated with index=None applies the failed validation to P2P
  credibility, so the P2P component recomputed by get_scores_for_weights decays
  instead of staying frozen at its last (inflated) value.
"""

import asyncio
import threading
import types
import unittest
from unittest.mock import AsyncMock, MagicMock

from rewards.data_value_calculator import DataValueCalculator
from rewards.miner_scorer import MinerScorer
from scraping.scraper import ValidationResult
from vali_utils.miner_evaluator import MinerEvaluator


def _failed_index_result() -> ValidationResult:
    return ValidationResult(
        is_valid=False,
        reason="No available miner index.",
        content_size_bytes_validated=0,
    )


class TestEvalMinerS3BeforeIndex(unittest.TestCase):
    def _stub_evaluator(self):
        ev = MinerEvaluator.__new__(MinerEvaluator)
        ev.lock = threading.Lock()
        ev.metagraph = MagicMock()
        ev.metagraph.axons = [MagicMock()]
        ev.metagraph.hotkeys = ["miner-hk"]
        ev.metagraph.block = 1000
        ev.wallet = MagicMock()
        ev.wallet.hotkey.ss58_address = "vali-hk"
        ev.scorer = MagicMock()
        ev.s3_storage = MagicMock()
        ev.s3_storage.get_validation_info.return_value = None  # forces S3 validation
        ev._evaluate_od = AsyncMock()
        ev._update_and_get_miner_index = AsyncMock(return_value=None)  # broken index
        ev._perform_s3_validation = AsyncMock(
            return_value=types.SimpleNamespace(
                is_valid=False,
                reason="stale files",
                effective_size_bytes=123_456,
            )
        )
        return ev

    def test_s3_update_applied_despite_broken_index(self):
        ev = self._stub_evaluator()
        asyncio.run(ev.eval_miner(0))

        ev._perform_s3_validation.assert_awaited_once()
        ev.scorer.update_s3_effective_size.assert_called_once_with(
            uid=0, effective_size=123_456, validation_passed=False
        )
        # The no-index path still reports the failed validation to the scorer.
        ev.scorer.on_miner_evaluated.assert_called_once()
        args = ev.scorer.on_miner_evaluated.call_args.args
        self.assertIsNone(args[1])
        self.assertFalse(args[2][0].is_valid)


class TestScorerNoIndexCredibility(unittest.TestCase):
    def test_no_index_eval_decays_p2p_credibility(self):
        scorer = MinerScorer(2, DataValueCalculator())
        scorer.miner_credibility[0] = 1.0

        scorer.on_miner_evaluated(0, None, [_failed_index_result()])

        expected = 1.0 - scorer.cred_alpha  # EMA toward 0
        self.assertAlmostEqual(float(scorer.miner_credibility[0]), expected, places=4)
        # Other miners untouched.
        self.assertAlmostEqual(
            float(scorer.miner_credibility[1]),
            MinerScorer.STARTING_CREDIBILITY,
            places=4,
        )

    def test_repeated_no_index_evals_decay_weights_score(self):
        """The composite in get_scores_for_weights must drop, not stay frozen."""
        scorer = MinerScorer(1, DataValueCalculator())
        scorer.miner_credibility[0] = 1.0
        # p2p = 1000 * 0.05 = 50, under the P2P<=S3+OD cap (100) so decay is visible
        scorer.scorable_bytes[0] = 1_000
        scorer.ondemand_boosts[0] = 100.0
        scorer.ondemand_credibility[0] = 1.0  # keeps the caps from zeroing P2P

        before = float(scorer.get_scores_for_weights()[0])
        for _ in range(5):
            scorer.on_miner_evaluated(0, None, [_failed_index_result()])
        after = float(scorer.get_scores_for_weights()[0])

        self.assertLess(after, before)
        # P2P credibility decayed by (1-alpha)^5; OD component is untouched.
        self.assertAlmostEqual(
            float(scorer.miner_credibility[0]),
            (1.0 - scorer.cred_alpha) ** 5,
            places=4,
        )
        self.assertAlmostEqual(float(scorer.ondemand_boosts[0]), 100.0, places=4)


if __name__ == "__main__":
    unittest.main()
