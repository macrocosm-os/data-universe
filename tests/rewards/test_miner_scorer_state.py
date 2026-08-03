"""Tests for MinerScorer state save/load migrations."""

import os
import tempfile
import unittest

import torch

from rewards.data_value_calculator import DataValueCalculator
from rewards.miner_scorer import MinerScorer


class TestStateMigrationV17(unittest.TestCase):
    """v17 resets only P2P state invalidated by the filler exploit."""

    def _roundtrip(self, mutate_saved_version=None):
        n = 8
        scorer = MinerScorer(n, DataValueCalculator())
        scorer.scores = torch.rand(n)
        scorer.miner_credibility = torch.rand(n, 1)
        scorer.scorable_bytes = torch.rand(n) * 100
        scorer.s3_boosts = torch.rand(n)
        scorer.s3_credibility = torch.rand(n, 1)
        scorer.ondemand_boosts = torch.rand(n) * 100
        scorer.ondemand_credibility = torch.rand(n, 1)
        scorer.effective_sizes = torch.rand(n, dtype=torch.float64) * 100

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "scorer.pickle")
            scorer.save_state(path)
            if mutate_saved_version is not None:
                state = torch.load(path, weights_only=True)
                state["state_version"] = mutate_saved_version
                torch.save(state, path)

            loaded = MinerScorer(n, DataValueCalculator())
            loaded.load_state(path)
            return scorer, loaded

    def test_old_state_resets_p2p_only(self):
        saved, loaded = self._roundtrip(mutate_saved_version=16)

        self.assertTrue(torch.all(loaded.scores == 0))
        self.assertTrue(torch.all(loaded.scorable_bytes == 0))
        self.assertTrue(
            torch.all(
                loaded.miner_credibility == MinerScorer.STARTING_CREDIBILITY
            )
        )

        self.assertTrue(torch.equal(loaded.s3_boosts, saved.s3_boosts))
        self.assertTrue(torch.equal(loaded.s3_credibility, saved.s3_credibility))
        self.assertTrue(torch.equal(loaded.ondemand_boosts, saved.ondemand_boosts))
        self.assertTrue(
            torch.equal(loaded.ondemand_credibility, saved.ondemand_credibility)
        )
        self.assertTrue(torch.equal(loaded.effective_sizes, saved.effective_sizes))

    def test_current_state_untouched(self):
        saved, loaded = self._roundtrip()

        self.assertTrue(torch.equal(loaded.scores, saved.scores))
        self.assertTrue(torch.equal(loaded.scorable_bytes, saved.scorable_bytes))
        self.assertTrue(torch.equal(loaded.miner_credibility, saved.miner_credibility))
        self.assertTrue(torch.equal(loaded.ondemand_boosts, saved.ondemand_boosts))
        self.assertTrue(
            torch.equal(loaded.ondemand_credibility, saved.ondemand_credibility)
        )


if __name__ == "__main__":
    unittest.main()
