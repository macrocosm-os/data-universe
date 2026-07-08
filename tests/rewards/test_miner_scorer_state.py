"""Tests for MinerScorer state save/load migrations."""

import os
import tempfile
import unittest

import torch

from rewards.data_value_calculator import DataValueCalculator
from rewards.miner_scorer import MinerScorer


class TestStateMigrationV16(unittest.TestCase):
    """v16 resets OnDemand boost/credibility only; everything else survives."""

    def _roundtrip(self, mutate_saved_version=None):
        n = 8
        scorer = MinerScorer(n, DataValueCalculator())
        scorer.scores = torch.rand(n)
        scorer.miner_credibility = torch.rand(n, 1)
        scorer.s3_boosts = torch.rand(n)
        scorer.s3_credibility = torch.rand(n, 1)
        scorer.ondemand_boosts = torch.rand(n) * 100
        scorer.ondemand_credibility = torch.rand(n, 1)

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

    def test_old_state_resets_od_only(self):
        saved, loaded = self._roundtrip(mutate_saved_version=15)

        # OD reset
        self.assertTrue(torch.all(loaded.ondemand_boosts == 0))
        self.assertTrue(
            torch.all(
                loaded.ondemand_credibility
                == MinerScorer.STARTING_ONDEMAND_CREDIBILITY
            )
        )
        # everything else preserved
        self.assertTrue(torch.equal(loaded.scores, saved.scores))
        self.assertTrue(torch.equal(loaded.miner_credibility, saved.miner_credibility))
        self.assertTrue(torch.equal(loaded.s3_boosts, saved.s3_boosts))
        self.assertTrue(torch.equal(loaded.s3_credibility, saved.s3_credibility))

    def test_current_state_untouched(self):
        saved, loaded = self._roundtrip()

        self.assertTrue(torch.equal(loaded.ondemand_boosts, saved.ondemand_boosts))
        self.assertTrue(
            torch.equal(loaded.ondemand_credibility, saved.ondemand_credibility)
        )


if __name__ == "__main__":
    unittest.main()
