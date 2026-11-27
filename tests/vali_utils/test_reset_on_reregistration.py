"""A uid re-registered with the same hotkey (burned_register + swap_hotkey_v2) is reset like a new miner."""

import threading
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from vali_utils.miner_evaluator import MinerEvaluator


def _metagraph(hotkeys, reg_blocks):
    return SimpleNamespace(hotkeys=list(hotkeys), block_at_registration=list(reg_blocks))


def _evaluator(old_metagraph):
    ev = MinerEvaluator.__new__(MinerEvaluator)
    ev.lock = threading.RLock()
    ev.metagraph = old_metagraph
    ev.scorer = MagicMock()
    ev.storage = MagicMock()
    ev.miner_iterator = MagicMock()
    ev.vpermit_rao_limit = 0
    ev._last_od_eval_at = {"HK_A": 1, "HK_B": 1}
    return ev


@patch("vali_utils.miner_evaluator.utils.get_miner_uids", return_value=[0, 1])
@patch("vali_utils.miner_evaluator.utils.is_validator", return_value=False)
@patch("vali_utils.miner_evaluator.utils.is_miner", return_value=True)
class TestResetOnReregistration(unittest.TestCase):
    def test_same_hotkey_new_registration_block_resets(self, *_):
        ev = _evaluator(_metagraph(["HK_A", "HK_B"], [9549070, 9500000]))
        ev._on_metagraph_updated(_metagraph(["HK_A", "HK_B"], [9561070, 9500000]), netuid=13)
        ev.scorer.reset.assert_called_once_with(0)
        ev.storage.delete_miner.assert_called_once_with("HK_A")
        self.assertNotIn("HK_A", ev._last_od_eval_at)

    def test_unchanged_uid_not_reset(self, *_):
        ev = _evaluator(_metagraph(["HK_A", "HK_B"], [9549070, 9500000]))
        ev._on_metagraph_updated(_metagraph(["HK_A", "HK_B"], [9549070, 9500000]), netuid=13)
        ev.scorer.reset.assert_not_called()

    def test_hotkey_change_still_resets(self, *_):
        ev = _evaluator(_metagraph(["HK_A", "HK_B"], [9549070, 9500000]))
        ev._on_metagraph_updated(_metagraph(["HK_A", "HK_NEW"], [9549070, 9600000]), netuid=13)
        ev.scorer.reset.assert_called_once_with(1)

    def test_metagraph_without_registration_blocks_keeps_old_behaviour(self, *_):
        old = SimpleNamespace(hotkeys=["HK_A", "HK_B"])
        ev = _evaluator(old)
        ev._on_metagraph_updated(SimpleNamespace(hotkeys=["HK_A", "HK_B"]), netuid=13)
        ev.scorer.reset.assert_not_called()


if __name__ == "__main__":
    unittest.main()
