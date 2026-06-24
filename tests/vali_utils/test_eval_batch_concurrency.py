"""Regression test for the validator OOM caused by eval-batch thread stacking.

Root cause (fixed): `MinerEvaluator.run_next_eval_batch` used to join its worker
threads against a SHARED 300s budget, then `return 0` so the main loop immediately
launched the next batch. `thread.join(timeout=...)` stops WAITING but does not kill
the thread, and a Python thread cannot be force-killed — so when a batch's DuckDB
phase ran longer than 300s, the abandoned threads kept running (holding DuckDB
scans in RAM) while fresh batches stacked on top. Peak concurrent evaluations grew
as `miners_to_eval * ceil(eval_time / cap)` -> memory spike -> OOM-kill.

The fix joins the batch to (near-)completion behind a generous 600s backstop, so a
slow batch drains before the next one starts and peak concurrency stays at
`miners_to_eval`.

This test drives the REAL `run_next_eval_batch` (built via __new__ with only the
attributes that method touches stubbed) in the same spawn -> join -> return-0 ->
re-enter loop the main loop uses, with `eval_miner_sync` replaced by a tracker that
takes longer than the old cap. It asserts the property that matters for memory
safety: PEAK CONCURRENT EVALUATIONS never exceeds one batch.
"""
import asyncio
import threading
import time
import unittest
from unittest import mock

from vali_utils.miner_evaluator import MinerEvaluator


class _ConcurrencyTracker:
    """Counts workers currently inside the eval section; records the peak."""

    def __init__(self):
        self.active = 0
        self.peak = 0
        self._lock = threading.Lock()

    def enter(self):
        with self._lock:
            self.active += 1
            self.peak = max(self.peak, self.active)

    def exit(self):
        with self._lock:
            self.active -= 1


class _FakeIterator:
    """Cycles uids 0..n-1 forever; peek() returns the next without consuming."""

    def __init__(self, n):
        self._uids = list(range(n))
        self._i = 0

    def peek(self):
        return self._uids[self._i % len(self._uids)]

    def __next__(self):
        uid = self._uids[self._i % len(self._uids)]
        self._i += 1
        return uid


class _FakeMetagraph:
    def __init__(self, n):
        self.hotkeys = [f"hk{i}" for i in range(n)]


def _make_evaluator(tracker, eval_time):
    """Build a MinerEvaluator with only the attributes run_next_eval_batch uses.

    eval_miner_sync is replaced with a tracker that models a slow per-miner eval
    (DuckDB phase). The slow eval exceeds the OLD 300s cap conceptually; here we
    scale time down so the test runs fast while preserving the ratio that matters.
    """
    ev = MinerEvaluator.__new__(MinerEvaluator)
    n = 8  # more uids than one batch so re-entry would draw a fresh batch
    ev.lock = threading.RLock()
    ev.metagraph = _FakeMetagraph(n)
    ev.miner_iterator = _FakeIterator(n)
    ev.storage = mock.Mock()
    ev.storage.read_miner_last_updated.return_value = None  # always due -> always eval
    ev.wallet = mock.Mock()
    ev.wallet.hotkey.ss58_address = "validator_hk"

    def fake_eval(uid):
        tracker.enter()
        try:
            time.sleep(eval_time)
        finally:
            tracker.exit()

    ev.eval_miner_sync = fake_eval
    return ev


class TestEvalBatchConcurrency(unittest.TestCase):
    MINERS_TO_EVAL = 5  # must match run_next_eval_batch's `miners_to_eval = 5`
    EVAL_TIME = 0.20    # per-miner eval longer than the time between re-entries

    def _drive_main_loop(self, ev, n_batches):
        """Mirror neurons/validator.py: call run_next_eval_batch, and on a 0 return
        immediately re-enter — exactly the loop that used to stack batches."""
        loop = asyncio.new_event_loop()
        try:
            for _ in range(n_batches):
                delay = loop.run_until_complete(ev.run_next_eval_batch())
                # return 0 -> no wait -> immediate re-entry (the stacking trigger).
                if delay and delay > 0:
                    break
        finally:
            loop.close()

    def test_peak_concurrency_bounded_to_one_batch(self):
        """With join-to-completion, peak concurrency can never exceed one batch,
        even when every per-miner eval is slow and the loop re-enters immediately."""
        tracker = _ConcurrencyTracker()
        ev = _make_evaluator(tracker, eval_time=self.EVAL_TIME)

        self._drive_main_loop(ev, n_batches=4)

        # let any in-flight threads finish before asserting
        for t in threading.enumerate():
            if t is not threading.current_thread() and t.name.startswith("Thread-"):
                t.join(timeout=2)

        self.assertEqual(
            tracker.peak,
            self.MINERS_TO_EVAL,
            f"join-to-completion must cap peak concurrency at {self.MINERS_TO_EVAL}; "
            f"got {tracker.peak} (a value > {self.MINERS_TO_EVAL} means batches stacked -> OOM regression)",
        )

    def test_return_zero_triggers_immediate_reentry(self):
        """Guards the precondition the bug depends on: a due miner makes
        run_next_eval_batch return 0 (immediate re-entry), not a wait."""
        tracker = _ConcurrencyTracker()
        ev = _make_evaluator(tracker, eval_time=0.01)
        loop = asyncio.new_event_loop()
        try:
            delay = loop.run_until_complete(ev.run_next_eval_batch())
        finally:
            loop.close()
        self.assertEqual(delay, 0, "a due batch must return 0 so the loop re-enters immediately")


if __name__ == "__main__":
    unittest.main()
