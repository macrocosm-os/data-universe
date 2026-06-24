"""Concurrency-bound regression test for the miner-evaluation batch scheduler.

Background — the validator OOM:
`MinerEvaluator.run_next_eval_batch` spawns `miners_to_eval` worker threads and
joins them with a fixed total cap (`thread.join(timeout=...)`, total 300s), then
returns 0 so the main loop immediately starts the next batch. `thread.join(timeout)`
STOPS WAITING but does NOT cancel the thread, and `eval_miner` has no overall
per-miner wall-clock timeout (only per-request httpx timeouts). So once a miner's
S3/DuckDB validation runs longer than the cap, threads from prior batches keep
running while new batches launch → concurrent evaluations (each holding a DuckDB
process + file reads in RAM) grow as `miners_to_eval * ceil(eval_time / cap)` →
out-of-memory.

This test is self-contained (no chain / S3 / DuckDB): it models the spawn+join
scheduling pattern and asserts the property that matters for memory safety —
PEAK CONCURRENT EVALUATIONS — under the current "abandon at cap" scheme vs the
"join to completion" fix, and shows why a hard per-miner timeout is needed so the
to-completion join cannot hang on a single stuck miner.
"""
import threading
import time
import unittest

MINERS_TO_EVAL = 5  # mirrors run_next_eval_batch: `miners_to_eval = 5`


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


def _worker(tracker, eval_time, per_miner_timeout):
    """Stand-in for `eval_miner_sync = asyncio.run(eval_miner(uid))`.

    `eval_time` models the (slow) S3/DuckDB validation duration; `per_miner_timeout`
    models a hard wall-clock bound that abandons-and-skips instead of running forever.
    """
    tracker.enter()
    try:
        budget = eval_time if per_miner_timeout is None else min(eval_time, per_miner_timeout)
        time.sleep(budget)
    finally:
        tracker.exit()


def _run_scheduler(n_batches, eval_time_fn, join_mode, cap=None, per_miner_timeout=None):
    """Mirror run_next_eval_batch's spawn+join, looped `n_batches` times.

    join_mode="abandon"  -> current code: join each thread with a total `cap`, then
                            return and start the next batch immediately.
    join_mode="complete" -> the fix: join each thread with no cap (wait for the batch).

    Returns (peak_concurrency, [per_batch_wall_clock_seconds]).
    """
    tracker = _ConcurrencyTracker()
    spawned = []
    durations = []
    for _ in range(n_batches):
        started = time.perf_counter()
        batch = [
            threading.Thread(
                target=_worker,
                args=(tracker, eval_time_fn(uid), per_miner_timeout),
                daemon=True,
            )
            for uid in range(MINERS_TO_EVAL)
        ]
        for t in batch:
            t.start()
        spawned += batch
        if join_mode == "abandon":
            end = time.perf_counter() + cap
            for t in batch:
                t.join(timeout=max(0.0, end - time.perf_counter()))
        elif join_mode == "complete":
            for t in batch:
                t.join()
        else:  # pragma: no cover
            raise ValueError(join_mode)
        durations.append(time.perf_counter() - started)
    for t in spawned:  # let any abandoned threads finish before the next test
        t.join()
    return tracker.peak, durations


class TestEvalBatchConcurrency(unittest.TestCase):
    # eval_time > cap mirrors "DuckDB phase runs longer than the join cap" (real: >1h vs 5 min).
    EVAL = 0.30
    CAP = 0.05  # ratio 6 -> expect ~6x peak growth under "abandon"

    def _uniform(self, _uid):
        return self.EVAL

    def test_current_abandon_grows_unbounded(self):
        """Current 'join with cap, abandon' lets peak concurrency exceed a single batch."""
        peak, _ = _run_scheduler(8, self._uniform, "abandon", cap=self.CAP)
        self.assertGreaterEqual(
            peak,
            2 * MINERS_TO_EVAL,
            f"abandon-at-cap should let concurrency grow unbounded; peak={peak}",
        )

    def test_join_to_completion_bounds_peak(self):
        """The fix (join to completion) caps peak concurrency at miners_to_eval."""
        peak, _ = _run_scheduler(8, self._uniform, "complete")
        self.assertEqual(
            peak, MINERS_TO_EVAL, f"join-to-completion should cap peak at {MINERS_TO_EVAL}; got {peak}"
        )

    def test_hard_timeout_prevents_hang_on_stuck_miner(self):
        """With one stuck miner, join-to-completion waits for it (would hang if truly
        stuck). A hard per-miner timeout bounds the batch wall-clock and keeps peak bounded."""
        stuck, timeout = 1.0, 0.2
        eval_fn = lambda uid: stuck if uid == 0 else 0.05  # only uid 0 is slow

        _, dur_no_timeout = _run_scheduler(1, eval_fn, "complete")
        peak_timeout, dur_timeout = _run_scheduler(1, eval_fn, "complete", per_miner_timeout=timeout)

        self.assertGreaterEqual(
            dur_no_timeout[0], stuck * 0.9, "join-to-completion should block on the stuck miner"
        )
        self.assertLessEqual(
            dur_timeout[0], timeout * 2.5, "a hard per-miner timeout should bound the batch wall-clock"
        )
        self.assertEqual(peak_timeout, MINERS_TO_EVAL)


if __name__ == "__main__":
    unittest.main()
