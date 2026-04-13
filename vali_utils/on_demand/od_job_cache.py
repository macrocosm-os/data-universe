import threading
import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import bittensor as bt


@dataclass
class CachedMinerODResult:
    """A single OD job result for one miner, stored in the cache."""
    job_id: str
    submitted_at: Optional[dt.datetime]
    returned_count: int
    requested_limit: Optional[int]
    passed_validation: Optional[bool]  # True=passed, False=failed, None=not sampled
    speed_multiplier: float
    volume_multiplier: float
    failure_reason: Optional[str] = None


class ODJobCache:
    """Thread-safe cache for on-demand job results.

    The OD poller (validator.py) writes results here for ALL submitters per job.
    The evaluator (miner_evaluator.py) reads and drains results per-miner when
    eval_miner() runs.

    This decouples OD scoring from the polling loop: the poller validates and
    caches, the evaluator applies scores at eval time.
    """

    def __init__(self):
        self._lock = threading.Lock()
        # hotkey -> list of pending results
        self._results: Dict[str, List[CachedMinerODResult]] = {}
        # Set of job IDs already processed (replaces processed_job_ids_cache in validator)
        self._processed_jobs: set = set()
        # Max processed jobs to keep (LRU-style cap)
        self._max_processed_jobs = 10_000

    def add_results(self, job_id: str, results: Dict[str, CachedMinerODResult]) -> None:
        """Add OD results for multiple miners from a single job.

        Args:
            job_id: The OD job ID.
            results: Mapping of hotkey -> CachedMinerODResult for all submitters.
        """
        with self._lock:
            for hotkey, result in results.items():
                if hotkey not in self._results:
                    self._results[hotkey] = []
                self._results[hotkey].append(result)

            self._processed_jobs.add(job_id)

            # Cap the processed jobs set
            if len(self._processed_jobs) > self._max_processed_jobs:
                # Remove oldest entries (set is unordered, but for LRU-style we just trim)
                excess = len(self._processed_jobs) - self._max_processed_jobs
                to_remove = list(self._processed_jobs)[:excess]
                for jid in to_remove:
                    self._processed_jobs.discard(jid)

            bt.logging.trace(
                f"ODJobCache: cached {len(results)} miner results for job {job_id}"
            )

    def get_and_drain(self, hotkey: str) -> List[CachedMinerODResult]:
        """Return all pending results for a miner and remove them from the cache.

        Drain semantics prevent double-counting: once eval_miner() processes
        results, they're gone.

        Args:
            hotkey: The miner's hotkey.

        Returns:
            List of cached results (empty if none pending).
        """
        with self._lock:
            results = self._results.pop(hotkey, [])
            if results:
                bt.logging.trace(
                    f"ODJobCache: drained {len(results)} results for {hotkey[:16]}..."
                )
            return results

    def is_job_processed(self, job_id: str) -> bool:
        """Check if a job has already been cached."""
        with self._lock:
            return job_id in self._processed_jobs

    def get_pending_miner_count(self) -> int:
        """Return the number of miners with pending results (for monitoring)."""
        with self._lock:
            return sum(1 for results in self._results.values() if results)
