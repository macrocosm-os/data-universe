"""Parent-side pool of dedup worker subprocesses.

Each worker is `python -m vali_utils.dedup_worker` (dependency-light: 0.03s
spawn, ~24MB RSS — measured against 2.8s/363MB for a spawn-context
ProcessPoolExecutor worker, which re-imports the validator's __main__).
Protocol: one JSON request line on stdin -> one JSON result line on stdout.

Why not ProcessPoolExecutor: (1) the __main__ re-import tax above; (2) its
tasks are unkillable — a worker wedged in native code poisons a slot
forever. Here a deadline miss or a crash is handled by SIGKILLing the child
and respawning (~30ms), so one adversarial file can never degrade the pool.

Thread-safety: DedupPool is shared by all eval threads. Workers are handed
out through a Queue, so each subprocess is used by exactly one thread at a
time; respawn happens inside the borrowing thread.
"""
import json
import os
import queue
import select
import subprocess
import sys
import threading
import time

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class WorkerCrashed(Exception):
    """The worker subprocess died (or was killed) before answering."""


class WorkerTimeout(Exception):
    """The worker did not answer within the deadline (killed + respawned)."""


class _Worker:
    def __init__(self):
        self._proc = None
        self._spawn()

    def _spawn(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = _REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
        # Binary pipes: replies are read with raw os.read() after select() so
        # a child that writes a PARTIAL line and wedges can never block the
        # parent (readline() would wait forever for the newline).
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "vali_utils.dedup_worker"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            cwd=_REPO_ROOT,
            env=env,
        )

    def kill(self):
        try:
            self._proc.kill()
            self._proc.wait(timeout=5)
        except Exception:
            pass

    def _ensure_alive(self):
        if self._proc is None or self._proc.poll() is not None:
            self._spawn()

    def request(self, req: dict, deadline: float) -> dict:
        """Send one request, wait until the monotonic `deadline` for the reply.

        On timeout or crash the child is killed and respawned so the worker
        object is always healthy when returned to the pool."""
        self._ensure_alive()
        try:
            self._proc.stdin.write((json.dumps(req) + "\n").encode())
            self._proc.stdin.flush()
        except (BrokenPipeError, OSError):
            self.kill()
            self._spawn()
            raise WorkerCrashed("worker pipe broken on send")

        # Wait for exactly one newline-terminated reply. select() + raw
        # os.read() keeps this fully non-blocking: neither a wedged child nor
        # a partial line can ever hold this thread past the deadline.
        fd = self._proc.stdout.fileno()
        buf = b""
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.kill()
                self._spawn()
                raise WorkerTimeout("dedup worker exceeded its deadline")
            ready, _, _ = select.select([fd], [], [], min(remaining, 1.0))
            if not ready:
                if self._proc.poll() is not None:
                    rc = self._proc.returncode
                    self._spawn()
                    raise WorkerCrashed(f"worker died mid-scan (exit {rc})")
                continue
            chunk = os.read(fd, 65536)
            if chunk == b"":  # EOF — child died
                rc = self._proc.poll()
                self.kill()
                self._spawn()
                raise WorkerCrashed(f"worker died mid-scan (exit {rc})")
            buf += chunk
            if b"\n" in buf:
                line, _, _rest = buf.partition(b"\n")
                # _rest must be empty (one reply per request); discard
                # defensively rather than desync.
                return json.loads(line.decode())


class DedupPool:
    """Fixed-size pool of dedup workers, checkout/checkin via a Queue."""

    def __init__(self, size: int):
        self.size = size
        self._q: "queue.Queue[_Worker]" = queue.Queue()
        self._lock = threading.Lock()
        self._started = False

    # Cap on how long a scan may wait for a free worker before failing. With
    # >=size concurrent eval threads all scanning ~equal-length files, waits
    # are short and bounded; this only fires under genuine saturation. Kept
    # SEPARATE from the scan budget so queue wait never shortens the scan (a
    # queued file must get the same time to prove itself as an unqueued one).
    QUEUE_WAIT_SECS = 120

    def _ensure_started(self):
        with self._lock:
            if not self._started:
                for _ in range(self.size):
                    self._q.put(_Worker())
                self._started = True

    def scan(self, path: str, max_iterations: int, batch_size: int,
             timeout_secs: float) -> dict:
        """Run one dedup scan on a free worker. Raises WorkerTimeout /
        WorkerCrashed.

        timeout_secs is the SCAN budget (already clamped by the caller to the
        remaining per-miner deadline). Queue wait is bounded separately by
        QUEUE_WAIT_SECS so waiting for a worker never eats into a file's scan
        time — a queued file gets the same budget to prove itself as an
        unqueued one, preserving verdict equivalence with the inline path."""
        self._ensure_started()
        try:
            worker = self._q.get(timeout=min(self.QUEUE_WAIT_SECS, timeout_secs))
        except queue.Empty:
            raise WorkerTimeout("no dedup worker free")
        try:
            # Child self-bounds at timeout_secs via its internal conn.interrupt
            # watchdog, then ANSWERS with an interrupt error; the outer deadline
            # (+15s) only catches a wedged child that ignored its own watchdog.
            return worker.request(
                {"path": path, "max_iterations": max_iterations,
                 "batch_size": batch_size, "timeout_secs": timeout_secs},
                deadline=time.monotonic() + timeout_secs + 15,
            )
        finally:
            self._q.put(worker)

    def shutdown(self):
        with self._lock:
            while not self._q.empty():
                try:
                    self._q.get_nowait().kill()
                except queue.Empty:
                    break
            self._started = False
