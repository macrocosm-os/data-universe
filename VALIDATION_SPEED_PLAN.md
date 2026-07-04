# Validation Loop Speed Plan

**Goal:** full metagraph pass from ~17h to ~2.5–3.5h, without changing any verdict.
**Status:** Lever 1 (dedup worker pool) implemented + tested locally, 2026-07-04.

## Where the time goes (measured, July 2026)

Per-miner eval at 5 concurrent threads: ~20–23 min.

| Phase | Time | Bound by |
|---|---|---|
| Dedup hash loop (normalize+blake2b, ~20M rows) | **~1000s** | **CPU — GIL-serialized across eval threads** |
| Scraper/Apify spot checks | ~2–3 min | external API |
| Downloads (~3–4GB sampled) | ~90–100s | bandwidth |
| Listing (whale miners) | up to ~190s | S3 round trips |
| Job matching + entity sampling | ~2s | fixed by eval-scoped file cache (PR #863) |

GIL proof: the exact production loop on 5×1M-row files —
sequential 15.7s, **5 threads 18.9s (0.8x — slower than sequential)**, 5 processes 5.3s.

## Levers, in order

### 1. Dedup worker-process pool ← DONE (this change)
Move the per-file dedup loop from eval threads into dedicated worker
subprocesses (`python -m vali_utils.dedup_worker`, JSON-lines over pipes).

- `vali_utils/url_normalizer.py` — normalize fn extracted (stdlib-only) so workers skip bittensor
- `vali_utils/dedup_worker.py` — the loop, verbatim semantics, in-child 180s conn.interrupt watchdog
- `vali_utils/dedup_pool.py` — fixed pool, checkout via Queue; wedged/crashed worker ⇒ SIGKILL + ~30ms respawn
- `s3_utils` — `DEDUP_POOL_ENABLED` toggle (inline path retained for A/B), child errors re-raised as the same duckdb exception types ⇒ per-file fail-open/fail-closed routing unchanged

Design decision: NOT `ProcessPoolExecutor` — spawn workers re-import the
validator's `__main__` (measured 2.8s + 363MB per worker vs 0.03s + 24MB),
and PPE tasks are unkillable (a natively-wedged worker poisons a slot forever).

Local test results (all passed):
- counts identical to inline loop incl. None/whitespace/junk urls; overrun cap identical
- corrupt file → `duckdb.Error` (fail-closed), timeout → `InterruptException` (fail-closed), SIGKILLed worker → `duckdb.Error` + pool self-heals
- **5 eval threads: inline 13.0s vs pool 2.4s = 5.5x**, counts identical
- 5 workers total RSS ~800MB; pool cold-start ~1s
- parent SIGKILL → all workers exit on stdin EOF (no orphans); worker starvation → clean `WorkerTimeout`

Adversarial self-review (3 lenses: verdict-equivalence / lifecycle / security).
NOTE: the review workflow hit a session limit mid-run, so it was completed
inline. Two real issues found and FIXED before commit:
- **DuckDB subclass fidelity** — the child flattened every `duckdb.Error` to
  the base class; but the per-file handler routes `IOException` (a
  `duckdb.Error` subclass) to fail-OPEN-skip and all others to
  fail-CLOSED-page-decode. Flattening would flip an honest local-disk IO
  error from skip to fail. Fix: child carries `error_type`, parent re-raises
  the exact subclass. Test T8/T8b/T8c.
- **Queue-wait ate the scan budget** — a file that waited for a free worker
  got a shorter scan than an unqueued file (spurious fail-closed risk under
  15-thread saturation). Fix: `QUEUE_WAIT_SECS` bounds queue wait separately;
  the scan always gets the full per-file budget.
Also hardened: binary pipes + raw `os.read` after `select()` so a child that
writes a partial line then wedges can never block the parent past its deadline.

Host verification (run from repo root, needs UID 89 wallet):
```
python scripts/diag_cache_equiv.py --wallet.name cfusion --wallet.hotkey v3 \
    --compare pool --top_n 3 --skip_uids 162
```
Expect: PASS 3/3, diffs=0. NOTE: this A/B runs one eval at a time, so it
proves equivalence, not speed — the speedup only exists under concurrency
(dedup= lines in production logs are the speed evidence: ~100s → ~15-20s per
2M-row file once deployed).

### 2. Raise concurrent miners 5 → 10 → 15 (after lever 1 is deployed)
Without lever 1 this is useless (GIL) and at 15 harmful (evals exceed the
3900s per-miner timeout). With it: batch throughput scales until bandwidth
binds. One-line change (`miner_evaluator.py: miners_to_eval`). Watch: `dl=`
times (bandwidth contention), batch wall time, RSS, `.s3_validation_tmp` size
(disk = concurrent_evals × ~3GB).

### 3. Download‖parse pipeline (dedup phase)
Prefetch file N+1 while N parses; hides ~90–100s/miner. Verdict-neutral.
Do after lever 2 — bandwidth contention data from lever 2 informs sizing.

### 4. API-side latest-only listing (data-universe-api)
The API records every upload key at presign time; serving latest-per-job from
Postgres kills the 190s whale-listing tax AND the silent 200-page truncation.
Separate repo/deploy; team sign-off needed.

### (parked) Stale-file reaper cron — storage cost + fairness, not speed.
### (rejected) Polars engine swap — 1.08x measured; see PR #863 discussion.

## Expected end state

| Milestone | Per-miner (excl. Apify) | Full pass |
|---|---|---|
| Today (post-cache) | ~18 min | ~17h |
| + worker pool | ~6–8 min | ~7h |
| + 10–15 concurrent | ~7–9 min | **~2.5–3.5h** |
| + pipeline & listing | ~4–6 min | ~2h |
