#!/usr/bin/env python3
"""Full-flow concurrent benchmark: the REAL production batch shape, with wallet.

Runs the COMPLETE validate_miner_s3_data flow (real S3 listing snapshot, real
downloads, real dedup scans, job matching, scraper sampling with a stubbed
scraper API) on the top-N miners — all N AT THE SAME TIME in threads, exactly
like the live validator's eval batch. Twice:

  batch 1: INLINE  — today's production path (dedup loop on the eval threads)
  batch 2: POOL    — dedup loop in worker processes

The per-file "timing: dl=..s dedup=..s" log lines stream live from all N
threads interleaved — the same lines `pm2 logs | grep "timing:"` shows in
production. The dedup= numbers are CPU-bound and immune to network noise;
dl= and wall totals wobble with the network.

This is a SPEED benchmark, not a correctness gate: with N threads drawing
from the shared RNG concurrently, file/row sampling interleaves
nondeterministically, so verdict fields can differ between batches in
sample-dependent ways. Verdict equivalence is proven separately by
diag_cache_equiv.py --compare pool (sequential, seeded).

Cost: ~2x N full evals of network download (~40-80GB total), ~25-40 min.
Disk: up to ~N x 4GB concurrently in the validation temp dir (production
already runs this exact load). Scraper API stubbed — zero Apify cost.

  cd ~/testing_playground
  nohup python -u scripts/bench_concurrent_flow.py \
      --wallet.name cfusion --wallet.hotkey v3 \
      --top_n 5 --skip_uids 162 > bench_concurrent.log 2>&1 &
  tail -f bench_concurrent.log
"""
import argparse
import asyncio
import builtins
import functools
import inspect
import os
import sys
import threading
import time
from types import SimpleNamespace

import bittensor as bt

print = functools.partial(builtins.print, flush=True)


def ts():
    return time.strftime("%H:%M:%S")


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vali_utils.validator_s3_access import ValidatorS3Access  # noqa: E402
from vali_utils.s3_utils import (  # noqa: E402
    DuckDBSampledValidator,
    load_expected_jobs_from_gravity,
)

_S3_UTILS_FILE = inspect.getfile(DuckDBSampledValidator)
if not hasattr(DuckDBSampledValidator, "_pooled_url_scan"):
    sys.exit(
        f"ABORT: imported s3_utils has NO _pooled_url_scan — this checkout "
        f"predates the pool commit.\n  imported from: {_S3_UTILS_FILE}"
    )


async def eval_one(config, miner_hotkey, expected_jobs, files_snapshot,
                   pool_enabled, out):
    """One full production miner eval against a frozen listing snapshot."""
    wallet = bt.wallet(config=config)
    s3_reader = ValidatorS3Access(wallet=wallet, s3_auth_url=config.s3_auth_url)

    async def frozen_listing(hk):
        return [dict(f) for f in files_snapshot]
    s3_reader.list_all_files_with_metadata = frozen_listing

    validator = DuckDBSampledValidator(
        wallet=wallet, s3_auth_url=config.s3_auth_url, s3_reader=s3_reader
    )
    validator.DEDUP_POOL_ENABLED = pool_enabled

    async def fake_scraper(entities, platform):
        return [SimpleNamespace(is_valid=True, reason="bench stub (no scraper API)")
                for _ in entities]
    validator._validate_with_scraper = fake_scraper

    phases = {}

    def timed(name, fn):
        async def wrapper(*a, **k):
            t0 = time.monotonic()
            try:
                return await fn(*a, **k)
            finally:
                phases[name] = phases.get(name, 0.0) + (time.monotonic() - t0)
        return wrapper

    validator._sampled_duckdb_validation = timed(
        "dedup_phase", validator._sampled_duckdb_validation)

    t0 = time.monotonic()
    try:
        result = await validator.validate_miner_s3_data(miner_hotkey, expected_jobs)
        out["is_valid"] = getattr(result, "is_valid", None)
        out["effective_gb"] = getattr(result, "effective_size_bytes", 0) / 1e9
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    finally:
        validator.close()
    out["total_s"] = time.monotonic() - t0
    out["dedup_phase_s"] = phases.get("dedup_phase", 0.0)


def run_batch(label, pool_enabled, config, targets, expected_jobs, snapshots):
    """All N miner evals concurrently in threads — the production batch shape."""
    print(f"\n{'='*72}\n[{ts()}] BATCH [{label}] — {len(targets)} miners "
          f"CONCURRENTLY (pool={pool_enabled})\n{'='*72}")
    results = [dict() for _ in targets]

    def worker(i, hotkey):
        asyncio.run(eval_one(config, hotkey, expected_jobs,
                             snapshots[hotkey], pool_enabled, results[i]))

    t0 = time.monotonic()
    threads = [threading.Thread(target=worker, args=(i, hk), daemon=True)
               for i, (uid, hk, inc) in enumerate(targets)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    wall = time.monotonic() - t0

    for (uid, hk, _), r in zip(targets, results):
        if "error" in r:
            print(f"  [{label}] UID {uid:>3}  ERROR: {r['error']}")
        else:
            print(f"  [{label}] UID {uid:>3}  total={r['total_s']:6.1f}s  "
                  f"dedup_phase={r['dedup_phase_s']:6.1f}s  "
                  f"valid={r['is_valid']}  eff={r['effective_gb']:.1f}GB")
    print(f"  [{label}] BATCH WALL: {wall:.1f}s ({wall/60:.1f} min)")
    return wall, results


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--top_n", type=int, default=5)
    p.add_argument("--skip_uids", default="162")
    p.add_argument("--network", default="finney")
    p.add_argument("--s3_auth_url",
                   default="https://data-universe-api.api.macrocosmos.ai")
    p.add_argument("--no_refresh_dd", action="store_true")
    bt.wallet.add_args(p)
    config = bt.config(p)

    try:
        bt.logging.set_info(True)
    except Exception:
        pass

    print(f"[{ts()}] s3_utils imported from: {_S3_UTILS_FILE}")

    if not config.no_refresh_dd:
        try:
            from common.api_client import DataUniverseApiClient
            from dynamic_desirability.desirability_retrieval import (
                run_retrieval_from_api,
            )
            wallet = bt.wallet(config=config)
            async with DataUniverseApiClient(
                base_url=config.s3_auth_url, keypair=wallet.hotkey, timeout=60
            ) as client:
                await run_retrieval_from_api(client, mode="validator")
            print("DD list: force-refreshed from the API endpoint")
        except Exception as e:
            print(f"DD refresh failed ({e}); using the on-disk total.json")

    expected_jobs = load_expected_jobs_from_gravity()
    print(f"expected jobs loaded: {len(expected_jobs)}")

    skip = {int(u) for u in str(config.skip_uids).split(",") if u.strip()}
    print(f"querying metagraph (netuid 13, {config.network}) for top "
          f"{config.top_n} by incentive, skipping UIDs {sorted(skip)} ...")
    st = bt.subtensor(network=config.network)
    mg = st.metagraph(netuid=13)
    ranked = sorted(
        ((uid, mg.hotkeys[uid], float(mg.I[uid])) for uid in range(len(mg.hotkeys))
         if uid not in skip and float(mg.I[uid]) > 0),
        key=lambda t: -t[2])
    targets = ranked[:config.top_n]
    for uid, hk, inc in targets:
        print(f"  UID {uid:>3}  I={inc:.5f}  {hk}")

    # One listing snapshot per miner, shared by BOTH batches: fair comparison
    # and no double whale-listing tax.
    snapshot_wallet = bt.wallet(config=config)
    snapshot_reader = ValidatorS3Access(
        wallet=snapshot_wallet, s3_auth_url=config.s3_auth_url)
    snapshots = {}
    for uid, hk, _ in targets:
        print(f"[{ts()}] snapshot listing UID {uid} ...")
        snapshots[hk] = await snapshot_reader.list_all_files_with_metadata(hk)
        print(f"  {len(snapshots[hk])} files, "
              f"{sum(f.get('size', 0) for f in snapshots[hk])/1e9:.1f}GB")

    wall_in, res_in = run_batch("INLINE", False, config, targets,
                                expected_jobs, snapshots)
    wall_po, res_po = run_batch("POOL", True, config, targets,
                                expected_jobs, snapshots)

    print(f"\n{'='*72}\nSUMMARY — {len(targets)} miners evaluated concurrently "
          f"(the production batch shape)\n{'='*72}")
    print(f" uid    inline_total  inline_dedup    pool_total  pool_dedup")
    for (uid, hk, _), a, b in zip(targets, res_in, res_po):
        ia, ib = a.get('dedup_phase_s', 0), b.get('dedup_phase_s', 0)
        print(f" {uid:>3}  {a.get('total_s', 0):11.1f}s  {ia:11.1f}s  "
              f"{b.get('total_s', 0):11.1f}s  {ib:9.1f}s")
    print(f"\n BATCH WALL:  inline={wall_in/60:.1f} min   pool={wall_po/60:.1f} min"
          f"   -> {wall_in/max(wall_po, 0.001):.1f}x")
    print("\n dedup_phase is the GIL-sensitive number (CPU-bound).")
    print(" dl=/wall include network variance — compare per-file dedup= lines")
    print(" above with production's `pm2 logs | grep \"timing:\"` for the 1:1 map.")


if __name__ == "__main__":
    asyncio.run(main())
