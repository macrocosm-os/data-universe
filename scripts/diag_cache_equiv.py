#!/usr/bin/env python3
"""Prove the eval-scoped local file cache (commit ad808af) is verdict-identical
and measure its real speedup — by running the REAL validate_miner_s3_data twice
per miner:

  CACHED  : the code as shipped — dedup phase downloads + registers files, job
            content matching and scraper entity sampling read the local copies.
  NETWORK : same code, same seed, but the cache READ is neutered (a dict whose
            .get() always misses), so job matching + scraper sampling fall back
            to the presigned URL exactly like the pre-cache code. The dedup
            phase still downloads per-file either way (that shipped Jun 25),
            so the diff isolates precisely what the cache change altered.

Miners: by default the TOP-N by on-chain incentive (netuid 13), skipping
--skip_uids; or a single explicit --miner_hotkey.

Both runs per miner:
  - seed random identically (all file/row-group/entity sampling uses the global
    `random` module), so sampling decisions match run-to-run;
  - stub _validate_with_scraper (NO Apify/scraper API calls — entity SAMPLING,
    the part the cache touches, is fully real);
  - time each phase; count cache hits (CACHED run); snapshot the temp dir
    before/after to prove close() leaves no leftovers.

Expected jobs come from dynamic_desirability/total.json relative to cwd — run
from the LIVE repo root (~/data-universe): the running validator refreshes that
file every eval cycle, so it is the latest DD list.

RUNTIME WARNING: each big miner costs 2 full validations (~2x listing + 2x
dedup downloads) ~ 15-25 min. Top-5 ~ 1-2 hours. Run it in tmux/nohup.

Run on the validator host as UID 89, FROM ~/data-universe:
  python scripts/diag_cache_equiv.py --wallet.name cfusion --wallet.hotkey v3 \
      --top_n 5 --skip_uids 162
or a single miner:
  python scripts/diag_cache_equiv.py --wallet.name cfusion --wallet.hotkey v3 \
      --miner_hotkey <HOTKEY>
"""
import argparse
import asyncio
import builtins
import dataclasses
import functools
import inspect
import os
import random
import sys
import time
from types import SimpleNamespace

import bittensor as bt

# Real-time-watchable output: flush every print (nohup/pipe block-buffers
# otherwise) and timestamp the diag's own lines.
print = functools.partial(builtins.print, flush=True)


def ts():
    return time.strftime("%H:%M:%S")

from vali_utils.validator_s3_access import ValidatorS3Access
from vali_utils.s3_utils import (
    DuckDBSampledValidator,
    load_expected_jobs_from_gravity,
)

SEED = 1337

# Guard: this diag tests the eval-scoped cache (commit ad808af). If the
# imported s3_utils predates it (e.g. run from a stale checkout like
# ~/testing_playground), the whole comparison is meaningless — abort loudly.
_S3_UTILS_FILE = inspect.getfile(DuckDBSampledValidator)
if not hasattr(DuckDBSampledValidator, "_local_file_for"):
    sys.exit(
        f"ABORT: imported s3_utils has NO _local_file_for — this checkout "
        f"predates the cache commit.\n  imported from: {_S3_UTILS_FILE}\n"
        f"  Run from the up-to-date repo: cd ~/data-universe && git pull "
        f"&& python scripts/diag_cache_equiv.py ..."
    )


class CountingDict(dict):
    """Cache dict that counts read hits/misses (CACHED run)."""
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.hits = 0
        self.misses = 0

    def get(self, key, default=None):
        v = super().get(key, default)
        if v is None:
            self.misses += 1
        else:
            self.hits += 1
        return v


class NeverHit(dict):
    """Cache dict whose reads always miss (NETWORK control run).

    Writes still land (so close() sweeps the dedup phase's downloads), but
    every .get() returns None: the cache-only lookups in job matching /
    scraper sampling fall back to the presigned URL, i.e. the pre-cache
    network read path."""
    def get(self, key, default=None):
        return None


# ---------------------------------------------------------------------------
# POLARS engine shim (--compare polars): serves ONLY the dedup url scan
# ("SELECT url FROM read_parquet('<path>')") from polars; every other query
# (parquet_metadata, parquet_schema), the watchdog, the Python normalize/hash
# loop, and all verdict logic run the byte-identical production code.
# ---------------------------------------------------------------------------
import re as _re
_DEDUP_SQL_RE = _re.compile(r"^SELECT url FROM read_parquet\('(.+)'\)$")


class _PolarsUrlCursor:
    """Duck-typed cursor: fetchmany() over the url column read by polars."""
    def __init__(self, path):
        import polars as pl
        self._urls = pl.read_parquet(path, columns=["url"]).get_column("url").to_list()
        self._pos = 0

    def fetchmany(self, n):
        batch = self._urls[self._pos:self._pos + n]
        self._pos += n
        return [(u,) for u in batch]


class _PatchedConn:
    """Wraps a real DuckDB connection; reroutes only the dedup scan to polars."""
    def __init__(self, real):
        self._real = real

    def execute(self, sql, *a, **k):
        m = _DEDUP_SQL_RE.match(sql.strip())
        if m and os.path.exists(m.group(1)):
            return _PolarsUrlCursor(m.group(1))
        return self._real.execute(sql, *a, **k)

    def __getattr__(self, name):  # interrupt/close/etc pass through
        return getattr(self._real, name)


class _DuckdbPolarsProxy:
    """Module proxy: connect() returns patched conns; exception classes and
    everything else delegate to the real duckdb module."""
    def __init__(self, real_module):
        self._real = real_module

    def connect(self, *a, **k):
        return _PatchedConn(self._real.connect(*a, **k))

    def __getattr__(self, name):
        return getattr(self._real, name)


async def run_once(label, cache_dict_cls, config, miner_hotkey, expected_jobs,
                   files_snapshot, engine="duckdb", pool_enabled=None):
    print(f"  [{ts()}] [{label}] run starting (frozen listing snapshot: "
          f"{len(files_snapshot)} files, dedup engine={engine}, "
          f"pool={'default' if pool_enabled is None else pool_enabled})")
    wallet = bt.wallet(config=config)
    s3_reader = ValidatorS3Access(wallet=wallet, s3_auth_url=config.s3_auth_url)

    # Swap the dedup-scan engine for this run only (restored in the finally).
    import vali_utils.s3_utils as s3u
    real_duckdb = s3u.duckdb
    if engine == "polars":
        s3u.duckdb = _DuckdbPolarsProxy(real_duckdb)

    # FREEZE the listing: live miners upload continuously, so two real listings
    # minutes apart see different files -> different samples -> spurious diffs
    # (observed: +6 files between runs shifted total_size/effective_size).
    # Both runs must validate the exact same snapshot for the diff to be
    # meaningful. Fresh dict copies per run in case validation mutates entries.
    async def frozen_listing(hk):
        return [dict(f) for f in files_snapshot]
    s3_reader.list_all_files_with_metadata = frozen_listing

    validator = DuckDBSampledValidator(
        wallet=wallet, s3_auth_url=config.s3_auth_url, s3_reader=s3_reader
    )
    validator._local_files = cache_dict_cls()
    if pool_enabled is not None:
        validator.DEDUP_POOL_ENABLED = pool_enabled  # instance shadow of class attr

    # Stub the external scraper API (deterministic, no Apify calls). The entity
    # SAMPLING inside _perform_scraper_validation stays fully real.
    async def fake_scraper(entities, platform):
        return [
            SimpleNamespace(is_valid=True, reason="diag stub (no scraper API)")
            for _ in entities
        ]
    validator._validate_with_scraper = fake_scraper

    # Time each phase by wrapping the real methods.
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
    validator._perform_job_content_matching = timed(
        "job_matching", validator._perform_job_content_matching)
    validator._perform_scraper_validation = timed(
        "scraper_sampling", validator._perform_scraper_validation)

    temp_dir = validator.LOCAL_VALIDATION_TEMP_DIR
    before = set(os.listdir(temp_dir)) if os.path.isdir(temp_dir) else set()

    random.seed(SEED)
    t0 = time.monotonic()
    try:
        result = await validator.validate_miner_s3_data(
            miner_hotkey, expected_jobs
        )
    finally:
        validator.close()
        s3u.duckdb = real_duckdb
    total = time.monotonic() - t0

    after = set(os.listdir(temp_dir)) if os.path.isdir(temp_dir) else set()
    leaked = after - before
    cache = validator._local_files
    stats = {
        "total_s": total,
        "phases": dict(phases),
        "listing_setup_s": total - sum(phases.values()),
        "cache_hits": getattr(cache, "hits", None),
        "cache_misses": getattr(cache, "misses", None),
        "leaked_files": sorted(leaked),
    }
    print(f"  [{ts()}] [{label}] total={total:.1f}s  "
          f"listing+setup={stats['listing_setup_s']:.1f}s  "
          + "  ".join(f"{k}={v:.1f}s" for k, v in phases.items()))
    if stats["cache_hits"] is not None:
        print(f"  [{label}] cache reads: {stats['cache_hits']} hits / "
              f"{stats['cache_misses']} misses")
    print(f"  [{label}] temp leftovers after close(): "
          f"{len(leaked)} {'OK' if not leaked else '*** LEAK: ' + str(sorted(leaked)[:5])}")
    print(f"  [{label}] result: is_valid={result.is_valid} "
          f"active_jobs={result.total_active_jobs} "
          f"files={result.recent_files_count} "
          f"effective_size={getattr(result, 'effective_size_bytes', 0)/1e9:.2f}GB "
          f"reason={str(result.reason)[:90]!r}")
    return result, stats


# Display-only fields: consumed exclusively by the rich-table logger
# (s3_logging_utils.py) — never by the scorer (which reads effective_size_bytes
# + is_valid). Their ORDER depends on the RNG stream, which legitimately
# diverges between runs when a network read consumes a different number of
# random() calls. Compared order-insensitively and reported as INFO, not DIFF.
DISPLAY_ONLY_FIELDS = {"sample_validation_results", "sample_job_mismatches"}


def diff_results(r1, r2):
    """Returns (strict_diffs, info_diffs)."""
    d1, d2 = dataclasses.asdict(r1), dataclasses.asdict(r2)
    strict, info = [], []
    for k in d1:
        v1, v2 = d1[k], d2.get(k)
        if k in DISPLAY_ONLY_FIELDS:
            if isinstance(v1, list) and isinstance(v2, list):
                if sorted(map(str, v1)) != sorted(map(str, v2)):
                    info.append((k, v1, v2))
                elif v1 != v2:
                    info.append((k + " (order-only)", v1, v2))
            elif v1 != v2:
                info.append((k, v1, v2))
            continue
        if isinstance(v1, float) and isinstance(v2, float):
            if abs(v1 - v2) > 1e-9:
                strict.append((k, v1, v2))
        elif v1 != v2:
            strict.append((k, v1, v2))
    return strict, info


def top_miners_by_incentive(n, skip_uids, network, netuid=13):
    st = bt.subtensor(network=network)
    mg = st.metagraph(netuid=netuid)
    order = sorted(range(len(mg.hotkeys)),
                   key=lambda u: float(mg.I[u]), reverse=True)
    out = []
    for uid in order:
        if uid in skip_uids or float(mg.I[uid]) <= 0:
            continue
        out.append((uid, mg.hotkeys[uid], float(mg.I[uid])))
        if len(out) >= n:
            break
    return out


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--miner_hotkey", default=None,
                   help="single-miner mode (overrides --top_n)")
    p.add_argument("--top_n", type=int, default=5,
                   help="run the top-N miners by incentive")
    p.add_argument("--skip_uids", default="162",
                   help="comma-separated UIDs to skip")
    p.add_argument("--network", default="finney")
    p.add_argument("--s3_auth_url",
                   default="https://data-universe-api.api.macrocosmos.ai")
    p.add_argument("--no_refresh_dd", action="store_true",
                   help="skip the live DD-endpoint pull; use total.json as-is")
    p.add_argument("--compare", choices=["network", "polars", "pool"], default="network",
                   help="network: cached-vs-network reads (cache equivalence). "
                        "polars: duckdb-vs-polars dedup engine, both cached. "
                        "pool: inline-thread dedup vs worker-process pool "
                        "(GIL fix — verdict equivalence + time saving)")
    bt.wallet.add_args(p)
    config = bt.config(p)

    # Stream bittensor INFO logs (listing progress, the per-file
    # "timing: dl=..s meta=..s dedup=..s" lines) so the run is watchable
    # in real time via tail -f.
    try:
        bt.logging.set_info(True)
    except Exception:
        pass

    print(f"[{ts()}] s3_utils imported from: {_S3_UTILS_FILE}")

    # Pull the FRESHEST DD list straight from the API (same call the live
    # validator makes after every eval batch: validator_get_latest_dd_list).
    # On success this rewrites dynamic_desirability/total.json; on failure we
    # fall back to the existing file (which the running validator keeps fresh).
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

    dd_path = os.path.join(os.getcwd(), "dynamic_desirability", "total.json")
    if os.path.exists(dd_path):
        age_min = (time.time() - os.path.getmtime(dd_path)) / 60
        print(f"DD list: {dd_path} (age {age_min:.0f} min)")
    expected_jobs = load_expected_jobs_from_gravity()
    print(f"expected jobs loaded: {len(expected_jobs)} (cwd={os.getcwd()})")

    if config.miner_hotkey:
        targets = [(-1, config.miner_hotkey, 0.0)]
    else:
        skip = {int(u) for u in str(config.skip_uids).split(",") if u.strip()}
        print(f"querying metagraph (netuid 13, {config.network}) for top "
              f"{config.top_n} by incentive, skipping UIDs {sorted(skip)} ...")
        targets = top_miners_by_incentive(config.top_n, skip, config.network)
        for uid, hk, inc in targets:
            print(f"  UID {uid:>3}  I={inc:.5f}  {hk}")

    snapshot_wallet = bt.wallet(config=config)
    snapshot_reader = ValidatorS3Access(
        wallet=snapshot_wallet, s3_auth_url=config.s3_auth_url)

    rows = []
    t_batch = time.monotonic()
    for i, (uid, hotkey, inc) in enumerate(targets):
        elapsed = (time.monotonic() - t_batch) / 60
        print(f"\n{'='*72}\n[{ts()}] MINER {i+1}/{len(targets)}  UID={uid}  "
              f"{hotkey}  (batch elapsed {elapsed:.0f} min)")

        # One real listing per miner; BOTH runs validate this exact snapshot.
        print(f"  [{ts()}] listing once (snapshot frozen for both runs)...")
        files_snapshot = await snapshot_reader.list_all_files_with_metadata(hotkey)
        print(f"  [{ts()}] snapshot: {len(files_snapshot)} files, "
              f"{sum(f.get('size', 0) for f in files_snapshot)/1e9:.1f}GB")

        if config.compare == "pool":
            # Both runs cached; only WHERE the dedup loop runs differs
            # (inline on the eval thread vs worker-process pool).
            cached_result, cached = await run_once(
                "INLINE", CountingDict, config, hotkey, expected_jobs,
                files_snapshot, pool_enabled=False)
            network_result, network = await run_once(
                "POOL", CountingDict, config, hotkey, expected_jobs,
                files_snapshot, pool_enabled=True)
        elif config.compare == "polars":
            # Both runs use the cache; only the dedup-scan engine differs.
            cached_result, cached = await run_once(
                "DUCKDB", CountingDict, config, hotkey, expected_jobs,
                files_snapshot, engine="duckdb")
            network_result, network = await run_once(
                "POLARS", CountingDict, config, hotkey, expected_jobs,
                files_snapshot, engine="polars")
        else:
            cached_result, cached = await run_once(
                "CACHED", CountingDict, config, hotkey, expected_jobs, files_snapshot)
            network_result, network = await run_once(
                "NETWORK", NeverHit, config, hotkey, expected_jobs, files_snapshot)

        diffs, info_diffs = diff_results(cached_result, network_result)

        if config.compare in ("polars", "pool"):
            a_name = "duckdb" if config.compare == "polars" else "inline"
            b_name = "polars" if config.compare == "polars" else "pool"
            dd_a = cached["phases"].get("dedup_phase", 0.0)
            dd_b = network["phases"].get("dedup_phase", 0.0)
            sav = ((cached["total_s"] - network["total_s"])
                   / cached["total_s"] * 100 if cached["total_s"] else 0.0)
            print(f"  ENGINE: dedup_phase {a_name}={dd_a:.1f}s {b_name}={dd_b:.1f}s "
                  f"({(dd_a - dd_b):+.1f}s) | eval total {cached['total_s']:.1f}s "
                  f"-> {network['total_s']:.1f}s = {sav:+.1f}% of whole eval")
        vacuous = (cached["phases"].get("dedup_phase", 0) == 0
                   or network["phases"].get("dedup_phase", 0) == 0)
        leaked = bool(cached["leaked_files"] or network["leaked_files"])
        if diffs:
            print(f"  *** {len(diffs)} VERDICT FIELD(S) DIFFER ***")
            for k, v1, v2 in diffs:
                print(f"    {k}: CACHED={str(v1)[:70]}  NETWORK={str(v2)[:70]}")
        for k, v1, v2 in info_diffs:
            print(f"  info: display-only field '{k}' differs "
                  f"(not consumed by scoring; RNG-order sensitive)")

        def spd(name):
            c = cached["phases"].get(name, 0.0)
            n = network["phases"].get(name, 0.0)
            return (n / c) if c > 0 else float("nan")

        status = ("VACUOUS" if vacuous else
                  "DIFF" if diffs else
                  "LEAK" if leaked else "PASS")
        rows.append({
            "uid": uid, "hotkey": hotkey[:12], "status": status,
            "cached_s": cached["total_s"], "network_s": network["total_s"],
            "jm_spd": spd("job_matching"), "ss_spd": spd("scraper_sampling"),
            "hits": cached["cache_hits"], "misses": cached["cache_misses"],
            "n_diffs": len(diffs),
        })
        print(f"  -> {status}")

    print(f"\n{'='*72}\nSUMMARY")
    print(f"{'uid':>4} {'hotkey':<13} {'status':<8} {'cached':>8} {'network':>8} "
          f"{'jm_spd':>7} {'ss_spd':>7} {'hits':>5} {'miss':>5} {'diffs':>5}")
    for r in rows:
        print(f"{r['uid']:>4} {r['hotkey']:<13} {r['status']:<8} "
              f"{r['cached_s']:>7.0f}s {r['network_s']:>7.0f}s "
              f"{r['jm_spd']:>6.1f}x {r['ss_spd']:>6.1f}x "
              f"{r['hits'] if r['hits'] is not None else '-':>5} "
              f"{r['misses'] if r['misses'] is not None else '-':>5} "
              f"{r['n_diffs']:>5}")

    passed = [r for r in rows if r["status"] == "PASS"]
    vacuous = [r for r in rows if r["status"] == "VACUOUS"]
    bad = [r for r in rows if r["status"] in ("DIFF", "LEAK")]
    if bad:
        print(f"\n*** {len(bad)} miner(s) FAILED equivalence — investigate "
              f"before deploying. ***")
        sys.exit(1)
    if not passed:
        print("\n*** All runs vacuous — nothing was tested (check DD list / "
              "active jobs). ***")
        sys.exit(2)
    print(f"\nPASS on {len(passed)}/{len(rows)} miners"
          + (f" ({len(vacuous)} vacuous — no active jobs)" if vacuous else "")
          + " — cache is verdict-equivalent and leak-free.")


if __name__ == "__main__":
    asyncio.run(main())
