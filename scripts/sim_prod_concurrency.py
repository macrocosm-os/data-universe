#!/usr/bin/env python3
"""Production-shape concurrency simulation for the dedup worker pool.

Reproduces, on this machine, exactly what the live validator does during an
eval batch: N eval threads each running the URL-dedup scan on a 2M-row
parquet file AT THE SAME TIME. Runs it three ways:

  1. solo baseline      — one file, no concurrency (what `dedup=` costs alone)
  2. N threads, INLINE  — today's production path (GIL-bound)
  3. N threads, POOL    — the worker-process pool

Per-file `dedup=` lines are printed in the same shape as the pm2 production
logs, so the numbers here map 1:1 onto what `pm2 logs | grep "timing: dl="`
shows before and after deploy.

No wallet, no network, no S3 — synthetic files only. Safe anywhere.
Runtime: ~4-6 min (most of it generating 10M rows and the slow inline pass).

  cd <dir containing vali_utils/> && python scripts/sim_prod_concurrency.py
"""
import hashlib
import os
import random
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

N_MINERS = 5          # production: miners_to_eval
ROWS = 2_000_000      # production file class behind dedup=47-104s log lines
BATCH = 10_000


def make_file(dirpath, name, seed):
    rng = random.Random(seed)
    clean = int(ROWS * 0.95)
    urls = [f"https://x.com/user{rng.randrange(500)}/status/{10**15 + j}"
            for j in range(clean)]
    urls += [f"https://x.com/user1/status/{10**15 + rng.randrange(clean)}"
             for _ in range(ROWS - clean)]
    rng.shuffle(urls)
    path = os.path.join(dirpath, name)
    pq.write_table(pa.table({"url": urls}), path, row_group_size=10_000)
    return path


def inline_scan(path, max_iterations):
    """The exact production inline loop (today's code path)."""
    from vali_utils.url_normalizer import normalize_url_for_dedup
    conn = duckdb.connect(":memory:")
    conn.execute("SET memory_limit='1GB';")
    conn.execute("SET threads=1;")
    cur = conn.execute(f"SELECT url FROM read_parquet('{path}')")
    seen, rows, dups = set(), 0, 0
    for _ in range(max_iterations):
        batch = cur.fetchmany(BATCH)
        if not batch:
            break
        for (u,) in batch:
            h = hashlib.blake2b(
                normalize_url_for_dedup(u).encode(), digest_size=8).digest()
            rows += 1
            if h in seen:
                dups += 1
            else:
                seen.add(h)
        del batch
    conn.close()
    return {"rows": rows, "dups": dups}


def run_concurrent(label, paths, scan_fn):
    """N threads, one file each — the shape of a production eval batch."""
    results, t_wall = {}, time.monotonic()

    def work(i, p):
        t0 = time.monotonic()
        r = scan_fn(p)
        secs = time.monotonic() - t0
        results[i] = (r, secs)
        print(f"  [{label}] miner-{i + 1} file {os.path.basename(p)} "
              f"({ROWS:,} rows) dedup={secs:.1f}s dups={r['dups']:,}")

    threads = [threading.Thread(target=work, args=(i, p))
               for i, p in enumerate(paths)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    wall = time.monotonic() - t_wall
    per_file = [secs for _, secs in results.values()]
    return wall, per_file, results


def main():
    import vali_utils.s3_utils as s3u
    print(f"s3_utils: {s3u.__file__}")
    v = object.__new__(s3u.DuckDBSampledValidator)
    cap = (ROWS // BATCH) + 2
    print(f"simulating {N_MINERS} concurrent miner evals, "
          f"{ROWS:,}-row file each (pool workers="
          f"{s3u.DuckDBSampledValidator.DEDUP_POOL_WORKERS})\n")

    with tempfile.TemporaryDirectory(prefix="dedup_sim_") as d:
        print(f"generating {N_MINERS} x {ROWS:,}-row files (~1-2 min) ...")
        paths = [make_file(d, f"miner{i + 1}.parquet", i) for i in range(N_MINERS)]

        # 1. solo baseline
        t0 = time.monotonic()
        ref = inline_scan(paths[0], cap)
        solo = time.monotonic() - t0
        print(f"\n1. SOLO baseline (no concurrency): dedup={solo:.1f}s "
              f"per {ROWS:,}-row file\n")

        # 2. today's production shape: N threads, inline loop, one GIL
        print(f"2. INLINE x{N_MINERS} threads — today's production path:")
        wall_in, per_in, res_in = run_concurrent(
            "inline", paths, lambda p: inline_scan(p, cap))
        print(f"   -> wall={wall_in:.1f}s, per-file dedup "
              f"{min(per_in):.0f}-{max(per_in):.0f}s "
              f"(solo cost is {solo:.0f}s — the rest is GIL queueing)\n")

        # 3. the fix: N threads, each scan in its own worker process
        print(f"3. POOL x{N_MINERS} threads — the deploy candidate:")
        wall_po, per_po, res_po = run_concurrent(
            "pool", paths, lambda p: v._pooled_url_scan(p, cap, BATCH, 600))
        print(f"   -> wall={wall_po:.1f}s, per-file dedup "
              f"{min(per_po):.0f}-{max(per_po):.0f}s\n")

        # counts must be identical across all three paths
        for i in range(N_MINERS):
            a, b = res_in[i][0], res_po[i][0]
            assert (a["rows"], a["dups"]) == (b["rows"], b["dups"]), (i, a, b)
        assert (ref["rows"], ref["dups"]) == \
               (res_in[0][0]["rows"], res_in[0][0]["dups"])
        print("counts: inline == pool == solo on every file  OK")

        print("\n" + "=" * 68)
        print(f"{'':24}per-file dedup=      batch wall")
        print(f"  today  (inline x{N_MINERS}):  "
              f"{min(per_in):5.0f}-{max(per_in):.0f}s        {wall_in:6.1f}s")
        print(f"  deploy (pool   x{N_MINERS}):  "
              f"{min(per_po):5.0f}-{max(per_po):.0f}s        {wall_po:6.1f}s")
        print(f"  speedup: {wall_in / wall_po:.1f}x wall  |  "
              f"solo cost per file was {solo:.0f}s — pool keeps it, "
              f"inline multiplies it by ~{max(per_in) / solo:.1f}")
        print("=" * 68)
        print("\nThis is the same physics as production `dedup=` log lines:\n"
              f"  before deploy: dedup={min(per_in):.0f}-{max(per_in):.0f}s "
              f"per 2M-row file while {N_MINERS} evals run\n"
              f"  after  deploy: dedup={min(per_po):.0f}-{max(per_po):.0f}s "
              "on the same class of files")


if __name__ == "__main__":
    main()
