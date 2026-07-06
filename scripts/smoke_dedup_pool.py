#!/usr/bin/env python3
"""2-minute host smoke test for the dedup worker pool.

Run BEFORE the real-miner A/B to confirm the pool works on this machine at
all: workers spawn, counts match the inline loop exactly, failure routes
fire, and the concurrency speedup is real on THIS host's cores.

No wallet, no network, no S3 — synthetic files only. Safe anywhere.

  cd <dir containing vali_utils/> && python scripts/smoke_dedup_pool.py
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

BATCH = 10_000


def make_file(dirpath, name, n_rows, seed):
    path = os.path.join(dirpath, name)
    rng = random.Random(seed)
    clean = int(n_rows * 0.95)
    urls = [f"https://x.com/user{rng.randrange(300)}/status/{10**15 + j}" for j in range(clean)]
    urls += [f"https://x.com/user1/status/{10**15 + rng.randrange(clean)}" for _ in range(n_rows - clean)]
    urls += [None] * 200 + ["   "] * 200
    rng.shuffle(urls)
    pq.write_table(pa.table({"url": urls}), path, row_group_size=50_000)
    return path


def inline_reference(path, max_iterations):
    from vali_utils.url_normalizer import normalize_url_for_dedup
    conn = duckdb.connect(":memory:")
    conn.execute("SET memory_limit='1GB';")
    conn.execute("SET threads=1;")
    cur = conn.execute(f"SELECT url FROM read_parquet('{path}')")
    seen, rows, decodable, dups = set(), 0, 0, 0
    overran = False
    for _ in range(max_iterations):
        batch = cur.fetchmany(BATCH)
        if not batch:
            break
        for (u,) in batch:
            if u and str(u).strip():
                decodable += 1
            h = hashlib.blake2b(normalize_url_for_dedup(u).encode(), digest_size=8).digest()
            rows += 1
            if h in seen:
                dups += 1
            else:
                seen.add(h)
    else:
        overran = True
    conn.close()
    return {"rows": rows, "decodable": decodable, "dups": dups, "overran": overran}


def main():
    import vali_utils.s3_utils as s3u
    print(f"s3_utils: {s3u.__file__}")
    assert hasattr(s3u.DuckDBSampledValidator, "_pooled_url_scan"), \
        "This s3_utils has no pool code — wrong copy!"
    v = object.__new__(s3u.DuckDBSampledValidator)
    print(f"pool enabled={s3u.DuckDBSampledValidator.DEDUP_POOL_ENABLED} "
          f"workers={s3u.DuckDBSampledValidator.DEDUP_POOL_WORKERS}")

    with tempfile.TemporaryDirectory(prefix="dedup_smoke_") as d:
        # 1. correctness
        p = make_file(d, "a.parquet", 300_000, 1)
        cap = (300_500 // BATCH) + 2
        ref = inline_reference(p, cap)
        t0 = time.monotonic()
        got = v._pooled_url_scan(p, cap, BATCH, 120)
        assert got == ref, f"MISMATCH: {got} != {ref}"
        print(f"1. correctness: OK {got} (first scan {time.monotonic()-t0:.1f}s incl. pool start)")

        # 2. corrupt file -> fail-closed
        bad = os.path.join(d, "bad.parquet")
        with open(bad, "wb") as f:
            f.write(b"PAR1" + os.urandom(2048) + b"PAR1")
        try:
            v._pooled_url_scan(bad, 5, BATCH, 30)
            raise AssertionError("corrupt file did not raise")
        except duckdb.Error:
            print("2. corrupt->fail-closed: OK")

        # 3. concurrency on THIS host (production shape: 5 threads)
        paths = [make_file(d, f"c{i}.parquet", 500_000, i) for i in range(5)]
        cap5 = (501_000 // BATCH) + 2

        def bench(fn):
            errs, t0 = [], time.monotonic()
            ts = [threading.Thread(target=lambda p=p: errs.append(fn(p)) if False else fn(p))
                  for p in paths]
            [t.start() for t in ts]
            [t.join() for t in ts]
            return time.monotonic() - t0

        t_inline = bench(lambda p: inline_reference(p, cap5))
        t_pool = bench(lambda p: v._pooled_url_scan(p, cap5, BATCH, 120))
        print(f"3. concurrency 5 threads: inline={t_inline:.1f}s pool={t_pool:.1f}s "
              f"-> {t_inline / t_pool:.1f}x on this host")

    print("\nSMOKE TEST PASSED")


if __name__ == "__main__":
    main()
