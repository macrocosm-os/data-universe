"""Dedup scan worker — runs as a dedicated child PROCESS, not an eval thread.

The per-row normalize+blake2b loop is pure Python and therefore GIL-bound:
N eval threads running it concurrently serialize on one core (measured:
5 threads = 0.8x of sequential — slower than no concurrency at all — while
5 processes = 3x+).

Run as `python -m vali_utils.dedup_worker`: a JSON-lines server on
stdin/stdout — one request line in, one result line out (see main()).
Dedicated subprocesses instead of ProcessPoolExecutor because spawn-context
pool workers re-import the parent's __main__ (neurons/validator.py: 2.8s +
363MB EACH, measured); this module imports in 0.03s / 24MB. And a wedged or
crashed worker can simply be SIGKILLed and respawned by the parent-side pool
(vali_utils/dedup_pool.py) — ProcessPoolExecutor tasks are unkillable.

HARD RULE: import only stdlib + duckdb + url_normalizer here.

Semantics are byte-identical to the inline loop this replaces
(s3_utils._sampled_duckdb_validation): same SELECT, same fetchmany batch
size, same decodable gate, same normalize + blake2b(8) + per-file set, same
iteration-cap overrun detection, and the same per-file DuckDB watchdog
(conn.interrupt after timeout) — it just runs inside the child, so a timeout
or a native parser crash takes down one disposable worker, not the validator.
"""
import hashlib
import json
import sys
import threading

import duckdb

from vali_utils.url_normalizer import normalize_url_for_dedup


def scan_urls(path: str, max_iterations: int, batch_size: int,
              timeout_secs: float) -> dict:
    """Stream the url column of a LOCAL parquet file and count duplicates.

    Returns {"rows", "decodable", "dups", "overran"} on a completed scan
    (overran=True means the cursor outlived the footer's row count — the
    caller fails the file, exactly like the inline path).

    On failure returns {"error_kind": ..., "error_msg": ...} instead:
      "interrupt" — the in-child watchdog fired (scan exceeded timeout_secs)
      "duckdb"    — DuckDB raised (corrupt pages etc.)
      "other"     — unexpected Python error
    The parent re-raises the matching exception type so the existing per-file
    routing (transient-skip / page-decode fail-closed / generic-skip) is
    preserved unchanged.
    """
    conn = None
    watchdog = None
    interrupted = threading.Event()
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("SET memory_limit='1GB';")
        conn.execute("SET threads=1;")

        # Same per-file watchdog as the parent used to arm: bound this scan
        # to timeout_secs. Depending on where the query is when interrupt()
        # lands, DuckDB surfaces it as InterruptException OR another
        # duckdb.Error (e.g. InvalidInputException on a poisoned pending
        # result) — the `interrupted` flag keeps classification faithful.
        def _fire():
            interrupted.set()
            conn.interrupt()

        watchdog = threading.Timer(max(1.0, timeout_secs), _fire)
        watchdog.daemon = True
        watchdog.start()

        file_hashes: set = set()
        rows = decodable = dups = 0
        cur = conn.execute(f"SELECT url FROM read_parquet('{path}')")
        overran = False
        for _ in range(max_iterations):
            batch = cur.fetchmany(batch_size)
            if not batch:
                break
            for (url_val,) in batch:
                if url_val and str(url_val).strip():
                    decodable += 1
                normalized = normalize_url_for_dedup(url_val)
                h = hashlib.blake2b(normalized.encode(), digest_size=8).digest()
                rows += 1
                if h in file_hashes:
                    dups += 1
                else:
                    file_hashes.add(h)
            del batch
        else:
            # Hit the iteration cap without EOF — cursor returned more rows
            # than the footer claimed. Malformed/adversarial.
            overran = True
        return {"rows": rows, "decodable": decodable, "dups": dups,
                "overran": overran}
    except duckdb.InterruptException as e:
        return {"error_kind": "interrupt", "error_msg": str(e)}
    except duckdb.Error as e:
        if interrupted.is_set():
            return {"error_kind": "interrupt",
                    "error_msg": f"watchdog interrupt ({type(e).__name__}: {e})"}
        # Carry the exact DuckDB subclass name so the parent can re-raise the
        # SAME type — the per-file handler routes IOException/HTTPException/
        # ConnectionException to fail-open-skip but every other duckdb.Error
        # to page-decode-fail-closed, and IOException IS a duckdb.Error
        # subclass, so flattening would flip an honest local-disk IO error
        # from skip to fail.
        return {"error_kind": "duckdb", "error_type": type(e).__name__,
                "error_msg": f"{type(e).__name__}: {e}"}
    except Exception as e:  # noqa: BLE001 — must never leak raw across the pool
        return {"error_kind": "other", "error_msg": f"{type(e).__name__}: {e}"}
    finally:
        if watchdog is not None:
            watchdog.cancel()
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def main():
    """JSON-lines server loop: read one request per line from stdin, write
    exactly one result line to stdout. Exits on stdin EOF (parent closed the
    pipe or died). Any malformed request produces an error result rather than
    killing the worker."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            res = scan_urls(
                req["path"], int(req["max_iterations"]),
                int(req["batch_size"]), float(req["timeout_secs"]),
            )
        except Exception as e:  # noqa: BLE001 — the worker must never die on bad input
            res = {"error_kind": "other", "error_msg": f"{type(e).__name__}: {e}"}
        sys.stdout.write(json.dumps(res) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
