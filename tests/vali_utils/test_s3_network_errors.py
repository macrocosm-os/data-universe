"""Validator-side network failures must never become a miner verdict.

Before this fix, a download that R2 cut mid-body (requests'
ChunkedEncodingError) was an OSError, and pyarrow.lib.ArrowIOError *is*
OSError, so it landed in the fail-closed page-decode handler: "Corrupt data
pages", hard_invalid, s3 credibility x0.7 for an honest miner. A listing page
that failed mid-pagination ended the listing early and scored the partial list.

The other half of the contract: the neutral path is reachable only through
transport failures the miner cannot produce, and it cannot freeze a score.
"""
import asyncio
import datetime as dt
import http.server
import os
import random
import tempfile
import threading
import time
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from tests.vali_utils.test_job_window_full_scan import _reddit_frame
from vali_utils.miner_evaluator import MinerEvaluator
from vali_utils.s3_utils import DuckDBSampledValidator, TransientDownloadError
from vali_utils.validator_s3_access import S3Inconclusive, ValidatorS3Access


class _Server:
    """Local HTTP server; each request pops the next behaviour from `plan`
    (the last one repeats): ok, break (half body, then close), slow, or an
    integer HTTP status."""

    def __init__(self, routes):
        self.routes = routes  # path -> (body bytes, [behaviours])
        self.hits = {}
        server = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                body, plan = server.routes[self.path]
                n = server.hits.get(self.path, 0)
                server.hits[self.path] = n + 1
                how = plan[min(n, len(plan) - 1)]
                if isinstance(how, int):
                    self.send_response(how)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if how == "break":
                    self.wfile.write(body[: len(body) // 2])
                    self.wfile.flush()
                    self.connection.shutdown(2)
                elif how == "slow":
                    for i in range(0, len(body), 512):
                        self.wfile.write(body[i:i + 512])
                        self.wfile.flush()
                        time.sleep(0.05)
                else:
                    self.wfile.write(body)

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def url(self, path):
        return f"http://127.0.0.1:{self.httpd.server_port}{path}"

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def _parquet_bytes(tmpdir, n=2000):
    path = os.path.join(tmpdir, "src.parquet")
    pq.write_table(pa.table({"url": [f"https://x.com/u/status/{i}" for i in range(n)]}), path)
    with open(path, "rb") as f:
        return f.read()


class DownloadRetryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.body = _parquet_bytes(self.tmp.name)
        self.v = object.__new__(DuckDBSampledValidator)
        self.v.LOCAL_VALIDATION_TEMP_DIR = self.tmp.name

    def _serve(self, plan):
        srv = _Server({"/f": (self.body, plan)})
        self.addCleanup(srv.close)
        return srv

    def _temp_files(self):
        return [f for f in os.listdir(self.tmp.name) if f != "src.parquet"]

    def test_broken_stream_is_retried_and_the_file_is_complete(self):
        srv = self._serve(["break", "ok"])
        path = self.v._download_to_temp(srv.url("/f"), len(self.body))
        with open(path, "rb") as f:
            self.assertEqual(f.read(), self.body)
        self.assertEqual(srv.hits["/f"], 2)
        self.assertEqual(pq.ParquetFile(path).metadata.num_rows, 2000)

    def test_storage_5xx_is_retried(self):
        srv = self._serve([503, "ok"])
        path = self.v._download_to_temp(srv.url("/f"), len(self.body))
        self.assertEqual(os.path.getsize(path), len(self.body))

    def test_persistent_network_failure_is_transient_not_a_decode_failure(self):
        srv = self._serve(["break"])
        with self.assertRaises(TransientDownloadError) as cm:
            self.v._download_to_temp(srv.url("/f"), len(self.body))
        self.assertEqual(srv.hits["/f"], DuckDBSampledValidator.DOWNLOAD_ATTEMPTS)
        # Must not be catchable by the fail-closed (duckdb.Error, ArrowIOError) handler.
        self.assertNotIsInstance(cm.exception, (OSError, pa.lib.ArrowIOError))
        self.assertEqual(self._temp_files(), [])

    def test_stalled_stream_is_abandoned_and_retried(self):
        self.v.DOWNLOAD_GRACE_SECS = 0.2
        self.v.MIN_DOWNLOAD_BYTES_PER_SEC = 10 * 1024 * 1024
        srv = self._serve(["slow", "ok"])
        path = self.v._download_to_temp(srv.url("/f"), len(self.body))
        self.assertEqual(os.path.getsize(path), len(self.body))
        self.assertEqual(srv.hits["/f"], 2)

    def test_missing_object_fails_closed_without_retry(self):
        # 4xx is about the miner's object (deleted after listing, forbidden), so it
        # must keep failing the miner instead of buying a neutral skip.
        srv = self._serve([404])
        with self.assertRaises(requests.HTTPError) as cm:
            self.v._download_to_temp(srv.url("/f"), len(self.body))
        self.assertIsInstance(cm.exception, OSError)  # -> fail-closed handler
        self.assertEqual(srv.hits["/f"], 1)


class SampledValidationVerdictTest(unittest.TestCase):
    """Drives the real per-file loop against real parquet served over HTTP."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.body = _parquet_bytes(self.tmp.name)
        cache = os.path.join(self.tmp.name, "cache")
        os.makedirs(cache)  # the class sweep that creates it is rate-limited
        patcher = mock.patch.object(
            DuckDBSampledValidator, "LOCAL_VALIDATION_TEMP_DIR", cache)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.v = object.__new__(DuckDBSampledValidator)
        self.v._rng = random.Random(1)
        self.v._local_files = {}
        self.v._cached_bytes = 0

    def _run(self, routes):
        srv = _Server(routes)
        self.addCleanup(srv.close)
        files, urls = [], {}
        for i, path in enumerate(routes):
            key = f"data/hotkey=hk/job_id=j{i}/data_20261030_000000_2000_{'a' * 16}.parquet"
            files.append({"key": key, "size": len(routes[path][0])})
            urls[key] = srv.url(path)
        return asyncio.run(self.v._sampled_duckdb_validation(
            files, {}, urls, miner_hotkey="hk"))

    def test_every_download_failing_is_inconclusive(self):
        with self.assertRaises(S3Inconclusive):
            self._run({"/a": (self.body, ["break"]), "/b": (self.body, ["break"])})

    def test_a_corrupt_file_still_fails_closed_next_to_a_network_failure(self):
        path = os.path.join(self.tmp.name, "reddit.parquet")
        now = dt.datetime(2026, 10, 30, tzinfo=dt.timezone.utc)
        _reddit_frame([now] * 12000).to_parquet(path, row_group_size=10_000)
        with open(path, "rb") as f:
            corrupt = bytearray(f.read())
        for i in range(len(corrupt) // 3, len(corrupt) // 3 + 64):
            corrupt[i] ^= 0xFF
        res = self._run({
            "/net": (self.body, ["break"]),
            "/bad": (bytes(corrupt), ["ok"]),
        })
        self.assertFalse(res["success"])
        self.assertTrue(res.get("hard_invalid"))
        self.assertIn("Corrupt data pages", res["reason"])


class ListingTest(unittest.TestCase):
    NS = "http://s3.amazonaws.com/doc/2006-03-01/"

    def _page(self, keys, token=None):
        items = "".join(
            f"<Contents><Key>data/hotkey=hk/job_id=j/{k}</Key><Size>100000</Size>"
            f"<LastModified>2026-10-30T00:00:00Z</LastModified></Contents>" for k in keys)
        trunc = f"<IsTruncated>true</IsTruncated><NextContinuationToken>{token}</NextContinuationToken>" \
            if token else "<IsTruncated>false</IsTruncated>"
        return f'<ListBucketResult xmlns="{self.NS}">{items}{trunc}</ListBucketResult>'.encode()

    def _access(self, page2_plan):
        srv = _Server({
            "/p1": (self._page(["a.parquet", "b.parquet"], token="t2"), ["ok"]),
            "/p2": (self._page(["c.parquet"]), page2_plan),
        })
        self.addCleanup(srv.close)
        access = object.__new__(ValidatorS3Access)

        async def presigned(hotkey, token=None):
            return srv.url("/p2" if token == "t2" else "/p1")

        access._request_presigned_list_url = presigned
        return access, srv

    def test_flaky_page_is_retried_and_the_listing_is_complete(self):
        access, srv = self._access([502, "ok"])
        files = asyncio.run(access.list_all_files_with_metadata("hk"))
        self.assertEqual(len(files), 3)
        self.assertEqual(srv.hits["/p2"], 2)

    def test_a_page_that_keeps_failing_raises_instead_of_truncating(self):
        access, _ = self._access([503])
        with self.assertRaises(S3Inconclusive):
            asyncio.run(access.list_all_files_with_metadata("hk"))


class InconclusiveStreakTest(unittest.TestCase):
    def setUp(self):
        self.ev = MinerEvaluator.__new__(MinerEvaluator)
        self.ev._s3_inconclusive = {}
        self.ev._last_s3_verdict = 0.0

    def test_validator_wide_outage_never_fails_anyone(self):
        for _ in range(10):
            self.assertTrue(self.ev._s3_no_verdict("hk", S3Inconclusive("outage")))

    def test_miner_specific_inconclusive_streak_is_failed(self):
        err = S3Inconclusive("x")
        self.assertTrue(self.ev._s3_no_verdict("hk", err))
        self.ev._last_s3_verdict = time.monotonic() + 1  # others validated since
        for _ in range(MinerEvaluator.MAX_S3_INCONCLUSIVE_STREAK - 2):
            self.assertTrue(self.ev._s3_no_verdict("hk", err))
        self.assertFalse(self.ev._s3_no_verdict("hk", err))
        # The streak starts over after the forced verdict.
        self.assertTrue(self.ev._s3_no_verdict("hk", err))


if __name__ == "__main__":
    unittest.main()
