"""Storage transport failures on the OD download are neutral, not miner failures.

`_validate_od_submission` downloads the miner's upload from a presigned URL.
A body cut off mid-stream (`httpx.RemoteProtocolError`) and HTTP 429 come from
storage, not from the miner, so they are neutral like 5xx and timeouts. The
bulk path treats the same failures as validator-side since #928. A 404 or an
unparseable upload is still the miner's.

A real httpx client talks to a local HTTP server; only the job's URL is fake.
"""

import asyncio
import datetime as dt
import http.server
import json
import threading
import unittest
from unittest import mock

from common.api_client import OnDemandJob, OnDemandJobSubmission
from vali_utils.miner_evaluator import MinerEvaluator


class _Storage:
    """Local stand-in for the object store behind the presigned URL."""

    def __init__(self, body: bytes, behaviour):
        outer = self
        self.hits = 0

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                outer.hits += 1
                if isinstance(behaviour, int):
                    self.send_response(behaviour)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if behaviour == "garbage":
                    self.wfile.write(b"not json" + b" " * (len(body) - 8))
                    return
                if behaviour == "cut":
                    self.wfile.write(body[: len(body) // 2])
                    self.wfile.flush()
                    self.connection.shutdown(2)
                else:
                    self.wfile.write(body)

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.httpd.server_port}/upload.json"

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def _job() -> OnDemandJob:
    now = dt.datetime.now(dt.timezone.utc)
    return OnDemandJob(
        id="job-x",
        created_at=now - dt.timedelta(minutes=30),
        expire_at=now - dt.timedelta(minutes=28),
        job={"platform": "x", "keywords": ["bittensor"]},
        limit=100,
    )


class OdDownloadTransportNeutralTest(unittest.TestCase):
    # A well-formed upload; large enough that half of it is a real partial body.
    BODY = json.dumps({"data_entities": [], "pad": "x" * 4096}).encode()

    def _validate(self, behaviour):
        storage = _Storage(self.BODY, behaviour)
        self.addCleanup(storage.close)
        ev = MinerEvaluator.__new__(MinerEvaluator)
        ev.on_demand_validator = mock.MagicMock()
        # "No data for this query" -> an empty answer would pass on the merits.
        ev.on_demand_validator.check_data_exists = mock.AsyncMock(return_value=False)
        submission = OnDemandJobSubmission(
            job_id="job-x", s3_presigned_url=storage.url, s3_content_length=len(self.BODY)
        )
        result = asyncio.run(ev._validate_od_submission(7, "hk7", _job(), submission, "job-x"))
        self.assertGreaterEqual(storage.hits, 1)
        return result

    def test_complete_download_is_judged_on_the_merits(self):
        self.assertEqual(self._validate("ok"), (True, 0))

    def test_storage_5xx_is_neutral(self):
        self.assertEqual(self._validate(503), (None, 0))

    def test_storage_429_is_neutral(self):
        self.assertEqual(self._validate(429), (None, 0))

    def test_body_cut_off_by_storage_is_neutral(self):
        self.assertEqual(self._validate("cut"), (None, 0))

    def test_missing_upload_still_fails(self):
        self.assertEqual(self._validate(404), (False, 0))

    def test_unparseable_upload_still_fails(self):
        self.assertEqual(self._validate("garbage"), (False, 0))


if __name__ == "__main__":
    unittest.main()
