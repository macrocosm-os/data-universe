"""Tests for S3ValidationResultsClient against a local in-process HTTP server."""
import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import MagicMock

import pytest

from vali_utils.s3_validation_results_client import (
    MACROCOSMOS_VALIDATOR_UID,
    S3ValidationResultsClient,
)


class _Handler(BaseHTTPRequestHandler):
    received_post: dict = None
    served_results: list = []

    def log_message(self, *args, **kwargs):
        pass

    def _read_body(self):
        n = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(n) if n else b""

    def do_POST(self):
        if self.path == "/s3-validation/results":
            body = self._read_body()
            _Handler.received_post = json.loads(body.decode())
            payload = json.dumps(
                {"upserted": len(_Handler.received_post.get("results", []))}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path == "/s3-validation/results":
            payload = json.dumps(
                {
                    "validator_hotkey": "5VALI",
                    "results": _Handler.served_results,
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_response(404)
            self.end_headers()


@pytest.fixture
def server():
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()
    _Handler.received_post = None
    _Handler.served_results = []


@pytest.fixture
def fake_wallet():
    # Create a real Keypair via substrateinterface so TaoSigner.headers() can sign.
    from substrateinterface.keypair import Keypair

    kp = Keypair.create_from_mnemonic(Keypair.generate_mnemonic())
    wallet = MagicMock()
    wallet.hotkey = kp
    return wallet


def test_publish_sends_signed_payload(server, fake_wallet):
    client = S3ValidationResultsClient(wallet=fake_wallet, api_base_url=server)
    n = asyncio.run(
        client.publish(
            {
                "5MINER_A": {"effective_size_bytes": 12345.0, "validation_passed": True},
                "5MINER_B": {"effective_size_bytes": 0.0, "validation_passed": False},
            }
        )
    )
    assert n == 2
    assert _Handler.received_post is not None
    by_hotkey = {r["miner_hotkey"]: r["score"] for r in _Handler.received_post["results"]}
    assert by_hotkey["5MINER_A"]["effective_size_bytes"] == 12345.0
    assert by_hotkey["5MINER_A"]["validation_passed"] is True
    assert by_hotkey["5MINER_B"]["validation_passed"] is False


def test_publish_empty_short_circuits(server, fake_wallet):
    client = S3ValidationResultsClient(wallet=fake_wallet, api_base_url=server)
    n = asyncio.run(client.publish({}))
    assert n == 0
    assert _Handler.received_post is None


def test_fetch_all_parses_response(server, fake_wallet):
    _Handler.served_results = [
        {
            "miner_hotkey": "5MINER_A",
            "validator_hotkey": "5VALI",
            "validator_uid": MACROCOSMOS_VALIDATOR_UID,
            "score": {"effective_size_bytes": 999.0, "validation_passed": True},
            "updated_at": "2026-06-17T00:00:00Z",
        },
        {
            "miner_hotkey": "5MINER_B",
            "validator_hotkey": "5VALI",
            "validator_uid": MACROCOSMOS_VALIDATOR_UID,
            "score": {"effective_size_bytes": 0.0, "validation_passed": False},
            "updated_at": "2026-06-17T00:00:00Z",
        },
    ]
    client = S3ValidationResultsClient(wallet=fake_wallet, api_base_url=server)
    out = asyncio.run(client.fetch_all())
    assert set(out) == {"5MINER_A", "5MINER_B"}
    assert out["5MINER_A"].effective_size_bytes == 999.0
    assert out["5MINER_A"].validation_passed is True
    assert out["5MINER_A"].validator_uid == MACROCOSMOS_VALIDATOR_UID
    assert out["5MINER_B"].validation_passed is False


def test_fetch_all_empty_returns_empty_dict(server, fake_wallet):
    client = S3ValidationResultsClient(wallet=fake_wallet, api_base_url=server)
    out = asyncio.run(client.fetch_all())
    assert out == {}
