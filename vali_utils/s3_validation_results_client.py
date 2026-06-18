"""Client for the data-universe-api /s3-validation/results endpoints.

UID 89 publishes per-miner S3 validation scores; all other validators fetch
those scores and apply them locally instead of running S3 validation themselves.
"""
import asyncio
import json
from dataclasses import dataclass
from typing import Dict

import bittensor as bt
import requests

from common.api_client import TaoSigner


MACROCOSMOS_VALIDATOR_UID = 89


@dataclass
class PublishedScore:
    miner_hotkey: str
    validator_hotkey: str
    validator_uid: int
    effective_size_bytes: float
    validation_passed: bool


class S3ValidationResultsClient:
    """HTTP client for /s3-validation/results on data-universe-api."""

    def __init__(self, wallet: bt.wallet, api_base_url: str, timeout: int = 30):
        self.api_base_url = api_base_url.rstrip("/")
        self._signer = TaoSigner(keypair=wallet.hotkey)
        self._timeout = timeout

    async def publish(self, scores: Dict[str, Dict]) -> int:
        """POST per-miner scores. Returns number of rows upserted.

        scores: {miner_hotkey: {"effective_size_bytes": float, "validation_passed": bool, ...}}
        """
        if not scores:
            return 0

        payload = {
            "results": [
                {"miner_hotkey": hk, "score": score}
                for hk, score in scores.items()
            ]
        }
        body = json.dumps(payload).encode()
        headers = self._signer.headers(body)
        headers["Content-Type"] = "application/json"

        loop = asyncio.get_event_loop()
        try:
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(
                    f"{self.api_base_url}/s3-validation/results",
                    data=body,
                    headers=headers,
                    timeout=self._timeout,
                ),
            )
        except Exception as e:
            bt.logging.error(f"publish S3 validation results exception: {e}")
            return 0

        if response.status_code != 200:
            bt.logging.warning(
                f"publish S3 validation results failed: {response.status_code} {response.text[:200]}"
            )
            return 0

        try:
            return int(response.json().get("upserted", 0))
        except Exception:
            return 0

    async def fetch_all(self) -> Dict[str, PublishedScore]:
        """GET all published scores. Returns {miner_hotkey: PublishedScore}."""
        body = b""
        headers = self._signer.headers(body)

        loop = asyncio.get_event_loop()
        try:
            response = await loop.run_in_executor(
                None,
                lambda: requests.get(
                    f"{self.api_base_url}/s3-validation/results",
                    headers=headers,
                    timeout=self._timeout,
                ),
            )
        except Exception as e:
            bt.logging.error(f"fetch S3 validation results exception: {e}")
            return {}

        if response.status_code != 200:
            bt.logging.warning(
                f"fetch S3 validation results failed: {response.status_code} {response.text[:200]}"
            )
            return {}

        try:
            data = response.json()
        except Exception as e:
            bt.logging.error(f"fetch S3 validation results: bad JSON {e}")
            return {}

        out: Dict[str, PublishedScore] = {}
        for entry in data.get("results", []) or []:
            score = entry.get("score") or {}
            out[entry["miner_hotkey"]] = PublishedScore(
                miner_hotkey=entry["miner_hotkey"],
                validator_hotkey=entry.get("validator_hotkey", ""),
                validator_uid=int(entry.get("validator_uid", -1)),
                effective_size_bytes=float(score.get("effective_size_bytes", 0.0)),
                validation_passed=bool(score.get("validation_passed", False)),
            )
        return out
