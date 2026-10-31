import json
import requests
import bittensor as bt
from typing import Optional, Any, List
import xml.etree.ElementTree as ET
import urllib.parse
import asyncio

from common.api_client import TaoSigner


class S3Inconclusive(Exception):
    """Validator-side S3 failure (listing pages, file downloads) that no miner
    can cause. The validation has no verdict: the miner's score is left as is
    and it is re-validated on its next evaluation."""


LIST_PAGE_ATTEMPTS = 3


class ValidatorS3Access:
    """S3 access for validators — lists miner files via presigned URLs."""

    def __init__(self, wallet: bt.Wallet, s3_auth_url: str, debug: bool = False):
        self.wallet = wallet
        self.s3_auth_url = s3_auth_url
        self._signer = TaoSigner(keypair=wallet.hotkey)

    async def _request_presigned_list_url(
        self, miner_hotkey: str, continuation_token: Optional[str] = None
    ) -> Optional[str]:
        """Request presigned list URL from /get-miner-list using Tao v2 auth."""
        try:
            payload = {"miner_hotkey": miner_hotkey}
            if continuation_token:
                payload["continuation_token"] = continuation_token

            body = json.dumps(payload).encode()
            headers = self._signer.headers(body)
            headers["Content-Type"] = "application/json"

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(
                    f"{self.s3_auth_url.rstrip('/')}/get-miner-list",
                    data=body,
                    headers=headers,
                    timeout=30,
                ),
            )

            if response.status_code != 200:
                bt.logging.warning(
                    f"get-miner-list failed: {response.status_code}"
                )
                return None

            data = response.json()
            return data.get("list_url", "")

        except Exception as e:
            bt.logging.error(f"get-miner-list exception: {e}")
            return None

    async def list_all_files_with_metadata(
        self, miner_hotkey: str
    ) -> List[dict[str, Any]]:
        """
        List ALL parquet files for a miner with metadata (size, last_modified).
        Uses pagination via presigned list URLs.

        Returns list of dicts: {'key': str, 'size': int, 'last_modified': str}
        """
        try:
            target_prefix = f"data/hotkey={miner_hotkey}/"

            all_files = []
            continuation_token = None
            page = 1
            max_pages = 200

            while page <= max_pages:
                root = await self._fetch_list_page(miner_hotkey, continuation_token)

                namespaces = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

                page_files = 0
                for content in root.findall(".//s3:Contents", namespaces):
                    key_elem = content.find("s3:Key", namespaces)
                    size_elem = content.find("s3:Size", namespaces)
                    modified_elem = content.find("s3:LastModified", namespaces)

                    if key_elem is not None and key_elem.text:
                        decoded_key = urllib.parse.unquote(key_elem.text)

                        if decoded_key.startswith(
                            target_prefix
                        ) and decoded_key.endswith(".parquet"):
                            all_files.append(
                                {
                                    "key": decoded_key,
                                    "size": int(size_elem.text)
                                    if size_elem is not None and size_elem.text
                                    else 0,
                                    "last_modified": modified_elem.text
                                    if modified_elem is not None
                                    else "",
                                }
                            )
                            page_files += 1

                bt.logging.debug(
                    f"S3 list page {page}: {page_files} files (total: {len(all_files)}) for {miner_hotkey}"
                )

                is_trunc = root.find(".//s3:IsTruncated", namespaces)
                if is_trunc is None or str(is_trunc.text).lower() != "true":
                    break

                token_elem = root.find(".//s3:NextContinuationToken", namespaces)
                if token_elem is None or not token_elem.text:
                    raise S3Inconclusive(
                        f"listing page {page} is truncated but has no continuation token"
                    )

                continuation_token = token_elem.text
                page += 1

            bt.logging.info(
                f"S3 listing complete: {len(all_files)} files across {page} pages for {miner_hotkey}"
            )
            return all_files

        except S3Inconclusive:
            raise
        except Exception as e:
            # Every byte here comes from the API or R2, never from the miner, so an
            # error is ours: no verdict rather than an empty (failing) listing.
            raise S3Inconclusive(f"S3 listing error: {e}") from e

    async def _fetch_list_page(
        self, miner_hotkey: str, continuation_token: Optional[str]
    ) -> ET.Element:
        """One parsed listing page, retried on a fresh presigned URL.

        A page that still fails raises S3Inconclusive instead of ending the
        listing early: a partial file list would score as missing jobs (coverage²)
        or as "No files found", penalising the miner for our outage."""
        loop = asyncio.get_event_loop()
        last_error = None
        for attempt in range(LIST_PAGE_ATTEMPTS):
            if attempt:
                await asyncio.sleep(2 * attempt)
            presigned_url = await self._request_presigned_list_url(
                miner_hotkey, continuation_token
            )
            if not presigned_url:
                last_error = "no presigned list URL"
                continue
            try:
                response = await loop.run_in_executor(
                    None, lambda: requests.get(presigned_url, timeout=60)
                )
                if response.status_code == 200:
                    return ET.fromstring(response.text)
                last_error = f"HTTP {response.status_code}"
            except (requests.RequestException, ET.ParseError) as e:
                last_error = f"{type(e).__name__}: {e}"
        raise S3Inconclusive(
            f"S3 listing failed after {LIST_PAGE_ATTEMPTS} attempts ({last_error})"
        )
