import json
import requests
import bittensor as bt
from typing import Optional, Any, List
import xml.etree.ElementTree as ET
import urllib.parse
import asyncio

from common.api_client import TaoSigner


class ValidatorS3Access:
    """S3 access for validators — lists miner files via presigned URLs."""

    def __init__(self, wallet: bt.wallet, s3_auth_url: str, debug: bool = False):
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
                    f"get-miner-list failed: {response.status_code} {response.text[:200]}"
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

            loop = asyncio.get_event_loop()

            while page <= max_pages:
                presigned_url = await self._request_presigned_list_url(
                    miner_hotkey, continuation_token
                )
                if not presigned_url:
                    break

                response = await loop.run_in_executor(
                    None, lambda: requests.get(presigned_url, timeout=60)
                )
                if response.status_code != 200:
                    break

                try:
                    root = ET.fromstring(response.text)
                except Exception:
                    break

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
                    break

                continuation_token = token_elem.text
                page += 1

            bt.logging.info(
                f"S3 listing complete: {len(all_files)} files across {page} pages for {miner_hotkey}"
            )
            return all_files

        except Exception as e:
            bt.logging.error(f"Exception in list_all_files_with_metadata: {str(e)}")
            return []
