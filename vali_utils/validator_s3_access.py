import json
import requests
import bittensor as bt
from typing import Optional, Any, List
import xml.etree.ElementTree as ET
import urllib.parse

from common.api_client import TaoSigner


# The listing XML is parsed WITHOUT pinning the S3 namespace URI.
#
# `http://s3.amazonaws.com/doc/2006-03-01/` is an XML namespace identifier, not an
# endpoint, and S3-compatible backends (R2 included) emit it for compatibility —
# so hardcoding it works today. It is still the wrong thing to depend on: if a
# backend or proxy ever returns the listing under a different namespace, or with
# none at all, a namespace-pinned lookup silently matches ZERO <Contents>. The XML
# still parses, so the listing looks like a completed "miner has no files" result
# and the miner is FAILED for an infrastructure quirk. Matching on the local tag
# name removes that failure mode entirely.
def _local_name(tag: str) -> str:
    """'{http://...}Contents' -> 'Contents' (unqualified tags pass through)."""
    return tag.rsplit("}", 1)[-1]


def _findall(parent, name: str) -> List[Any]:
    """Every descendant with this local tag name, in any (or no) namespace."""
    return [el for el in parent.iter() if _local_name(el.tag) == name]


def _find(parent, name: str, recursive: bool = False):
    """First direct child (or descendant, if recursive) with this local name."""
    for el in (parent.iter() if recursive else parent):
        if _local_name(el.tag) == name:
            return el
    return None


class ValidatorS3Access:
    """S3 access for validators — lists miner files via presigned URLs."""

    def __init__(self, wallet: bt.Wallet, s3_auth_url: str, debug: bool = False):
        self.wallet = wallet
        self.s3_auth_url = s3_auth_url
        self._signer = TaoSigner(keypair=wallet.hotkey)

    async def _request_presigned_list_url(
        self, miner_hotkey: str, continuation_token: Optional[str] = None
    ) -> str:
        """Request a presigned list URL from /get-miner-list using Tao v2 auth.

        Retries transient failures; raises S3ValidationSkip if the URL cannot be
        obtained, because without it the listing would be incomplete.
        """
        from vali_utils.s3_utils import S3ValidationSkip, http_with_retry

        payload = {"miner_hotkey": miner_hotkey}
        if continuation_token:
            payload["continuation_token"] = continuation_token

        body = json.dumps(payload).encode()
        headers = self._signer.headers(body)
        headers["Content-Type"] = "application/json"

        response = await http_with_retry(
            lambda: requests.post(
                f"{self.s3_auth_url.rstrip('/')}/get-miner-list",
                data=body,
                headers=headers,
                timeout=30,
            ),
            what="get-miner-list",
        )

        list_url = response.json().get("list_url", "")
        if not list_url:
            raise S3ValidationSkip("get-miner-list returned no list_url")
        return list_url

    async def list_all_files_with_metadata(
        self, miner_hotkey: str
    ) -> List[dict[str, Any]]:
        """
        List ALL parquet files for a miner with metadata (size, last_modified).
        Uses pagination via presigned list URLs.

        Returns list of dicts: {'key': str, 'size': int, 'last_modified': str}
        """
        # Lazy import (keeps this leaf module free of the heavy s3_utils import
        # chain at load time and avoids any import cycle).
        from vali_utils.s3_utils import S3ValidationSkip, http_with_retry

        try:
            target_prefix = f"data/hotkey={miner_hotkey}/"

            all_files = []
            continuation_token = None
            page = 1
            max_pages = 200
            completed = False

            while page <= max_pages:
                # Each step retries internally and raises S3ValidationSkip when it
                # cannot succeed, so a mid-listing failure aborts the whole listing
                # instead of returning a PARTIAL file set (which would understate
                # coverage and crater the miner's S3 score).
                presigned_url = await self._request_presigned_list_url(
                    miner_hotkey, continuation_token
                )
                response = await http_with_retry(
                    lambda u=presigned_url: requests.get(u, timeout=60),
                    what=f"s3 list page {page}",
                )
                try:
                    root = ET.fromstring(response.text)
                except ET.ParseError as e:
                    raise S3ValidationSkip(f"s3 list page {page} returned unparseable XML: {e}")

                # An error page ("<html>502 Bad Gateway</html>") or an S3 <Error>
                # document parses as perfectly good XML — it just contains no
                # <Contents>. Without this check the page reads as a completed,
                # empty listing and the miner is failed for having "no files".
                if _local_name(root.tag) != "ListBucketResult":
                    raise S3ValidationSkip(
                        f"s3 list page {page} is not a listing "
                        f"(root element <{_local_name(root.tag)}>)"
                    )

                page_files = 0
                for content in _findall(root, "Contents"):
                    key_elem = _find(content, "Key")
                    size_elem = _find(content, "Size")
                    modified_elem = _find(content, "LastModified")

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

                is_trunc = _find(root, "IsTruncated", recursive=True)
                if is_trunc is None or str(is_trunc.text).lower() != "true":
                    completed = True  # reached the genuine end of the listing
                    break

                token_elem = _find(root, "NextContinuationToken", recursive=True)
                if token_elem is None or not token_elem.text:
                    completed = True
                    break

                continuation_token = token_elem.text
                page += 1

            if not completed:
                # Ran off max_pages while still truncated → incomplete listing.
                raise S3ValidationSkip(
                    f"file listing exceeded {max_pages} pages for {miner_hotkey} — incomplete"
                )

            bt.logging.info(
                f"S3 listing complete: {len(all_files)} files across {page} pages for {miner_hotkey}"
            )
            return all_files

        except S3ValidationSkip:
            # Incomplete listing (transient/infra) — propagate so the caller SKIPS
            # (neutral). NEVER fall through to returning a partial or empty set,
            # which would be scored as authoritative.
            raise
        except Exception as e:
            # Any other unexpected listing failure is also validator-side; treat it
            # as a skip rather than returning [] (which the caller reads as
            # "No files found" → a FAIL that drops the miner's score).
            bt.logging.error(f"Exception in list_all_files_with_metadata: {str(e)}")
            raise S3ValidationSkip(f"file listing failed for {miner_hotkey}: {e}")
