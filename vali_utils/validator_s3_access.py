import requests
import time
import datetime as dt
import threading
import bittensor as bt
from typing import Dict, Optional, Any, List
import os
import xml.etree.ElementTree as ET
import urllib.parse
import asyncio


class ValidatorS3Access:
    """Clean S3 access for job-based validation - data/hotkey={hotkey}/job_id={job_id}/"""

    def __init__(self, wallet: bt.wallet, s3_auth_url: str, debug: bool = False):
        self.wallet = wallet
        self.s3_auth_url = s3_auth_url
        self.access_data = None
        self.expiry_time = 0
        self.lock = threading.RLock()
        self.debug = debug
        self._cached_all_files = None
        self._cache_expiry = 0

    def _debug_print(self, message: str):
        """Print debug message if debug mode is enabled"""
        if self.debug:
            print(f"DEBUG S3: {message}")

    async def _request_presigned_list_url(self, miner_hotkey: str, continuation_token: Optional[str] = None) -> Optional[str]:
        """Request fresh presigned URL from /get-miner-list endpoint for specific miner"""
        try:
            hotkey = self.wallet.hotkey.ss58_address
            timestamp = int(time.time())
            commitment = f"s3:validator:miner:{miner_hotkey}:{timestamp}"
            signature = self.wallet.hotkey.sign(commitment.encode()).hex()

            payload = {
                "hotkey": hotkey,
                "timestamp": timestamp,
                "signature": signature,
                "miner_hotkey": miner_hotkey,
            }

            if continuation_token:
                payload["continuation_token"] = continuation_token

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(
                    f"{self.s3_auth_url.rstrip('/')}/get-miner-list",
                    json=payload,
                    timeout=30
                )
            )

            if response.status_code != 200:
                self._debug_print(f"get-miner-list failed: {response.status_code}")
                return None

            data = response.json()
            return data.get("list_url", "")

        except Exception as e:
            self._debug_print(f"get-miner-list exception: {e}")
            return None

    async def ensure_access(self) -> bool:
        """Ensure valid S3 access is available, refreshing if needed"""
        with self.lock:
            current_time = time.time()
            # Check if credentials are still valid (with 1 hour buffer)
            if self.access_data and current_time < self.expiry_time - 3600:
                self._debug_print("Using cached S3 access")
                return True

            # Get new access
            self._debug_print("Getting new S3 access from auth server")
            access_data = await self.get_validator_access()

            if not access_data:
                self._debug_print("Failed to get S3 access from auth server")
                return False

            self.access_data = access_data
            self._debug_print(f"Got S3 access data with keys: {list(access_data.keys())}")

            # Set expiry time based on the returned expiry
            if 'expiry_seconds' in access_data:
                self.expiry_time = current_time + access_data['expiry_seconds'] - 600  # 10 minute buffer
            else:
                # Parse ISO format expiry string if available
                try:
                    expiry_str = access_data.get('expiry')
                    if expiry_str:
                        expiry_dt = dt.datetime.fromisoformat(expiry_str)
                        self.expiry_time = expiry_dt.timestamp()
                    else:
                        self.expiry_time = current_time + 23 * 3600  # Default 23 hours
                except Exception:
                    self.expiry_time = current_time + 23 * 3600  # Default 23 hours

            return True

    async def get_validator_access(self) -> Optional[Dict[str, Any]]:
        """Get S3 access using validator signature authentication"""
        try:
            coldkey = self.wallet.get_coldkeypub().ss58_address
            hotkey = self.wallet.hotkey.ss58_address
            timestamp = int(time.time())

            self._debug_print(f"Requesting S3 access for validator {hotkey}")

            # Create commitment string
            commitment = f"s3:validator:access:{timestamp}"

            # Sign the commitment with hotkey
            signature = self.wallet.hotkey.sign(commitment.encode())
            signature_hex = signature.hex()

            # Create request payload
            payload = {
                "hotkey": hotkey,
                "timestamp": timestamp,
                "signature": signature_hex
            }

            self._debug_print(f"Sending request to: {self.s3_auth_url}/get-validator-access")

            # Use asyncio for HTTP request
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(
                    f"{self.s3_auth_url.rstrip('/')}/get-validator-access",
                    json=payload,
                    timeout=30
                )
            )

            self._debug_print(f"Auth server response status: {response.status_code}")

            if response.status_code != 200:
                self._debug_print(f"Auth server error: {response.text}")
                return None

            result = response.json()
            self._debug_print(f"Auth server response structure: {list(result.keys())}")

            return result

        except Exception as e:
            self._debug_print(f"Exception getting validator access: {str(e)}")
            bt.logging.error(f"S3 validator access error: {str(e)}")
            return None

    async def get_miner_specific_access(self, miner_hotkey: str) -> str:
        """Get presigned URL for specific miner's data"""
        try:
            hotkey = self.wallet.hotkey.ss58_address
            timestamp = int(time.time())

            # Create commitment for miner-specific access
            commitment = f"s3:validator:miner:{miner_hotkey}:{timestamp}"
            signature = self.wallet.hotkey.sign(commitment.encode())
            signature_hex = signature.hex()

            payload = {
                "hotkey": hotkey,
                "timestamp": timestamp,
                "signature": signature_hex,
                "miner_hotkey": miner_hotkey
            }

            self._debug_print(f"Getting miner-specific access for {miner_hotkey}")

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.post(
                    f"{self.s3_auth_url.rstrip('/')}/get-miner-specific-access",
                    json=payload,
                    timeout=30
                )
            )

            if response.status_code == 200:
                result = response.json()
                return result.get('miner_url', '')
            else:
                self._debug_print(f"Miner-specific access failed: {response.status_code}")
                return ""

        except Exception as e:
            self._debug_print(f"Exception getting miner-specific access: {str(e)}")
            return ""

    async def list_jobs_direct(self, miner_hotkey: str) -> List[str]:
        """List jobs for specific miner with pagination support"""
        try:
            target_prefix = f"data/hotkey={miner_hotkey}/"
            self._debug_print(f"Listing jobs for miner with pagination: {miner_hotkey}")

            jobs = set()
            continuation_token = None
            page = 1
            max_pages = 200

            loop = asyncio.get_event_loop()

            while page <= max_pages:
                presigned_url = await self._request_presigned_list_url(miner_hotkey, continuation_token)
                if not presigned_url:
                    break

                response = await loop.run_in_executor(None, lambda: requests.get(presigned_url, timeout=60))
                if response.status_code != 200:
                    break

                try:
                    root = ET.fromstring(response.text)
                except:
                    break

                namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

                for content in root.findall('.//s3:Contents', namespaces):
                    key_elem = content.find('s3:Key', namespaces)
                    if key_elem is not None and key_elem.text:
                        decoded_key = urllib.parse.unquote(key_elem.text)
                        if decoded_key.startswith(target_prefix) and '/job_id=' in decoded_key:
                            job_part_full = decoded_key[len(target_prefix):]
                            if job_part_full.startswith('job_id='):
                                jobs.add(job_part_full.split('/')[0][7:])

                is_trunc = root.find('.//s3:IsTruncated', namespaces)
                if is_trunc is None or str(is_trunc.text).lower() != 'true':
                    break

                token_elem = root.find('.//s3:NextContinuationToken', namespaces)
                if token_elem is None or not token_elem.text:
                    break

                continuation_token = token_elem.text
                page += 1

            self._debug_print(f"Found {len(jobs)} jobs across {page} pages")
            return list(jobs)

        except Exception as e:
            self._debug_print(f"Exception in list_jobs_direct: {str(e)}")
            return []

    async def _get_all_s3_data(self) -> List[str]:
        """Get ALL S3 data with proper pagination support"""
        current_time = time.time()

        # Check if cache is still valid (10 minutes)
        if (self._cached_all_files and
                current_time < self._cache_expiry):
            self._debug_print("Using cached S3 file list")
            return self._cached_all_files

        if not await self.ensure_access():
            return []

        try:
            all_files = []
            continuation_token = None
            page = 1
            max_pages = 200  # Safety limit

            self._debug_print("Starting to collect all S3 files with pagination...")

            loop = asyncio.get_event_loop()

            while page <= max_pages:
                # Request fresh presigned URL for this page
                presigned_url = await self._request_presigned_list_url(continuation_token)

                if not presigned_url:
                    self._debug_print(f"Failed to get presigned URL for page {page}")
                    break

                # Fetch this page
                response = await loop.run_in_executor(None, lambda: requests.get(presigned_url, timeout=60))

                if response.status_code != 200:
                    self._debug_print(f"Page {page} failed: {response.status_code}")
                    break

                # Parse XML
                try:
                    root = ET.fromstring(response.text)
                except Exception as e:
                    self._debug_print(f"XML parse error on page {page}: {e}")
                    break

                namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

                # Collect files from this page
                page_files = 0
                for content in root.findall('.//s3:Contents', namespaces):
                    key_elem = content.find('s3:Key', namespaces)
                    if key_elem is not None and key_elem.text:
                        decoded = urllib.parse.unquote(key_elem.text)
                        all_files.append(decoded)
                        page_files += 1

                self._debug_print(f"Page {page}: collected {page_files} files (total: {len(all_files)})")

                # Check if more pages exist
                is_trunc = root.find('.//s3:IsTruncated', namespaces)
                if is_trunc is None or str(is_trunc.text).lower() != 'true':
                    self._debug_print("No more pages")
                    break

                # Get continuation token for next page
                token_elem = root.find('.//s3:NextContinuationToken', namespaces)
                if token_elem is None or not token_elem.text:
                    self._debug_print("IsTruncated=true but no NextContinuationToken")
                    break

                continuation_token = token_elem.text
                page += 1

            self._debug_print(f"Total files collected across {page} pages: {len(all_files)}")

            # Cache the results for 10 minutes
            self._cached_all_files = all_files
            self._cache_expiry = current_time + 600  # 10 minutes

            return all_files

        except Exception as e:
            self._debug_print(f"Exception getting all S3 data: {str(e)}")
            return []

    async def _list_jobs_cached(self, miner_hotkey: str) -> List[str]:
        """Original cached method - renamed"""
        try:
            target_prefix = f"data/hotkey={miner_hotkey}/"
            self._debug_print(f"Looking for jobs with prefix: {target_prefix}")

            # Get all S3 data (cached if available)
            all_files = await self._get_all_s3_data()

            if not all_files:
                self._debug_print("No S3 data available")
                return []

            jobs = set()
            files_found = 0

            # Filter for our specific miner
            for file_path in all_files:
                if file_path.startswith(target_prefix) and '/job_id=' in file_path:
                    files_found += 1
                    # Extract job_id from path: data/hotkey=XXXX/job_id=YYYY/file
                    job_part_full = file_path[len(target_prefix):]  # Remove prefix
                    if job_part_full.startswith('job_id='):
                        job_part = job_part_full.split('/')[0][7:]  # Remove 'job_id=' and get first part
                        jobs.add(job_part)

            self._debug_print(f"Found {files_found} files for {miner_hotkey}")
            jobs_list = list(jobs)
            self._debug_print(f"Extracted {len(jobs_list)} unique jobs: {jobs_list}")
            return jobs_list

        except Exception as e:
            self._debug_print(f"Exception in _list_jobs_cached: {str(e)}")
            bt.logging.error(f"S3 list jobs error for {miner_hotkey}: {str(e)}")
            return []

    async def list_jobs(self, miner_hotkey: str) -> List[str]:
        """List jobs - try direct method first, fallback to cached"""
        # Try direct method first
        try:
            jobs = await self.list_jobs_direct(miner_hotkey)
            if jobs:
                self._debug_print(f"Direct method found {len(jobs)} jobs for {miner_hotkey}")
                return jobs
        except Exception as e:
            self._debug_print(f"Direct method failed for {miner_hotkey}: {str(e)}")

        # Fallback to cached method (for miners in first 1000)
        self._debug_print(f"Using cached method for {miner_hotkey}")
        return await self._list_jobs_cached(miner_hotkey)

    async def list_files(self, miner_hotkey: str, job_id: str) -> List[Dict[str, Any]]:
        """List files for a specific miner and job using cached S3 data"""
        try:
            target_prefix = f"data/hotkey={miner_hotkey}/job_id={job_id}/"
            self._debug_print(f"Looking for files with prefix: {target_prefix}")

            # Get all S3 data (cached if available)
            all_files = await self._get_all_s3_data()

            if not all_files:
                return []

            # We need file metadata (size, last modified), so we still need to make an S3 API call
            # But we can optimize by checking if files exist first
            matching_files = [f for f in all_files if f.startswith(target_prefix)]

            if not matching_files:
                self._debug_print(f"No files found with prefix {target_prefix}")
                return []

            self._debug_print(f"Found {len(matching_files)} matching files, getting metadata...")

            # Get file metadata from S3 API
            if not await self.ensure_access():
                return []

            urls = self.access_data.get('urls', {})
            global_urls = urls.get('global', {})
            list_url = global_urls.get('list_all_data', '')

            if not list_url:
                return []

            # Make API call to get metadata
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, lambda: requests.get(list_url))

            if response.status_code != 200:
                self._debug_print(f"Failed files response: {response.status_code}")
                return []

            root = ET.fromstring(response.text)
            namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            files = []

            # Extract metadata for matching files
            for content in root.findall('.//s3:Contents', namespaces):
                key = content.find('s3:Key', namespaces).text
                size = content.find('s3:Size', namespaces).text
                last_modified = content.find('s3:LastModified', namespaces).text

                if key:
                    decoded_key = urllib.parse.unquote(key)
                    if decoded_key.startswith(target_prefix):
                        files.append({
                            'Key': decoded_key,
                            'Size': int(size) if size else 0,
                            'LastModified': last_modified
                        })
                        self._debug_print(f"Found file: {os.path.basename(decoded_key)}")

            self._debug_print(f"Found {len(files)} files in {miner_hotkey}/{job_id}")
            return files

        except Exception as e:
            self._debug_print(f"Exception in list_files: {str(e)}")
            bt.logging.error(f"S3 list files error for {miner_hotkey}/{job_id}: {str(e)}")
            return []