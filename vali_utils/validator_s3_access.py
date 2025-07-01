import requests
import time
import datetime as dt
import threading
import bittensor as bt
from typing import Dict, Optional, Any, List
import os
import xml.etree.ElementTree as ET
import urllib.parse


class ValidatorS3Access:
    """Clean S3 access for job-based validation - data/hotkey={hotkey}/job_id={job_id}/"""

    def __init__(self, wallet: bt.wallet, s3_auth_url: str, debug: bool = False):
        self.wallet = wallet
        self.s3_auth_url = s3_auth_url
        self.access_data = None
        self.expiry_time = 0
        self.lock = threading.RLock()
        self.debug = debug

    def _debug_print(self, message: str):
        """Print debug message if debug mode is enabled"""
        if self.debug:
            print(f"DEBUG S3: {message}")

    def ensure_access(self) -> bool:
        """Ensure valid S3 access is available, refreshing if needed"""
        with self.lock:
            current_time = time.time()
            # Check if credentials are still valid (with 1 hour buffer)
            if self.access_data and current_time < self.expiry_time - 3600:
                self._debug_print("Using cached S3 access")
                return True

            # Get new access
            self._debug_print("Getting new S3 access from auth server")
            access_data = self.get_validator_access()

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

    def get_validator_access(self) -> Optional[Dict[str, Any]]:
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

            # Send request to S3 auth service
            response = requests.post(
                f"{self.s3_auth_url.rstrip('/')}/get-validator-access",
                json=payload,
                timeout=30
            )

            self._debug_print(f"Auth server response status: {response.status_code}")

            if response.status_code != 200:
                self._debug_print(f"Auth server error: {response.text}")
                return None

            result = response.json()
            self._debug_print(f"Auth server response structure: {list(result.keys())}")
            
            if 'urls' in result:
                urls = result['urls']
                self._debug_print(f"URLs structure: {list(urls.keys())}")
                if 'miners' in urls:
                    miners_urls = urls['miners']
                    self._debug_print(f"Miners URLs: {list(miners_urls.keys())}")

            return result

        except Exception as e:
            self._debug_print(f"Exception getting validator access: {str(e)}")
            return None

    def list_miners_new_format(self) -> List[str]:
        """List all miners (hotkeys) using format: data/hotkey={hotkey_id}/"""
        if not self.ensure_access():
            self._debug_print("Failed to ensure S3 access")
            return []

        try:
            urls = self.access_data.get('urls', {})
            miners_urls = urls.get('miners', {})

            if not miners_urls or 'list_all_miners' not in miners_urls:
                self._debug_print(f"No 'list_all_miners' URL found. Available: {list(miners_urls.keys())}")
                return []

            # Use the presigned URL to list miners
            list_url = miners_urls['list_all_miners']
            self._debug_print(f"Using miners list URL: {list_url[:100]}...")
            
            response = requests.get(list_url)
            self._debug_print(f"Miners list response status: {response.status_code}")

            if response.status_code != 200:
                self._debug_print(f"Failed miners list response: {response.text[:500]}")
                return []

            self._debug_print(f"Response content preview: {response.text[:300]}...")

            root = ET.fromstring(response.text)

            # Extract the CommonPrefixes which represent miners (hotkey folders)
            namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            miners = []

            # Show all prefixes found for debugging
            all_prefixes = []
            for prefix in root.findall('.//s3:CommonPrefixes', namespaces):
                prefix_text = prefix.find('s3:Prefix', namespaces).text
                if prefix_text:
                    all_prefixes.append(prefix_text)
                    # URL decode the prefix first
                    decoded_prefix = urllib.parse.unquote(prefix_text)
                    # Extract the hotkey from the prefix: data/hotkey={hotkey_id}/
                    if decoded_prefix.startswith('data/hotkey=') and decoded_prefix.endswith('/'):
                        hotkey_id = decoded_prefix[12:-1]  # Remove 'data/hotkey=' prefix and '/' suffix
                        miners.append(hotkey_id)

            self._debug_print(f"All prefixes found: {all_prefixes}")
            self._debug_print(f"Extracted miners: {miners}")
            
            return miners
            
        except Exception as e:
            self._debug_print(f"Exception in list_miners_new_format: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def list_jobs(self, miner_hotkey: str) -> List[str]:
        """List jobs for a specific miner using format: data/hotkey={hotkey_id}/job_id={job_id}/"""
        if not self.ensure_access():
            self._debug_print("Failed to ensure S3 access for jobs listing")
            return []

        try:
            target_prefix = f"data/hotkey={miner_hotkey}/"
            self._debug_print(f"Looking for jobs with prefix: {target_prefix}")

            # Use the presigned URL AS-IS - DON'T modify it
            urls = self.access_data.get('urls', {})
            global_urls = urls.get('global', {})

            if not global_urls or 'list_all_data' not in global_urls:
                self._debug_print("No 'list_all_data' URL found")
                return []

            list_url = global_urls['list_all_data']
            self._debug_print(f"Using unmodified URL: {list_url[:150]}...")

            # Use the URL exactly as provided by auth server
            response = requests.get(list_url)
            self._debug_print(f"Jobs response status: {response.status_code}")

            if response.status_code != 200:
                self._debug_print(f"Failed jobs response: {response.text[:500]}")
                return []

            root = ET.fromstring(response.text)

            # Extract ALL content (both files and prefixes) and filter client-side
            namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            jobs = set()  # Use set to avoid duplicates

            # Check Contents (actual files) for job paths
            for content in root.findall('.//s3:Contents', namespaces):
                key = content.find('s3:Key', namespaces).text
                if key:
                    # URL decode the key first
                    decoded_key = urllib.parse.unquote(key)
                    self._debug_print(f"Found file: {decoded_key}")
                    
                    # Extract job from file path: data/hotkey={hotkey_id}/job_id={job_id}/filename
                    if decoded_key.startswith(target_prefix) and '/job_id=' in decoded_key:
                        # Extract the job_id part
                        job_part_full = decoded_key[len(target_prefix):]  # Remove miner prefix
                        if job_part_full.startswith('job_id='):
                            job_part = job_part_full.split('/')[0][7:]  # Remove 'job_id=' and get first part
                            jobs.add(job_part)
                            self._debug_print(f"Extracted job from file: {job_part}")

            # Also check CommonPrefixes (folders) 
            for job_prefix in root.findall('.//s3:CommonPrefixes', namespaces):
                prefix_text = job_prefix.find('s3:Prefix', namespaces).text
                if prefix_text:
                    # URL decode the prefix first
                    decoded_prefix = urllib.parse.unquote(prefix_text)
                    self._debug_print(f"Found prefix: {decoded_prefix}")
                    
                    # Extract job_id from: data/hotkey={hotkey_id}/job_id={job_id}/
                    if decoded_prefix.startswith(target_prefix) and '/job_id=' in decoded_prefix:
                        job_part_full = decoded_prefix[len(target_prefix):]  # Remove miner prefix
                        if job_part_full.startswith('job_id='):
                            job_part = job_part_full.split('/')[0][7:]  # Remove 'job_id=' and get first part
                            jobs.add(job_part)
                            self._debug_print(f"Extracted job from prefix: {job_part}")

            jobs_list = list(jobs)
            self._debug_print(f"Found {len(jobs_list)} jobs for {miner_hotkey}: {jobs_list}")
            return jobs_list
            
        except Exception as e:
            self._debug_print(f"Exception in list_jobs: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def list_files(self, miner_hotkey: str, job_id: str) -> List[Dict[str, Any]]:
        """List files for a specific miner and job using format: data/hotkey={hotkey_id}/job_id={job_id}/"""
        if not self.ensure_access():
            return []

        try:
            target_prefix = f"data/hotkey={miner_hotkey}/job_id={job_id}/"
            self._debug_print(f"Looking for files with prefix: {target_prefix}")

            # Use the presigned URL AS-IS - don't modify it
            urls = self.access_data.get('urls', {})
            global_urls = urls.get('global', {})

            if not global_urls or 'list_all_data' not in global_urls:
                return []

            list_url = global_urls['list_all_data']
            
            # Use the URL exactly as provided by auth server
            response = requests.get(list_url)

            if response.status_code != 200:
                self._debug_print(f"Failed files response: {response.text[:500]}")
                return []

            root = ET.fromstring(response.text)

            # Extract ALL files and filter client-side
            namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            files = []

            # Look for actual files (Contents) not folders (CommonPrefixes)
            for content in root.findall('.//s3:Contents', namespaces):
                key = content.find('s3:Key', namespaces).text
                size = content.find('s3:Size', namespaces).text
                last_modified = content.find('s3:LastModified', namespaces).text

                if key:
                    # URL decode the key
                    decoded_key = urllib.parse.unquote(key)
                    
                    # Filter: only files that start with our target prefix
                    if decoded_key.startswith(target_prefix):
                        files.append({
                            'Key': decoded_key,  # Use decoded key
                            'Size': int(size) if size else 0,
                            'LastModified': last_modified
                        })
                        self._debug_print(f"Found file: {os.path.basename(decoded_key)}")

            self._debug_print(f"Found {len(files)} files in {miner_hotkey}/{job_id}")
            return files
            
        except Exception as e:
            self._debug_print(f"Exception in list_files: {str(e)}")
            return []

    def test_direct_url(self, url: str) -> Dict[str, Any]:
        """Test a presigned URL directly and return response info"""
        try:
            self._debug_print(f"Testing direct URL: {url[:100]}...")
            response = requests.get(url)
            
            result = {
                'status_code': response.status_code,
                'headers': dict(response.headers),
                'content_preview': response.text[:1000] if response.status_code == 200 else response.text,
                'success': response.status_code == 200
            }
            
            self._debug_print(f"Direct URL test result: status={result['status_code']}, success={result['success']}")
            return result
            
        except Exception as e:
            self._debug_print(f"Exception testing direct URL: {str(e)}")
            return {'success': False, 'error': str(e)}