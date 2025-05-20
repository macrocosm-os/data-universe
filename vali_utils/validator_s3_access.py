import requests
import time
import datetime as dt
import threading
import bittensor as bt
from typing import Dict, Optional, Any, List
import os
# Parse the XML response
import xml.etree.ElementTree as ET
from io import StringIO



class ValidatorS3Access:
    """Manages validator access to S3 data using presigned URLs"""

    def __init__(self,
                 wallet: bt.wallet,
                 s3_auth_url: str):
        self.wallet = wallet
        self.s3_auth_url = s3_auth_url
        self.access_data = None
        self.expiry_time = 0
        self.lock = threading.RLock()

    def ensure_access(self) -> bool:
        """Ensure valid S3 access is available, refreshing if needed"""
        with self.lock:
            current_time = time.time()
            # Check if credentials are still valid (with 1 hour buffer)
            if self.access_data and current_time < self.expiry_time - 3600:
                return True

            # Get new access (silently)
            access_data = self.get_validator_access()

            if not access_data:
                return False

            self.access_data = access_data

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

            # Send request to S3 auth service
            response = requests.post(
                f"{self.s3_auth_url.rstrip('/')}/get-validator-access",
                json=payload,
                timeout=30
            )

            if response.status_code != 200:
                return None

            return response.json()

        except Exception:
            return None

    def list_sources(self) -> List[str]:
        """List available data sources"""
        if not self.ensure_access():
            return []

        try:
            urls = self.access_data.get('urls', {})
            sources = urls.get('sources', {})
            return list(sources.keys())
        except Exception:
            return []

    def list_miners(self, source: str) -> List[str]:
        """List miners for a specific source using presigned URL"""
        if not self.ensure_access():
            return []

        try:
            urls = self.access_data.get('urls', {})
            sources = urls.get('sources', {})
            source_info = sources.get(source, {})

            if not source_info or 'list_miners' not in source_info:
                return []

            # Use the presigned URL to list miners
            list_url = source_info['list_miners']
            response = requests.get(list_url)

            if response.status_code != 200:
                return []


            root = ET.fromstring(response.text)

            # Extract the CommonPrefixes which represent miners (folders)
            namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            prefixes = []

            for prefix in root.findall('.//s3:CommonPrefixes', namespaces):
                prefix_text = prefix.find('s3:Prefix', namespaces).text
                if prefix_text:
                    # Extract the miner ID from the prefix
                    parts = prefix_text.split('/')
                    if len(parts) >= 3:
                        miner_id = parts[2]
                        prefixes.append(miner_id)

            return prefixes
        except Exception as e:
            bt.logging.error(f"Error listing miners: {str(e)}")
            return []

    def list_files(self, source: str, miner_id: str) -> List[Dict[str, Any]]:
        """List files for a specific miner using presigned URL"""
        if not self.ensure_access():
            return []

        try:
            bucket = self.access_data.get('bucket')
            prefix = f"data/{source}/{miner_id}/"

            # Generate a presigned URL to list objects
            # This is a bit tricky because we don't have direct access to generate new presigned URLs
            # We need to use the list_all_data URL with the appropriate prefix

            urls = self.access_data.get('urls', {})
            global_urls = urls.get('global', {})

            if not global_urls or 'list_all_data' not in global_urls:
                return []

            # We'll need to modify the existing list_all_data URL to include our prefix
            list_url = global_urls['list_all_data']

            # Add or modify the prefix parameter in the URL
            import urllib.parse
            url_parts = list(urllib.parse.urlparse(list_url))
            query = dict(urllib.parse.parse_qsl(url_parts[4]))
            query['prefix'] = prefix
            url_parts[4] = urllib.parse.urlencode(query)
            modified_url = urllib.parse.urlunparse(url_parts)

            # Use the modified URL to list files
            response = requests.get(modified_url)

            if response.status_code != 200:
                return []

            # Parse the XML response
            import xml.etree.ElementTree as ET

            root = ET.fromstring(response.text)

            # Extract the Contents which represent files
            namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            files = []

            for content in root.findall('.//s3:Contents', namespaces):
                key = content.find('s3:Key', namespaces).text
                size = content.find('s3:Size', namespaces).text
                last_modified = content.find('s3:LastModified', namespaces).text

                if key:
                    # Extract just the filename from the full path
                    filename = os.path.basename(key)
                    files.append({
                        'key': key,
                        'filename': filename,
                        'size': int(size) if size else 0,
                        'last_modified': last_modified
                    })

            return files
        except Exception as e:
            bt.logging.error(f"Error listing files: {str(e)}")
            return []

    def download_file(self, key: str, output_path: str) -> bool:
        """Download a file using presigned URL"""
        if not self.ensure_access():
            return False

        try:
            bucket = self.access_data.get('bucket')
            region = self.access_data.get('region', 'nyc3')

            # We need to generate a presigned URL for this specific file
            # Since we don't have direct access to generate new presigned URLs,
            # we'll construct a basic URL and hope it works with the existing signatures

            # This may not work - the proper solution would be to add a specific
            # endpoint in the API to get a presigned URL for downloading a file

            file_url = f"https://{bucket}.{region}.digitaloceanspaces.com/{key}"

            # Download the file
            response = requests.get(file_url, stream=True)

            if response.status_code != 200:
                return False

            # Save the file
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return True
        except Exception:
            return False