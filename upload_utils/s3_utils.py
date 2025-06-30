import time
import os
import requests
import bittensor as bt
from typing import Dict, Any, Optional


class S3Auth:
    """Handles S3 authentication with blockchain commitments and Keypair signatures for job-based structure"""

    def __init__(self, s3_auth_url: str):
        self.s3_auth_url = s3_auth_url

    def get_credentials(self,
                        subtensor: bt.subtensor,
                        wallet: bt.wallet) -> Optional[Dict[str, Any]]:
        """Get S3 credentials using blockchain commitments and hotkey signature"""
        try:
            coldkey = wallet.get_coldkeypub().ss58_address
            hotkey = wallet.hotkey.ss58_address
            timestamp = int(time.time())

            # Updated commitment format - generic "data" instead of source
            commitment = f"s3:data:access:{coldkey}:{hotkey}:{timestamp}"

            # Sign the commitment
            signature = wallet.hotkey.sign(commitment.encode())
            signature_hex = signature.hex()

            payload = {
                "coldkey": coldkey,
                "hotkey": hotkey,
                "timestamp": timestamp,
                "signature": signature_hex
            }

            response = requests.post(
                f"{self.s3_auth_url.rstrip('/')}/get-folder-access",
                json=payload,
                timeout=30
            )

            if response.status_code != 200:
                try:
                    error_detail = response.json().get("detail", "Unknown error")
                except Exception:
                    error_detail = response.text or "Unknown error"
                bt.logging.error(f"❌ Failed to get S3 credentials: {error_detail}")
                return None

            creds = response.json()
            bt.logging.info(f"✅ Got S3 credentials for folder: {creds.get('folder', 'unknown')}")

            # Log structure info if available
            if 'structure_info' in creds:
                bt.logging.info(f"📁 Folder structure: {creds['structure_info']['folder_structure']}")

            return creds

        except Exception as e:
            bt.logging.error(f"❌ Error getting S3 credentials: {str(e)}")
            return None

    def upload_file(self, file_path: str, creds: Dict[str, Any]) -> bool:
        """Upload file to the basic folder structure"""
        try:
            key = f"{creds['folder']}{os.path.basename(file_path)}"
            post_data = dict(creds['fields'])  # clone all fields (V4-compatible)
            post_data['key'] = key  # overwrite key with actual file key

            with open(file_path, 'rb') as f:
                files = {'file': f}
                response = requests.post(creds['url'], data=post_data, files=files)

            if response.status_code == 204:
                bt.logging.info(f"✅ Upload success: {key}")
                return True
            else:
                bt.logging.error(f"❌ Upload failed: {response.status_code} — {response.text}")
                return False

        except Exception as e:
            bt.logging.error(f"❌ S3 Upload Exception for {file_path}: {e}")
            return False

    def upload_file_with_path(self, file_path: str, s3_path: str, creds: Dict[str, Any]) -> bool:
        """Upload file with custom S3 path for job-based uploads

        Args:
            file_path: Local file path
            s3_path: Relative path within the folder (e.g., "hotkey={hotkey_id}/job_id={job_id}/filename.parquet")
            creds: S3 credentials from API
        """
        try:
            # Get the folder prefix from credentials (base_url)
            folder_prefix = creds.get('folder', '')

            # Construct the full S3 path by appending our relative path to the folder prefix
            # This creates: base_url/hotkey={hotkey_id}/job_id={job_id}/filename.parquet
            full_s3_path = f"{folder_prefix}{s3_path}"

            bt.logging.info(f"🔄 Uploading to S3 path: {full_s3_path}")

            post_data = dict(creds['fields'])  # clone all fields (V4-compatible)
            post_data['key'] = full_s3_path  # use the full path

            with open(file_path, 'rb') as f:
                files = {'file': f}
                response = requests.post(creds['url'], data=post_data, files=files)

            if response.status_code == 204:
                bt.logging.success(f"✅ S3 upload success: {full_s3_path}")
                return True
            else:
                bt.logging.error(f"❌ S3 upload failed: {response.status_code} — {response.text}")
                return False

        except Exception as e:
            bt.logging.error(f"❌ S3 Upload Exception for {file_path} -> {s3_path}: {e}")
            return False

    def get_structure_info(self) -> Optional[Dict[str, Any]]:
        """Get information about the current folder structure"""
        try:
            response = requests.get(
                f"{self.s3_auth_url.rstrip('/')}/structure-info",
                timeout=30
            )

            if response.status_code == 200:
                return response.json()
            else:
                bt.logging.warning(f"Failed to get structure info: {response.status_code}")
                return None

        except Exception as e:
            bt.logging.error(f"Error getting structure info: {str(e)}")
            return None

    def test_connection(self) -> bool:
        """Test connection to S3 auth server"""
        try:
            response = requests.get(
                f"{self.s3_auth_url.rstrip('/')}/healthcheck",
                timeout=10
            )

            if response.status_code == 200:
                health_data = response.json()
                bt.logging.info(f"✅ S3 Auth server healthy: {health_data.get('folder_structure', 'N/A')}")
                return True
            else:
                bt.logging.error(f"❌ S3 Auth server unhealthy: {response.status_code}")
                return False

        except Exception as e:
            bt.logging.error(f"❌ Failed to connect to S3 auth server: {str(e)}")
            return False