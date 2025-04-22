import time
import os
import requests
import bittensor as bt
from typing import Dict, Any, Optional


class S3Auth:
    """Handles S3 authentication with blockchain commitments"""

    def __init__(self, s3_auth_url: str):
        self.s3_auth_url = s3_auth_url

    def get_credentials(self,
                        coldkey: str,
                        hotkey: str,
                        source_name: str,
                        subtensor: bt.subtensor,
                        wallet: bt.wallet,
                        netuid: int) -> Optional[Dict[str, Any]]:
        """Get S3 credentials using blockchain commitments (non-async version)"""
        try:
            timestamp = int(time.time())
            commitment = f"s3:access:{coldkey}:{source_name}:{timestamp}"

            bt.logging.info(f"Committing to blockchain: {commitment}")
            success = subtensor.commit(wallet=wallet, netuid=netuid, data=commitment)

            if not success:
                bt.logging.error("Failed to commit to blockchain")
                return None

            # Request credentials within 1-minute window
            payload = {
                "coldkey": coldkey,
                "hotkey": hotkey,
                "source": source_name,
                "netuid": netuid,
                "timestamp": timestamp
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
                bt.logging.error(f"Failed to get S3 credentials: {error_detail}")
                return None

            return response.json()

        except Exception as e:
            bt.logging.error(f"Error getting S3 credentials: {str(e)}")
            return None

    def upload_file(self, file_path: str, creds: Dict[str, Any]) -> bool:
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
