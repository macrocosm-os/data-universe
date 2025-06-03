from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
import os
import sqlite3
from typing import Dict, Optional, List
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta
import threading
import secrets
import bittensor as bt

load_dotenv()

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME)


class APIKeyManager:
    def __init__(self, db_path: str = None):
        # Master key from environment
        self.master_key = os.getenv('MASTER_KEY')
        if not self.master_key:
            bt.logging.error("MASTER_KEY not found in environment. API will be disabled.")
            raise ValueError(
                "MASTER_KEY environment variable is required to enable API. "
                "Please set MASTER_KEY in your .env file."
            )

        # Use provided path or default to current directory
        if db_path is None:
            db_path = "api_keys.db"

        self.db_path = db_path
        self.lock = threading.RLock()
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database"""
        with sqlite3.connect(self.db_path) as conn:
            # Create API keys table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)
            # Create rate limiting table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rate_limits (
                    key TEXT,
                    request_time TIMESTAMP,
                    FOREIGN KEY(key) REFERENCES api_keys(key)
                )
            """)

    def create_api_key(self, name: str) -> str:
        """Create a new API key"""
        api_key = f"sk_live_{secrets.token_urlsafe(32)}"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO api_keys (key, name) VALUES (?, ?)",
                (api_key, name)
            )
        return api_key

    def deactivate_api_key(self, key: str):
        """Deactivate an API key"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE api_keys SET is_active = FALSE WHERE key = ?",
                (key,)
            )

    def list_api_keys(self) -> List[Dict]:
        """List all API keys"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT key, name, created_at, is_active FROM api_keys"
            )
            return [dict(row) for row in cursor.fetchall()]

    def is_valid_key(self, api_key: str) -> bool:
        """Check if API key is valid"""
        if api_key == self.master_key:
            return True

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT is_active FROM api_keys WHERE key = ?",
                (api_key,)
            )
            result = cursor.fetchone()
            return bool(result and result[0])

    def is_master_key(self, api_key: str) -> bool:
        """Check if key is the master key"""
        return api_key == self.master_key

    def _clean_old_requests(self):
        """Remove requests older than 1 hour"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM rate_limits WHERE request_time < datetime('now', '-1 hour')"
            )

    def check_rate_limit(self, api_key: str) -> tuple[bool, Dict]:
        """Check if request is within rate limits"""
        with self.lock:
            self._clean_old_requests()

            is_master = self.is_master_key(api_key)
            rate_limit = 100_000 if is_master else 100  # Master key gets higher limit

            with sqlite3.connect(self.db_path) as conn:
                # Count recent requests
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM rate_limits 
                    WHERE key = ? AND request_time > datetime('now', '-1 hour')
                """, (api_key,))
                count = cursor.fetchone()[0]

                if count >= rate_limit:
                    # Get reset time
                    cursor = conn.execute("""
                        SELECT request_time FROM rate_limits 
                        WHERE key = ? 
                        ORDER BY request_time ASC 
                        LIMIT 1
                    """, (api_key,))
                    oldest = cursor.fetchone()
                    reset_time = datetime.fromisoformat(oldest[0]) + timedelta(hours=1)

                    return False, {
                        "X-RateLimit-Limit": str(rate_limit),
                        "X-RateLimit-Reset": reset_time.isoformat()
                    }

                # Record new request
                conn.execute(
                    "INSERT INTO rate_limits (key, request_time) VALUES (?, datetime('now'))",
                    (api_key,)
                )

                return True, {
                    "X-RateLimit-Limit": str(rate_limit),
                    "X-RateLimit-Remaining": str(rate_limit - count - 1),
                    "X-RateLimit-Reset": (datetime.utcnow() + timedelta(hours=1)).isoformat()
                }


# Create global instance
key_manager = APIKeyManager()


async def verify_api_key(api_key_header: str = Security(api_key_header)):
    """Verify API key and check rate limits"""
    if not key_manager.is_valid_key(api_key_header):
        raise HTTPException(
            status_code=403,
            detail="Invalid API key"
        )

    # Check rate limits
    within_limit, headers = key_manager.check_rate_limit(api_key_header)
    if not within_limit:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers=headers
        )

    return api_key_header


async def require_master_key(api_key_header: str = Security(api_key_header)):
    """Verify master API key"""
    if not key_manager.is_master_key(api_key_header):
        raise HTTPException(
            status_code=403,
            detail="Invalid master key"
        )

    # Check rate limits even for master key
    within_limit, headers = key_manager.check_rate_limit(api_key_header)
    if not within_limit:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers=headers
        )

    return True