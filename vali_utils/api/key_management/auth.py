from fastapi import HTTPException, Security, Depends
from fastapi.security import APIKeyHeader
import sqlite3
from datetime import datetime
import secrets
import os
from typing import Optional, List
from .models import APIKey
import bittensor as bt

api_key_header = APIKeyHeader(name="X-API-Key")


class APIKeyManager:
    def __init__(self, db_path: str = "api_keys.db"):
        self.db_path = db_path
        # Get master key from environment or generate one on first run
        self.master_key = os.getenv('API_MASTER_KEY')
        if not self.master_key:
            self.master_key = secrets.token_urlsafe(32)
            bt.logging.info(f"\n[IMPORTANT] Master API Key: {self.master_key}\nStore this safely!\n") # TODO

        self._init_db()

    def _init_db(self):
        """Initialize SQLite database with API keys table"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    key TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)

    def create_api_key(self, name: str) -> APIKey:
        """Generate a new API key"""
        api_key = f"sk_live_{secrets.token_urlsafe(24)}"
        now = datetime.utcnow()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO api_keys (key, name, created_at) VALUES (?, ?, ?)",
                (api_key, name, now)
            )

        return APIKey(key=api_key, name=name, created_at=now)

    def get_api_key(self, key: str) -> Optional[APIKey]:
        """Retrieve API key information"""
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute(
                "SELECT key, name, created_at, is_active FROM api_keys WHERE key = ?",
                (key,)
            ).fetchone()

            if result:
                return APIKey(
                    key=result[0],
                    name=result[1],
                    created_at=datetime.fromisoformat(result[2]),
                    is_active=bool(result[3])
                )
        return None

    def list_api_keys(self) -> List[APIKey]:
        """List all API keys (master key only)"""
        with sqlite3.connect(self.db_path) as conn:
            results = conn.execute(
                "SELECT key, name, created_at, is_active FROM api_keys"
            ).fetchall()

            return [
                APIKey(
                    key=row[0],
                    name=row[1],
                    created_at=datetime.fromisoformat(row[2]),
                    is_active=bool(row[3])
                )
                for row in results
            ]

    def deactivate_api_key(self, key: str):
        """Deactivate an API key"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE api_keys SET is_active = FALSE WHERE key = ?",
                (key,)
            )


async def verify_api_key(
        api_key: str = Security(api_key_header),
        key_manager: APIKeyManager = None
) -> str:
    """Verify regular API key"""
    if not key_manager:
        raise HTTPException(status_code=500, detail="API authentication not configured")

    key_info = key_manager.get_api_key(api_key)
    if not key_info or not key_info.is_active:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    return api_key


async def verify_master_key(
        api_key: str = Security(api_key_header),
        key_manager: APIKeyManager = None
) -> bool:
    """Verify master API key for management operations"""
    if not key_manager:
        raise HTTPException(status_code=500, detail="API authentication not configured")

    if api_key != key_manager.master_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid master key"
        )
    return True