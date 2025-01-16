from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
import os
from typing import List, Set
from dotenv import load_dotenv

load_dotenv()

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME)

# Get API keys from environment variable - can be comma-separated for multiple keys
API_KEYS: Set[str] = {key.strip() for key in os.getenv("API_KEYS", "").split(",")}


async def verify_api_key(api_key_header: str = Security(api_key_header)):
    """Verify API key from request header"""
    if not API_KEYS:
        raise HTTPException(
            status_code=500,
            detail="No API keys configured. Please set API_KEYS environment variable."
        )

    if api_key_header in API_KEYS:
        return api_key_header

    raise HTTPException(
        status_code=403,
        detail="Invalid API key"
    )