from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional


class APIKey(BaseModel):
    """Model for API key information"""
    key: str
    name: str
    created_at: datetime
    is_active: bool = True


class CreateKeyRequest(BaseModel):
    """Request model for creating new API key"""
    name: str


class APIKeyList(BaseModel):
    """Response model for listing API keys"""
    keys: List[APIKey]


class APIKeyResponse(BaseModel):
    """Response model for API key creation"""
    key: str
    name: str