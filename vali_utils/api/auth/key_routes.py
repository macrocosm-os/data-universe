from fastapi import APIRouter, Depends
from .auth import require_master_key, key_manager
from pydantic import BaseModel
from typing import List


class APIKeyCreate(BaseModel):
    name: str


class APIKeyResponse(BaseModel):
    key: str
    name: str


router = APIRouter(prefix="/keys", tags=["key management"])


@router.post("", response_model=APIKeyResponse)
async def create_api_key(
    request: APIKeyCreate,
    _: bool = Depends(require_master_key)
):
    """Create new API key (requires master key)"""
    key = key_manager.create_api_key(request.name)
    return {"key": key, "name": request.name}


@router.get("")
async def list_api_keys(_: bool = Depends(require_master_key)):
    """List all API keys (requires master key)"""
    return {"keys": key_manager.list_api_keys()}


@router.post("/{key}/deactivate")
async def deactivate_api_key(
    key: str,
    _: bool = Depends(require_master_key)
):
    """Deactivate an API key (requires master key)"""
    key_manager.deactivate_api_key(key)
    return {"status": "success"}