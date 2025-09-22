from fastapi import APIRouter, Depends
from .auth import require_master_key, APIKeyManager
from pydantic import BaseModel
from typing import List
from vali_utils.api.utils import endpoint_error_handler

class APIKeyCreate(BaseModel):
    name: str


class APIKeyResponse(BaseModel):
    key: str
    name: str


router = APIRouter(tags=["key management"])

# Global reference for dependency injection (similar to get_validator pattern)
class GetKeyManager:
    api = None

get_key_manager = GetKeyManager()

def get_api_key_manager() -> APIKeyManager:
    """Dependency to get the APIKeyManager instance"""
    if get_key_manager.api is None:
        raise RuntimeError("API not initialized")
    return get_key_manager.api.key_manager


@router.post("", response_model=APIKeyResponse)
@endpoint_error_handler
async def create_api_key(
    request: APIKeyCreate,
    _: bool = Depends(require_master_key),
    key_manager: APIKeyManager = Depends(get_api_key_manager)
):
    """Create new API key (requires master key)"""
    key = key_manager.create_api_key(request.name)
    return {"key": key, "name": request.name}


@router.get("")
@endpoint_error_handler
async def list_api_keys(
    _: bool = Depends(require_master_key),
    key_manager: APIKeyManager = Depends(get_api_key_manager)
):
    """List all API keys (requires master key)"""
    return {"keys": key_manager.list_api_keys()}


@router.post("/{key}/deactivate")
@endpoint_error_handler
async def deactivate_api_key(
    key: str,
    _: bool = Depends(require_master_key),
    key_manager: APIKeyManager = Depends(get_api_key_manager)
):
    """Deactivate an API key (requires master key)"""
    key_manager.deactivate_api_key(key)
    return {"status": "success"}