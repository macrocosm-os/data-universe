from fastapi import APIRouter, Depends
from .models import CreateKeyRequest, APIKeyResponse, APIKeyList
from .auth import verify_master_key, APIKeyManager

# Create a separate router for key management
key_router = APIRouter(prefix="/manage/keys", tags=["key management"])


@key_router.post("", response_model=APIKeyResponse)
async def create_api_key(
    request: CreateKeyRequest,
    _: bool = Depends(verify_master_key),
    key_manager: APIKeyManager = None
):
    """Create new API key (master key required)"""
    key = key_manager.create_api_key(request.name)
    return {"key": key.key, "name": key.name}


@key_router.get("", response_model=APIKeyList)
async def list_api_keys(
    _: bool = Depends(verify_master_key),
    key_manager: APIKeyManager = None
):
    """List all API keys (master key required)"""
    keys = key_manager.list_api_keys()
    return {"keys": keys}


@key_router.post("/{key}/deactivate")
async def deactivate_api_key(
    key: str,
    _: bool = Depends(verify_master_key),
    key_manager: APIKeyManager = None
):
    """Deactivate an API key (master key required)"""
    key_manager.deactivate_api_key(key)
    return {"status": "success"}
