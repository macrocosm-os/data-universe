from fastapi import APIRouter, Depends
from .auth import APIKeyManager
from pydantic import BaseModel
from typing import List
from vali_utils.api.utils import endpoint_error_handler


class APIKeyCreate(BaseModel):
    name: str


class APIKeyResponse(BaseModel):
    key: str
    name: str


def create_key_routes(key_manager: APIKeyManager, require_master_key):
    """Create key management routes with the given key_manager and auth function"""
    router = APIRouter(tags=["key management"])

    @router.post("", response_model=APIKeyResponse)
    @endpoint_error_handler
    async def create_api_key(
        request: APIKeyCreate, _: bool = Depends(require_master_key)
    ):
        """Create new API key (requires master key)"""
        key = key_manager.create_api_key(request.name)
        return {"key": key, "name": request.name}

    @router.get("")
    @endpoint_error_handler
    async def list_api_keys(_: bool = Depends(require_master_key)):
        """List all API keys (requires master key)"""
        return {"keys": key_manager.list_api_keys()}

    @router.post("/{key}/deactivate")
    @endpoint_error_handler
    async def deactivate_api_key(key: str, _: bool = Depends(require_master_key)):
        """Deactivate an API key (requires master key)"""
        key_manager.deactivate_api_key(key)
        return {"status": "success"}

    return router
