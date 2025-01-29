import bittensor as bt
from fastapi import HTTPException
from functools import wraps


def endpoint_error_handler(func):
    """Return 500 status code if endpoint failed"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException:
            # Re-raise FastAPI HTTP exceptions
            raise
        except Exception as e:
            bt.logging.error(f"API endpoint error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Internal server error"
            )
    return wrapper