import bittensor as bt
import random
from fastapi import HTTPException
from functools import wraps


def select_validation_samples(data, sample_size: int = 1):
    """Select random samples from the data for validation"""
    if not data:
        return []

    # Select up to sample_size random items, or all items if less than sample_size
    sample_count = min(sample_size, len(data))
    return random.sample(data, sample_count)


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