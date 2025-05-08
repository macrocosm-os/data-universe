import bittensor as bt
import random
from fastapi import HTTPException
from functools import wraps
from common.organic_protocol import OrganicRequest

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

async def query_validator(
    wallet: bt.wallet,
    validator_host: str,
    validator_port: int,
    validator_hotkey: str,
    source: str,
    keywords: list = [],
    usernames: list = [],
    start_date: str = None,
    end_date: str = None,
    limit: int = 1000
):
    """
    Query a validator using the OrganicRequest protocol
    
    Args:
        wallet: Bittensor wallet for signing the request
        validator_host: Validator IP address or hostname
        validator_port: Validator port number
        validator_hotkey: Validator hotkey (str)
        source: Data source (X or REDDIT)
        keywords: List of keywords to search for
        usernames: List of usernames to search for
        start_date: ISO-formatted start date
        end_date: ISO-formatted end date
        limit: Maximum number of results to return
        
    Returns:
        OrganicRequest response with data or error information
    """
    bt.logging.info(f"Querying validator at {validator_host}:{validator_port} for {source} data")
    
    # Create an AxonInfo with required fields
    axon_info = bt.AxonInfo(
        ip=validator_host, 
        port=validator_port,
        ip_type=0,  # v4
        hotkey=validator_hotkey, 
        coldkey="",  # Not needed
        protocol=0,
        version=1
    )
    
    # Prepare the OrganicRequest synapse
    synapse = OrganicRequest(
        source=source.upper(),
        usernames=usernames,
        keywords=keywords,
        start_date=start_date,
        end_date=end_date,
        limit=limit
    )
    
    # Send the request to the validator
    try:
        async with bt.dendrite(wallet=wallet) as dendrite:
            response = await dendrite.forward(
                axons=[axon_info],
                synapse=synapse,
                timeout=180  # 3 minute timeout
            )
            
        if not response or len(response) == 0:
            bt.logging.error("No response received from validator")
            return None
            
        return response[0]
    except Exception as e:
        bt.logging.error(f"Error querying validator at {validator_host}:{validator_port}: {str(e)}")
        raise
