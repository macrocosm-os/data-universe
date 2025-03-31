"""
Functions for managing S3 storage configuration through on-chain subnet configuration.
"""
import json
import bittensor as bt
from typing import Dict, Any, Optional
from storage.s3.config import S3Config, DualStorageConfig


def get_storage_config_from_chain(subtensor: bt.subtensor, netuid: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve S3 storage configuration from the on-chain subnet configuration.
    
    Args:
        subtensor: Bittensor subtensor connection
        netuid: Subnet network UID
        
    Returns:
        Dictionary containing storage configuration, or None if not found
    """
    try:
        # Get raw bytes from chain
        config_bytes = subtensor.get_subnet_custom_config(netuid=netuid)
        if not config_bytes:
            bt.logging.info(f"No custom subnet configuration found for subnet {netuid}")
            return None
            
        # Decode and parse the JSON configuration
        config_str = config_bytes.decode('utf-8')
        config_dict = json.loads(config_str)
        
        # Check if storage configuration exists
        if 'storage' not in config_dict:
            bt.logging.info(f"No storage configuration found in subnet {netuid} config")
            return None
            
        return config_dict['storage']
        
    except Exception as e:
        bt.logging.error(f"Error retrieving storage config from chain: {str(e)}")
        return None


def update_storage_config_on_chain(
    subtensor: bt.subtensor, 
    wallet: 'bt.wallet',
    netuid: int,
    storage_config: Dict[str, Any]
) -> bool:
    """
    Update the on-chain subnet configuration with storage settings.
    
    Args:
        subtensor: Bittensor subtensor connection
        wallet: Bittensor wallet with subnet owner permissions
        netuid: Subnet network UID
        storage_config: Storage configuration dictionary
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # First get existing config to merge with
        existing_bytes = subtensor.get_subnet_custom_config(netuid=netuid)
        if existing_bytes:
            existing_config = json.loads(existing_bytes.decode('utf-8'))
        else:
            existing_config = {}
        
        # Update with new storage config
        existing_config['storage'] = storage_config
        
        # Convert to bytes
        config_bytes = json.dumps(existing_config).encode('utf-8')
        
        # Update on chain
        success = subtensor.set_subnet_custom_config(
            netuid=netuid,
            config=config_bytes,
            wallet=wallet
        )
        
        if success:
            bt.logging.success(f"Successfully updated storage configuration on subnet {netuid}")
        else:
            bt.logging.error(f"Failed to update storage configuration on subnet {netuid}")
            
        return success
        
    except Exception as e:
        bt.logging.error(f"Error updating storage config on chain: {str(e)}")
        return False


def create_config_from_chain_or_env(
    subtensor: bt.subtensor, 
    netuid: int
) -> DualStorageConfig:
    """
    Create a storage configuration object from chain config or environment variables.
    Chain configuration takes precedence over environment variables.
    
    Args:
        subtensor: Bittensor subtensor connection
        netuid: Subnet network UID
        
    Returns:
        DualStorageConfig: Configuration for storage system
    """
    import os
    
    # First try to get config from chain
    chain_config = get_storage_config_from_chain(subtensor, netuid)
    
    # Check environment variables
    env_config = {
        'use_s3': os.getenv('DATA_UNIVERSE_USE_S3', 'false').lower() == 'true',
        'use_huggingface': os.getenv('DATA_UNIVERSE_USE_HF', 'true').lower() == 'true',
        's3': {
            'bucket_name': os.getenv('DATA_UNIVERSE_S3_BUCKET'),
            'auth_endpoint': os.getenv('DATA_UNIVERSE_AUTH_ENDPOINT'),
            'region': os.getenv('DATA_UNIVERSE_S3_REGION', 'us-east-1')
        }
    }
    
    # Merge configurations, with chain taking precedence
    final_config = env_config.copy()
    if chain_config:
        # First level merge
        for key in ['use_s3', 'use_huggingface']:
            if key in chain_config:
                final_config[key] = chain_config[key]
        
        # S3 config merge
        if 's3' in chain_config and isinstance(chain_config['s3'], dict):
            for key, value in chain_config['s3'].items():
                if value is not None:  # Only override if not None
                    final_config['s3'][key] = value
    
    # Check required values
    if final_config['use_s3']:
        if not final_config['s3']['bucket_name']:
            bt.logging.warning("S3 bucket name is required but not provided. Disabling S3 storage.")
            final_config['use_s3'] = False
        elif not final_config['s3']['auth_endpoint']:
            bt.logging.warning("S3 auth endpoint is required but not provided. Disabling S3 storage.")
            final_config['use_s3'] = False
    
    # Create and return the config object
    s3_config = S3Config.from_dict(final_config['s3'])
    return DualStorageConfig(
        s3_config=s3_config,
        use_s3=final_config['use_s3'],
        use_huggingface=final_config['use_huggingface']
    )


def enable_s3_storage_for_hotkey(miner_hotkey: str, percentage: int = 100) -> bool:
    """
    Deterministically decide if S3 storage should be enabled for a specific hotkey
    based on a rollout percentage. This allows for controlled, gradual rollout
    without needing to specify individual hotkeys.
    
    Args:
        miner_hotkey: The miner's hotkey
        percentage: Percentage of miners to enable (0-100)
        
    Returns:
        True if S3 should be enabled for this hotkey, False otherwise
    """
    if percentage >= 100:
        return True
    if percentage <= 0:
        return False
        
    # Create a deterministic hash value from the hotkey
    import hashlib
    hash_value = int(hashlib.md5(miner_hotkey.encode()).hexdigest(), 16)
    
    # Use modulo to get a value 0-99
    position = hash_value % 100
    
    # Enable if position is within the percentage
    return position < percentage