"""
Factory functions for creating storage instances based on configuration.
"""
import os
import bittensor as bt
from typing import Optional, Any, Dict

from storage.s3.config import DualStorageConfig, S3Config
from storage.s3.dual_storage import DualStorage
from storage.s3.subnet_config import create_config_from_chain_or_env, enable_s3_storage_for_hotkey
from huggingface_utils.huggingface_uploader import HuggingFaceUploader
from huggingface_utils.encoding_system import EncodingKeyManager


def create_storage_from_config(
    subtensor: bt.subtensor,
    netuid: int,
    db_path: str,
    miner_hotkey: str,
    wallet: Any,
    encoding_key_manager: EncodingKeyManager,
    private_encoding_key_manager: EncodingKeyManager,
    state_file: str,
    output_dir: str = 'storage_data',
    force_s3: bool = False,
    force_huggingface: bool = False
) -> DualStorage:
    """
    Create a storage instance based on subnet configuration with optional overrides.
    
    Args:
        subtensor: Bittensor subtensor connection
        netuid: Subnet network UID
        db_path: Path to the SQLite database
        miner_hotkey: The miner's hotkey
        wallet: The miner's bittensor wallet
        encoding_key_manager: Manager for encoding usernames
        private_encoding_key_manager: Manager for encoding URLs
        state_file: Path to store the state file
        output_dir: Local directory for temporary storage
        force_s3: Force S3 to be enabled regardless of config (for testing)
        force_huggingface: Force HuggingFace to be enabled regardless of config (for testing)
        
    Returns:
        Configured DualStorage instance
    """
    # Get configuration from chain or environment
    config = create_config_from_chain_or_env(subtensor, netuid)
    
    # Apply rollout logic if enabled in chain config
    storage_config = get_storage_config_from_chain(subtensor, netuid) or {}
    rollout_percentage = storage_config.get("rollout_percentage", 100)
    
    # Make a copy of the config to modify
    modified_config = DualStorageConfig(
        s3_config=config.s3_config,
        use_s3=config.use_s3,
        use_huggingface=config.use_huggingface
    )
    
    # Apply rollout logic and overrides
    if not force_s3 and config.use_s3:
        # Only enable S3 if this miner is part of the rollout
        modified_config.use_s3 = enable_s3_storage_for_hotkey(miner_hotkey, rollout_percentage)
    elif force_s3:
        modified_config.use_s3 = True
        
    if force_huggingface:
        modified_config.use_huggingface = True
    
    bt.logging.info(f"Storage configuration: S3={modified_config.use_s3}, HF={modified_config.use_huggingface}")
    if modified_config.use_s3:
        bt.logging.info(f"S3 Bucket: {modified_config.s3_config.bucket_name}, " 
                        f"Region: {modified_config.s3_config.region}")
    
    # Create storage instance
    return DualStorage.create(
        config=modified_config,
        db_path=db_path,
        miner_hotkey=miner_hotkey,
        wallet=wallet,
        encoding_key_manager=encoding_key_manager,
        private_encoding_key_manager=private_encoding_key_manager,
        state_file=state_file,
        output_dir=output_dir
    )


def get_storage_config_from_chain(subtensor: bt.subtensor, netuid: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve storage configuration from the on-chain subnet configuration.
    
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
            return None
            
        # Decode and parse the JSON configuration
        import json
        config_str = config_bytes.decode('utf-8')
        config_dict = json.loads(config_str)
        
        # Check if storage configuration exists
        if 'storage' not in config_dict:
            return None
            
        return config_dict['storage']
        
    except Exception as e:
        bt.logging.error(f"Error retrieving storage config from chain: {str(e)}")
        return None