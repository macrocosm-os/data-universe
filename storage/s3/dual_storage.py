"""
Dual Storage implementation that supports both HuggingFace and S3 storage.
"""
import bittensor as bt
from typing import List, Dict, Any, Optional

from huggingface_utils.huggingface_uploader import HuggingFaceUploader
from storage.s3.s3_miner_storage import S3MinerStorage
from storage.miner.miner_storage import MinerStorage
from storage.s3.config import DualStorageConfig


class DualStorage(MinerStorage):
    """
    Storage implementation that supports both HuggingFace and S3.
    
    This class provides a migration path from HuggingFace to S3 storage,
    allowing for data to be stored in both systems during the transition.
    """
    
    def __init__(self, config: DualStorageConfig, 
                 hf_uploader: Optional[HuggingFaceUploader] = None,
                 s3_storage: Optional[S3MinerStorage] = None):
        """
        Initialize dual storage with both storage providers.
        
        Args:
            config: Configuration for dual storage
            hf_uploader: Optional HuggingFace uploader instance
            s3_storage: Optional S3 storage instance
        """
        super().__init__()
        self.config = config
        self.hf_uploader = hf_uploader
        self.s3_storage = s3_storage
        
        self.use_huggingface = config.use_huggingface
        self.use_s3 = config.use_s3
        
        bt.logging.info(f"Initialized DualStorage with HuggingFace: {self.use_huggingface}, S3: {self.use_s3}")
        
    def upload_data(self) -> List[Dict[str, Any]]:
        """
        Upload data to enabled storage systems.
        
        Returns:
            Combined list of metadata from all storage systems
        """
        metadata = []
        
        # Upload to S3 if enabled
        if self.use_s3 and self.s3_storage:
            bt.logging.info("Uploading data to S3 storage")
            try:
                s3_metadata = self.s3_storage.upload_data_to_s3()
                metadata.extend(s3_metadata)
                bt.logging.success(f"Successfully uploaded data to S3: {len(s3_metadata)} datasets")
            except Exception as e:
                bt.logging.error(f"Error uploading to S3: {str(e)}")
        
        # Upload to HuggingFace if enabled
        if self.use_huggingface and self.hf_uploader:
            bt.logging.info("Uploading data to HuggingFace")
            try:
                hf_metadata = self.hf_uploader.upload_sql_to_huggingface()
                metadata.extend(hf_metadata)
                bt.logging.success(f"Successfully uploaded data to HuggingFace: {len(hf_metadata)} datasets")
            except Exception as e:
                bt.logging.error(f"Error uploading to HuggingFace: {str(e)}")
        
        return metadata
    
    @classmethod
    def create(cls, 
               config: DualStorageConfig, 
               db_path: str,
               miner_hotkey: str,
               wallet: Any,
               encoding_key_manager: Any,
               private_encoding_key_manager: Any,
               state_file: str,
               output_dir: str = 'storage_data') -> 'DualStorage':
        """
        Factory method to create a DualStorage instance with appropriate uploader instances.
        
        Args:
            config: Dual storage configuration
            db_path: Path to the SQLite database
            miner_hotkey: The miner's hotkey
            wallet: The miner's bittensor wallet
            encoding_key_manager: Manager for encoding usernames
            private_encoding_key_manager: Manager for encoding URLs
            state_file: Path to store the state file
            output_dir: Local directory for temporary storage
            
        Returns:
            Configured DualStorage instance
        """
        hf_uploader = None
        s3_storage = None
        
        if config.use_huggingface:
            hf_uploader = HuggingFaceUploader(
                db_path=db_path,
                miner_hotkey=miner_hotkey,
                encoding_key_manager=encoding_key_manager,
                private_encoding_key_manager=private_encoding_key_manager,
                state_file=f"{state_file}_hf",
                output_dir=f"{output_dir}/hf"
            )
        
        if config.use_s3:
            s3_storage = S3MinerStorage(
                db_path=db_path,
                miner_hotkey=miner_hotkey,
                wallet=wallet,
                encoding_key_manager=encoding_key_manager,
                private_encoding_key_manager=private_encoding_key_manager,
                state_file=f"{state_file}_s3",
                output_dir=f"{output_dir}/s3",
                s3_config={
                    'bucket_name': config.s3_config.bucket_name,
                    'region': config.s3_config.region,
                    'auth_endpoint': config.s3_config.auth_endpoint,
                    'endpoint_url': config.s3_config.endpoint_url
                }
            )
        
        return cls(
            config=config,
            hf_uploader=hf_uploader,
            s3_storage=s3_storage
        )