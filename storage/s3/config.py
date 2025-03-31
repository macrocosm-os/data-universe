"""
Configuration classes for S3 storage in Data Universe.
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class S3Config:
    """
    Configuration for S3 storage.
    """
    # Required settings
    bucket_name: str
    auth_endpoint: str
    
    # Optional settings with defaults
    region: str = "us-east-1"
    use_s3: bool = True
    
    # Optional advanced settings
    endpoint_url: Optional[str] = None  # For S3-compatible storage (e.g., MinIO)
    max_chunk_size: int = 1_000_000  # 1M rows per chunk
    max_thread_workers: int = 5  # Maximum threads for parallel uploads
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'S3Config':
        """
        Create an S3Config from a dictionary.
        """
        return cls(
            bucket_name=config_dict.get('bucket_name'),
            auth_endpoint=config_dict.get('auth_endpoint'),
            region=config_dict.get('region', 'us-east-1'),
            use_s3=config_dict.get('use_s3', True),
            endpoint_url=config_dict.get('endpoint_url'),
            max_chunk_size=config_dict.get('max_chunk_size', 1_000_000),
            max_thread_workers=config_dict.get('max_thread_workers', 5)
        )


@dataclass
class DualStorageConfig:
    """
    Configuration for dual storage (both HuggingFace and S3).
    """
    s3_config: S3Config
    use_s3: bool = True
    use_huggingface: bool = True
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DualStorageConfig':
        """
        Create a DualStorageConfig from a dictionary.
        """
        s3_config = S3Config.from_dict(config_dict.get('s3', {}))
        return cls(
            s3_config=s3_config,
            use_s3=config_dict.get('use_s3', True),
            use_huggingface=config_dict.get('use_huggingface', True)
        )