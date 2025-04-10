"""
S3 Storage implementation for validators in Data Universe.
"""
import os
import json
import hashlib
import time
import tempfile
import boto3
import requests
import pandas as pd
import bittensor as bt
from typing import List, Dict, Any, Optional, Tuple
from common.data import DataSource, DataEntity, DataLabel
from common.date_range import DateRange
from storage.validator.validator_storage import ValidatorStorage


class S3ValidatorStorage(ValidatorStorage):
    """
    S3 Storage implementation for validators. Reads data from S3 using blockchain-based authentication.
    """
    
    def __init__(self, config: Any, validator_wallet: Any):
        """
        Initialize S3 storage for validators.
        
        Args:
            config: Configuration containing S3 settings
            validator_wallet: The validator's bittensor wallet
        """
        self.config = config
        self.wallet = validator_wallet
        self.hotkey = validator_wallet.hotkey.ss58_address
        
        # S3 configuration
        self.bucket_name = config.s3_bucket_name
        self.region = config.s3_region
        self.auth_endpoint = config.s3_auth_endpoint
        
        # Cache for credentials to reduce auth requests
        self._credentials_cache = {}
        self._cache_expiry = {}
    
    def get_read_credentials(self, miner_hotkey: str) -> Dict[str, Any]:
        """
        Get blockchain-authenticated credentials for reading from S3.
        
        Args:
            miner_hotkey: The miner's hotkey address to access data from
            
        Returns:
            Dictionary with AWS credentials
        """
        # Check cache first
        cache_key = f"read:{miner_hotkey}"
        current_time = int(time.time())
        
        if cache_key in self._credentials_cache and self._cache_expiry.get(cache_key, 0) > current_time:
            return self._credentials_cache[cache_key]
        
        # Create authentication request
        timestamp = current_time
        expiry = timestamp + 3600  # 1 hour validity
        
        # Create signed message
        message = f"read:{self.hotkey}:{miner_hotkey}:{timestamp}:{expiry}"
        signature = self.wallet.sign(message).hex()
        
        # Request credentials from the auth service
        response = requests.post(
            self.auth_endpoint,
            json={
                "action": "read",
                "validator_hotkey": self.hotkey,
                "miner_hotkey": miner_hotkey,
                "timestamp": timestamp,
                "expiry": expiry,
                "signature": signature
            },
            timeout=30
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to get read credentials: {response.text}")
        
        # Cache the credentials
        credentials = response.json()['credentials']
        self._credentials_cache[cache_key] = credentials
        self._cache_expiry[cache_key] = expiry - 300  # Expire 5 minutes before actual expiry
        
        return credentials
    
    def _get_s3_client(self, credentials: Dict[str, str]) -> Any:
        """
        Get a configured S3 client using the provided credentials.
        
        Args:
            credentials: AWS credentials from get_read_credentials
            
        Returns:
            Configured boto3 S3 client
        """
        return boto3.client(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            region_name=self.region
        )
    
    def read_miner_files(self, miner_hotkey: str, source: int, max_files: int = 10) -> List[str]:
        """
        List parquet files available for a miner.
        
        Args:
            miner_hotkey: The miner's hotkey
            source: Data source ID
            max_files: Maximum number of files to list
            
        Returns:
            List of file keys in S3
        """
        credentials = self.get_read_credentials(miner_hotkey)
        s3 = self._get_s3_client(credentials)
        
        # List objects in the miner's folder for this source
        prefix = f"data/{source}/{miner_hotkey}/"
        
        try:
            response = s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
                
            # Sort by last modified (newest first)
            sorted_files = sorted(
                response['Contents'],
                key=lambda x: x['LastModified'],
                reverse=True
            )
            
            # Return the S3 keys
            return [obj['Key'] for obj in sorted_files[:max_files]]
            
        except Exception as e:
            bt.logging.error(f"Error listing files for miner {miner_hotkey}: {str(e)}")
            return []
    
    def read_miner_data(self, miner_hotkey: str, source: int, 
                        date_range: Optional[DateRange] = None, 
                        max_entities: int = 100,
                        labels: Optional[List[DataLabel]] = None) -> List[DataEntity]:
        """
        Read data entities from a miner's S3 storage.
        
        Args:
            miner_hotkey: The miner's hotkey
            source: Data source ID
            date_range: Optional date range to filter data
            max_entities: Maximum number of entities to return
            labels: Optional list of labels to filter by
            
        Returns:
            List of DataEntity objects
        """
        credentials = self.get_read_credentials(miner_hotkey)
        s3 = self._get_s3_client(credentials)
        
        # Get files for this miner
        files = self.read_miner_files(miner_hotkey, source)
        if not files:
            return []
        
        entities = []
        
        # Process each file until we have enough entities
        for file_key in files:
            if len(entities) >= max_entities:
                break
                
            with tempfile.NamedTemporaryFile(suffix='.parquet') as temp:
                try:
                    # Download the file
                    s3.download_file(self.bucket_name, file_key, temp.name)
                    
                    # Read the parquet file
                    df = pd.read_parquet(temp.name)
                    
                    # Apply date filtering if needed
                    if date_range:
                        df = df[
                            (df['datetime'] >= date_range.start) & 
                            (df['datetime'] <= date_range.end)
                        ]
                    
                    # Apply label filtering if needed
                    if labels:
                        label_values = [label.value for label in labels]
                        df = df[df['label'].isin(label_values)]
                    
                    # Convert rows to DataEntity objects
                    for _, row in df.iterrows():
                        if len(entities) >= max_entities:
                            break
                            
                        # Assuming URL decoding is done here if needed
                        entity = self._row_to_data_entity(row, source)
                        if entity:
                            entities.append(entity)
                            
                except Exception as e:
                    bt.logging.error(f"Error processing file {file_key}: {str(e)}")
                    continue
        
        return entities
    
    def _row_to_data_entity(self, row: pd.Series, source: int) -> Optional[DataEntity]:
        """
        Convert a dataframe row to a DataEntity.
        
        Args:
            row: The pandas Series containing entity data
            source: Data source ID
            
        Returns:
            DataEntity object or None if conversion fails
        """
        try:
            return DataEntity(
                uri=row.get('url') or row.get('uri'),
                datetime=row['datetime'],
                source=source,
                label=DataLabel(value=row['label']) if row['label'] != 'NULL' else None,
                content=row['content'].encode('utf-8') if isinstance(row['content'], str) else row['content'],
                content_size_bytes=len(row['content']) if isinstance(row['content'], str) else len(str(row['content']))
            )
        except Exception as e:
            bt.logging.error(f"Error converting row to DataEntity: {str(e)}")
            return None
    
    def validate_miner_data(self, miner_hotkey: str, entity_uris: List[str]) -> Dict[str, bool]:
        """
        Validate that a miner has the specified data entities.
        
        Args:
            miner_hotkey: The miner's hotkey
            entity_uris: List of URIs to validate
            
        Returns:
            Dictionary mapping URIs to validation results (True if found)
        """
        # Implementation approach:
        # 1. First check Reddit entities, then X entities
        # 2. For each source, get miner data
        # 3. Check if URIs are present
        
        results = {uri: False for uri in entity_uris}
        
        for source in [DataSource.REDDIT.value, DataSource.X.value]:
            credentials = self.get_read_credentials(miner_hotkey)
            s3 = self._get_s3_client(credentials)
            files = self.read_miner_files(miner_hotkey, source)
            
            if not files:
                continue
                
            # Download and check each file
            for file_key in files:
                with tempfile.NamedTemporaryFile(suffix='.parquet') as temp:
                    try:
                        # Download the file
                        s3.download_file(self.bucket_name, file_key, temp.name)
                        
                        # Read the parquet file
                        df = pd.read_parquet(temp.name)
                        
                        # Check for matches by URL/URI
                        for uri in entity_uris:
                            if results[uri]:  # Already found
                                continue
                                
                            # Check in both url and uri columns if they exist
                            if 'url' in df.columns and uri in df['url'].values:
                                results[uri] = True
                            elif 'uri' in df.columns and uri in df['uri'].values:
                                results[uri] = True
                                
                    except Exception as e:
                        bt.logging.error(f"Error checking file {file_key}: {str(e)}")
                        continue
                        
                # If all URIs are found, we can stop
                if all(results.values()):
                    break
        
        return results
    
    def get_miner_stats(self, miner_hotkey: str) -> Dict[str, Any]:
        """
        Get statistics about a miner's data.
        
        Args:
            miner_hotkey: The miner's hotkey
            
        Returns:
            Dictionary with statistics about the miner's data
        """
        stats = {
            "miner_hotkey": miner_hotkey,
            "sources": {}
        }
        
        for source_id, source_name in [
            (DataSource.REDDIT.value, "reddit"),
            (DataSource.X.value, "twitter")
        ]:
            credentials = self.get_read_credentials(miner_hotkey)
            s3 = self._get_s3_client(credentials)
            
            # List all files for this source
            prefix = f"data/{source_id}/{miner_hotkey}/"
            
            try:
                response = s3.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=prefix
                )
                
                if 'Contents' not in response:
                    stats["sources"][source_name] = {
                        "file_count": 0,
                        "total_size_bytes": 0,
                        "last_modified": None
                    }
                    continue
                
                files = response['Contents']
                
                # Calculate statistics
                file_count = len(files)
                total_size_bytes = sum(obj['Size'] for obj in files)
                last_modified = max(obj['LastModified'] for obj in files) if files else None
                
                stats["sources"][source_name] = {
                    "file_count": file_count,
                    "total_size_bytes": total_size_bytes,
                    "last_modified": last_modified.isoformat() if last_modified else None
                }
                
            except Exception as e:
                bt.logging.error(f"Error getting stats for miner {miner_hotkey}, source {source_name}: {str(e)}")
                stats["sources"][source_name] = {
                    "file_count": 0,
                    "total_size_bytes": 0,
                    "last_modified": None,
                    "error": str(e)
                }
        
        return stats