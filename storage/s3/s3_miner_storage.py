"""
S3 Storage implementation for miners in Data Universe.
"""
import os
import json
import datetime as dt
import time
import hashlib
import pandas as pd
import bittensor as bt
import boto3
import requests
import tempfile
from typing import List, Dict, Union, Any
from contextlib import contextmanager
import sqlite3
from functools import wraps
from common.data import HuggingFaceMetadata, DataSource
from huggingface_utils.utils import (
    preprocess_reddit_df,
    preprocess_twitter_df,
    generate_static_integer
)
from huggingface_utils.encoding_system import EncodingKeyManager
from storage.miner.miner_storage import MinerStorage
from concurrent.futures import ThreadPoolExecutor


def retry_upload(max_retries: int = 3, delay: int = 5):
    """Decorator to retry uploads on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        bt.logging.error(f"Upload failed after {max_retries} attempts. Final error: {str(e)}")
                        raise
                    bt.logging.warning(f"Upload failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                    time.sleep(delay)
        return wrapper
    return decorator


class S3MinerStorage(MinerStorage):
    """
    S3 Storage implementation for miners. Uploads data to S3 buckets using blockchain-based auth.
    """
    def __init__(self, 
                 db_path: str,
                 miner_hotkey: str,
                 wallet: Any,
                 encoding_key_manager: EncodingKeyManager,  # Used for encoding usernames
                 private_encoding_key_manager: EncodingKeyManager,  # Used for encoding URLs
                 state_file: str,
                 s3_config: Dict[str, Any],
                 output_dir: str = 'temp_s3_storage',
                 chunk_size: int = 1_000_000):
        """
        Initialize the S3 storage for miners.
        
        Args:
            db_path: Path to the SQLite database
            miner_hotkey: The miner's hotkey address
            wallet: The miner's bittensor wallet
            encoding_key_manager: Manager for encoding usernames
            private_encoding_key_manager: Manager for encoding URLs
            state_file: Path to store the state file
            s3_config: Configuration for S3 storage
            output_dir: Local directory for temporary storage
            chunk_size: Size of chunks for processing data
        """
        super().__init__()
        self.db_path = db_path
        self.miner_hotkey = miner_hotkey
        self.wallet = wallet
        self.unique_id = generate_static_integer(self.miner_hotkey)
        self.encoding_key_manager = encoding_key_manager
        self.private_encoding_key_manager = private_encoding_key_manager
        self.state_file = f"{state_file.split('.json')[0]}_{self.unique_id}.json"
        self.output_dir = os.path.join(output_dir, self.miner_hotkey)
        self.chunk_size = chunk_size
        self.wal_size_limit_mb = 2000  # 2 GB WAL size limit
        
        # S3 configuration
        self.bucket_name = s3_config.get('bucket_name')
        self.region = s3_config.get('region', 'us-east-1')
        self.auth_endpoint = s3_config.get('auth_endpoint')
        
        # Ensure output directory exists
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

    def compute_file_hash(self, file_path: str) -> str:
        """
        Compute SHA-256 hash of a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Hash of the file as a hexadecimal string
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def get_upload_credentials(self, file_path: str, source: int) -> Dict[str, Any]:
        """
        Get blockchain-authenticated credentials for uploading to S3.
        
        Args:
            file_path: Path to the file being uploaded
            source: Data source ID
            
        Returns:
            Dictionary with upload credentials
        """
        # Create a signed request using the miner's wallet
        timestamp = int(time.time())
        expiry = timestamp + 3600  # 1 hour validity
        
        # Create a hash of the file to upload for content verification
        file_hash = self.compute_file_hash(file_path)
        
        # Path in S3 where the file will be stored
        file_basename = os.path.basename(file_path)
        s3_path = f"data/{source}/{self.miner_hotkey}/{file_basename}"
        
        # Create signed message with file data & timestamp
        message = f"{self.miner_hotkey}:{s3_path}:{file_hash}:{timestamp}:{expiry}"
        signature = self.wallet.sign(message).hex()
        
        # Request credentials from the auth service
        response = requests.post(
            self.auth_endpoint,
            json={
                "hotkey": self.miner_hotkey,
                "timestamp": timestamp,
                "expiry": expiry,
                "file_hash": file_hash,
                "signature": signature,
                "s3_path": s3_path
            },
            timeout=30
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to get upload credentials: {response.text}")
            
        return response.json()

    @contextmanager
    def get_db_connection(self):
        """Get a connection to the SQLite database with optimizations."""
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        try:
            # Enhanced optimization settings
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA cache_size=-2000000")  # 2GB cache
            conn.execute("PRAGMA page_size=16384")
            conn.execute("PRAGMA mmap_size=30000000000")  # 30GB memory mapping
            yield conn
        finally:
            conn.close()

    def load_state(self) -> Dict[str, Any]:
        """Load the state file containing upload statistics."""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.loads(f.read())
                for source in state['last_upload']:
                    if state['last_upload'][source] and not isinstance(state['last_upload'][source], float):
                        try:
                            state['last_upload'][source] = dt.datetime.strptime(state['last_upload'][source],
                                                                              '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            bt.logging.warning(f"Invalid datetime format for source {source}. Setting to None.")
                            state['last_upload'][source] = None
                    else:
                        state['last_upload'][source] = None
                return state
        return {'last_upload': {'1': None, '2': None}, 'total_rows': {'1': 0, '2': 0}}

    def save_state(self, state: Dict[str, Any]) -> None:
        """Save the current state to the state file."""
        state_to_save = {
            'last_upload': {
                source: (last_upload.strftime('%Y-%m-%d %H:%M:%S') if isinstance(last_upload, dt.datetime) else None)
                for source, last_upload in state['last_upload'].items()
            },
            'total_rows': state['total_rows']
        }
        with open(self.state_file, 'w') as f:
            json.dump(state_to_save, f)

    def get_data_for_upload(self, source: int, last_upload: dt.datetime) -> pd.DataFrame:
        """
        Get data from the database for upload in chunks.
        
        Args:
            source: Data source ID
            last_upload: Timestamp of the last upload
            
        Yields:
            DataFrame chunks with data for upload
        """
        if last_upload is None:
            query = """
                SELECT datetime, label, content
                FROM DataEntity
                WHERE source = ?
                ORDER BY datetime ASC
                LIMIT 200000000
            """
            params = [source]
        else:
            query = """
                SELECT datetime, label, content
                FROM DataEntity
                WHERE source = ?
                AND datetime > ?
                ORDER BY datetime ASC
            """
            params = [source, last_upload]

        with self.get_db_connection() as conn:
            for chunk in pd.read_sql_query(
                    sql=query,
                    con=conn,
                    params=params,
                    chunksize=self.chunk_size,
                    parse_dates=['datetime']
            ):
                yield chunk

    def preprocess_data(self, df: pd.DataFrame, source: int) -> pd.DataFrame:
        """
        Preprocess data before upload based on the source.
        
        Args:
            df: DataFrame to preprocess
            source: Data source ID
            
        Returns:
            Preprocessed DataFrame
        """
        if source == DataSource.REDDIT.value:
            return preprocess_reddit_df(df, self.encoding_key_manager, self.private_encoding_key_manager)
        else:
            return preprocess_twitter_df(df, self.encoding_key_manager, self.private_encoding_key_manager)

    @retry_upload(max_retries=5)
    def upload_file_to_s3(self, file_path: str, source: int) -> str:
        """
        Upload a single file to S3 using blockchain-based auth.
        
        Args:
            file_path: Path to the local file
            source: Data source ID
            
        Returns:
            S3 path where the file was uploaded
        """
        bt.logging.info(f"Uploading {file_path} to S3...")
        
        # Get credentials for this file
        creds = self.get_upload_credentials(file_path, source)
        
        # Configure S3 client with temporary credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=creds['credentials']['AccessKeyId'],
            aws_secret_access_key=creds['credentials']['SecretAccessKey'],
            aws_session_token=creds['credentials']['SessionToken'],
            region_name=self.region
        )
        
        # Upload the file
        s3_path = creds['s3_path']
        s3_client.upload_file(
            file_path, 
            self.bucket_name,
            s3_path
        )
        
        bt.logging.info(f"Successfully uploaded {file_path} to s3://{self.bucket_name}/{s3_path}")
        return s3_path

    def upload_data_to_s3(self) -> List[Dict[str, Any]]:
        """
        Upload all new data to S3.
        
        Returns:
            List of metadata for uploaded data
        """
        state = self.load_state()
        metadata_list = []
        
        # Process both Reddit and Twitter data
        for source in [DataSource.REDDIT.value, DataSource.X.value]:
            platform = 'reddit' if source == DataSource.REDDIT.value else 'x'
            last_upload = state['last_upload'].get(str(source))
            total_rows = state['total_rows'].get(str(source), 0)
            
            bt.logging.info(f"Starting S3 upload for {platform} data")
            
            chunk_count = 0
            file_paths = []
            new_rows = 0
            
            try:
                # Process data in chunks
                for df in self.get_data_for_upload(source, last_upload):
                    if df.empty:
                        bt.logging.info(f"No new data for {platform}. Skipping.")
                        continue
                        
                    if total_rows >= 200_000_000:
                        bt.logging.info(f"Reached 200 million rows limit for {platform}. Stopping upload.")
                        break
                        
                    # Update last upload timestamp
                    last_upload = df['datetime'].max()
                    
                    # Preprocess the data
                    df = self.preprocess_data(df, source)
                    rows_to_upload = min(len(df), 200_000_000 - total_rows)
                    
                    if rows_to_upload < len(df):
                        df = df.iloc[:rows_to_upload]
                        
                    # Save chunk to parquet file
                    parquet_path = os.path.join(
                        self.output_dir, 
                        f"{platform}_data_chunk_{chunk_count}_{int(time.time())}.parquet"
                    )
                    df.to_parquet(parquet_path, index=False)
                    
                    file_paths.append(parquet_path)
                    chunk_count += 1
                    total_rows += len(df)
                    new_rows += len(df)
                    
                    # Upload in batches of 10 files
                    if chunk_count % 10 == 0:
                        self.upload_batch(file_paths, source)
                        file_paths = []
                
                # Upload any remaining files
                if file_paths:
                    self.upload_batch(file_paths, source)
                
                # Update state
                state['last_upload'][str(source)] = last_upload
                state['total_rows'][str(source)] = total_rows
                self.save_state(state)
                
                # Create metadata entry
                if new_rows > 0:
                    metadata = {
                        'source': source,
                        'rows_uploaded': new_rows,
                        'total_rows': total_rows,
                        'last_upload': last_upload.isoformat() if last_upload else None,
                        'storage_type': 's3',
                        'bucket': self.bucket_name,
                        'miner_hotkey': self.miner_hotkey,
                        'encoding_key': self.encoding_key_manager.sym_key.decode()
                    }
                    metadata_list.append(metadata)
                    
                    bt.logging.success(
                        f"Finished uploading {platform} data to S3. New rows: {new_rows}, Total: {total_rows}"
                    )
            
            except Exception as e:
                bt.logging.error(f"Error during upload for {platform}: {str(e)}")
        
        return metadata_list
        
    def upload_batch(self, file_paths: List[str], source: int) -> None:
        """
        Upload a batch of files to S3 in parallel.
        
        Args:
            file_paths: List of file paths to upload
            source: Data source ID
        """
        bt.logging.info(f"Uploading batch of {len(file_paths)} files to S3...")
        
        # Use ThreadPoolExecutor for parallel uploads
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit upload tasks to the executor
            futures = [
                executor.submit(self.upload_file_to_s3, file_path, source)
                for file_path in file_paths
            ]
            
            # Wait for all uploads to complete
            for i, future in enumerate(futures):
                try:
                    s3_path = future.result()
                    bt.logging.info(f"Batch upload {i+1}/{len(file_paths)} succeeded: {s3_path}")
                except Exception as e:
                    bt.logging.error(f"Error in batch upload {i+1}/{len(file_paths)}: {str(e)}")
        
        # Clean up local files after successful upload
        for file_path in file_paths:
            try:
                os.remove(file_path)
                bt.logging.debug(f"Removed local file: {file_path}")
            except Exception as e:
                bt.logging.warning(f"Failed to remove local file {file_path}: {str(e)}")
                
        bt.logging.info("Batch upload completed")

    def check_wal_size(self) -> float:
        """Check the size of the WAL file in MB."""
        wal_file = f"{self.db_path}-wal"
        if os.path.exists(wal_file):
            size_mb = os.path.getsize(wal_file) / (1024 * 1024)
            bt.logging.info(f"Current WAL file size: {size_mb:.2f} MB")
            return size_mb
        return 0

    def manage_wal(self, conn) -> None:
        """Checkpoint the WAL if it's too large."""
        wal_size = self.check_wal_size()
        if wal_size > self.wal_size_limit_mb:
            bt.logging.warning(f"WAL file exceeded {self.wal_size_limit_mb} MB. Performing checkpoint.")
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            new_size = self.check_wal_size()
            bt.logging.info(f"After checkpoint, WAL size: {new_size:.2f} MB")