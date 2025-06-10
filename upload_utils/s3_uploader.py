import os
import json
import datetime as dt
import pandas as pd
import bittensor as bt
import sqlite3
import time
import requests
from contextlib import contextmanager
from upload_utils.utils import generate_static_integer
from common.data import DataSource
from typing import Dict, Any, List
from functools import wraps


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


class S3Auth:
    """Enhanced S3 authentication with blockchain commitments and keypair signatures"""

    def __init__(self, s3_auth_url: str):
        self.s3_auth_url = s3_auth_url

    def get_credentials(self, wallet, source_name: str, subtensor) -> Dict[str, Any]:
        """Get S3 credentials using blockchain commitments and hotkey signature"""
        try:
            coldkey = wallet.get_coldkeypub().ss58_address
            hotkey = wallet.hotkey.ss58_address
            timestamp = int(time.time())

            commitment = f"s3:access:{coldkey}:{source_name}:{timestamp}"

            # Sign the commitment
            signature = wallet.hotkey.sign(commitment.encode())
            signature_hex = signature.hex()

            payload = {
                "coldkey": coldkey,
                "hotkey": hotkey,
                "source": source_name,
                "timestamp": timestamp,
                "signature": signature_hex
            }

            response = requests.post(
                f"{self.s3_auth_url.rstrip('/')}/get-folder-access",
                json=payload,
                timeout=30
            )

            if response.status_code != 200:
                try:
                    error_detail = response.json().get("detail", "Unknown error")
                except Exception:
                    error_detail = response.text or "Unknown error"
                bt.logging.error(f"❌ Failed to get S3 credentials: {error_detail}")
                return None

            return response.json()

        except Exception as e:
            bt.logging.error(f"❌ Error getting S3 credentials: {str(e)}")
            return None

    def upload_file_with_key(self, file_path: str, s3_key: str, creds: Dict[str, Any]) -> bool:
        """Upload file to S3 with specific key path"""
        try:
            post_data = dict(creds['fields'])  # Clone all fields
            post_data['key'] = s3_key  # Set the full S3 key path

            with open(file_path, 'rb') as f:
                files = {'file': f}
                response = requests.post(creds['url'], data=post_data, files=files)

            if response.status_code == 204:
                bt.logging.info(f"✅ Upload success: {s3_key}")
                return True
            else:
                bt.logging.error(f"❌ Upload failed: {response.status_code} — {response.text}")
                return False

        except Exception as e:
            bt.logging.error(f"❌ S3 Upload Exception for {file_path}: {e}")
            return False


class PartitionedS3Uploader:
    """
    Complete S3 uploader replacement for HuggingFace uploader
    Maintains all reliability features with partitioned structure
    """

    def __init__(self,
                 db_path: str,
                 subtensor: bt.subtensor,
                 wallet: bt.wallet,
                 s3_auth_url: str,
                 state_file: str,
                 output_dir: str = 'partitioned_storage',
                 chunk_size: int = 1_000_000,
                 upload_batch_size: int = 10):
        self.db_path = db_path
        self.wallet = wallet
        self.miner_hotkey = self.wallet.hotkey.ss58_address
        self.miner_coldkey = self.wallet.get_coldkeypub().ss58_address
        self.subtensor = subtensor
        self.unique_id = generate_static_integer(self.miner_hotkey)
        self.output_dir = os.path.join(output_dir, self.miner_hotkey)
        self.state_file = f"{state_file.split('.json')[0]}_{self.unique_id}.json"
        self.chunk_size = chunk_size
        self.upload_batch_size = upload_batch_size  # Upload every N partitions
        self.s3_auth = S3Auth(s3_auth_url)
        self.wal_size_limit_mb = 2000

        # Create output directory
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    @contextmanager
    def get_db_connection(self):
        """Get optimized database connection with same settings as HF uploader"""
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        try:
            # Enhanced optimization settings (same as original)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA cache_size=-2000000")  # 2GB
            conn.execute("PRAGMA page_size=16384")
            conn.execute("PRAGMA mmap_size=30000000000")  # 30GB memory mapping
            yield conn
        finally:
            conn.close()

    def check_wal_size(self):
        """Check WAL file size (same as original)"""
        wal_file = f"{self.db_path}-wal"
        if os.path.exists(wal_file):
            size_mb = os.path.getsize(wal_file) / (1024 * 1024)
            bt.logging.info(f"Current WAL file size: {size_mb:.2f} MB")
            return size_mb
        return 0

    def manage_wal(self, conn):
        """Manage WAL file size (same as original)"""
        wal_size = self.check_wal_size()
        if wal_size > self.wal_size_limit_mb:
            bt.logging.warning(f"WAL file exceeded {self.wal_size_limit_mb} MB. Performing checkpoint.")
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            new_size = self.check_wal_size()
            bt.logging.info(f"After checkpoint, WAL size: {new_size:.2f} MB")

    def load_state(self) -> Dict[str, Any]:
        """Load upload state with comprehensive error handling"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    content = f.read()
                    # Sanitize JSON (same as original)
                    sanitized_content = self.sanitize_json(content)
                    state = json.loads(sanitized_content)

                # Convert datetime strings back to datetime objects
                for source in state['last_upload']:
                    if state['last_upload'][source]:
                        try:
                            state['last_upload'][source] = dt.datetime.strptime(
                                state['last_upload'][source], '%Y-%m-%d %H:%M:%S'
                            )
                        except (ValueError, TypeError):
                            bt.logging.warning(f"Invalid datetime for source {source}. Resetting.")
                            state['last_upload'][source] = None
                return state
            except Exception as e:
                bt.logging.error(f"Error loading state file: {e}")

        return {
            'last_upload': {str(DataSource.REDDIT.value): None, str(DataSource.X.value): None},
            'total_rows_uploaded': {str(DataSource.REDDIT.value): 0, str(DataSource.X.value): 0},
            'upload_sessions': [],
            'failed_partitions': []
        }

    def save_state(self, state: Dict[str, Any]):
        """Save upload state with error handling"""
        try:
            state_to_save = {
                'last_upload': {
                    source: (
                        last_upload.strftime('%Y-%m-%d %H:%M:%S') if isinstance(last_upload, dt.datetime) else None)
                    for source, last_upload in state['last_upload'].items()
                },
                'total_rows_uploaded': state['total_rows_uploaded'],
                'upload_sessions': state.get('upload_sessions', []),
                'failed_partitions': state.get('failed_partitions', [])
            }
            with open(self.state_file, 'w') as f:
                json.dump(state_to_save, f, indent=2, default=str)
        except Exception as e:
            bt.logging.error(f"Error saving state: {e}")

    def sanitize_json(self, json_string: str) -> str:
        """Remove any non-printable characters from JSON string (same as original)"""
        import re
        return re.sub(r'[\x00-\x1F\x7F-\x9F]', '', json_string)

    def get_data_for_upload(self, source: int, last_upload: dt.datetime = None) -> pd.DataFrame:
        """
        Get data from database with same query logic as HF uploader
        """
        bt.logging.info(f"Fetching data for source {source}")

        if last_upload is None:
            query = """
                SELECT datetime, label, content
                FROM DataEntity
                WHERE source = ?
                ORDER BY datetime ASC
                LIMIT ?
            """
            params = [source, self.chunk_size]
        else:
            query = """
                SELECT datetime, label, content
                FROM DataEntity
                WHERE source = ? AND datetime > ?
                ORDER BY datetime ASC
                LIMIT ?
            """
            params = [source, last_upload, self.chunk_size]

        with self.get_db_connection() as conn:
            # Use chunksize like original HF uploader for memory efficiency
            df_chunks = []
            for chunk in pd.read_sql_query(
                    sql=query,
                    con=conn,
                    params=params,
                    chunksize=self.chunk_size,
                    parse_dates=['datetime']
            ):
                df_chunks.append(chunk)
                break  # Only get first chunk for now

            if df_chunks:
                df = df_chunks[0]
            else:
                df = pd.DataFrame(columns=['datetime', 'label', 'content'])

        bt.logging.info(f"Retrieved {len(df)} rows for source {source}")
        return df

    def clean_label_for_path(self, label: str, source: int) -> str:
        """Clean label for filesystem path with comprehensive sanitization"""
        if not label or label == 'NULL' or pd.isna(label):
            return 'no_label'

        clean_label = str(label).strip()

        # Source-specific cleaning
        if source == DataSource.REDDIT.value:
            # Remove r/ prefix if present
            if clean_label.lower().startswith('r/'):
                clean_label = clean_label[2:]
        elif source == DataSource.X.value:
            # Remove # prefix if present
            if clean_label.startswith('#'):
                clean_label = clean_label[1:]

        # Comprehensive filesystem sanitization
        clean_label = clean_label.replace('/', '_').replace('\\', '_')
        clean_label = clean_label.replace(' ', '_').replace('\t', '_')
        clean_label = clean_label.replace('|', '_').replace(':', '_')
        clean_label = clean_label.replace('*', '_').replace('?', '_')
        clean_label = clean_label.replace('"', '_').replace('<', '_').replace('>', '_')

        # Remove any remaining problematic characters
        clean_label = ''.join(c for c in clean_label if c.isalnum() or c in '-_.')

        # Ensure not empty and not too long
        clean_label = clean_label[:100] if clean_label else 'unknown'

        return clean_label

    def partition_by_label_and_date(self, df: pd.DataFrame, source: int) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Partition DataFrame by label and date with robust error handling
        """
        bt.logging.info(f"Partitioning {len(df)} rows by label and date")

        if df.empty:
            return {}

        try:
            # Add date column for partitioning
            df['date_partition'] = df['datetime'].dt.strftime('%Y-%m-%d')

            partitions = {}

            # Group by label and date with error handling
            for (label, date), group in df.groupby(['label', 'date_partition'], dropna=False):
                try:
                    clean_label = self.clean_label_for_path(label, source)

                    if clean_label not in partitions:
                        partitions[clean_label] = {}

                    # Store the group (excluding our temporary date_partition column)
                    partitions[clean_label][date] = group.drop('date_partition', axis=1).copy()

                except Exception as e:
                    bt.logging.warning(f"Error processing partition for label {label}, date {date}: {e}")
                    continue

            bt.logging.info(
                f"Created {len(partitions)} label partitions with {sum(len(dates) for dates in partitions.values())} date partitions")
            return partitions

        except Exception as e:
            bt.logging.error(f"Error in partitioning: {e}")
            return {}

    @retry_upload(max_retries=5)  # More retries for reliability
    def upload_partition_to_s3(self, df: pd.DataFrame, source_name: str, label: str, date: str,
                               s3_creds: Dict[str, Any]) -> bool:
        """
        Upload a single partition to S3 with comprehensive error handling
        """
        if df.empty:
            bt.logging.warning(f"Empty partition for {source_name}/{label}/{date}, skipping")
            return True

        local_path = None
        try:
            # Generate unique filename with more entropy
            timestamp = dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')[:-3]  # Include milliseconds
            filename = f"data_{timestamp}.parquet"

            # Create local parquet file
            local_path = os.path.join(self.output_dir, filename)

            # Save with compression for efficiency
            df.to_parquet(local_path, index=False, compression='snappy')

            # Verify file was created and has reasonable size
            if not os.path.exists(local_path):
                raise Exception(f"Parquet file was not created: {local_path}")

            file_size = os.path.getsize(local_path)
            if file_size == 0:
                raise Exception(f"Parquet file is empty: {local_path}")

            # Create S3 key: source/coldkey/label/date/filename
            s3_key = f"{source_name}/{self.miner_coldkey}/{label}/{date}/{filename}"

            bt.logging.info(f"Uploading {len(df)} rows ({file_size / 1024 / 1024:.2f} MB) to {s3_key}")

            # Upload using S3Auth
            success = self.s3_auth.upload_file_with_key(local_path, s3_key, s3_creds)

            if success:
                bt.logging.info(f"✅ Successfully uploaded {s3_key}")
                return True
            else:
                bt.logging.error(f"❌ Failed to upload {s3_key}")
                return False

        except Exception as e:
            bt.logging.error(f"❌ Error uploading partition {source_name}/{label}/{date}: {e}")
            return False
        finally:
            # Always clean up local file
            if local_path and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception as e:
                    bt.logging.warning(f"Could not remove local file {local_path}: {e}")

    def upload_partitions_batch(self, partitions: Dict[str, Dict[str, pd.DataFrame]], source_name: str,
                                s3_creds: Dict[str, Any]) -> int:
        """
        Upload a batch of partitions with progress tracking
        """
        total_uploaded = 0
        partition_count = 0

        for label, date_partitions in partitions.items():
            for date, partition_df in date_partitions.items():
                try:
                    success = self.upload_partition_to_s3(partition_df, source_name, label, date, s3_creds)
                    if success:
                        total_uploaded += len(partition_df)

                    partition_count += 1

                    # Progress logging
                    if partition_count % 10 == 0:
                        bt.logging.info(f"Processed {partition_count} partitions, uploaded {total_uploaded} records")

                except Exception as e:
                    bt.logging.error(f"Failed to process partition {label}/{date}: {e}")
                    continue

        return total_uploaded

    def upload_source_data(self, source: int) -> int:
        """
        Upload all data for a specific source with full reliability features
        """
        source_name = 'reddit' if source == DataSource.REDDIT.value else 'x'
        bt.logging.info(f"Starting upload for {source_name} data")

        # Get S3 credentials for this source
        s3_creds = self.s3_auth.get_credentials(
            wallet=self.wallet,
            source_name=source_name,
            subtensor=self.subtensor
        )

        if not s3_creds:
            bt.logging.error(f"Failed to get S3 credentials for {source_name}")
            return 0

        state = self.load_state()
        last_upload = state['last_upload'].get(str(source))
        total_uploaded = 0
        batch_count = 0

        # Session tracking
        session_start = dt.datetime.utcnow()
        session_id = session_start.strftime('%Y%m%d_%H%M%S')

        try:
            while True:
                # Get chunk of data
                df = self.get_data_for_upload(source, last_upload)

                if df.empty:
                    bt.logging.info(f"No more data to upload for {source_name}")
                    break

                # Partition by label and date
                partitions = self.partition_by_label_and_date(df, source)

                if not partitions:
                    bt.logging.warning(f"No valid partitions created from {len(df)} rows")
                    break

                # Upload partitions in batch
                chunk_uploaded = self.upload_partitions_batch(partitions, source_name, s3_creds)
                total_uploaded += chunk_uploaded
                batch_count += 1

                # Update state after each successful batch
                if not df.empty:
                    last_upload = df['datetime'].max()
                    state['last_upload'][str(source)] = last_upload
                    state['total_rows_uploaded'][str(source)] += chunk_uploaded

                    # Add session info
                    state['upload_sessions'].append({
                        'session_id': session_id,
                        'source': source_name,
                        'batch': batch_count,
                        'records': chunk_uploaded,
                        'timestamp': dt.datetime.utcnow().isoformat()
                    })

                    self.save_state(state)

                bt.logging.info(f"Batch {batch_count}: Uploaded {chunk_uploaded} records. Total: {total_uploaded}")

                # WAL management (same as original)
                if batch_count % 10 == 0:
                    with self.get_db_connection() as conn:
                        self.manage_wal(conn)

                # If we got less than chunk_size, we're done
                if len(df) < self.chunk_size:
                    break

        except Exception as e:
            bt.logging.error(f"Error during {source_name} upload: {e}")
            # Save error state
            state['failed_partitions'].append({
                'source': source_name,
                'error': str(e),
                'timestamp': dt.datetime.utcnow().isoformat()
            })
            self.save_state(state)
            raise

        bt.logging.success(f"Completed {source_name} upload. Total records: {total_uploaded}")
        return total_uploaded

    def run_upload(self) -> List[Dict[str, Any]]:
        """
        Run the complete partitioned upload process
        Returns metadata similar to HF uploader for compatibility
        """
        bt.logging.info("🚀 Starting partitioned S3 upload process")

        results = []
        total_records = 0

        try:
            # Upload Reddit data
            bt.logging.info("📱 Processing Reddit data...")
            reddit_records = self.upload_source_data(DataSource.REDDIT.value)
            total_records += reddit_records

            if reddit_records > 0:
                results.append({
                    'source': DataSource.REDDIT.value,
                    'records_uploaded': reddit_records,
                    'updated_at': dt.datetime.utcnow(),
                    'storage_type': 's3_partitioned'
                })

            # Upload X data
            bt.logging.info("🐦 Processing X data...")
            x_records = self.upload_source_data(DataSource.X.value)
            total_records += x_records

            if x_records > 0:
                results.append({
                    'source': DataSource.X.value,
                    'records_uploaded': x_records,
                    'updated_at': dt.datetime.utcnow(),
                    'storage_type': 's3_partitioned'
                })

            bt.logging.success(f"🎉 Upload completed successfully!")
            bt.logging.success(f"📊 Reddit: {reddit_records:,} records")
            bt.logging.success(f"📊 X: {x_records:,} records")
            bt.logging.success(f"📊 Total: {total_records:,} records")

            # Final WAL cleanup
            with self.get_db_connection() as conn:
                self.manage_wal(conn)

        except Exception as e:
            bt.logging.error(f"❌ Upload failed: {e}")
            raise

        return results


# Drop-in replacement function for existing code
def upload_sql_to_s3_partitioned(db_path: str,
                                 subtensor: bt.subtensor,
                                 wallet: bt.wallet,
                                 s3_auth_url: str,
                                 state_file: str = "upload_state.json") -> List[Dict[str, Any]]:
    """
    Drop-in replacement for upload_sql_to_huggingface function
    """
    uploader = PartitionedS3Uploader(
        db_path=db_path,
        subtensor=subtensor,
        wallet=wallet,
        s3_auth_url=s3_auth_url,
        state_file=state_file
    )

    return uploader.run_upload()


def main():
    """Example usage"""
    # This replaces your DualUploader usage
    uploader = PartitionedS3Uploader(
        db_path="path/to/database.db",
        subtensor=None,  # Your subtensor instance
        wallet=None,  # Your wallet instance
        s3_auth_url="https://your-auth-service.com",
        state_file="upload_state.json"
    )

    results = uploader.run_upload()
    print(f"Upload completed: {len(results)} sources processed")


if __name__ == "__main__":
    main()