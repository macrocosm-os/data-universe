#!/usr/bin/env python3
"""
Local MinIO Uploader for Miners
Stores data locally by job_id for direct validator querying with DuckDB
"""

import os
import json
import datetime as dt
import pandas as pd
import bittensor as bt
import sqlite3
import subprocess
import time
import signal
import atexit
from pathlib import Path
from contextlib import contextmanager
from typing import Dict, List, Optional
import logging
from minio import Minio
from minio.error import S3Error

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_dynamic_lookup() -> Dict[str, List[Dict]]:
    """Load dynamic desirability lookup from dynamic_desirability/total.json"""
    try:
        # Look for dynamic_desirability/total.json in current directory or parent directories
        current_dir = os.getcwd()
        for _ in range(3):  # Check up to 3 levels up
            total_json_path = os.path.join(current_dir, "dynamic_desirability", "total.json")
            if os.path.exists(total_json_path):
                with open(total_json_path, 'r') as f:
                    data = json.load(f)
                    logger.info(f"Loaded dynamic lookup from {total_json_path}")
                    return data
            current_dir = os.path.dirname(current_dir)

        logger.warning("dynamic_desirability/total.json not found, returning empty lookup")
        return {}
    except Exception as e:
        logger.error(f"Error loading dynamic lookup: {e}")
        return {}


class LocalMinIOUploader:
    """
    Local MinIO uploader for miners that stores data by job_id
    Enables validators to query directly with DuckDB using shared credentials
    """

    def __init__(
        self,
        db_path: str,
        miner_hotkey: str,
        state_file: str = "local_minio_state.json",
        chunk_size: int = 100_000,
        minio_port: int = 9000,
        minio_console_port: int = 9001,
        data_retention_days: int = 30
    ):
        self.db_path = db_path
        self.miner_hotkey = miner_hotkey
        self.chunk_size = chunk_size
        self.minio_port = minio_port
        self.minio_console_port = minio_console_port
        self.data_retention_days = data_retention_days
        
        # MinIO credentials (should be shared via chain for validator access)
        self.access_key = f"miner_{miner_hotkey[:8]}"
        self.secret_key = f"key_{miner_hotkey[-12:]}_secret"
        self.bucket_name = "miner-data"
        
        # Setup directories
        self.base_dir = os.path.join(os.getcwd(), "miner_storage")
        self.minio_data_dir = os.path.join(self.base_dir, "minio_data")
        os.makedirs(self.minio_data_dir, exist_ok=True)
        
        # State management
        self.state_file = os.path.join(self.base_dir, state_file)
        self.processed_state = self._load_processed_state()
        
        # MinIO process and client
        self.minio_process = None
        self.minio_client = None
        
        # Register cleanup
        atexit.register(self.cleanup)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.cleanup()

    @contextmanager
    def get_db_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA cache_size=-2000000")
            yield conn
        finally:
            conn.close()

    def _load_processed_state(self) -> Dict[str, Dict]:
        """Load processed state - tracks last processed info per job"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load processed state: {e}")
        return {}

    def _save_processed_state(self):
        """Save processed state"""
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(self.processed_state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save processed state: {e}")

    def _get_last_processed_offset(self, job_id: str) -> int:
        """Get the last processed offset for a job"""
        job_state = self.processed_state.get(job_id, {})
        return job_state.get('last_offset', 0)

    def _update_processed_state(self, job_id: str, new_offset: int, records_processed: int):
        """Update the processed state for a job"""
        if job_id not in self.processed_state:
            self.processed_state[job_id] = {}

        self.processed_state[job_id].update({
            'last_offset': new_offset,
            'total_records_processed': self.processed_state[job_id].get('total_records_processed', 0) + records_processed,
            'last_processed_time': dt.datetime.now().isoformat(),
            'processing_completed': False  # Always check for new data
        })

    def download_minio(self) -> Optional[str]:
        """Download MinIO binary"""
        minio_binary = os.path.join(self.base_dir, "minio")
        
        if os.path.exists(minio_binary):
            logger.info("✅ MinIO binary already exists")
            return minio_binary

        try:
            import urllib.request
            import stat
            import platform

            logger.info("📥 Downloading MinIO binary...")

            system = platform.system().lower()
            machine = platform.machine().lower()

            if machine in ["x86_64", "amd64"]:
                arch = "amd64"
            elif machine in ["aarch64", "arm64"]:
                arch = "arm64"
            else:
                logger.error(f"❌ Unsupported architecture: {machine}")
                return None

            if system == "windows":
                url = f"https://dl.min.io/server/minio/release/windows-{arch}/minio.exe"
                minio_binary += ".exe"
            elif system == "darwin":
                url = f"https://dl.min.io/server/minio/release/darwin-{arch}/minio"
            else:  # linux
                url = f"https://dl.min.io/server/minio/release/linux-{arch}/minio"

            urllib.request.urlretrieve(url, minio_binary)

            if system != "windows":
                os.chmod(minio_binary, stat.S_IRWXU)

            logger.info("✅ MinIO binary downloaded successfully")
            return minio_binary

        except Exception as e:
            logger.error(f"❌ Failed to download MinIO: {e}")
            return None

    def start_minio_server(self) -> bool:
        """Start MinIO server for local storage"""
        try:
            minio_binary = self.download_minio()
            if not minio_binary:
                return False

            cmd = [
                minio_binary,
                "server",
                self.minio_data_dir,
                "--address", f"0.0.0.0:{self.minio_port}",  # Listen on all interfaces for validator access
                "--console-address", f"0.0.0.0:{self.minio_console_port}"
            ]

            env = os.environ.copy()
            env.update({
                'MINIO_ROOT_USER': self.access_key,
                'MINIO_ROOT_PASSWORD': self.secret_key
            })

            logger.info(f"🚀 Starting MinIO server...")
            logger.info(f"   📍 Server: http://0.0.0.0:{self.minio_port}")
            logger.info(f"   🎛️  Console: http://0.0.0.0:{self.minio_console_port}")
            logger.info(f"   🔑 Access Key: {self.access_key}")
            logger.info(f"   🔐 Secret Key: {self.secret_key}")

            self.minio_process = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Wait for startup
            time.sleep(3)

            # Test connection
            if self.setup_minio_client():
                logger.info("✅ MinIO server started successfully!")
                return True
            else:
                logger.error("❌ Failed to connect to MinIO")
                self.stop_minio_server()
                return False

        except Exception as e:
            logger.error(f"❌ Failed to start MinIO: {e}")
            return False

    def setup_minio_client(self) -> bool:
        """Setup MinIO client"""
        try:
            self.minio_client = Minio(
                f"localhost:{self.minio_port}",
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=False
            )

            # Test connection
            self.minio_client.list_buckets()

            # Create bucket
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                logger.info(f"📁 Created bucket: {self.bucket_name}")

            return True

        except Exception as e:
            logger.error(f"❌ MinIO client setup failed: {e}")
            return False

    def _load_jobs_from_gravity(self) -> Dict[str, Dict]:
        """Load job configurations from Gravity total.json"""
        try:
            lookup = load_dynamic_lookup()
            result = {}

            if isinstance(lookup, list):
                for item in lookup:
                    if not isinstance(item, dict) or 'params' not in item:
                        continue

                    job_id = item.get('id')
                    if not job_id:
                        continue

                    params = item['params']
                    platform = params.get('platform', '').lower()
                    weight = item.get('weight', 1.0)

                    # Map platform to source integer (1=Reddit, 2=X/Twitter)
                    if platform == 'reddit':
                        source_int = 1
                    elif platform in ['x', 'twitter']:
                        source_int = 2
                    else:
                        continue

                    # Store job configuration by job_id
                    if params.get('label') and params.get('keyword') is None:
                        result[job_id] = {
                            "source": source_int,
                            "type": "label",
                            "value": params['label'],
                            "weight": weight
                        }
                    elif params.get('keyword'):
                        result[job_id] = {
                            "source": source_int,
                            "type": "keyword",
                            "value": params['keyword'],
                            "weight": weight
                        }

            logger.info(f"Loaded {len(result)} jobs from Gravity")
            return result

        except Exception as e:
            logger.error(f"Failed to load jobs from Gravity: {e}")
            return {}

    def _get_data_for_job(self, job_config: Dict, offset: int = 0) -> pd.DataFrame:
        """Get data for a specific job from SQLite"""
        source = job_config["source"]
        search_type = job_config["type"]
        value = job_config["value"]

        try:
            with self.get_db_connection() as conn:
                if search_type == "label":
                    # Normalize label for SQL query
                    normalized_label = value.lower().strip()
                    
                    if source == 1:  # Reddit
                        label_conditions = [
                            f"LOWER(label) = '{normalized_label}'",
                            f"LOWER(label) = 'r/{normalized_label.removeprefix('r/')}'",
                        ]
                    else:  # X/Twitter
                        label_conditions = [
                            f"LOWER(label) = '{normalized_label}'",
                            f"LOWER(label) = '#{normalized_label.removeprefix('#')}'",
                        ]
                    
                    label_condition_sql = " OR ".join(label_conditions)
                    
                    query = f"""
                        SELECT uri, datetime, label, content
                        FROM DataEntity
                        WHERE source = ? AND ({label_condition_sql})
                        ORDER BY datetime ASC
                        LIMIT ? OFFSET ?
                    """
                    params = [source, self.chunk_size, offset]
                    
                else:  # keyword search
                    normalized_keyword = value.lower().strip()
                    
                    if source == 1:  # Reddit
                        content_conditions = [
                            f"LOWER(JSON_EXTRACT(content, '$.body')) LIKE '%{normalized_keyword}%'",
                            f"LOWER(JSON_EXTRACT(content, '$.title')) LIKE '%{normalized_keyword}%'"
                        ]
                    else:  # X/Twitter
                        content_conditions = [
                            f"LOWER(JSON_EXTRACT(content, '$.text')) LIKE '%{normalized_keyword}%'"
                        ]
                    
                    content_condition_sql = " OR ".join(content_conditions)
                    
                    query = f"""
                        SELECT uri, datetime, label, content
                        FROM DataEntity
                        WHERE source = ? AND ({content_condition_sql})
                        ORDER BY datetime ASC
                        LIMIT ? OFFSET ?
                    """
                    params = [source, self.chunk_size, offset]

                df = pd.read_sql_query(query, conn, params=params, parse_dates=['datetime'])
                return df

        except Exception as e:
            logger.error(f"Error querying data for job: {e}")
            return pd.DataFrame()

    def _create_parquet_dataframe(self, df: pd.DataFrame, source: int) -> pd.DataFrame:
        """Create parquet-ready dataframe with decoded content"""
        if df.empty:
            return df

        try:
            def decode_content(content_bytes):
                try:
                    if isinstance(content_bytes, bytes):
                        return json.loads(content_bytes.decode('utf-8'))
                    return json.loads(content_bytes)
                except:
                    return {}

            df['decoded_content'] = df['content'].apply(decode_content)

            # Extract fields based on source type
            if source == 1:  # Reddit
                result_df = pd.DataFrame({
                    'uri': df['uri'],
                    'datetime': df['datetime'],
                    'label': df['label'],
                    'id': df['decoded_content'].apply(lambda x: x.get('id')),
                    'username': df['decoded_content'].apply(lambda x: x.get('username')),
                    'communityName': df['decoded_content'].apply(lambda x: x.get('communityName')),
                    'body': df['decoded_content'].apply(lambda x: x.get('body')),
                    'title': df['decoded_content'].apply(lambda x: x.get('title')),
                    'createdAt': df['decoded_content'].apply(lambda x: x.get('createdAt')),
                    'dataType': df['decoded_content'].apply(lambda x: x.get('dataType')),
                    'parentId': df['decoded_content'].apply(lambda x: x.get('parentId')),
                    'url': df['decoded_content'].apply(lambda x: x.get('url'))
                })
            else:  # X/Twitter
                result_df = pd.DataFrame({
                    'uri': df['uri'],
                    'datetime': df['datetime'],
                    'label': df['label'],
                    'username': df['decoded_content'].apply(lambda x: x.get('username')),
                    'text': df['decoded_content'].apply(lambda x: x.get('text')),
                    'tweet_hashtags': df['decoded_content'].apply(lambda x: x.get('tweet_hashtags', [])),
                    'timestamp': df['decoded_content'].apply(lambda x: x.get('timestamp')),
                    'url': df['decoded_content'].apply(lambda x: x.get('url')),
                    'media': df['decoded_content'].apply(lambda x: x.get('media')),
                    'user_id': df['decoded_content'].apply(lambda x: x.get('user_id')),
                    'user_display_name': df['decoded_content'].apply(lambda x: x.get('user_display_name')),
                    'user_verified': df['decoded_content'].apply(lambda x: x.get('user_verified')),
                    'tweet_id': df['decoded_content'].apply(lambda x: x.get('tweet_id')),
                    'is_reply': df['decoded_content'].apply(lambda x: x.get('is_reply')),
                    'is_quote': df['decoded_content'].apply(lambda x: x.get('is_quote')),
                    'conversation_id': df['decoded_content'].apply(lambda x: x.get('conversation_id')),
                    'in_reply_to_user_id': df['decoded_content'].apply(lambda x: x.get('in_reply_to_user_id'))
                })

            return result_df

        except Exception as e:
            logger.error(f"Error creating parquet dataframe: {e}")
            return pd.DataFrame()

    def _upload_to_minio(self, df: pd.DataFrame, job_id: str) -> bool:
        """Upload dataframe to MinIO in job-specific folder"""
        if df.empty:
            return True

        try:
            # Generate filename with timestamp and record count
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_{timestamp}_{len(df)}.parquet"
            
            # Create local temp file
            temp_dir = os.path.join(self.base_dir, "temp")
            os.makedirs(temp_dir, exist_ok=True)
            temp_file = os.path.join(temp_dir, filename)
            
            # Save to parquet
            df.to_parquet(temp_file, index=False, compression='snappy')
            
            # Upload to MinIO: job_id/filename.parquet (no hotkey prefix for local storage)
            minio_path = f"{job_id}/{filename}"
            
            self.minio_client.fput_object(
                bucket_name=self.bucket_name,
                object_name=minio_path,
                file_path=temp_file
            )
            
            # Clean up temp file
            os.remove(temp_file)
            
            logger.info(f"✅ Uploaded {len(df)} records to: {self.bucket_name}/{minio_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Upload failed for job {job_id}: {e}")
            return False

    def _process_job(self, job_id: str, job_config: Dict) -> bool:
        """Process a single job and upload to MinIO"""
        offset = self._get_last_processed_offset(job_id)
        total_processed = 0

        logger.info(f"Processing job {job_id} ({job_config['type']}: {job_config['value']}), starting from offset: {offset}")

        while True:
            # Get next chunk of data
            chunk_df = self._get_data_for_job(job_config, offset)

            if chunk_df.empty:
                logger.info(f"No new data for job {job_id}, total processed this run: {total_processed}")
                break

            # Create parquet-ready dataframe
            parquet_df = self._create_parquet_dataframe(chunk_df, job_config["source"])
            if parquet_df.empty:
                logger.warning(f"No data after processing for job {job_id}")
                break

            # Upload to MinIO
            success = self._upload_to_minio(parquet_df, job_id)
            if not success:
                logger.error(f"Failed to upload chunk for job {job_id}")
                return False

            total_processed += len(chunk_df)
            offset += len(chunk_df)

            # Update state after each successful chunk
            self._update_processed_state(job_id, offset, len(chunk_df))
            self._save_processed_state()

            # If we got less than chunk_size, we've reached the end for now
            if len(chunk_df) < self.chunk_size:
                break

        logger.info(f"Completed job {job_id}: {total_processed} records processed")
        return True

    def cleanup_old_data(self):
        """Clean up old data based on retention policy"""
        try:
            cutoff_date = dt.datetime.now() - dt.timedelta(days=self.data_retention_days)
            cutoff_prefix = cutoff_date.strftime("data_%Y%m%d")
            
            # List all objects in bucket
            objects = self.minio_client.list_objects(self.bucket_name, recursive=True)
            
            deleted_count = 0
            for obj in objects:
                # Extract date from filename
                filename = os.path.basename(obj.object_name)
                if filename.startswith("data_") and filename < cutoff_prefix:
                    try:
                        self.minio_client.remove_object(self.bucket_name, obj.object_name)
                        deleted_count += 1
                    except S3Error as e:
                        logger.warning(f"Failed to delete {obj.object_name}: {e}")
            
            if deleted_count > 0:
                logger.info(f"🗑️  Cleaned up {deleted_count} old files")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def process_all_jobs(self) -> bool:
        """Process all jobs from Gravity configuration"""
        logger.info("🔄 Starting job processing...")

        jobs = self._load_jobs_from_gravity()
        if not jobs:
            logger.warning("No jobs found from Gravity")
            return False

        overall_success = True

        for job_id, job_config in jobs.items():
            try:
                job_success = self._process_job(job_id, job_config)
                if not job_success:
                    overall_success = False
            except Exception as e:
                logger.error(f"Error processing job {job_id}: {e}")
                overall_success = False

        # Clean up old data after processing
        self.cleanup_old_data()

        logger.info("✅ Completed all job processing")
        return overall_success

    def stop_minio_server(self):
        """Stop MinIO server"""
        try:
            if self.minio_process:
                self.minio_process.terminate()
                self.minio_process.wait(timeout=10)
                logger.info("🛑 MinIO server stopped")
        except Exception as e:
            logger.warning(f"Error stopping MinIO: {e}")

    def cleanup(self):
        """Clean up resources"""
        self.stop_minio_server()

    def get_validator_connection_info(self) -> Dict:
        """Get connection info for validators to query this miner's data"""
        return {
            "miner_hotkey": self.miner_hotkey,
            "minio_endpoint": f"localhost:{self.minio_port}",  # Replace with public IP
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "bucket_name": self.bucket_name,
            "duckdb_query_example": f"""
# Connect to this miner's data
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")
conn.execute("SET s3_endpoint='localhost:{self.minio_port}'")
conn.execute("SET s3_access_key_id='{self.access_key}'")
conn.execute("SET s3_secret_access_key='{self.secret_key}'")
conn.execute("SET s3_use_ssl=false")

# Query specific job data
result = conn.execute('''
    SELECT * FROM read_parquet('s3://{self.bucket_name}/default_0/*.parquet')
    WHERE datetime >= '2024-01-01'
    LIMIT 10
''').fetchdf()
"""
        }

    def run_continuous(self, process_interval_minutes: int = 60):
        """Run continuous processing loop"""
        logger.info(f"🔄 Starting continuous processing every {process_interval_minutes} minutes")
        
        while True:
            try:
                self.process_all_jobs()
                logger.info(f"⏰ Sleeping for {process_interval_minutes} minutes...")
                time.sleep(process_interval_minutes * 60)
            except KeyboardInterrupt:
                logger.info("👋 Stopping continuous processing...")
                break
            except Exception as e:
                logger.error(f"Error in continuous loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying


def main():
    """Main function to run the miner uploader"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Local MinIO Uploader for Miners")
    parser.add_argument("--db-path", required=True, help="Path to SQLite database")
    parser.add_argument("--miner-hotkey", required=True, help="Miner hotkey")
    parser.add_argument("--minio-port", type=int, default=9000, help="MinIO server port")
    parser.add_argument("--console-port", type=int, default=9001, help="MinIO console port")
    parser.add_argument("--process-interval", type=int, default=60, help="Processing interval in minutes")
    parser.add_argument("--chunk-size", type=int, default=100000, help="Chunk size for processing")
    parser.add_argument("--retention-days", type=int, default=30, help="Data retention in days")
    parser.add_argument("--one-time", action="store_true", help="Run once instead of continuous")
    
    args = parser.parse_args()
    
    # Validate database file exists
    if not os.path.exists(args.db_path):
        logger.error(f"❌ Database file not found: {args.db_path}")
        return False
    
    # Create uploader
    uploader = LocalMinIOUploader(
        db_path=args.db_path,
        miner_hotkey=args.miner_hotkey,
        chunk_size=args.chunk_size,
        minio_port=args.minio_port,
        minio_console_port=args.console_port,
        data_retention_days=args.retention_days
    )
    
    # Start MinIO server
    if not uploader.start_minio_server():
        logger.error("❌ Failed to start MinIO server")
        return False
    
    try:
        if args.one_time:
            # Run once
            success = uploader.process_all_jobs()
            logger.info("📊 Validator connection info:")
            connection_info = uploader.get_validator_connection_info()
            print(json.dumps(connection_info, indent=2))
            return success
        else:
            # Run continuously
            uploader.run_continuous(args.process_interval)
            return True
            
    except KeyboardInterrupt:
        logger.info("👋 Shutting down...")
        return True
    finally:
        uploader.cleanup()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)