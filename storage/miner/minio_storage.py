#!/usr/bin/env python3
"""
MinIO Test Script for Miners
Run this to test MinIO + Hive partitioning with your existing database
"""

import os
import sys
import json
import datetime as dt
import pandas as pd
import sqlite3
import subprocess
import time
from pathlib import Path
from contextlib import contextmanager
from typing import Dict, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MinIOTester:
    def __init__(self, db_path: str, miner_hotkey: str = "test_miner"):
        self.db_path = db_path
        self.miner_hotkey = miner_hotkey
        self.minio_port = 9000
        self.access_key = "minioadmin"
        self.secret_key = "minioadmin"
        self.bucket_name = "data"

        # Setup directories
        self.test_dir = os.path.join(os.getcwd(), "minio_test")
        self.minio_data_dir = os.path.join(self.test_dir, "minio_data")
        os.makedirs(self.minio_data_dir, exist_ok=True)

        self.minio_process = None
        self.minio_client = None

    @contextmanager
    def get_db_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        try:
            yield conn
        finally:
            conn.close()

    def check_database(self) -> bool:
        """Check if database exists and has data"""
        try:
            with self.get_db_connection() as conn:
                # Check if DataEntity table exists
                cursor = conn.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='DataEntity'
                """)

                if not cursor.fetchone():
                    logger.error("❌ DataEntity table not found in database")
                    return False

                # Check data count
                cursor = conn.execute("SELECT COUNT(*) FROM DataEntity")
                count = cursor.fetchone()[0]

                logger.info(f"✅ Database check passed: {count} records found")

                # Show data sources
                cursor = conn.execute("SELECT source, COUNT(*) FROM DataEntity GROUP BY source")
                sources = cursor.fetchall()

                for source, count in sources:
                    source_name = "Reddit" if source == 1 else "X/Twitter" if source == 2 else f"Source_{source}"
                    logger.info(f"   - {source_name}: {count} records")

                return count > 0

        except Exception as e:
            logger.error(f"❌ Database check failed: {e}")
            return False

    def download_minio(self) -> Optional[str]:
        """Download MinIO binary"""
        minio_binary = os.path.join(self.test_dir, "minio")

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

    def start_minio(self) -> bool:
        """Start MinIO server"""
        try:
            minio_binary = self.download_minio()
            if not minio_binary:
                return False

            cmd = [
                minio_binary,
                "server",
                self.minio_data_dir,
                "--address", f":{self.minio_port}",
                "--console-address", ":9001"
            ]

            env = os.environ.copy()
            env.update({
                'MINIO_ROOT_USER': self.access_key,
                'MINIO_ROOT_PASSWORD': self.secret_key
            })

            logger.info(f"🚀 Starting MinIO server on port {self.minio_port}...")

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
                logger.info(f"   📍 Server: http://localhost:{self.minio_port}")
                logger.info(f"   🎛️  Console: http://localhost:9001")
                logger.info(f"   🔑 Username: {self.access_key}")
                logger.info(f"   🔐 Password: {self.secret_key}")
                return True
            else:
                logger.error("❌ Failed to connect to MinIO")
                self.stop_minio()
                return False

        except Exception as e:
            logger.error(f"❌ Failed to start MinIO: {e}")
            return False

    def setup_minio_client(self) -> bool:
        """Setup MinIO client"""
        try:
            from minio import Minio

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

        except ImportError:
            logger.error("❌ minio package not installed. Run: pip install minio")
            return False
        except Exception as e:
            logger.error(f"❌ MinIO client setup failed: {e}")
            return False

    def get_sample_data(self) -> Dict[str, pd.DataFrame]:
        """Get sample data from database"""
        samples = {}

        try:
            with self.get_db_connection() as conn:
                # Get Reddit data
                reddit_query = """
                    SELECT uri, datetime, label, content, source
                    FROM DataEntity
                    WHERE source = 1
                    ORDER BY datetime DESC
                    LIMIT 10
                """

                reddit_df = pd.read_sql_query(reddit_query, conn, parse_dates=['datetime'])
                if not reddit_df.empty:
                    samples['reddit_sample'] = reddit_df
                    logger.info(f"📊 Found {len(reddit_df)} Reddit samples")

                # Get X/Twitter data
                twitter_query = """
                    SELECT uri, datetime, label, content, source
                    FROM DataEntity
                    WHERE source = 2
                    ORDER BY datetime DESC
                    LIMIT 10
                """

                twitter_df = pd.read_sql_query(twitter_query, conn, parse_dates=['datetime'])
                if not twitter_df.empty:
                    samples['twitter_sample'] = twitter_df
                    logger.info(f"🐦 Found {len(twitter_df)} Twitter/X samples")

                if not samples:
                    logger.warning("⚠️  No sample data found")

        except Exception as e:
            logger.error(f"❌ Error getting sample data: {e}")

        return samples

    def create_hive_dataframe(self, df: pd.DataFrame, job_id: str, source: int) -> pd.DataFrame:
        """Create Hive-partitioned dataframe"""
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

            # Base columns with partition keys
            hive_data = {
                'uri': df['uri'],
                'datetime': pd.to_datetime(df['datetime']),
                'label': df['label'],
                'source': df['source'].astype('int32'),
                'hotkey': self.miner_hotkey,  # Partition column
                'job_id': job_id,  # Partition column
            }

            if source == 1:  # Reddit
                hive_data.update({
                    'username': df['decoded_content'].apply(lambda x: x.get('username', '')),
                    'communityName': df['decoded_content'].apply(lambda x: x.get('communityName', '')),
                    'body': df['decoded_content'].apply(lambda x: x.get('body', '')),
                    'title': df['decoded_content'].apply(lambda x: x.get('title', '')),
                    'dataType': df['decoded_content'].apply(lambda x: x.get('dataType', ''))
                })
            else:  # X/Twitter
                hive_data.update({
                    'username': df['decoded_content'].apply(lambda x: x.get('username', '')),
                    'text': df['decoded_content'].apply(lambda x: x.get('text', '')),
                    'tweet_hashtags': df['decoded_content'].apply(lambda x: str(x.get('tweet_hashtags', []))),
                    'tweet_id': df['decoded_content'].apply(lambda x: x.get('tweet_id', ''))
                })

            result_df = pd.DataFrame(hive_data)
            logger.info(f"📝 Created Hive dataframe: {len(result_df)} rows, {len(result_df.columns)} columns")
            return result_df

        except Exception as e:
            logger.error(f"❌ Error creating Hive dataframe: {e}")
            return pd.DataFrame()

    def upload_to_minio(self, df: pd.DataFrame, job_id: str) -> bool:
        """Upload dataframe to MinIO"""
        try:
            if df.empty:
                return True

            # Create parquet file
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_{timestamp}_{len(df)}.parquet"
            temp_file = os.path.join(self.test_dir, filename)

            df.to_parquet(temp_file, index=False, compression='snappy')

            # Upload with Hive path
            minio_path = f"hotkey={self.miner_hotkey}/job_id={job_id}/{filename}"

            self.minio_client.fput_object(
                bucket_name=self.bucket_name,
                object_name=minio_path,
                file_path=temp_file
            )

            os.remove(temp_file)

            logger.info(f"✅ Uploaded {len(df)} records to: s3://{self.bucket_name}/{minio_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            return False

    def process_data(self) -> bool:
        """Process and upload sample data"""
        logger.info("🔄 Processing sample data...")

        samples = self.get_sample_data()
        if not samples:
            logger.error("❌ No sample data available")
            return False

        success_count = 0

        for job_id, df in samples.items():
            source = df['source'].iloc[0] if not df.empty else 1

            # Create Hive dataframe
            hive_df = self.create_hive_dataframe(df, job_id, source)

            # Upload to MinIO
            if self.upload_to_minio(hive_df, job_id):
                success_count += 1

        logger.info(f"✅ Processed {success_count}/{len(samples)} jobs successfully")
        return success_count > 0

    def show_query_examples(self):
        """Show DuckDB query examples"""
        logger.info("📊 DuckDB Query Examples:")
        logger.info("=" * 60)

        query_examples = f"""
# Install DuckDB (if not already installed):
pip install duckdb

# Python DuckDB example:
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")

# Configure MinIO connection
conn.execute("SET s3_endpoint='localhost:{self.minio_port}'")
conn.execute("SET s3_access_key_id='{self.access_key}'")
conn.execute("SET s3_secret_access_key='{self.secret_key}'")
conn.execute("SET s3_use_ssl=false")

# Query 1: List all data with Hive partitioning
result = conn.execute('''
    SELECT * FROM read_parquet('s3://{self.bucket_name}/*/*/*.parquet', hive_partitioning=true)
    LIMIT 5
''').fetchdf()
print(result)

# Query 2: Filter by miner
result = conn.execute('''
    SELECT hotkey, job_id, COUNT(*) as records
    FROM read_parquet('s3://{self.bucket_name}/*/*/*.parquet', hive_partitioning=true)
    WHERE hotkey = '{self.miner_hotkey}'
    GROUP BY hotkey, job_id
''').fetchdf()
print(result)

# Query 3: Get specific job data
result = conn.execute('''
    SELECT * FROM read_parquet('s3://{self.bucket_name}/*/*/*.parquet', hive_partitioning=true)
    WHERE job_id = 'reddit_sample'
    LIMIT 3
''').fetchdf()
print(result)
        """

        print(query_examples)

    def stop_minio(self):
        """Stop MinIO server"""
        try:
            if self.minio_process:
                self.minio_process.terminate()
                self.minio_process.wait(timeout=10)
                logger.info("🛑 MinIO server stopped")
        except Exception as e:
            logger.warning(f"Error stopping MinIO: {e}")

    def run_test(self) -> bool:
        """Run complete test"""
        logger.info("🧪 Starting MinIO + Hive Partitioning Test")

        try:
            # Check database
            if not self.check_database():
                return False

            # Start MinIO
            if not self.start_minio():
                return False

            # Process data
            if not self.process_data():
                logger.error("❌ Data processing failed")
                return False

            # Show examples
            self.show_query_examples()

            logger.info("🎉 Test completed successfully!")
            logger.info("💡 MinIO server is running. Press Ctrl+C to stop.")

            # Keep running for testing
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("👋 Stopping test...")

            return True

        finally:
            self.stop_minio()


def main():
    """Main function"""
    # Check if database file exists
    db_file = "SqliteMinerStorage.sqlite"

    if not os.path.exists(db_file):
        logger.error(f"❌ Database file not found: {db_file}")
        logger.info("Make sure you're running this script in the directory containing your database")
        return False

    # Create and run tester
    tester = MinIOTester(db_file)
    return tester.run_test()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)