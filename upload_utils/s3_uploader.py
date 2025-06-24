"""
S3 Partitioned Uploader for Dynamic Desirability data using Job IDs from Gravity
Uploads data using exact job IDs from Gravity as folder names:
hotkey={hotkey_id}/job_id={job_id}/parquet_files

NO ENCODING - Raw data upload to S3
Uses offset-based tracking for continuous processing of new data
"""

import os
import json
import datetime as dt
import pandas as pd
import bittensor as bt
import sqlite3
import re
from contextlib import contextmanager
from typing import List, Dict
from upload_utils.s3_utils import S3Auth
from common.data import DataSource


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
                    bt.logging.info(f"Loaded dynamic lookup from {total_json_path}")
                    return data
            current_dir = os.path.dirname(current_dir)

        bt.logging.warning("dynamic_desirability/total.json not found, returning empty lookup")
        return {}
    except Exception as e:
        bt.logging.error(f"Error loading dynamic lookup: {e}")
        return {}


class S3PartitionedUploader:
    """
    S3 Partitioned uploader using job IDs from Gravity.
    Handles both label matching and keyword text search.
    """

    def __init__(
        self,
        db_path: str,
        subtensor,
        wallet,
        s3_auth_url: str,
        state_file: str,
        output_dir: str = 's3_partitioned_storage',
        chunk_size: int = 1_000_000,
    ):
        self.db_path = db_path
        self.wallet = wallet
        self.subtensor = subtensor
        self.miner_hotkey = self.wallet.hotkey.ss58_address
        self.s3_auth = S3Auth(s3_auth_url)
        self.state_file = f"{state_file.split('.json')[0]}_s3_partitioned.json"
        self.output_dir = os.path.join(output_dir, self.miner_hotkey)
        self.chunk_size = chunk_size

        # Load processed state - tracks last processed info per job
        self.processed_state = self._load_processed_state()

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
                bt.logging.warning(f"Failed to load processed state: {e}")
        return {}

    def _save_processed_state(self):
        """Save processed state"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.processed_state, f, indent=2)
        except Exception as e:
            bt.logging.error(f"Failed to save processed state: {e}")

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
            'processing_completed': False  # Never mark as "completed" - always check for new data
        })

    def _load_dd_list(self) -> Dict[str, Dict]:
        """Load job configurations from Gravity and return by job_id"""
        try:
            lookup = load_dynamic_lookup()
            result = {}

            if isinstance(lookup, list):
                for item in lookup:
                    if not isinstance(item, dict) or 'params' not in item:
                        continue

                    job_id = item.get('id')  # Use the job_id from Gravity
                    if not job_id:
                        continue

                    params = item['params']
                    platform = params.get('platform', '').lower()
                    weight = item.get('weight', 1.0)

                    # Map platform to source integer
                    if platform == 'reddit':
                        source_int = DataSource.REDDIT.value
                    elif platform in ['x', 'twitter']:
                        source_int = DataSource.X.value
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

            # Handle old format for backward compatibility
            elif isinstance(lookup, dict):
                bt.logging.warning("Old DD format detected, consider updating to new job-based format")
                # Legacy handling code can go here if needed

            bt.logging.info(f"Loaded {len(result)} jobs from Gravity")
            return result

        except Exception as e:
            bt.logging.error(f"Failed to load jobs from Gravity: {e}")
            return {}

    def _get_label_data(self, source: int, label: str, offset: int = 0) -> pd.DataFrame:
        """Get data matching exact label"""
        # Normalize label for SQL query
        normalized_label = label.lower().strip()

        # Build label conditions based on source
        if source == DataSource.REDDIT.value:
            # For Reddit: check both with and without r/ prefix
            label_conditions = [
                f"LOWER(label) = '{normalized_label}'",
                f"LOWER(label) = 'r/{normalized_label.removeprefix('r/')}'",
            ]
        else:
            # For X: check hashtags with and without #
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

        try:
            with self.get_db_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params, parse_dates=['datetime'])

            bt.logging.debug(f"Found {len(df)} records for label '{label}' in source {source} (offset: {offset})")
            return df

        except Exception as e:
            bt.logging.error(f"Error querying label data for {source}/{label}: {e}")
            return pd.DataFrame()

    def _get_keyword_data_chunk(self, source: int, keyword: str, offset: int = 0) -> pd.DataFrame:
        """Get chunk of data where keyword appears in text content"""
        # Normalize keyword
        normalized_keyword = keyword.lower().strip()

        # Build content search conditions - focus on main text fields
        if source == DataSource.REDDIT.value:
            # Search Reddit body field specifically
            content_conditions = [
                f"LOWER(JSON_EXTRACT(content, '$.body')) LIKE '%{normalized_keyword}%'",
                f"LOWER(JSON_EXTRACT(content, '$.title')) LIKE '%{normalized_keyword}%'"
            ]
        else:
            # Search X text field specifically
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

        try:
            with self.get_db_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params, parse_dates=['datetime'])

            bt.logging.debug(f"Found {len(df)} records for keyword '{keyword}' in source {source} (offset: {offset})")
            return df

        except Exception as e:
            bt.logging.error(f"Error querying keyword data for {source}/{keyword}: {e}")
            return pd.DataFrame()

    def _create_raw_dataframe(self, df: pd.DataFrame, source: int) -> pd.DataFrame:
        """Create raw dataframe with decoded content - NO ENCODING"""
        if df.empty:
            return df

        try:
            # Decode content field to get the raw JSON data
            def decode_content(content_bytes):
                try:
                    if isinstance(content_bytes, bytes):
                        return json.loads(content_bytes.decode('utf-8'))
                    return json.loads(content_bytes)
                except:
                    return {}

            df['decoded_content'] = df['content'].apply(decode_content)

            # Extract fields based on source type
            if source == DataSource.REDDIT.value:
                # Reddit data structure
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
            else:
                # X/Twitter data structure
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
            bt.logging.error(f"Error creating raw dataframe: {e}")
            return pd.DataFrame()

    def _upload_data_chunk(self, df: pd.DataFrame, source: int, job_id: str, s3_creds: Dict) -> bool:
        """Upload chunk directly to job_id folder (no source or date partitioning)"""
        if df.empty:
            return True

        try:
            raw_df = self._create_raw_dataframe(df, source)
            if raw_df.empty:
                bt.logging.warning(f"No data after raw processing for job {job_id}")
                return True

            # Create local parquet file
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir, exist_ok=True)

            # Generate filename with timestamp and record count
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_{timestamp}_{len(raw_df)}.parquet"
            local_path = os.path.join(self.output_dir, filename)

            # Save to parquet
            raw_df.to_parquet(local_path, index=False)

            # Create S3 path: hotkey={hotkey_id}/job_id={job_id}/{filename}.parquet
            s3_path = f"job_id={job_id}/{filename}"

            # Upload to S3
            upload_success = self.s3_auth.upload_file_with_path(local_path, s3_path, s3_creds)

            if upload_success:
                bt.logging.success(f"Uploaded {len(raw_df)} records to job {job_id}")
            else:
                bt.logging.error(f"Failed to upload {len(raw_df)} records to job {job_id}")

            # Clean up local file
            if os.path.exists(local_path):
                os.remove(local_path)

            return upload_success

        except Exception as e:
            bt.logging.error(f"Error uploading data chunk for job {job_id}: {e}")
            return False

    def _process_job(self, job_id: str, job_config: Dict, s3_creds: Dict) -> bool:
        """Process a single job using exact job_id as folder name"""
        source = job_config["source"]
        search_type = job_config["type"]
        value = job_config["value"]

        offset = self._get_last_processed_offset(job_id)

        bt.logging.info(f"Processing job {job_id} ({search_type}: {value}), starting from offset: {offset}")

        total_processed = 0

        while True:
            # Get next chunk based on search type
            if search_type == "label":
                chunk_df = self._get_label_data(source, value, offset)
            else:  # keyword
                chunk_df = self._get_keyword_data_chunk(source, value, offset)

            if chunk_df.empty:
                bt.logging.info(f"No new data for job {job_id}, total processed this run: {total_processed}")
                break

            # Upload chunk using job_id as folder name
            success = self._upload_data_chunk(chunk_df, source, job_id, s3_creds)
            if not success:
                bt.logging.error(f"Failed to upload chunk for job {job_id}")
                return False

            total_processed += len(chunk_df)
            offset += len(chunk_df)

            # Update state after each successful chunk
            self._update_processed_state(job_id, offset, len(chunk_df))
            self._save_processed_state()

            bt.logging.info(f"Processed {total_processed} new records for job {job_id}")

            # If we got less than chunk_size, we've reached the end for now
            if len(chunk_df) < self.chunk_size:
                break

        bt.logging.info(f"Completed job {job_id}: {total_processed} records processed")
        return True

    def upload_dd_data(self) -> bool:
        """Main method to upload data using job_ids from Gravity"""
        bt.logging.info("Starting S3 upload using Gravity job IDs")

        try:
            # Load jobs by job_id (from Gravity via total.json)
            jobs = self._load_dd_list()
            if not jobs:
                bt.logging.warning("No jobs found from Gravity")
                return False

            # Get credentials once for all jobs (no source-specific credentials needed)
            bt.logging.info("Getting S3 credentials for all job uploads")
            s3_creds = self.s3_auth.get_credentials(
                subtensor=self.subtensor,
                wallet=self.wallet,
            )

            if not s3_creds:
                bt.logging.error("Failed to get S3 credentials")
                return False

            bt.logging.success("Got S3 credentials for all jobs")

            overall_success = True

            # Process each job using the same credentials
            for job_id, job_config in jobs.items():
                bt.logging.info(f"Processing job: {job_id}")

                job_success = self._process_job(job_id, job_config, s3_creds)
                if not job_success:
                    overall_success = False

            bt.logging.info("Completed S3 upload using job IDs")
            return overall_success

        except Exception as e:
            bt.logging.error(f"Error in job-based S3 upload: {e}")
            return False