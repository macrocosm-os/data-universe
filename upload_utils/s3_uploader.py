"""
S3 Partitioned Uploader for Dynamic Desirability data using Job IDs from Gravity.

Each cycle, for every job, queries all matching rows from the miner's local
SQLite (capped at max_rows) and uploads them as a single parquet file under
hotkey={hotkey}/job_id={job_id}/. The validator keeps only the newest file
per job_id, so the snapshot model matches its semantics directly.

Upload flow: per-file presigned URL via POST /get-file-upload-url.
"""

import os
import json
import datetime as dt
import pandas as pd
import bittensor as bt
import sqlite3
import secrets
from contextlib import contextmanager
from typing import List, Dict
from common.api_client import DataUniverseApiClient
from common.data import DataSource


def _to_iso_string(v):
    """Coerce scraped_at-style values to ISO strings.

    PyArrow infers parquet dtype from values: a chunk of pure ISO strings
    becomes string, a chunk containing any datetime object becomes
    timestamp[ns]. Forcing strings here keeps the column type stable
    across miners and chunks.
    """
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.isoformat()
    return str(v)


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
    Each cycle re-snapshots the whole job into a single parquet file.
    """

    # Fallback row cap when a job config doesn't specify max_rows.
    DEFAULT_JOB_MAX_ROWS = 2_000_000

    def __init__(
        self,
        db_path: str,
        subtensor,
        wallet,
        s3_auth_url: str,
        state_file: str,
        output_dir: str = 's3_partitioned_storage',
    ):
        self.db_path = db_path
        self.wallet = wallet
        self.subtensor = subtensor
        self.miner_hotkey = self.wallet.hotkey.ss58_address
        self.api_base_url = s3_auth_url
        self.keypair = wallet.hotkey
        self.state_file = f"{state_file.split('.json')[0]}_s3_partitioned.json"
        self.output_dir = os.path.join(output_dir, self.miner_hotkey)

        # Load processed state - records last snapshot stats per job (for logging).
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
                    max_rows = item.get('max_rows')

                    if params.get('label') and params.get('keyword'):
                        # Both label and keyword required
                        result[job_id] = {
                            "source": source_int,
                            "type": "label_and_keyword",
                            "label": params['label'],
                            "keyword": params['keyword'],
                            "weight": weight,
                            "max_rows": max_rows,
                        }
                    elif params.get('label'):
                        # Label only
                        result[job_id] = {
                            "source": source_int,
                            "type": "label",
                            "value": params['label'],
                            "weight": weight,
                            "max_rows": max_rows,
                        }
                    elif params.get('keyword'):
                        # Keyword only
                        result[job_id] = {
                            "source": source_int,
                            "type": "keyword",
                            "value": params['keyword'],
                            "weight": weight,
                            "max_rows": max_rows,
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

    @staticmethod
    def _build_label_conditions(source: int, label: str) -> str:
        """Source-aware SQL WHERE fragment matching ``label`` (joined by OR)."""
        normalized = label.lower().strip()
        if source == DataSource.REDDIT.value:
            parts = [
                f"LOWER(label) = '{normalized}'",
                f"LOWER(label) = 'r/{normalized.removeprefix('r/')}'",
            ]
        else:
            without_hash = normalized.lstrip('#')
            with_hash = f"#{without_hash}"
            parts = [
                f"EXISTS (SELECT 1 FROM json_each(content, '$.tweet_hashtags') WHERE LOWER(value) = '{with_hash}')",
                f"EXISTS (SELECT 1 FROM json_each(content, '$.tweet_hashtags') WHERE LOWER(value) = '{without_hash}')",
                f"LOWER(label) = '{with_hash}'",
                f"LOWER(label) = '{without_hash}'",
            ]
        return " OR ".join(parts)

    @staticmethod
    def _build_keyword_conditions(source: int, keyword: str) -> str:
        """Source-aware SQL WHERE fragment matching ``keyword`` in text fields (joined by OR)."""
        normalized = keyword.lower().strip()
        if source == DataSource.REDDIT.value:
            parts = [
                f"LOWER(JSON_EXTRACT(content, '$.body')) LIKE '%{normalized}%'",
                f"LOWER(JSON_EXTRACT(content, '$.title')) LIKE '%{normalized}%'",
            ]
        else:
            parts = [f"LOWER(JSON_EXTRACT(content, '$.text')) LIKE '%{normalized}%'"]
        return " OR ".join(parts)

    def _get_label_data(self, source: int, label: str, limit: int) -> pd.DataFrame:
        """Get all rows matching exact label, capped at ``limit``."""
        label_condition_sql = self._build_label_conditions(source, label)

        query = f"""
            SELECT uri, datetime, label, content
            FROM DataEntity
            WHERE source = ? AND ({label_condition_sql})
            ORDER BY datetime ASC, uri ASC
            LIMIT ?
        """
        params = [source, limit]

        try:
            with self.get_db_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params, parse_dates=['datetime'])

            bt.logging.debug(f"Found {len(df)} records for label '{label}' in source {source}")
            return df

        except Exception as e:
            bt.logging.error(f"Error querying label data for {source}/{label}: {e}")
            return pd.DataFrame()

    def _get_keyword_data(self, source: int, keyword: str, limit: int) -> pd.DataFrame:
        """Get all rows where the keyword appears in text content, capped at ``limit``."""
        content_condition_sql = self._build_keyword_conditions(source, keyword)

        query = f"""
            SELECT uri, datetime, label, content
            FROM DataEntity
            WHERE source = ? AND ({content_condition_sql})
            ORDER BY datetime ASC, uri ASC
            LIMIT ?
        """
        params = [source, limit]

        try:
            with self.get_db_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params, parse_dates=['datetime'])

            bt.logging.debug(f"Found {len(df)} records for keyword '{keyword}' in source {source}")
            return df

        except Exception as e:
            bt.logging.error(f"Error querying keyword data for {source}/{keyword}: {e}")
            return pd.DataFrame()

    def _get_label_and_keyword_data(self, source: int, label: str, keyword: str, limit: int) -> pd.DataFrame:
        """Get all rows matching BOTH the label AND the keyword, capped at ``limit``."""
        label_condition_sql = self._build_label_conditions(source, label)
        content_condition_sql = self._build_keyword_conditions(source, keyword)

        query = f"""
            SELECT uri, datetime, label, content
            FROM DataEntity
            WHERE source = ?
                AND ({label_condition_sql})
                AND ({content_condition_sql})
            ORDER BY datetime ASC, uri ASC
            LIMIT ?
        """
        params = [source, limit]

        try:
            with self.get_db_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params, parse_dates=['datetime'])

            bt.logging.debug(f"Found {len(df)} records for label '{label}' AND keyword '{keyword}' in source {source}")
            return df

        except Exception as e:
            bt.logging.error(f"Error querying label+keyword data for {source}/{label}/{keyword}: {e}")
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
                # Reddit data structure (uri removed — validator uses url column)
                result_df = pd.DataFrame({
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
                    'url': df['decoded_content'].apply(lambda x: x.get('url')),
                    'media': df['decoded_content'].apply(lambda x: x.get('media')),
                    'is_nsfw': df['decoded_content'].apply(lambda x: x.get('is_nsfw')),
                    'score': df['decoded_content'].apply(lambda x: x.get('score')),
                    'upvote_ratio': df['decoded_content'].apply(lambda x: x.get('upvote_ratio')),
                    'num_comments': df['decoded_content'].apply(lambda x: x.get('num_comments')),
                    'scrapedAt': df['decoded_content'].apply(lambda x: _to_iso_string(x.get('scrapedAt'))),
                })
            else:
                # X/Twitter data structure (uri removed — validator uses url column)
                result_df = pd.DataFrame({
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
                    'in_reply_to_user_id': df['decoded_content'].apply(lambda x: x.get('in_reply_to_user_id')),
                    'language': df['decoded_content'].apply(lambda x: x.get('language')),
                    'in_reply_to_username': df['decoded_content'].apply(lambda x: x.get('in_reply_to_username')),
                    'quoted_tweet_id': df['decoded_content'].apply(lambda x: x.get('quoted_tweet_id')),
                    'like_count': df['decoded_content'].apply(lambda x: x.get('like_count')),
                    'retweet_count': df['decoded_content'].apply(lambda x: x.get('retweet_count')),
                    'reply_count': df['decoded_content'].apply(lambda x: x.get('reply_count')),
                    'quote_count': df['decoded_content'].apply(lambda x: x.get('quote_count')),
                    'view_count': df['decoded_content'].apply(lambda x: x.get('view_count')),
                    'bookmark_count': df['decoded_content'].apply(lambda x: x.get('bookmark_count')),
                    'user_blue_verified': df['decoded_content'].apply(lambda x: x.get('user_blue_verified')),
                    'user_description': df['decoded_content'].apply(lambda x: x.get('user_description')),
                    'user_location': df['decoded_content'].apply(lambda x: x.get('user_location')),
                    'profile_image_url': df['decoded_content'].apply(lambda x: x.get('profile_image_url')),
                    'cover_picture_url': df['decoded_content'].apply(lambda x: x.get('cover_picture_url')),
                    'user_followers_count': df['decoded_content'].apply(lambda x: x.get('user_followers_count')),
                    'user_following_count': df['decoded_content'].apply(lambda x: x.get('user_following_count')),
                    'scraped_at': df['decoded_content'].apply(lambda x: _to_iso_string(x.get('scraped_at'))),
                })

            return result_df

        except Exception as e:
            bt.logging.error(f"Error creating raw dataframe: {e}")
            return pd.DataFrame()

    async def _upload_job_snapshot(self, client: DataUniverseApiClient, df: pd.DataFrame, source: int, job_id: str) -> bool:
        """Upload the snapshot DataFrame as one parquet file via per-file presigned URL."""
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

            # Generate filename with timestamp, record count, and random hash
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            random_hash = secrets.token_hex(8)  # 16 character random hex string
            filename = f"data_{timestamp}_{len(raw_df)}_{random_hash}.parquet"
            local_path = os.path.join(self.output_dir, filename)

            # Save to parquet with snappy compression
            raw_df.to_parquet(local_path, index=False, compression='snappy', row_group_size=10_000)

            # Request presigned URL and upload via API
            try:
                await client.miner_upload_parquet_file(
                    job_id=job_id, filename=filename, file_path=local_path
                )
                bt.logging.success(f"Uploaded {len(raw_df)} records to job {job_id}")
            except Exception as upload_err:
                err_str = str(upload_err)
                if "409" in err_str:
                    bt.logging.warning(f"File already exists for job {job_id}/{filename}, skipping")
                else:
                    raise

            # Clean up local file
            if os.path.exists(local_path):
                os.remove(local_path)

            return True

        except Exception as e:
            bt.logging.error(f"Error uploading data chunk for job {job_id}: {e}")
            return False

    async def _process_job(self, client: DataUniverseApiClient, job_id: str, job_config: Dict) -> bool:
        """Re-snapshot a single job: query all matching rows up to max_rows and upload as one file."""
        source = job_config["source"]
        search_type = job_config["type"]
        max_rows = job_config.get("max_rows") or self.DEFAULT_JOB_MAX_ROWS

        if search_type == "label_and_keyword":
            label = job_config["label"]
            keyword = job_config["keyword"]
            log_msg = f"label='{label}' AND keyword='{keyword}'"
            df = self._get_label_and_keyword_data(source, label, keyword, max_rows)
        elif search_type == "label":
            value = job_config["value"]
            log_msg = f"label: {value}"
            df = self._get_label_data(source, value, max_rows)
        elif search_type == "keyword":
            value = job_config["value"]
            log_msg = f"keyword: {value}"
            df = self._get_keyword_data(source, value, max_rows)
        else:
            bt.logging.error(f"Unknown search type: {search_type}")
            return False

        bt.logging.info(f"Snapshotting job {job_id} ({log_msg}), max_rows={max_rows}, rows={len(df)}")

        if df.empty:
            bt.logging.info(f"No data for job {job_id}, skipping upload")
            return True

        success = await self._upload_job_snapshot(client, df, source, job_id)
        if not success:
            bt.logging.error(f"Failed to upload snapshot for job {job_id}")
            return False

        self.processed_state[job_id] = {
            'last_snapshot_rows': len(df),
            'last_snapshot_time': dt.datetime.now().isoformat(),
        }
        self._save_processed_state()

        bt.logging.info(f"Completed job {job_id}: {len(df)} records snapshotted")
        return True

    async def upload_dd_data(self) -> bool:
        """Main method to upload data using job_ids from Gravity.

        Each file upload requests its own presigned URL via POST /get-file-upload-url.
        No up-front credential fetch needed.
        """
        bt.logging.info("Starting S3 upload using Gravity job IDs")

        try:
            # Load jobs by job_id (from Gravity via total.json)
            jobs = self._load_dd_list()
            if not jobs:
                bt.logging.warning("No jobs found from Gravity")
                return False

            overall_success = True

            # Each file upload gets its own presigned URL via the API
            async with DataUniverseApiClient(
                base_url=self.api_base_url,
                keypair=self.keypair,
                verify_ssl="localhost" not in self.api_base_url,
            ) as client:
                for job_id, job_config in jobs.items():
                    bt.logging.info(f"Processing job: {job_id}")

                    job_success = await self._process_job(client, job_id, job_config)
                    if not job_success:
                        overall_success = False

            bt.logging.info("Completed S3 upload using job IDs")
            return overall_success

        except Exception as e:
            bt.logging.error(f"Error in job-based S3 upload: {e}")
            return False
