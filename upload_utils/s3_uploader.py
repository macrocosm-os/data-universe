"""
S3 Partitioned Uploader for Dynamic Desirability data ONLY
Uploads ONLY data that matches current DD list in partitioned format:
data/source/coldkey/label_keyword/YYYY-MM-DD/parquet_files

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
    S3 Partitioned uploader for ONLY Dynamic Desirability data.
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
        self.miner_coldkey = self.wallet.get_coldkeypub().ss58_address
        self.s3_auth = S3Auth(s3_auth_url)
        self.state_file = f"{state_file.split('.json')[0]}_s3_partitioned.json"
        self.output_dir = os.path.join(output_dir, self.miner_coldkey)
        self.chunk_size = chunk_size

        # Load processed state - tracks last processed info per DD item
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
        """Load processed state - tracks last processed info per DD item"""
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

    def _get_last_processed_offset(self, processed_key: str) -> int:
        """Get the last processed offset for a DD item"""
        item_state = self.processed_state.get(processed_key, {})
        return item_state.get('last_offset', 0)

    def _update_processed_state(self, processed_key: str, new_offset: int, records_processed: int):
        """Update the processed state for a DD item"""
        if processed_key not in self.processed_state:
            self.processed_state[processed_key] = {}

        self.processed_state[processed_key].update({
            'last_offset': new_offset,
            'total_records_processed': self.processed_state[processed_key].get('total_records_processed', 0) + records_processed,
            'last_processed_time': dt.datetime.now().isoformat(),
            'processing_completed': False  # Never mark as "completed" - always check for new data
        })

    def _sanitize_label(self, label: str) -> str:
        """Sanitize label for use in S3 path"""
        if not label:
            return "unlabeled"

        # Remove prefixes
        sanitized = label.removeprefix("r/").removeprefix("#")

        # Replace invalid characters with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', sanitized)

        # Remove multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)

        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')

        if not sanitized:
            sanitized = "unlabeled"

        return sanitized.lower()

    def _load_dd_list(self) -> Dict[int, List[Dict]]:
        """Load and parse DD list with REAL structure from dynamic_desirability/total.json"""
        try:
            lookup = load_dynamic_lookup()
            result = {}

            # Handle the REAL format: list of aggregate objects
            if isinstance(lookup, list):
                for item in lookup:
                    if not isinstance(item, dict) or 'params' not in item:
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

                    if source_int not in result:
                        result[source_int] = []

                    # Check if it's a label or keyword
                    if params.get('label') and params.get('keyword') is None:
                        # It's a label-based search
                        result[source_int].append({
                            "type": "label",
                            "value": params['label'],
                            "job_weight": weight
                        })
                    elif params.get('keyword'):
                        # It's a keyword-based search
                        result[source_int].append({
                            "type": "keyword",
                            "value": params['keyword'],
                            "job_weight": weight
                        })
                    # If both label and keyword are null, skip this entry

            # Handle old format for backward compatibility
            elif isinstance(lookup, dict):
                for source_key, items in lookup.items():
                    # Map source names to integers
                    if isinstance(source_key, str):
                        if source_key.lower() == 'reddit':
                            source_int = DataSource.REDDIT.value
                        elif source_key.lower() in ['x', 'twitter']:
                            source_int = DataSource.X.value
                        else:
                            continue
                    else:
                        source_int = source_key

                    # Parse items - each can be label or keyword
                    parsed_items = []
                    for item in items:
                        if isinstance(item, dict):
                            if "label" in item:
                                parsed_items.append({
                                    "type": "label",
                                    "value": item["label"],
                                    "job_weight": item.get("job_weight", 1.0)
                                })
                            elif "keyword" in item:
                                parsed_items.append({
                                    "type": "keyword",
                                    "value": item["keyword"],
                                    "job_weight": item.get("job_weight", 1.0)
                                })
                        else:
                            # Backward compatibility - treat as keyword
                            parsed_items.append({
                                "type": "keyword",
                                "value": str(item),
                                "job_weight": 1.0
                            })

                    result[source_int] = parsed_items

            bt.logging.info(f"Loaded DD list: {result}")
            bt.logging.info(f"Total items - Reddit: {len(result.get(1, []))}, X: {len(result.get(2, []))}")
            return result

        except Exception as e:
            bt.logging.error(f"Failed to load DD list: {e}")
            return {}

    def _get_processed_key(self, source: int, search_type: str, value: str) -> str:
        """Get processed key for tracking completion"""
        source_name = 'reddit' if source == DataSource.REDDIT.value else 'x'
        sanitized_value = self._sanitize_label(value)
        return f"{source_name}/{search_type}/{sanitized_value}"

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

    def _upload_data_chunk(self, df: pd.DataFrame, source: int, folder_name: str, s3_creds: Dict) -> bool:
        """Upload a chunk of data to S3 with date partitioning"""
        if df.empty:
            return True

        try:
            # Create raw dataframe (no encoding)
            raw_df = self._create_raw_dataframe(df, source)
            if raw_df.empty:
                bt.logging.warning(f"No data after raw processing for {source}/{folder_name}")
                return True

            # Group by date for partitioning
            raw_df['date'] = pd.to_datetime(raw_df['datetime']).dt.strftime('%Y-%m-%d')
            date_groups = raw_df.groupby('date')

            success = True
            for date_str, date_df in date_groups:
                # Create local parquet file
                if not os.path.exists(self.output_dir):
                    os.makedirs(self.output_dir, exist_ok=True)

                # Generate filename with timestamp and record count
                timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"data_{timestamp}_{len(date_df)}.parquet"
                local_path = os.path.join(self.output_dir, filename)

                # Save to parquet (drop the temp 'date' column)
                date_df.drop('date', axis=1).to_parquet(local_path, index=False)

                # Create S3 path with date folder structure
                # Final structure: data/source/coldkey/label_keyword/YYYY-MM-DD/filename.parquet
                s3_path = f"{folder_name}/{date_str}/{filename}"

                # Upload to S3
                upload_success = self.s3_auth.upload_file_with_path(local_path, s3_path, s3_creds)

                if upload_success:
                    bt.logging.success(f"Uploaded {len(date_df)} records to {date_str} folder")
                else:
                    bt.logging.error(f"Failed to upload {len(date_df)} records to {date_str}")
                    success = False

                # Clean up local file
                if os.path.exists(local_path):
                    os.remove(local_path)

            return success

        except Exception as e:
            bt.logging.error(f"Error uploading data chunk: {e}")
            return False

    def _process_dd_item(self, source: int, dd_item: Dict, s3_creds: Dict) -> bool:
        """Process a single DD item (label or keyword) - IMPROVED VERSION"""
        search_type = dd_item["type"]
        value = dd_item["value"]

        processed_key = self._get_processed_key(source, search_type, value)

        # Get the last processed offset (starts from 0 for new items)
        offset = self._get_last_processed_offset(processed_key)

        bt.logging.info(f"Processing {processed_key}, starting from offset: {offset}")

        total_processed = 0

        while True:
            # Get next chunk based on search type
            if search_type == "label":
                chunk_df = self._get_label_data(source, value, offset)
            else:  # keyword
                chunk_df = self._get_keyword_data_chunk(source, value, offset)

            if chunk_df.empty:
                bt.logging.info(f"No new data for {processed_key}, total processed this run: {total_processed}")
                break

            # Create folder name with prefix: label_bitcoin or keyword_ethereum
            sanitized_value = self._sanitize_label(value)
            folder_name = f"{search_type}_{sanitized_value}"

            # Upload chunk (will be partitioned by date inside this method)
            success = self._upload_data_chunk(chunk_df, source, folder_name, s3_creds)
            if not success:
                bt.logging.error(f"Failed to upload chunk for {processed_key}")
                return False

            total_processed += len(chunk_df)
            offset += len(chunk_df)

            # Update state after each successful chunk
            self._update_processed_state(processed_key, offset, len(chunk_df))
            self._save_processed_state()

            bt.logging.info(f"Processed {total_processed} new records for {processed_key}")

            # If we got less than chunk_size, we've reached the end for now
            if len(chunk_df) < self.chunk_size:
                break

        return True

    def upload_dd_data(self) -> bool:
        """Main method to upload ONLY DD data in partitioned format"""
        bt.logging.info("Starting S3 partitioned upload for DD data only")

        try:
            # Load current DD list
            dd_list = self._load_dd_list()
            if not dd_list:
                bt.logging.warning("No DD list found, skipping S3 partitioned upload")
                return False

            # Get credentials ONCE per source type (reddit and x)
            credentials_cache = {}

            # Pre-fetch credentials for each source that has DD items
            for source in dd_list.keys():
                source_name = 'reddit' if source == DataSource.REDDIT.value else 'x'

                if source_name not in credentials_cache:
                    bt.logging.info(f"Getting S3 credentials for {source_name} (valid for 3 hours)")
                    s3_creds = self.s3_auth.get_credentials(
                        source_name=source_name,
                        subtensor=self.subtensor,
                        wallet=self.wallet,
                    )

                    if s3_creds:
                        credentials_cache[source_name] = s3_creds
                        bt.logging.success(f"Got S3 credentials for {source_name}")
                    else:
                        bt.logging.error(f"Failed to get S3 credentials for {source_name}")
                        return False

            overall_success = True

            # Now process each source using cached credentials
            for source, dd_items in dd_list.items():
                source_name = 'reddit' if source == DataSource.REDDIT.value else 'x'
                bt.logging.info(f"Processing DD data for source: {source_name} ({len(dd_items)} items)")

                # Use cached credentials
                s3_creds = credentials_cache[source_name]

                # Process ALL DD items for this source using the same credentials
                for dd_item in dd_items:
                    item_success = self._process_dd_item(source, dd_item, s3_creds)
                    if not item_success:
                        overall_success = False

                bt.logging.info(f"Completed processing all DD items for {source_name}")

            bt.logging.info("Completed S3 partitioned upload")
            return overall_success

        except Exception as e:
            bt.logging.error(f"Error in S3 partitioned upload: {e}")
            return False