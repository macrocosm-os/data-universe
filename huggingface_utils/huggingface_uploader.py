import os
import json
import datetime as dt
import pandas as pd
import bittensor as bt
import sqlite3
import re
import time
import requests
from contextlib import contextmanager
from huggingface_hub import HfApi, hf_hub_download
from huggingface_utils.utils import(
    preprocess_reddit_df,
    preprocess_twitter_df,
    generate_static_integer,
    migrate_stats_to_v2,
    get_default_stats_structure
)
from huggingface_utils.encoding_system import EncodingKeyManager
from huggingface_utils.s3_utils import S3Auth
from common.data import HuggingFaceMetadata, DataSource
from typing import List, Dict, Union, Any
from huggingface_utils.dataset_card import DatasetCardGenerator, NumpyEncoder
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


class DualUploader:
    """
    Dual uploader of miner data into HF and the S3 bucket
    HF is going to be deprecated in forthcoming weeks.
    """
    def __init__(self, db_path: str,
                 subtensor: bt.subtensor,  # need to commit the info into chain
                 wallet: bt.wallet,  # need to commit the info into chain
                 encoding_key_manager: EncodingKeyManager,  # USED FOR ENCODING USERNAMES
                 private_encoding_key_manager: EncodingKeyManager,   # USED FOR ENCODING URLS
                 s3_auth_url: str,  # For the getting upload credentials.
                 state_file: str,
                 output_dir: str = 'hf_storage',
                 chunk_size: int = 1_000_000):
        self.db_path = db_path
        self.wallet = wallet
        self.miner_hotkey = self.wallet.hotkey.ss58_address
        self.subtensor = subtensor
        self.output_dir = os.path.join(output_dir, self.miner_hotkey)
        self.unique_id = generate_static_integer(self.miner_hotkey)
        self.encoding_key_manager = encoding_key_manager
        self.private_encoding_key_manager = private_encoding_key_manager
        self.hf_token = os.getenv("HUGGINGFACE_TOKEN")
        self.hf_api = HfApi(token=self.hf_token)
        self.state_file = f"{state_file.split('.json')[0]}_{self.unique_id}.json"
        self.chunk_size = chunk_size
        self.s3_auth = S3Auth(s3_auth_url) if s3_auth_url else None
        self.wal_size_limit_mb = 2000  # 2 GB WAL size limit

    @contextmanager
    def get_db_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=60.0)  # Added timeout
        try:
            # Enhanced optimization settings
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA cache_size=-2000000")  # Increased to 2GB
            conn.execute("PRAGMA page_size=16384")  # Optimized page size
            conn.execute("PRAGMA mmap_size=30000000000")  # 30GB memory mapping
            yield conn
        finally:
            conn.close()

    def sanitize_json(self, json_string: str) -> str:
        """Remove any non-printable characters from the JSON string."""
        return re.sub(r'[\x00-\x1F\x7F-\x9F]', '', json_string)

    def ensure_unicode(self, s: Any) -> str:
        """Ensure the input is a Unicode string."""
        if isinstance(s, bytes):
            return s.decode('utf-8')
        return str(s)

    def check_hf_connection(self, url: str = "https://huggingface.co", timeout: int = 5) -> bool:
        """Check if the connection to Hugging Face is stable."""
        try:
            response = requests.get(url, timeout=timeout)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state_data = self.sanitize_json(f.read())
                state = json.loads(state_data)
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

    def save_state(self, state):
        state_to_save = {
            'last_upload': {
                source: (last_upload.strftime('%Y-%m-%d %H:%M:%S') if isinstance(last_upload, dt.datetime) else None)
                for source, last_upload in state['last_upload'].items()
            },
            'total_rows': state['total_rows']
        }
        with open(self.state_file, 'w') as f:
            json.dump(state_to_save, f)

    def get_next_chunk_id(self, repo_id):
        try:
            files = self.hf_api.list_repo_files(repo_id=repo_id, repo_type="dataset")
            parquet_files = [f for f in files if f.endswith('.parquet')]
            if not parquet_files:
                return 0
            max_id = max([int(f.split('_')[-1].split('.')[0]) for f in parquet_files])
            return max_id + 1
        except Exception as e:
            bt.logging.error(f"Error getting next chunk id: {e}")
            return 0

    def get_data_for_huggingface_upload(self, source, last_upload):
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

    def preprocess_data(self, df, source):
        if source == DataSource.REDDIT.value:
            return preprocess_reddit_df(df, self.encoding_key_manager, self.private_encoding_key_manager)
        else:
            return preprocess_twitter_df(df, self.encoding_key_manager, self.private_encoding_key_manager)

    @retry_upload(max_retries=5)
    def upload_parquet_to_hf(self, repo_id, s3_policy=None):
        """Upload parquet files to HuggingFace and S3 if configured"""
        success = False

        # Try HuggingFace upload if token available
        if self.hf_token and self.check_hf_connection():
            try:
                self.hf_api.upload_folder(
                    token=self.hf_token,
                    folder_path=self.output_dir,
                    repo_id=repo_id,
                    repo_type="dataset",
                    path_in_repo='data/',
                    allow_patterns="*.parquet",
                )
                bt.logging.info(f"Successfully uploaded files to HF repo {repo_id}")
                success = True
            except Exception as e:
                bt.logging.error(f"Error during HF upload: {str(e)}")
                # Don't re-raise so we can try S3 upload

        # Try S3 upload if auth client available
        try:
            if s3_policy:
                # Upload all parquet files in directory to S3
                s3_success_count = 0
                total_files = 0

                for filename in os.listdir(self.output_dir):
                    if filename.endswith(".parquet"):
                        total_files += 1
                        file_path = os.path.join(self.output_dir, filename)
                        if self.s3_auth.upload_file(file_path, s3_policy):
                            s3_success_count += 1

                if s3_success_count > 0:
                    bt.logging.info(f"Successfully uploaded {s3_success_count}/{total_files} files to S3")
                    success = True
        except Exception as e:
            bt.logging.error(f"Error during S3 upload: {str(e)}")

        # Only clean up if at least one upload method succeeded
        if success:
            for filename in os.listdir(self.output_dir):
                if filename.endswith(".parquet"):
                    os.remove(os.path.join(self.output_dir, filename))

        # If both uploads failed, raise exception to trigger retry
        if not success:
            raise Exception("Both HuggingFace and S3 uploads failed")

        return success

    def upload_sql_to_huggingface(self) -> List[HuggingFaceMetadata]:
        if not self.hf_token:
            bt.logging.error("Hugging Face token not found. Please check your environment variables.")
            return []

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        state = self.load_state()
        hf_metadata_list = []

        for source in [DataSource.REDDIT.value, DataSource.X.value]:
            platform = 'reddit' if source == DataSource.REDDIT.value else 'x'

            # Define the repository ID for each platform
            repo_id = f"{self.hf_api.whoami(self.hf_token)['name']}/{platform}_dataset_{self.unique_id}"

            # Create DatasetCardGenerator instance for each platform
            card_generator = DatasetCardGenerator(
                miner_hotkey=self.miner_hotkey,
                repo_id=repo_id,
                token=self.hf_token
            )

            try:
                # Check if repository exists
                self.hf_api.repo_info(repo_id=repo_id, repo_type="dataset")
                bt.logging.info(f"Repository {repo_id} already exists.")
                next_chunk_id = self.get_next_chunk_id(repo_id)
            except Exception:
                # Create new repository
                self.hf_api.create_repo(token=self.hf_token, repo_id=repo_id.split('/')[1], private=False, repo_type="dataset")
                bt.logging.info(f"Created new repository: {repo_id}")
                next_chunk_id = 0

            last_upload = state['last_upload'].get(str(source))
            total_rows = state['total_rows'].get(str(source), 0)
            chunk_count = 0

            all_stats = {}
            new_rows = 0

            s3_creds = self.s3_auth.get_credentials(
                source_name=platform,
                subtensor=self.subtensor,
                wallet=self.wallet,
            )

            try:
                for df in self.get_data_for_huggingface_upload(source, last_upload):
                    bt.logging.info(f"Processing new DataFrame for source {source}")

                    if df.empty:
                        bt.logging.info(f"Encountered empty DataFrame for source {source}. Skipping.")
                        continue

                    bt.logging.info(f"Current total rows: {total_rows}")
                    if total_rows >= 200_000_000: # TODO
                        bt.logging.info(f"Reached 200 million rows limit for source {source}. Stopping upload.")
                        break

                    last_upload = df['datetime'].max()

                    bt.logging.info(f"Starting preprocessing for DataFrame with {len(df)} rows")
                    df = self.preprocess_data(df, source)
                    rows_to_upload = min(len(df), 400_000_000 - total_rows)

                    if rows_to_upload < len(df):
                        df = df.iloc[:rows_to_upload]  # Trim the dataframe if necessary

                    parquet_path = os.path.join(self.output_dir,
                                                f"train-DataEntity_chunk_{next_chunk_id + chunk_count}.parquet")
                    df.to_parquet(parquet_path, index=False)

                    bt.logging.info(f"Saving chunk to Parquet file: {parquet_path}")

                    bt.logging.info("Collecting statistics for the current chunk")
                    chunk_stats = self.collect_statistics(df, source)
                    all_stats = self.merge_statistics(all_stats, chunk_stats)

                    chunk_count += 1
                    total_rows += len(df)
                    new_rows += len(df)

                    if chunk_count == 10:
                        self.upload_parquet_to_hf(repo_id, s3_creds)
                        bt.logging.info(f'Uploaded {chunk_count} chunks to {repo_id}')
                        next_chunk_id += chunk_count
                        chunk_count = 0
                        with self.get_db_connection() as conn:
                            self.manage_wal(conn)

                if chunk_count > 0:
                    self.upload_parquet_to_hf(repo_id, s3_creds)
                    with self.get_db_connection() as conn:
                        self.manage_wal(conn)
                    bt.logging.info(f'Uploaded final {chunk_count} chunks to {repo_id}')

                state['last_upload'][str(source)] = last_upload
                state['total_rows'][str(source)] = total_rows
                self.save_state(state)

                if new_rows > 0:
                    # Update stats
                    updated_stats = self.save_stats_json(all_stats, platform, new_rows, repo_id)

                    # Update README and save stats.json
                    update_history = updated_stats['summary']['update_history']
                    cumulative_total = 0
                    formatted_history = []
                    for item in update_history:
                        cumulative_total += item['count']
                        formatted_history.append((item['timestamp'], item['count'], cumulative_total))

                    card_generator.update_or_create_card(updated_stats, formatted_history)

                # Save metadata
                hf_metadata = HuggingFaceMetadata(
                    repo_name=repo_id,
                    source=source,
                    updated_at=dt.datetime.utcnow(),
                    encoding_key=self.encoding_key_manager.sym_key.decode()  # Use sym_key and decode it to string
                )
                hf_metadata_list.append(hf_metadata)

                bt.logging.success(
                    f"Finished uploading data for source {source} to {repo_id}. Total rows uploaded: {total_rows}")

            except Exception as e:
                bt.logging.error(f"Error during upload for source {source}: {e}")

        return hf_metadata_list

    def collect_statistics(self, df: pd.DataFrame, source: int) -> Dict[str, Any]:
        df['datetime'] = pd.to_datetime(df['datetime'])
        stats = {
            'total_rows': len(df),
            'start_date': df['datetime'].min().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'end_date': df['datetime'].max().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'metadata': {}
        }

        if source == DataSource.REDDIT.value:
            stats['posts_count'] = df[df['dataType'] == 'post'].shape[0]
            stats['comments_count'] = df[df['dataType'] == 'comment'].shape[0]
            stats['metadata']['posts_percentage'] = (stats['posts_count'] / len(df)) * 100
            stats['metadata']['comments_percentage'] = (stats['comments_count'] / len(df)) * 100
            subreddit_counts = df['communityName'].value_counts().to_dict()
            stats['subreddits'] = {subreddit: {'count': count, 'percentage': (count / len(df)) * 100}
                                   for subreddit, count in subreddit_counts.items()}
        else:  # X (Twitter)
            # Count tweets with and without hashtags
            tweets_with_hashtags = df[df['label'] != 'NULL']
            tweets_without_hashtags = df[df['label'] == 'NULL']
            stats['tweets_with_hashtags_count'] = len(tweets_with_hashtags)
            stats['tweets_without_hashtags_count'] = len(tweets_without_hashtags)
            stats['metadata']['tweets_with_hashtags_percentage'] = (stats['tweets_with_hashtags_count'] / len(df)) * 100
            stats['metadata']['tweets_without_hashtags_percentage'] = (stats['tweets_without_hashtags_count'] / len(
                df)) * 100

            # Extract and count hashtags
            all_hashtags = tweets_with_hashtags['label'].str.split().explode()
            hashtag_counts = all_hashtags.value_counts().to_dict()

            stats['hashtags'] = {
                hashtag: {
                    'count': count,
                    'percentage': (count / len(df)) * 100
                }
                for hashtag, count in hashtag_counts.items()
            }

            # Add "NULL" for tweets without hashtags
            stats['hashtags']['NULL'] = {
                'count': stats['tweets_without_hashtags_count'],
                'percentage': stats['metadata']['tweets_without_hashtags_percentage']
            }

        return stats

    def merge_statistics(self, old_stats: Dict[str, Any], new_stats: Dict[str, Any]) -> Dict[str, Any]:
        merged = old_stats.copy()
        for key, value in new_stats.items():
            if key in ['total_rows', 'posts_count', 'comments_count', 'tweets_with_hashtags_count',
                       'tweets_without_hashtags_count']:
                merged[key] = merged.get(key, 0) + value
            elif key in ['start_date', 'end_date']:
                if key == 'start_date':
                    merged[key] = min(merged.get(key, value), value)
                else:
                    merged[key] = max(merged.get(key, value), value)
            elif key in ['subreddits', 'hashtags']:
                merged[key] = self.merge_top_items(merged.get(key, {}), value)
            elif key == 'metadata':
                merged[key] = self.merge_metadata(merged.get(key, {}), value)
        return merged

    def merge_metadata(self, old_metadata: Dict[str, float], new_metadata: Dict[str, float]) -> Dict[str, float]:
        merged_metadata = old_metadata.copy()
        for key, value in new_metadata.items():
            if key in merged_metadata:
                # For percentages, we take the average
                merged_metadata[key] = (merged_metadata[key] + value) / 2
            else:
                merged_metadata[key] = value
        return merged_metadata

    def merge_top_items(self, old_items: Dict[str, Dict[str, Union[int, float]]],
                        new_items: Dict[str, Dict[str, Union[int, float]]]) -> Dict[str, Dict[str, Union[int, float]]]:
        merged = {k: v.copy() for k, v in old_items.items()}
        for k, v in new_items.items():
            if k in merged:
                merged[k]['count'] += v['count']
            else:
                merged[k] = v.copy()

        total_count = sum(item['count'] for item in merged.values())
        for item in merged.values():
            item['percentage'] = (item['count'] / total_count) * 100 if total_count > 0 else 0

        return merged

    def update_topics(self, existing_topics: List[Dict[str, Any]], new_topics: Dict[str, Dict[str, Any]],
                      platform: str) -> List[Dict[str, Any]]:
        """
        Updated method to handle topics without update history
        """
        topic_type = "subreddit" if platform == "reddit" else "hashtag"
        topic_dict = {topic["topic"]: topic for topic in existing_topics if topic["topic_type"] == topic_type}

        for topic_name, data in new_topics.items():
            if topic_name in topic_dict:
                # Update existing topic counts
                topic_dict[topic_name]["total_count"] += data["count"]
            else:
                # Create new topic
                topic_dict[topic_name] = {
                    "topic": topic_name,
                    "topic_type": topic_type,
                    "total_count": data["count"],
                    "total_percentage": 0  # Will be calculated below
                }

        # Recalculate total percentages
        total_count = sum(topic["total_count"] for topic in topic_dict.values())
        if total_count > 0:
            for topic in topic_dict.values():
                topic["total_percentage"] = (topic["total_count"] / total_count) * 100
        else:
            for topic in topic_dict.values():
                topic["total_percentage"] = 0

        return list(topic_dict.values())

    def load_existing_stats(self, repo_id: str) -> Dict[str, Any]:
        """
        Load and sanitize existing stats from stats.json in the HF repo.
        """
        filename = "stats.json"
        try:
            local_path = hf_hub_download(repo_id=repo_id, filename=filename, repo_type="dataset", token=self.hf_token)

            with open(local_path, 'r', encoding='utf-8') as f:
                content = f.read()

            sanitized_content = self.sanitize_json(content)
            stats = json.loads(sanitized_content)

            # Check if migration is needed
            if stats.get("version") != "2.0.0":
                bt.logging.info(f"Migrating stats from version {stats.get('version', '1.0.0')} to 2.0.0")
                stats = migrate_stats_to_v2(stats)

            bt.logging.info(f"Successfully loaded and sanitized existing stats from {repo_id}")
            return stats

        except json.JSONDecodeError as e:
            bt.logging.error(f"JSON Decode Error in existing stats file: {e}")
            bt.logging.error(f"Error location: line {e.lineno}, column {e.colno}")
            bt.logging.error(f"Problematic JSON snippet: {e.doc[max(0, e.pos - 20):e.pos + 20]}")
            return get_default_stats_structure()

        except Exception as e:
            bt.logging.error(f"Error loading existing stats: {e}")
            return get_default_stats_structure()

    @retry_upload()
    def save_stats_json(self, platform_stats: Dict[str, Any], platform: str, new_rows: int, repo_id: str) -> Dict[
        str, Any]:
        """
        Updated method to save stats with the new structure
        """
        filename = "stats.json"

        try:
            existing_stats = self.load_existing_stats(repo_id)

            # Ensure version is set to 2.0.0
            existing_stats["version"] = "2.0.0"

            if existing_stats["data_source"] is None:
                existing_stats["data_source"] = platform

            merged_stats = self.merge_statistics(existing_stats, platform_stats)

            merged_stats["summary"]["total_rows"] += new_rows
            merged_stats["summary"]["last_update_dt"] = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

            if merged_stats["summary"]["start_dt"] is None or platform_stats["start_date"] < merged_stats["summary"][
                "start_dt"]:
                merged_stats["summary"]["start_dt"] = platform_stats["start_date"]

            if merged_stats["summary"]["end_dt"] is None or platform_stats["end_date"] > merged_stats["summary"][
                "end_dt"]:
                merged_stats["summary"]["end_dt"] = platform_stats["end_date"]

            merged_stats["summary"]["update_history"].append({
                "timestamp": merged_stats["summary"]["last_update_dt"],
                "count": new_rows
            })

            # Update topics with simplified structure
            merged_stats["topics"] = self.update_topics(
                merged_stats.get("topics", []),
                platform_stats.get('subreddits' if platform == 'reddit' else 'hashtags', {}),
                platform
            )

            # Update metadata percentages
            total_rows = merged_stats["summary"]["total_rows"]
            if platform == 'reddit':
                merged_stats["summary"]["metadata"]["posts_percentage"] = (
                                merged_stats.get("posts_count", 0) / total_rows) * 100 if total_rows > 0 else 0
                merged_stats["summary"]["metadata"]["comments_percentage"] = (
                                merged_stats.get("comments_count", 0) / total_rows) * 100 if total_rows > 0 else 0
            else:  # X (Twitter)
                merged_stats["summary"]["metadata"]["tweets_with_hashtags_percentage"] = (
                                merged_stats.get("tweets_with_hashtags_count", 0) / total_rows) * 100 if total_rows > 0 else 0
                merged_stats["summary"]["metadata"]["tweets_without_hashtags_percentage"] = (
                                merged_stats.get("tweets_without_hashtags_count", 0) / total_rows) * 100 if total_rows > 0 else 0

            stats_json = json.dumps(merged_stats, indent=2, cls=NumpyEncoder)
            sanitized_stats_json = self.sanitize_json(stats_json)

            self.hf_api.upload_file(
                token=self.hf_token,
                path_or_fileobj=sanitized_stats_json.encode(),
                path_in_repo=filename,
                repo_id=repo_id,
                repo_type="dataset",
            )

            bt.logging.info(f"Successfully updated {filename} for {platform} dataset in {repo_id}")
            return merged_stats

        except Exception as e:
            bt.logging.error(f"Error saving merged stats JSON: {e}")
            raise
    def check_wal_size(self):
        wal_file = f"{self.db_path}-wal"
        if os.path.exists(wal_file):
            size_mb = os.path.getsize(wal_file) / (1024 * 1024)
            bt.logging.info(f"Current WAL file size: {size_mb:.2f} MB")
            return size_mb
        return 0

    def manage_wal(self, conn):
        wal_size = self.check_wal_size()
        if wal_size > self.wal_size_limit_mb:
            bt.logging.warning(f"WAL file exceeded {self.wal_size_limit_mb} MB. Performing checkpoint.")
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            new_size = self.check_wal_size()
            bt.logging.info(f"After checkpoint, WAL size: {new_size:.2f} MB")
