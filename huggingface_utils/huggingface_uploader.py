import os
import json
import datetime as dt
import pandas as pd
import bittensor as bt
import sqlite3
from contextlib import contextmanager
from huggingface_hub import HfApi
from huggingface_utils.utils import preprocess_reddit_df, preprocess_twitter_df, generate_static_integer
from huggingface_utils.encoding_system import EncodingKeyManager
from common.data import HuggingFaceMetadata, DataSource
from typing import List, Dict, Union, Any
from huggingface_utils.dataset_card import DatasetCardGenerator, NumpyEncoder

class HuggingFaceUploader:
    def __init__(self, db_path: str,
                 miner_hotkey: str,
                 encoding_key_manager: EncodingKeyManager,
                 state_file: str,
                 output_dir: str = 'hf_storage',
                 chunk_size: int = 1_000_000):
        self.db_path = db_path
        self.output_dir = output_dir
        self.hf_api = HfApi()
        self.miner_hotkey = miner_hotkey
        self.unique_id = generate_static_integer(self.miner_hotkey)
        self.encoding_key_manager = encoding_key_manager
        self.hf_token = os.getenv("HUGGINGFACE_TOKEN")
        self.state_file = f"{state_file.split('.json')[0]}_{self.unique_id}.json"
        self.chunk_size = chunk_size

    @contextmanager
    def get_db_connection(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
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
                LIMIT 400000000
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
            return preprocess_reddit_df(df, self.encoding_key_manager)
        else:
            return preprocess_twitter_df(df, self.encoding_key_manager)

    def upload_parquet_to_hf(self, repo_id):
        self.hf_api.upload_folder(
            token=self.hf_token,
            folder_path=self.output_dir,
            repo_id=repo_id,
            repo_type="dataset",
            path_in_repo='data/',
            allow_patterns="*.parquet",
        )

        # Clean up local parquet files after upload
        for filename in os.listdir(self.output_dir):
            if filename.endswith(".parquet"):
                os.remove(os.path.join(self.output_dir, filename))

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

            # Load existing stats for each platform
            full_stats = self.load_existing_stats(repo_id)

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

            try:
                for df in self.get_data_for_huggingface_upload(source, last_upload):
                    if total_rows >= 400_000_000:
                        bt.logging.info(f"Reached 400 million rows limit for source {source}. Stopping upload.")
                        break

                    if not df.empty:
                        last_upload = df['datetime'].max()

                        df = self.preprocess_data(df, source)
                        rows_to_upload = min(len(df), 400_000_000 - total_rows)

                        if rows_to_upload < len(df):
                            df = df.iloc[:rows_to_upload]  # Trim the dataframe if necessary

                        parquet_path = os.path.join(self.output_dir,
                                                    f"train-DataEntity_chunk_{next_chunk_id + chunk_count}.parquet")
                        df.to_parquet(parquet_path)

                        chunk_stats = self.collect_statistics(df, source)
                        all_stats = self.merge_statistics(all_stats, chunk_stats)

                        chunk_count += 1
                        total_rows += len(df)
                        new_rows += len(df)

                        if chunk_count == 10:
                            self.upload_parquet_to_hf(repo_id)
                            bt.logging.info(f'Uploaded {chunk_count} chunks to {repo_id}')
                            next_chunk_id += chunk_count
                            chunk_count = 0
                    else:
                        bt.logging.info(f"No new data for source {source}. Skipping.")
                        continue

                if chunk_count > 0:
                    self.upload_parquet_to_hf(repo_id)
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
                    updated_at=dt.datetime.now(dt.timezone.utc),
                    encoding_key=self.encoding_key_manager.sym_key.decode()
                )
                hf_metadata_list.append(hf_metadata)

                bt.logging.success(
                    f"Finished uploading data for source {source} to {repo_id}. Total rows uploaded: {total_rows}")

            except Exception as e:
                bt.logging.error(f"Error during upload for source {source}: {e}")

        return hf_metadata_list

    def collect_statistics(self, df: pd.DataFrame, source: int) -> Dict[str, Any]:
        """
        Collect statistics from a DataFrame chunk.

        Args:
            df (pd.DataFrame): The DataFrame chunk to analyze.
            source (int): The data source (Reddit or X).

        Returns:
            Dict[str, Any]: A dictionary containing the collected statistics.
        """
        df['datetime'] = pd.to_datetime(df['datetime'])
        stats = {
            'total_rows': len(df),
            'start_date': df['datetime'].min().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'end_date': df['datetime'].max().strftime('%Y-%m-%dT%H:%M:%SZ'),
        }

        if source == DataSource.REDDIT.value:
            stats['posts_count'] = df[df['dataType'] == 'post'].shape[0]
            stats['comments_count'] = df[df['dataType'] == 'comment'].shape[0]
            subreddit_counts = df['communityName'].value_counts().to_dict()
            stats['subreddits'] = {subreddit: {'count': count, 'percentage': 0}
                                   for subreddit, count in subreddit_counts.items()}
        else:  # X (Twitter)
            # Count tweets with hashtags
            has_hashtags = df[df['label'].notna() & (df['label'] != '')]
            stats['tweets_with_hashtags_count'] = len(has_hashtags)

            # Extract and count hashtags
            all_hashtags = df['label'].fillna('').str.split().explode()
            hashtag_counts = all_hashtags[all_hashtags != ''].value_counts().to_dict()

            stats['hashtags'] = {
                hashtag: {
                    'count': count,
                    'percentage': 0
                }
                for hashtag, count in hashtag_counts.items()
            }

        return stats

    def merge_statistics(self, old_stats: Dict[str, Any], new_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge two sets of statistics.

        Args:
            old_stats (Dict[str, Any]): The existing statistics.
            new_stats (Dict[str, Any]): The new statistics to merge.

        Returns:
            Dict[str, Any]: The merged statistics.
        """
        merged = old_stats.copy()
        for key, value in new_stats.items():
            if key in ['total_rows', 'posts_count', 'comments_count', 'tweets_with_hashtags_count']:
                merged[key] = merged.get(key, 0) + value
            elif key in ['start_date', 'end_date']:
                if key == 'start_date':
                    merged[key] = min(merged.get(key, value), value)
                else:
                    merged[key] = max(merged.get(key, value), value)
            elif key in ['subreddits', 'hashtags']:
                merged[key] = self.merge_top_items(merged.get(key, {}), value)
        return merged

    def merge_top_items(self, old_items: Dict[str, Dict[str, Union[int, float]]],
                        new_items: Dict[str, Dict[str, Union[int, float]]]) -> Dict[str, Dict[str, Union[int, float]]]:
        """
        Merge two dictionaries of items, updating counts and percentages.

        Args:
            old_items (Dict[str, Dict[str, Union[int, float]]]): Existing items.
            new_items (Dict[str, Dict[str, Union[int, float]]]): New items to merge.

        Returns:
            Dict[str, Dict[str, Union[int, float]]]: Merged items.
        """
        merged = {k: v.copy() for k, v in old_items.items()}
        for k, v in new_items.items():
            if k in merged:
                merged[k]['count'] += v['count']
            else:
                merged[k] = v.copy()

        total_count = sum(item['count'] for item in merged.values())
        for item in merged.values():
            item['percentage'] = (item['count'] / total_count) * 100

        return merged

    def update_topics(self, existing_topics: List[Dict[str, Any]], new_topics: Dict[str, Dict[str, Any]], platform: str) -> List[Dict[str, Any]]:
        """
        Merge existing topics with new topics and update their statistics.

        Args:
            existing_topics (List[Dict[str, Any]]): List of existing topics.
            new_topics (Dict[str, Dict[str, Any]]): New topics to merge.
            platform (str): The platform ('reddit' or 'x').

        Returns:
            List[Dict[str, Any]]: Updated list of topics.
        """
        topic_type = "subreddit" if platform == "reddit" else "hashtag"
        topic_dict = {topic["topic"]: topic for topic in existing_topics if topic["topic_type"] == topic_type}

        for topic_name, data in new_topics.items():
            if topic_name in topic_dict:
                topic = topic_dict[topic_name]
                topic["update_history"].append({
                    "timestamp": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "count": data["count"],
                    "percentage": data.get('percentage', 0)
                })
            else:
                topic_dict[topic_name] = {
                    "topic": topic_name,
                    "topic_type": topic_type,
                    "update_history": [{
                        "timestamp": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "count": data["count"],
                        "percentage": data.get('percentage', 0)
                    }]
                }

        return list(topic_dict.values())

    def load_existing_stats(self, repo_id: str) -> Dict[str, Any]:
        """
        Load existing stats from stats.json if it exists.

        Args:
            repo_id (str): The Hugging Face repository ID.

        Returns:
            Dict[str, Any]: Existing statistics or an empty structure.
        """
        filename = "stats.json"
        try:
            existing_stats_file = self.hf_api.hf_hub_download(repo_id=repo_id, filename=filename, repo_type="dataset")
            with open(existing_stats_file, 'r') as f:
                full_stats = json.load(f)
            return full_stats
        except Exception:
            # If stats.json does not exist, initialize it
            full_stats = {
                "version": "0.0.0",
                "data_source": None,
                "summary": {
                    "total_rows": 0,
                    "last_update_dt": None,
                    "start_dt": None,
                    "end_dt": None,
                    "update_history": [],
                    "metadata": {}
                },
                "topics": []
            }
            return full_stats

    def save_stats_json(self, platform_stats: Dict[str, Any], platform: str, new_rows: int, repo_id: str) -> Dict[str, Any]:
        """
        Save the statistics to stats.json file and upload it to the Hugging Face repository.

        Args:
            platform_stats (Dict[str, Any]): The new statistics collected for the current platform.
            platform (str): The platform for which the statistics are collected ('reddit' or 'x').
            new_rows (int): The number of new rows added in this update.
            repo_id (str): The Hugging Face repository ID.

        Returns:
            Dict[str, Any]: The updated statistics after merging and calculations.
        """
        filename = "stats.json"

        # Try to load existing stats
        full_stats = self.load_existing_stats(repo_id)

        # Update data source
        full_stats["data_source"] = platform

        # Update the summary
        summary = full_stats["summary"]
        summary["total_rows"] += new_rows
        summary["last_update_dt"] = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Update start and end dates
        if summary["start_dt"] is None or platform_stats["start_date"] < summary["start_dt"]:
            summary["start_dt"] = platform_stats["start_date"]

        if summary["end_dt"] is None or platform_stats["end_date"] > summary["end_dt"]:
            summary["end_dt"] = platform_stats["end_date"]

        # Update update_history
        summary["update_history"].append({
            "timestamp": summary["last_update_dt"],
            "count": new_rows
        })

        # Update metadata
        metadata = summary.get("metadata", {})
        if platform == "reddit":
            metadata["posts_count"] = metadata.get("posts_count", 0) + platform_stats.get("posts_count", 0)
            metadata["comments_count"] = metadata.get("comments_count", 0) + platform_stats.get("comments_count", 0)
            total = metadata["posts_count"] + metadata["comments_count"]
            if total > 0:
                metadata["posts_percentage"] = (metadata["posts_count"] / total) * 100
                metadata["comments_percentage"] = (metadata["comments_count"] / total) * 100
        else:  # X (Twitter)
            metadata["tweets_with_hashtags_count"] = metadata.get("tweets_with_hashtags_count", 0) + platform_stats.get("tweets_with_hashtags_count", 0)
            total = summary["total_rows"]
            if total > 0:
                metadata["tweets_with_hashtags_percentage"] = (metadata["tweets_with_hashtags_count"] / total) * 100

        summary["metadata"] = metadata

        # Update topics
        topics = full_stats.get("topics", [])
        platform_topics = platform_stats.get('subreddits' if platform == 'reddit' else 'hashtags', {})
        topics = self.update_topics(topics, platform_topics, platform)

        full_stats["topics"] = topics

        # Save stats.json
        stats_json = json.dumps(full_stats, indent=2, cls=NumpyEncoder)

        self.hf_api.upload_file(
            token=self.hf_token,
            path_or_fileobj=stats_json.encode(),
            path_in_repo=filename,
            repo_id=repo_id,
            repo_type="dataset",
        )

        # Remove old stats files if they exist
        for old_filename in [f"{platform}_stats.json"]:
            try:
                self.hf_api.delete_file(path_in_repo=old_filename, repo_id=repo_id, repo_type="dataset")
            except Exception:
                pass  # If the file doesn't exist, ignore

        return full_stats
