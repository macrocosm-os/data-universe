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
            # Use pandas to read the query in chunks
            for chunk in pd.read_sql_query(
                    sql=query,
                    con=conn,
                    params=params,
                    chunksize=self.chunk_size,
                    parse_dates=['datetime']
            ):
                yield chunk

    def preprocess_data(self, df, source):
        if source == 1:
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
            dataset_name = f'{"reddit" if source == DataSource.REDDIT.value else "x"}_dataset_{self.unique_id}'
            repo_id = f"{self.hf_api.whoami(self.hf_token)['name']}/{dataset_name}"

            # Create DatasetCardGenerator instance
            card_generator = DatasetCardGenerator(
                miner_hotkey=self.miner_hotkey,
                repo_id=repo_id,
                token=self.hf_token
            )

            try:
                self.hf_api.repo_info(repo_id=repo_id, repo_type="dataset")
                bt.logging.info(f"Repository {repo_id} already exists.")
                next_chunk_id = self.get_next_chunk_id(repo_id)
            except Exception:
                self.hf_api.create_repo(token=self.hf_token, repo_id=dataset_name, private=False, repo_type="dataset")
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

                # Update stats and README
                platform = 'reddit' if source == DataSource.REDDIT.value else 'x'

                if new_rows > 0:
                    updated_stats = self.save_stats_json(all_stats, platform, new_rows, repo_id)

                    # Generate and upload new README
                    update_history = [(item['date'], item['rows_added'],
                                       sum(h['rows_added'] for h in updated_stats['update_history'][:i + 1]))
                                      for i, item in enumerate(updated_stats['update_history'])]
                    card_generator.update_or_create_card(platform, updated_stats, update_history)

                # Save metadata
                hf_metadata = HuggingFaceMetadata(
                    repo_name=repo_id,
                    source=source,
                    updated_at=dt.datetime.now(dt.timezone.utc),
                    encoding_key=self.encoding_key_manager.sym_key.decode()  # Use sym_key and decode it to string
                )
                hf_metadata_list.append(hf_metadata)

                bt.logging.success(
                    f"Finished uploading data for source {source} to {repo_id}. Total rows uploaded: {total_rows}")

            except Exception as e:
                bt.logging.error(f"Error during upload for source {source}: {e}")

        return hf_metadata_list

    def merge_top_items(self, old_items: Dict[str, Dict[str, Union[int, float]]],
                        new_items: Dict[str, Dict[str, Union[int, float]]]) -> Dict[str, Dict[str, Union[int, float]]]:
        """
        Merge two dictionaries of items, updating counts and percentages, and return the top 100 items by count.

        Args:
            old_items (Dict[str, Dict[str, Union[int, float]]]): A dictionary of existing items,
                where each key is an item identifier and the value is a dictionary containing
                'count' and 'percentage' keys.
            new_items (Dict[str, Dict[str, Union[int, float]]]): A dictionary of new items to be merged,
                following the same structure as old_items.

        Returns:
            Dict[str, Dict[str, Union[int, float]]]: A dictionary of the top 100 merged items,
                sorted by count in descending order. Each item contains updated 'count' and
                'percentage' values based on the merged totals.

        The method performs the following steps:
        1. Merges the two input dictionaries, summing the counts for common items.
        2. Recalculates the percentage for each item based on the new total count.
        3. Sorts the merged items by count in descending order.
        4. Returns the top 100 items from the sorted list.
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

        return dict(sorted(merged.items(), key=lambda x: x[1]['count'], reverse=True)[:100])

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
                merged[key] = min(merged.get(key, value), value) if key == 'start_date' else max(merged.get(key, value),
                                                                                                 value)
            elif key in ['subreddits', 'hashtags']:
                merged[key] = self.merge_top_items(merged.get(key, {}), value)
        return merged

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
            'start_date': df['datetime'].min().strftime('%Y-%m-%d'),
            'end_date': df['datetime'].max().strftime('%Y-%m-%d'),
        }

        if source == DataSource.REDDIT.value:
            stats['posts_count'] = df[df['dataType'] == 'post'].shape[0]
            stats['comments_count'] = df[df['dataType'] == 'comment'].shape[0]
            subreddit_counts = df['communityName'].value_counts().to_dict()
            stats['subreddits'] = {subreddit: {'count': count, 'percentage': (count / len(df)) * 100}
                                   for subreddit, count in subreddit_counts.items()}
        else:  # X (Twitter)
            # Count tweets with hashtags (non-NULL and non-NaN labels)
            has_hashtags = df[df['label'].notna() & (df['label'] != 'NULL')]
            stats['tweets_with_hashtags_count'] = len(has_hashtags)

            # Extract and count hashtags
            all_hashtags = df['label'].fillna('').str.split().explode()
            hashtag_counts = all_hashtags[all_hashtags != ''].value_counts().to_dict()

            # Calculate hashtag statistics
            total_tweets = len(df)
            stats['hashtags'] = {
                hashtag: {
                    'count': count,
                    'percentage': (count / total_tweets) * 100
                }
                for hashtag, count in hashtag_counts.items()
            }

        return stats

    def save_stats_json(self, stats: Dict[str, Any], platform: str, new_rows: int, repo_id: str) -> Dict[str, Any]:
        """
        Save the full statistics to a JSON file and upload it to the Hugging Face repository.

        This function merges new statistics with existing ones, updates counts and percentages,
        and maintains an update history. It then saves this data to a JSON file and uploads it
        to the specified Hugging Face repository.

        Args:
            stats (Dict[str, Any]): The new statistics to save.
            platform (str): The platform for which to save statistics ('reddit' or 'x').
            new_rows (int): The number of new rows added in this update.
            repo_id (str): repo_id
        Returns:
            Dict[str, Any]: The updated full statistics after merging and calculations.
        """
        filename = f"{platform}_stats.json"

        # Try to load existing stats
        try:
            existing_stats_file = self.hf_api.hf_hub_download(repo_id=repo_id, filename=filename,
                                                              repo_type="dataset")
            with open(existing_stats_file, 'r') as f:
                full_stats = json.load(f)
        except Exception:
            full_stats = {'total_rows': 0, 'last_update': None, 'update_history': []}

        # Update the stats
        full_stats['total_rows'] += new_rows
        full_stats['last_update'] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        full_stats['update_history'].append({
            'date': full_stats['last_update'],
            'rows_added': new_rows
        })

        # Merge new statistics
        if platform == 'reddit':
            full_stats['subreddits'] = self.merge_top_items(full_stats.get('subreddits', {}),
                                                            stats.get('subreddits', {}))
            full_stats['posts_count'] = full_stats.get('posts_count', 0) + stats.get('posts_count', 0)
            full_stats['comments_count'] = full_stats.get('comments_count', 0) + stats.get('comments_count', 0)
        else:  # X (Twitter)
            full_stats['hashtags'] = self.merge_top_items(full_stats.get('hashtags', {}), stats.get('hashtags', {}))
            full_stats['tweets_with_hashtags_count'] = full_stats.get('tweets_with_hashtags_count', 0) + stats.get(
                'tweets_with_hashtags_count', 0)

        # Recalculate percentages
        if platform == 'reddit':
            full_stats['posts_percentage'] = (full_stats['posts_count'] / full_stats['total_rows']) * 100
            full_stats['comments_percentage'] = (full_stats['comments_count'] / full_stats['total_rows']) * 100
        else:  # X (Twitter)
            full_stats['tweets_with_hashtags_percentage'] = (full_stats['tweets_with_hashtags_count'] / full_stats[
                'total_rows']) * 100

        # Update date range
        full_stats['start_date'] = min(full_stats.get('start_date', stats['start_date']), stats['start_date'])
        full_stats['end_date'] = max(full_stats.get('end_date', stats['end_date']), stats['end_date'])

        # Revert order of update history to display it correctly from latest to earliest in README.md

        full_stats['update_history'] = full_stats['update_history'][::-1]

        stats_json = json.dumps(full_stats, indent=2, cls=NumpyEncoder)

        self.hf_api.upload_file(
            token=self.hf_token,
            path_or_fileobj=stats_json.encode(),
            path_in_repo=filename,
            repo_id=repo_id,
            repo_type="dataset",
        )

        return full_stats


# Usage example:
# uploader = HuggingFaceUploader(
#     db_path='path/to/your/database.sqlite',
#     miner_hotkey='your_miner_hotkey',
#     encoding_key_manager=your_encoding_key_manager,
#     state_file='path/to/state.json',
#     output_dir='hf_storage',
#     chunk_size=1_000_000
# )
# metadata_list = uploader.upload_sql_to_huggingface()
