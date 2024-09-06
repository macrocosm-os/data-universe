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
from common.data import HuggingFaceMetadata
from typing import List


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
                            state['last_upload'][source] = dt.datetime.strptime(state['last_upload'][source], '%Y-%m-%d %H:%M:%S')
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

        for source in [1, 2]:
            dataset_name = f'{"reddit" if source == 1 else "x"}_dataset_{self.unique_id}'
            repo_id = f"{self.hf_api.whoami(self.hf_token)['name']}/{dataset_name}"

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

                        chunk_count += 1
                        total_rows += len(df)
                        print(f'total rows. {len(df)}')

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

                hf_metadata = HuggingFaceMetadata(
                    repo_name=repo_id,
                    source=source,
                    updated_at=dt.datetime.now(dt.timezone.utc)
                )
                hf_metadata_list.append(hf_metadata)

                bt.logging.success(
                    f"Finished uploading data for source {source} to {repo_id}. Total rows uploaded: {total_rows}")

            except Exception as e:
                bt.logging.error(f"Error during upload for source {source}: {e}")

        return hf_metadata_list

# Usage example:
# uploader = OptimizedHuggingFaceUploader(
#     db_path='path/to/your/database.sqlite',
#     miner_hotkey='your_miner_hotkey',
#     encoding_key_manager=your_encoding_key_manager,
#     state_file='path/to/state.json',
#     output_dir='hf_storage',
#     chunk_size=1_000_000
# )
# metadata_list = uploader.upload_sql_to_huggingface()