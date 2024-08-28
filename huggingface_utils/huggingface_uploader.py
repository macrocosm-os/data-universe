import sqlite3
import pandas as pd
import bittensor as bt
import os
import json
from huggingface_hub import HfApi
from huggingface_utils.utils import preprocess_reddit_df, preprocess_twitter_df, generate_static_integer
from huggingface_utils.encoding_system import EncodingKeyManager
from dotenv import load_dotenv
import datetime as dt
from common.data import HuggingFaceMetadata

load_dotenv()


def remove_all_files_in_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                os.rmdir(file_path)
        except Exception as e:
            bt.logging.info(f'Failed to delete {file_path}. Reason: {e}')


class HuggingFaceUploader:
    def __init__(self, db_path: str, miner_hotkey: str, encoding_key_manager: EncodingKeyManager, output_dir: str = 'hf_storage'):
        self.db_path = db_path
        self.output_dir = output_dir
        self.hf_api = HfApi()
        self.miner_hotkey = miner_hotkey
        self.unique_id = '0102'# generate_static_integer(self.miner_hotkey)
        self.encoding_key_manager = encoding_key_manager
        self.hf_token = os.getenv("HUGGINGFACE_TOKEN")
        self.state_file = f'hf_uploader_state_{self.unique_id}.json'

    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                # Convert stored string dates back to datetime objects
                for source in state['last_upload']:
                    if state['last_upload'][source]:
                        state['last_upload'][source] = dt.datetime.strptime(state['last_upload'][source], '%Y-%m-%d %H:%M:%S')
                return state
        return {'last_upload': {'1': None, '2': None}, 'total_rows': {'1': 0, '2': 0}}

    def save_state(self, state):
        state_to_save = {
            'last_upload': {
                source: (last_upload.strftime('%Y-%m-%d %H:%M:%S') if isinstance(last_upload, dt.datetime) else last_upload) if last_upload else None
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

    def upload_sql_to_huggingface(self, storage, chunk_size=1_000_000):
        if not self.hf_token:
            bt.logging.error("Hugging Face token not found. Please check your environment variables.")
            return

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

            last_upload = state['last_upload'][str(source)]
            total_rows = state['total_rows'][str(source)]

            connection, df_iterator = storage.get_data_for_huggingface_upload(source, last_upload, chunk_size)
            chunk_count = 0

            try:
                for df in df_iterator:
                    if total_rows >= 400_000_000:
                        bt.logging.info(f"Reached 400 million rows limit for source {source}. Stopping upload.")
                        break

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

                    if chunk_count == 10:
                        self.upload_parquet_to_hf(repo_id)
                        bt.logging.info(f'Uploaded {chunk_count} chunks to {repo_id}')
                        next_chunk_id += chunk_count
                        chunk_count = 0

                if chunk_count > 0:
                    self.upload_parquet_to_hf(repo_id)
                    bt.logging.info(f'Uploaded final {chunk_count} chunks to {repo_id}')

                state['last_upload'][str(source)] = last_upload
                state['total_rows'][str(source)] = total_rows
                self.save_state(state)

                # Create and add HFMetadata object
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
            finally:
                if connection:
                    connection.close()

        return hf_metadata_list

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

# Example usage
# if __name__ == "__main__":
#     uploader = HuggingFaceUploader('SqliteMinerStorage.sqlite', encoding_key_manager, 'miner_hotkey', 'hf_storage')
#     uploader.upload_sql_to_huggingface()