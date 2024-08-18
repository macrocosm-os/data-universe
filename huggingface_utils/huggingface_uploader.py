import sqlite3
import pandas as pd
import bittensor as bt
import os
from huggingface_hub import HfApi
from huggingface_utils.utils import preprocess_reddit_df, preprocess_twitter_df, generate_static_integer
from huggingface_utils.encoding_system import EncodingKeyManager
from dotenv import load_dotenv
import datetime as dt
from common.data import HuggingFaceMetadata

load_dotenv()


def preprocess_data(df, source, encoding_key_manager):
    if source == 1:
        return preprocess_reddit_df(df, key_manager=encoding_key_manager)
    else:
        return preprocess_twitter_df(df, key_manager=encoding_key_manager)


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
    def __init__(self, db_path: str, encoding_key_manager: EncodingKeyManager, miner_hotkey: str,
                 output_dir: str = 'hf_storage'):
        self.db_path = db_path
        self.output_dir = output_dir
        self.hf_api = HfApi()
        self.miner_hotkey = miner_hotkey
        self.unique_id = generate_static_integer(self.miner_hotkey)
        self.encoding_key_manager = encoding_key_manager
        self.hf_token = os.getenv("HUGGINGFACE_TOKEN")

    def upload_sql_to_huggingface(self, storage, chunk_size=1_000_000):
        hf_values = []
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        for source in [1, 2]:
            try:
                query = f"SELECT datetime, label, content FROM DataEntity WHERE source = {source};"
                bt.logging.info(f"Started processing data for source: {source}")

                with sqlite3.connect(self.db_path) as conn:
                    df_iterator = pd.read_sql_query(query, conn, chunksize=chunk_size)

                    chunk_count = 0
                    is_first_upload = True

                    for i, df in enumerate(df_iterator):
                        df = preprocess_data(df, source, self.encoding_key_manager)
                        parquet_path = os.path.join(self.output_dir, f"train-DataEntity_chunk_{i % 10}.parquet")
                        df.to_parquet(parquet_path)
                        bt.logging.info(f"Saved Parquet file: {parquet_path}")

                        chunk_count += 1

                        if chunk_count == 10 or (
                                i == 0 and chunk_count > 0):  # Upload after 10 chunks or if it's the last batch
                            repo_id = self.upload_parquet_to_hf(source, is_first_upload)
                            bt.logging.info(f'Uploaded {chunk_count} chunks to {repo_id}')
                            remove_all_files_in_directory(self.output_dir)
                            chunk_count = 0
                            is_first_upload = False

                    # Upload any remaining chunks
                    if chunk_count > 0:
                        repo_id = self.upload_parquet_to_hf(source, is_first_upload)
                        bt.logging.info(f'Uploaded final {chunk_count} chunks to {repo_id}')
                        remove_all_files_in_directory(self.output_dir)

                hf_values.append(HuggingFaceMetadata(
                    repo_name=repo_id,
                    source=source,
                    updated_at=dt.datetime.utcnow(),
                ))

            except Exception as e:
                bt.logging.error(f"Failed to process and upload data for source {source}: {e}")

        storage.store_hf_dataset_info(hf_values)

    def upload_parquet_to_hf(self, source, is_first_upload):
        if not self.hf_token:
            bt.logging.error("Hugging Face token not found. Please check your environment variables.")
            return

        dataset_name = f'reddit_dataset_{self.unique_id}' if source == 1 else f'x_dataset_{self.unique_id}'
        repo_id = f"{self.hf_api.whoami(self.hf_token)['name']}/{dataset_name}"

        self.hf_api.create_repo(token=self.hf_token, repo_id=dataset_name, private=False, repo_type="dataset",
                                exist_ok=True)

        delete_patterns = "*.parquet" if is_first_upload else None

        self.hf_api.upload_folder(
            token=self.hf_token,
            folder_path=self.output_dir,
            repo_id=repo_id,
            path_in_repo='data/',
            repo_type='dataset',
            allow_patterns="*.parquet",  # Upload all local parquet files
            delete_patterns=delete_patterns,  # Delete all remote parquet files only on first upload
        )
        return repo_id

# Example usage
# if __name__ == "__main__":
#     uploader = HuggingFaceUploader('SqliteMinerStorage.sqlite', encoding_key_manager, 'miner_hotkey', 'hf_storage')
#     uploader.upload_sql_to_huggingface()