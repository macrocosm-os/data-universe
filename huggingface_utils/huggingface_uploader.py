import sqlite3
import pandas as pd
import bittensor as bt
import os
from huggingface_hub import HfApi
from huggingface_utils.utils import preprocess_reddit_df, preprocess_twitter_df
from dotenv import load_dotenv

load_dotenv()


def preprocess_data(df, source):
    if source == 1:
        return preprocess_reddit_df(df)
    else:
        return preprocess_twitter_df(df)


def remove_all_files_in_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                os.rmdir(file_path)
        except Exception as e:
            bt.logging.debug(f'Failed to delete {file_path}. Reason: {e}')


class HuggingFaceUploader:
    def __init__(self, db_path, table_name, output_dir):
        self.db_path = db_path
        self.table_name = table_name
        self.output_dir = output_dir
        self.hf_api = HfApi()
        self.hf_token = os.getenv("HUGGINGFACE_TOKEN")

    def upload_sql_to_huggingface(self, chunk_size=1_000_000):
        conn = sqlite3.connect(self.db_path)  # TODO REPLACE IT WITH CONTEXT MANAGER
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        for source in [1, 2]:
            try:
                query = f"SELECT datetime, label, content FROM {self.table_name} WHERE source = {source};"
                df_iterator = pd.read_sql_query(query, conn, chunksize=chunk_size)

                for i, df in enumerate(df_iterator):
                    df = preprocess_data(df, source)
                    parquet_path = os.path.join(self.output_dir, f"train-{self.table_name}_chunk_{i}.parquet")
                    df.to_parquet(parquet_path)
                    bt.logging.trace(f"Saved Parquet file: {parquet_path}")

                self.upload_parquet_to_hf(source)
                remove_all_files_in_directory(self.output_dir)

            except Exception as e:
                bt.logging.trace(f"Failed to load and save data from table {self.table_name}: {e}")

        conn.close()

    def upload_parquet_to_hf(self, source):
        if not self.hf_token:
            bt.logging.error("Hugging Face token not found. Please check your environment variables.")
            return

        dataset_name = 'reddit_dataset_1' if source == 1 else 'twitter_dataset_1'
        repo_id = f"{self.hf_api.whoami(self.hf_token)['name']}/{dataset_name}"

        self.hf_api.create_repo(token=self.hf_token, repo_id=dataset_name, private=False, repo_type="dataset",
                                exist_ok=True)
        self.hf_api.upload_folder(token=self.hf_token,
                                  folder_path=self.output_dir,
                                  repo_id=repo_id,
                                  path_in_repo='data/',
                                  repo_type='dataset',
                                  allow_patterns="*.parquet",  # Upload all local parquet files
                                  delete_patterns="*.parquet",  # Delete all remote parquet files before
                                  )


# Example usage
if __name__ == "__main__":
    uploader = HuggingFaceUploader('SqliteMinerStorage.sqlite', 'DataEntity', 'hf_storage')
    uploader.upload_sql_to_huggingface()
