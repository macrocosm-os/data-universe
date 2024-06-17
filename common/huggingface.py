import pandas as pd
import sqlite3
from datasets import Dataset
from huggingface_hub import HfApi
import os
import time
import json
import logging
import dotenv
from typing import Iterator

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


TWEET_DATASET_COLUMNS = ['text', 'label', 'tweet_hashtags', 'datetime']

REDDIT_DATASET_COLUMNS = ['text', 'label', 'dataType', 'communityName', 'datetime']


def preprocess_twitter_df(df: pd.DataFrame):
    df['content'] = df['content'].apply(lambda b: json.loads(b.decode('utf-8')))
    df['text'] = df['content'].apply(lambda x: x.get('text'))
    df['tweet_hashtags'] = df['content'].apply(lambda x: x.get('tweet_hashtags'))
    del df['content']
    # Hide datetime
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].dt.strftime('%Y-%d-%m')

    return df[TWEET_DATASET_COLUMNS]


def preprocess_reddit_df(df: pd.DataFrame) -> pd.DataFrame:

    df['content'] = df['content'].apply(lambda b: json.loads(b.decode('utf-8')))
    df['text'] = df['content'].apply(lambda x: x.get('body'))
    df['dataType'] = df['content'].apply(lambda x: x.get('body'))
    df['communityName'] = df['content'].apply(lambda x: x.get('communityName'))
    del df['content']

    # Hide the date
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].dt.strftime('%Y-%d-%m')

    return df[REDDIT_DATASET_COLUMNS]


def upload_large_dataset_in_chunks(db_path: str, table_name: str, chunksize: int = 500000) -> str:
    conn = sqlite3.connect(db_path)

    for source in [1,2]:
        try:
            df_iterator: Iterator[pd.DataFrame] = pd.read_sql_query(
                f"SELECT datetime, label, content FROM {table_name} where source = {source};", conn,
                chunksize=chunksize)

        except Exception as e:
            logging.error(f"Failed to load data from table {table_name}: {e}")
            return ""

        hf_token: str = os.getenv("HUGGINGFACE_TOKEN")
        if not hf_token:
            logging.error("Hugging Face token not found. Please check your environment variables.")
            return ""

        api = HfApi()
        try:
            dataset_name = 'reddit_dataset' if source == 1 else 'twitter'
            dataset_url: str = api.create_repo(token=hf_token, repo_id=dataset_name, private=False, repo_type="dataset")
        except Exception as e:
            logging.error(f"Failed to create dataset repository on Hugging Face Hub: {e}")
            return ""

        successful_chunks = 0

        for i, df in enumerate(df_iterator):
            dataset_chunk_name = f"{dataset_name}_chunk_{i}"
            df = preprocess_reddit_df(df) if source == 1 else preprocess_twitter_df(df)
            hf_dataset = Dataset.from_pandas(df)
            try:
                hf_dataset.push_to_hub(dataset_name, private=False, token=hf_token)
                logging.info(f"Uploaded chunk {i} successfully.")
                successful_chunks += 1
            except Exception as e:
                logging.error(f"Failed to upload chunk {i}: {e}")
                continue  # Optionally, decide if you want to abort or continue on failure

        conn.close()

        if successful_chunks > 0:
            logging.info(f"{successful_chunks} chunks have been successfully uploaded to the Hugging Face Hub.")
        else:
            logging.error("No chunks were successfully uploaded.")

        return dataset_url



# Example usage
# upload_large_dataset_in_chunks("path/to/your/database.db", "your_table_name", "your_dataset_name")





