"""Module for HuggingFace dataset utilities and validation."""

import random
from typing import List, Dict, Any, Tuple
import bittensor as bt
import pandas as pd
from datasets import load_dataset
import itertools
import asyncio
import datetime as dt
from huggingface_hub import HfApi
from huggingface_utils.encoding_system import SymKeyEncodingKeyManager, decode_url
from scraping.reddit.reddit_custom_scraper import RedditCustomScraper
from scraping.x.apidojo_scrapper import ApiDojoTwitterScraper
from common.data import DataSource, HuggingFaceMetadata
import re

def update_dataset_names(df):
    def transform_dataset_name(name):
        if name == 'no_dataset_provided':
            return [name]
        match = re.match(r'(.+)/reddit_dataset_(\d+)', name)
        if match:
            username, num = match.groups()
            return [f"{username}/reddit_dataset_{num}", f"{username}/x_dataset_{num}"]
        return [name]  # If it doesn't match the pattern, return the original name in a list

    df['repo_name'] = df['repo_name'].apply(transform_dataset_name)
    return df

def get_latest_commit_files(repo_id: str) -> List[str]:
    api = HfApi()
    try:
        # Get the commit history
        commits = api.list_repo_commits(repo_id=repo_id, repo_type="dataset")

        for i, commit in enumerate(commits):
            # Get files from this commit
            current_files = set(api.list_repo_files(repo_id=repo_id, revision=commit.commit_id, repo_type="dataset"))

            # Get files from previous commit (if it exists)
            previous_files = set()
            if i + 1 < len(commits):
                previous_files = set(
                    api.list_repo_files(repo_id=repo_id, revision=commits[i + 1].commit_id, repo_type="dataset"))

            # Find new or modified files
            new_or_modified_files = current_files - previous_files

            # Filter for new or modified parquet files
            new_parquet_files = [file for file in new_or_modified_files if
                                 file.endswith('.parquet') and file.startswith('data/')]

            if new_parquet_files:
                return new_parquet_files

        bt.logging.warning("No commits with new parquet files found")
        return []

    except Exception as e:
        bt.logging.error(f"An error occurred while fetching commit information: {str(e)}")
        return []


def select_random_rows_from_parquet(repo_id: str, files: List[str], encoding_key: str, num_rows: int = 10, buffer_size: int = 10_000) -> pd.DataFrame:
    """Efficiently select random rows from randomly chosen parquet files in a Hugging Face dataset."""
    if not files:
        raise ValueError("No parquet files found in the dataset.")

    # Select a random file
    selected_file = random.choice(files)
    bt.logging.trace(f"Selected file: {selected_file}")

    # Load the dataset in streaming mode
    dataset = load_dataset(
        repo_id,
        data_files={'train': selected_file},
        split='train',
        streaming=True
    )

    # Generate random seed and shuffle the dataset
    random_seed = random.randint(0, 2 ** 32 - 1)
    shuffled_dataset = dataset.shuffle(buffer_size=buffer_size, seed=random_seed)

    # Select the specified number of rows
    selected_rows = list(itertools.islice(shuffled_dataset, num_rows))

    # Convert to DataFrame
    df = pd.DataFrame(selected_rows)
    # Decode encrypted columns
    key_manager = SymKeyEncodingKeyManager(encoding_key)
    key_manager.sym_key = encoding_key.encode()
    fernet = key_manager.get_fernet()

    for column in ['url_encoded', 'username_encoded']:
        if column in df.columns:
            df[column.replace('_encoded', '')] = df[column].apply(lambda x: decode_url(x, fernet))
            df = df.drop(columns=[column])

    bt.logging.trace(df)

    return df


async def validate_huggingface_dataset(hf_metadata: HuggingFaceMetadata) -> bool:
    """Validate a HuggingFace dataset."""
    repo_id = hf_metadata.repo_name
    encoding_key = hf_metadata.encoding_key

    if not encoding_key:
        bt.logging.error(f"No encoding key provided for dataset {repo_id}")
        return False

    try:
        # Get new parquet files and commit info
        new_parquet_files = get_latest_commit_files(repo_id)

        if not new_parquet_files:
            bt.logging.warning(f"No new parquet files found for {repo_id}")
            return False

        # Select random rows from the dataset
        selected_rows = select_random_rows_from_parquet(repo_id, new_parquet_files, encoding_key)

        # Determine which scraper to use based on the data source todo replace with scraper_provider
        if hf_metadata.source == DataSource.REDDIT:
            scraper = RedditCustomScraper()
        elif hf_metadata.source == DataSource.X:
            scraper = ApiDojoTwitterScraper()
        else:
            bt.logging.error(f"Unknown data source {hf_metadata.source}")
            return False

        # Validate the selected rows
        validation_result = await scraper.validate_hf(entities=selected_rows.to_dict(orient='records'))
        return validation_result

    except Exception as e:
        bt.logging.error(f"Error validating dataset {repo_id}: {str(e)}")
        return False


async def test_hf(repo_id: str):
    print(f"Testing HuggingFace utilities with repo: {repo_id}")

    # Test encoding key (you should replace this with a real encoding key)
    test_encoding_key = "XdcRI9sPT2e43a8fda53H13HpGqpQLTZHHeIoHPtIMI="

    async def run_test(test_name, test_func, *args):
        print(f"\nTesting {test_name}:")
        try:
            result = await test_func(*args) if asyncio.iscoroutinefunction(test_func) else test_func(*args)
            print(f"Success: {result}")
            return result
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    # Test get_latest_commit_files
    new_parquet_files = await run_test("get_latest_commit_files", get_latest_commit_files, repo_id)

    if new_parquet_files:

        # Test select_random_rows_from_parquet
        df = await run_test("select_random_rows_from_parquet", select_random_rows_from_parquet, repo_id, new_parquet_files, test_encoding_key)

        if isinstance(df, pd.DataFrame) and not df.empty:

            # Test validate_huggingface_dataset
            test_metadata = HuggingFaceMetadata(
                repo_name=repo_id,
                source=DataSource.X,  # or DataSource.X
                updated_at=dt.datetime.now(),
                encoding_key=test_encoding_key
            )
            result = await run_test("validate_huggingface_dataset", validate_huggingface_dataset, test_metadata)
            print(f"validation result: {result}")
        else:
            print("Could not proceed with validation due to empty or invalid dataframe.")
    else:
        print("No new parquet files found.")

if __name__ == "__main__":
    # You can change this to any repository you have access to
    test_repo_id = "arrmlet/x_dataset_123456"
    asyncio.run(test_hf(test_repo_id))