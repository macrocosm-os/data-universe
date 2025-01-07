"""Module for HuggingFace dataset utilities and validation."""
import os
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
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.scraper import HFValidationResult
from common.data import DataSource, HuggingFaceMetadata
from dotenv import load_dotenv

load_dotenv()


def get_latest_commit_files(repo_id: str) -> List[str]:
    api = HfApi(token=os.getenv('HUGGINGFACE_TOKEN', ''))
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


def get_validation_data(repo_id: str, files: List[str], num_rows: int = 10) -> Tuple[List[str], pd.DataFrame]:
    """
    Get both encoded URLs and complete DataFrame (with encoded values) for validation.

    Returns:
        Tuple[List[str], pd.DataFrame]: (encoded_urls, complete_dataframe)
    """
    if not files:
        raise ValueError("No parquet files found in the dataset.")

    selected_file = random.choice(files)
    bt.logging.trace(f"Selected file: {selected_file}")

    dataset = load_dataset(
        repo_id,
        data_files={'train': selected_file},
        split='train',
        streaming=True
    )

    random_seed = random.randint(0, 2 ** 32 - 1)
    shuffled_dataset = dataset.shuffle(buffer_size=10_000, seed=random_seed)

    selected_rows = list(itertools.islice(shuffled_dataset, num_rows))
    df = pd.DataFrame(selected_rows)

    encoded_urls = []
    if 'url_encoded' in df.columns:
        encoded_urls = df['url_encoded'].dropna().tolist()[:10]

    return encoded_urls, df


def decode_dataframe(df: pd.DataFrame, encoding_key: str) -> pd.DataFrame:
    """
    Decode only username fields, leave URLs encoded for miner validation.
    """
    decoded_df = df.copy()
    key_manager = SymKeyEncodingKeyManager(encoding_key)
    key_manager.sym_key = encoding_key.encode()
    fernet = key_manager.get_fernet()

    # Only decode username, leave url_encoded as is
    if 'username_encoded' in decoded_df.columns:
        decoded_df['username'] = decoded_df['username_encoded'].apply(
            lambda x: decode_url(x, fernet) if x else None
        )
        decoded_df = decoded_df.drop(columns=['username_encoded'])

    # Keep url_encoded column for miner validation
    return decoded_df


async def validate_hf_content(df: pd.DataFrame, source: DataSource) -> HFValidationResult:
    """Validate DataFrame content using appropriate scraper."""
    try:
        if source == DataSource.REDDIT:
            scraper = RedditCustomScraper()
        elif source == DataSource.X:
            scraper = ApiDojoTwitterScraper() # todo
        else:
            bt.logging.error(f"Unknown data source {source}")
            return HFValidationResult(
                is_valid=False,
                reason=f"Unknown data source {source}",
                validation_percentage=0
            )

        return await scraper.validate_hf(entities=df.to_dict(orient='records'))

    except Exception as e:
        bt.logging.error(f"Error validating content: {str(e)}")
        return HFValidationResult(
            is_valid=False,
            reason=f"Error validating content: {str(e)}",
            validation_percentage=0
        )


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
        # Test get_validation_data
        encoded_urls, encoded_df = await run_test("get_validation_data", get_validation_data, repo_id,
                                                  new_parquet_files)

        if encoded_urls and not encoded_df.empty:
            # Test decoding
            decoded_df = await run_test("decode_dataframe", decode_dataframe, encoded_df, test_encoding_key)

            if isinstance(decoded_df, pd.DataFrame) and not decoded_df.empty:
                # Test content validation
                test_metadata = HuggingFaceMetadata(
                    repo_name=repo_id,
                    source=DataSource.X,
                    updated_at=dt.datetime.now(),
                    encoding_key=test_encoding_key
                )
                validation_result = await run_test("validate_hf_content", validate_hf_content, decoded_df,
                                                   test_metadata.source)
                print(f"validation result: {validation_result}")
            else:
                print("Could not proceed with validation due to decoding failure.")
        else:
            print("Could not proceed with validation due to empty or invalid data.")
    else:
        print("No new parquet files found.")


if __name__ == "__main__":
    # You can change this to any repository you have access to
    test_repo_id = "arrmlet/x_dataset_123456"
    asyncio.run(test_hf(test_repo_id))