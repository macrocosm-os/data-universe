"""Module for HuggingFace dataset utilities and validation."""
import os
import random
import bittensor as bt
import pandas as pd
from datasets import load_dataset
import itertools
import asyncio
import datetime as dt
import pyarrow as pa
import fsspec
from typing import List, Dict, Any, Tuple, Optional
from huggingface_hub import HfApi, hf_hub_url
from huggingface_utils.encoding_system import SymKeyEncodingKeyManager, decode_url
from scraping.reddit.reddit_custom_scraper import RedditCustomScraper
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.scraper import HFValidationResult
from common.data import DataSource, HuggingFaceMetadata
from dotenv import load_dotenv

load_dotenv()


def get_latest_commit_files(repo_id: str) -> Tuple[List[str], Optional[dt.datetime]]:
    """
    Retrieve new or modified parquet files from the latest commit along with its commit date.

    Args:
        repo_id (str): The HuggingFace dataset repository ID.

    Returns:
        Tuple[List[str], Optional[str]]: A tuple containing:
            - A list of new/modified parquet file paths.
            - The commit date (as a string) or None if unavailable.
    """
    api = HfApi(token=os.getenv('HUGGINGFACE_TOKEN', ''))
    try:
        commits = api.list_repo_commits(repo_id=repo_id, repo_type="dataset")
        for i, commit in enumerate(commits):
            current_files = set(api.list_repo_files(repo_id=repo_id, revision=commit.commit_id, repo_type="dataset"))
            previous_files = set()
            if i + 1 < len(commits):
                previous_files = set(api.list_repo_files(repo_id=repo_id, revision=commits[i + 1].commit_id, repo_type="dataset"))
            new_or_modified_files = current_files - previous_files
            new_parquet_files = [file for file in new_or_modified_files if file.endswith('.parquet') and file.startswith('data/')]
            if new_parquet_files:
                # Attempt to extract commit date from several potential attributes
                commit_date = getattr(commit, "created_at", None)
                if commit_date is None:
                    commit_date = getattr(commit, "commit_date", None)
                if commit_date is None and hasattr(commit, "commit"):
                    commit_date = commit.commit.get("author", {}).get("date", None)
                return new_parquet_files, commit_date
        bt.logging.warning("No commits with new parquet files found")
        return [], None
    except Exception as e:
        bt.logging.error(f"An error occurred while fetching commit information: {str(e)}")
        return [], None


def compare_latest_commits_parquet_files(repo_id: str) -> Tuple[bool, List[str], Optional[str]]:
    """
    Compare the parquet files between the two latest commits.

    Args:
        repo_id (str): The HuggingFace dataset repository ID.

    Returns:
        Tuple[bool, List[str], Optional[str]]: A tuple containing:
            - A boolean indicating whether the two latest commits have the same parquet files.
            - The list of parquet files from the latest commit.
            - The commit date of the latest commit (if available).
    """
    api = HfApi(token=os.getenv('HUGGINGFACE_TOKEN', ''))
    try:
        commits = api.list_repo_commits(repo_id=repo_id, repo_type="dataset")
        if len(commits) < 2:
            bt.logging.warning("Not enough commits to compare.")
            latest_files = list(api.list_repo_files(repo_id=repo_id, revision=commits[0].commit_id, repo_type="dataset"))
            parquet_files = [file for file in latest_files if file.endswith('.parquet') and file.startswith('data/')]
            commit_date = getattr(commits[0], "created_at", None)
            if commit_date is None:
                commit_date = getattr(commits[0], "commit_date", None)
            if commit_date is None and hasattr(commits[0], "commit"):
                commit_date = commits[0].commit.get("author", {}).get("date", None)
            return False, parquet_files, commit_date

        latest_commit = commits[0]
        second_commit = commits[1]
        latest_files = list(api.list_repo_files(repo_id=repo_id, revision=latest_commit.commit_id, repo_type="dataset"))
        second_files = list(api.list_repo_files(repo_id=repo_id, revision=second_commit.commit_id, repo_type="dataset"))
        latest_parquet = [file for file in latest_files if file.endswith('.parquet') and file.startswith('data/')]
        second_parquet = [file for file in second_files if file.endswith('.parquet') and file.startswith('data/')]
        same = set(latest_parquet) == set(second_parquet)
        commit_date = getattr(latest_commit, "created_at", None)
        if commit_date is None:
            commit_date = getattr(latest_commit, "commit_date", None)
        if commit_date is None and hasattr(latest_commit, "commit"):
            commit_date = latest_commit.commit.get("author", {}).get("date", None)
        return same, latest_parquet, commit_date
    except Exception as e:
        bt.logging.error(f"Error comparing commits: {str(e)}")
        return False, [], None


def schema_to_dict(schema: pa.Schema) -> dict:
    """ Manually convert a pyarrow Schema to a Python dict. """
    fields = []
    for field in schema:
        fields.append({
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable
            # we can add more fields
        })

    # Convert metadata (if present) to a dict of decoded strings.
    metadata = {}
    if schema.metadata is not None:
        for k, v in schema.metadata.items():
            metadata[k.decode("utf-8")] = v.decode("utf-8")

    return {
        "fields": fields,
        "metadata": metadata
    }


def get_parquet_file_structure(repo_id: str, file: str) -> dict:
    """
    Retrieve the schema (structure) of a parquet file from a HuggingFace dataset repository
    without downloading the entire file, returning a dictionary representation.
    """
    try:
        file_url = hf_hub_url(repo_id=repo_id, filename=file, repo_type="dataset")
        with fsspec.open(file_url, "rb") as f:
            parquet_file = pa.parquet.ParquetFile(f)
            schema = parquet_file.schema_arrow

            # Convert the pyarrow Schema to a dict manually:
            return schema_to_dict(schema)

    except Exception as e:
        import bittensor as bt
        bt.logging.error(f"Error retrieving parquet structure: {str(e)}")
        return {}


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
    Decode only username fields, leaving URLs encoded for miner validation.
    """
    decoded_df = df.copy()
    key_manager = SymKeyEncodingKeyManager(encoding_key)
    key_manager.sym_key = encoding_key.encode()
    fernet = key_manager.get_fernet()
    if 'username_encoded' in decoded_df.columns:
        decoded_df['username'] = decoded_df['username_encoded'].apply(
            lambda x: decode_url(x, fernet) if x else None
        )
        decoded_df = decoded_df.drop(columns=['username_encoded'])
    return decoded_df


async def validate_hf_content(df: pd.DataFrame, source: DataSource) -> HFValidationResult:
    """
    Validate DataFrame content using the appropriate scraper.
    """
    try:
        if source == DataSource.REDDIT:
            scraper = RedditCustomScraper()
        elif source == DataSource.X:
            scraper = ApiDojoTwitterScraper()  # todo: update as needed
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

    # Compare the two latest commits for parquet files.
    same_commits, latest_parquet_files, latest_commit_date = compare_latest_commits_parquet_files(repo_id)

    a = (dt.datetime.now(dt.timezone.utc) - latest_commit_date) < dt.timedelta(hours=17)
    print(f'========\n {a} \n==========')
    if same_commits:
        print("The latest two commits have the same parquet files.")
    else:
        print("The latest two commits have different parquet files.")
    if latest_commit_date:
        print(f"Latest parquet commit date: {latest_commit_date}")
    else:
        print("No commit date found for the latest parquet changes.")
    if same_commits and latest_parquet_files:
        structure = get_parquet_file_structure(repo_id, latest_parquet_files[0])
        print(f"Structure of parquet file '{latest_parquet_files[0]}': {structure}")
    else:
        print("Skipping parquet structure retrieval due to differences in commits or missing files.")

    async def run_test(test_name, test_func, *args):
        print(f"\nTesting {test_name}:")
        try:
            result = await test_func(*args) if asyncio.iscoroutinefunction(test_func) else test_func(*args)
            print(f"Success: {result}")
            return result
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    # Test get_latest_commit_files and show the commit date.
    result = await run_test("get_latest_commit_files", get_latest_commit_files, repo_id)
    if result:
        new_parquet_files, commit_date = result
        print(f"Latest commit date from get_latest_commit_files: {commit_date}")
    else:
        new_parquet_files, commit_date = [], None

    if new_parquet_files:
        encoded_urls, encoded_df = await run_test("get_validation_data", get_validation_data, repo_id, new_parquet_files)
        if encoded_urls and not encoded_df.empty:
            test_encoding_key = "XdcRI9sPT2e43a8fda53H13HpGqpQLTZHHeIoHPtIMI="
            decoded_df = await run_test("decode_dataframe", decode_dataframe, encoded_df, test_encoding_key)
            if isinstance(decoded_df, pd.DataFrame) and not decoded_df.empty:
                test_metadata = HuggingFaceMetadata(
                    repo_name=repo_id,
                    source=DataSource.X,
                    updated_at=dt.datetime.now(),
                    encoding_key=test_encoding_key
                )
                validation_result = await run_test("validate_hf_content", validate_hf_content, decoded_df, test_metadata.source)
                print(f"Validation result: {validation_result}")
            else:
                print("Could not proceed with validation due to decoding failure.")
        else:
            print("Could not proceed with validation due to empty or invalid data.")
    else:
        print("No new parquet files found.")


if __name__ == "__main__":
    test_repo_id = "icedwind/x_dataset_4561"  # Change to your repository ID as needed
    asyncio.run(test_hf(test_repo_id))
