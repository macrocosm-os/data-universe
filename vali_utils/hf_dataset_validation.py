import random
from typing import List, Dict, Any
import bittensor as bt
import requests
import pandas as pd
from datasets import load_dataset
from huggingface_utils.encoding_system import EncodingKeyManager, decode_dataframe_column


def get_parquet_files(repo_id: str) -> List[str]:
    """
    Fetch a list of parquet files from a Hugging Face dataset repository.

    Args:
        repo_id (str): The Hugging Face dataset repository ID.

    Returns:
        List[str]: A list of parquet file paths.

    Raises:
        requests.RequestException: If the API request fails.
    """
    api_url = f"https://huggingface.co/api/datasets/{repo_id}/tree/main/data"
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        files = [item['path'] for item in response.json() if item['path'].endswith('.parquet')]
        return files
    except requests.RequestException as e:
        raise requests.RequestException(f"Failed to fetch file list: {e}")


def select_random_rows_from_parquet(repo_id: str, num_rows: int = 10, buffer_size: int = 10_000) -> pd.DataFrame:
    """
    Efficiently select random rows from a randomly chosen parquet file in a Hugging Face dataset
    using a streaming approach with shuffling.

    Args:
        repo_id (str): The Hugging Face dataset repository ID.
        num_rows (int, optional): Number of random rows to select. Defaults to 10.
        buffer_size (int, optional): Size of the buffer for shuffling. Defaults to 10,000.
        seed (int, optional): Random seed for reproducibility. Defaults to 42.

    Returns:
        pd.DataFrame: A DataFrame containing the randomly selected rows.

    Raises:
        ValueError: If no parquet files are found in the dataset.
    """
    parquet_files = get_parquet_files(repo_id)

    if not parquet_files:
        raise ValueError("No parquet files found in the dataset.")

    selected_file = random.choice(parquet_files)
    bt.logging.trace(f"Selected file: {selected_file}")

    # Load the dataset in streaming mode
    dataset = load_dataset(
        repo_id,
        data_files={'train': selected_file},
        split='train',
        streaming=True
    )

    # generate random seed
    random_seed = random.randint(0, 2 ** 32 - 1)
    # Shuffle the dataset
    shuffled_dataset = dataset.shuffle(buffer_size=buffer_size, seed=random_seed)

    # Select the specified number of rows
    selected_rows = []
    for i, row in enumerate(shuffled_dataset):
        if i < num_rows:
            selected_rows.append(row)
        else:
            break

    # Convert to DataFrame
    a = EncodingKeyManager(key_path='/Users/volodymyrtruba/data-universe/huggingface_utils/encoding_key.json')
    df = pd.DataFrame(selected_rows)

    df = decode_dataframe_column(df, 'url_encoded', a)
    df = decode_dataframe_column(df, 'username_encoded', a)
    bt.logging.trace(df)
    return df


async def main():
    """Main function to demonstrate the usage of the script."""
    repo_id = "arrmlet/x_dataset_0"

    try:
        selected_rows = select_random_rows_from_parquet(repo_id)
        s = selected_rows.to_dict(orient='records')
        from scraping.x.apidojo_scrapper import ApiDojoTwitterScraper
        scrapper = ApiDojoTwitterScraper()
        valid = await scrapper.validate_hf(entities=s)
        bt.logging.trace(f"Number of rows: {len(selected_rows)}")
        bt.logging.trace(valid)

    except (requests.RequestException, ValueError) as e:
        bt.logging.trace(f"An error occurred: {e}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())