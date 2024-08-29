"""Module for preprocessing Twitter and Reddit data with URL encoding."""

import json
import hashlib
from typing import Any, Dict

import pandas as pd
from dotenv import load_dotenv

from huggingface_utils.encoding_system import EncodingKeyManager, encode_url

load_dotenv()

TWEET_DATASET_COLUMNS = ['text', 'label', 'tweet_hashtags', 'datetime', 'username_encoded', 'url_encoded']
REDDIT_DATASET_COLUMNS = ['text', 'label', 'dataType', 'communityName', 'datetime', 'username_encoded', 'url_encoded']


def generate_static_integer(hotkey: str, max_value: int = 256) -> int:
    """Generate a static integer from a string key."""
    hash_value = hashlib.md5(hotkey.encode()).digest()
    integer_value = int.from_bytes(hash_value, byteorder='big')
    return integer_value % max_value


def preprocess_twitter_df(df: pd.DataFrame, key_manager: EncodingKeyManager) -> pd.DataFrame:
    """Preprocess Twitter DataFrame."""
    df['content'] = df['content'].apply(lambda b: json.loads(b.decode('utf-8')))
    df['text'] = df['content'].apply(lambda x: x.get('text'))
    df['tweet_hashtags'] = df['content'].apply(lambda x: x.get('tweet_hashtags'))
    df['username'] = df['content'].apply(lambda x: x.get('username'))
    df['url'] = df['content'].apply(lambda x: x.get('url', ''))
    df = df.drop(columns=['content'])
    df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d')

    fernet = key_manager.get_fernet()
    df['url_encoded'] = df['url'].apply(lambda url: encode_url(url, fernet))
    df['username_encoded'] = df['username'].apply(lambda username: encode_url(username, fernet))

    return df[TWEET_DATASET_COLUMNS]


def preprocess_reddit_df(df: pd.DataFrame, key_manager: EncodingKeyManager) -> pd.DataFrame:
    """Preprocess Reddit DataFrame."""
    df['content'] = df['content'].apply(lambda b: json.loads(b.decode('utf-8')))
    df['text'] = df['content'].apply(lambda x: x.get('body'))
    df['dataType'] = df['content'].apply(lambda x: x.get('dataType'))
    df['communityName'] = df['content'].apply(lambda x: x.get('communityName'))
    df['username'] = df['content'].apply(lambda x: x.get('username'))
    df['url'] = df['content'].apply(lambda x: x.get('url', ''))
    df = df.drop(columns=['content'])

    df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d')

    fernet = key_manager.get_fernet()
    df['url_encoded'] = df['url'].apply(lambda url: encode_url(url, fernet))
    df['username_encoded'] = df['username'].apply(lambda username: encode_url(username, fernet))

    return df[REDDIT_DATASET_COLUMNS]


def main():
    """Main function to demonstrate preprocessing."""
    key_manager = EncodingKeyManager()

    sample_twitter_data: Dict[str, Any] = {
        'text': 'Sample tweet',
        'tweet_hashtags': '#sample',
        'username': 'sampleuser',
        'url': 'https://twitter.com/sample'
    }
    sample_twitter_df = pd.DataFrame({
        'content': [json.dumps(sample_twitter_data).encode()],
        'datetime': ['2023-01-01']
    })

    sample_reddit_data: Dict[str, Any] = {
        'body': 'Sample post',
        'dataType': 'comment',
        'communityName': 'sample',
        'username': 'redditor',
        'url': 'https://reddit.com/r/sample'
    }
    sample_reddit_df = pd.DataFrame({
        'content': [json.dumps(sample_reddit_data).encode()],
        'datetime': ['2023-01-01']
    })

    processed_twitter = preprocess_twitter_df(sample_twitter_df, key_manager)
    processed_reddit = preprocess_reddit_df(sample_reddit_df, key_manager)

    print("Processed Twitter Data:")
    print(processed_twitter)
    print("\nProcessed Reddit Data:")
    print(processed_reddit)


if __name__ == "__main__":
    main()