import pandas as pd
import json
import hashlib
from dotenv import load_dotenv
from huggingface_utils.encoding_system import EncodingKeyManager, encode_url  # Import the encryption functionality

load_dotenv()

TWEET_DATASET_COLUMNS = ['text', 'label', 'tweet_hashtags', 'datetime', 'username_encoded', 'url_encoded']
REDDIT_DATASET_COLUMNS = ['text', 'label', 'dataType', 'communityName', 'datetime', 'username_encoded', 'url_encoded']


def generate_static_integer(hotkey: str, max_value=256) -> int:
    hash_value = hashlib.md5(hotkey.encode()).digest()
    integer_value = int.from_bytes(hash_value, byteorder='big')
    return integer_value % max_value


def preprocess_twitter_df(df: pd.DataFrame, key_manager: EncodingKeyManager):
    df['content'] = df['content'].apply(lambda b: json.loads(b.decode('utf-8')))
    df['text'] = df['content'].apply(lambda x: x.get('text'))
    df['tweet_hashtags'] = df['content'].apply(lambda x: x.get('tweet_hashtags'))
    df['username_encoded'] = df['content'].apply(lambda x: x.get('username'))
    df['url'] = df['content'].apply(lambda x: x.get('url', ''))  # Add URL column
    del df['content']

    # Hide datetime
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].dt.strftime('%Y-%d-%m')

    # Encode URL
    public_key = key_manager.get_public_key()
    df['url_encoded'] = df['url'].apply(lambda url: encode_url(url, public_key))
    df['username_encoded'] = df['username_encoded'].apply(lambda username: encode_url(username, public_key))

    return df[TWEET_DATASET_COLUMNS]


def preprocess_reddit_df(df: pd.DataFrame, key_manager: EncodingKeyManager) -> pd.DataFrame:
    df['content'] = df['content'].apply(lambda b: json.loads(b.decode('utf-8')))
    df['text'] = df['content'].apply(lambda x: x.get('body'))
    df['dataType'] = df['content'].apply(lambda x: x.get('dataType'))
    df['communityName'] = df['content'].apply(lambda x: x.get('communityName'))
    df['username_encoded'] = df['content'].apply(lambda x: x.get('username'))
    df['url'] = df['content'].apply(lambda x: x.get('url', ''))  # Add URL column
    del df['content']

    # Hide the date
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].dt.strftime('%Y-%d-%m')

    # Encode URL
    public_key = key_manager.get_public_key()
    df['url_encoded'] = df['url'].apply(lambda url: encode_url(url, public_key))
    df['username_encoded'] = df['username_encoded'].apply(lambda username: encode_url(username, public_key))

    return df[REDDIT_DATASET_COLUMNS]


# Usage example
if __name__ == "__main__":
    # Initialize EncodingKeyManager
    key_manager = EncodingKeyManager()

    # Example usage with a sample DataFrame (you would load your actual data here)
    sample_twitter_df = pd.DataFrame({
        'content': [json.dumps(
            {'text': 'Sample tweet', 'tweet_hashtags': '#sample', 'url': 'https://twitter.com/sample'}).encode()],
        'datetime': ['2023-01-01']
    })

    sample_reddit_df = pd.DataFrame({
        'content': [json.dumps({'body': 'Sample post', 'dataType': 'comment', 'communityName': 'sample',
                                'url': 'https://reddit.com/r/sample'}).encode()],
        'datetime': ['2023-01-01']
    })

    processed_twitter = preprocess_twitter_df(sample_twitter_df, key_manager)
    processed_reddit = preprocess_reddit_df(sample_reddit_df, key_manager)

    print("Processed Twitter Data:")
    print(processed_twitter)
    print("\nProcessed Reddit Data:")
    print(processed_reddit)