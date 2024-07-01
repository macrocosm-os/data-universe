import pandas as pd
import json
from dotenv import load_dotenv

load_dotenv()

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
    df['dataType'] = df['content'].apply(lambda x: x.get('dataType'))
    df['communityName'] = df['content'].apply(lambda x: x.get('communityName'))
    del df['content']

    # Hide the date
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].dt.strftime('%Y-%d-%m')

    return df[REDDIT_DATASET_COLUMNS]


