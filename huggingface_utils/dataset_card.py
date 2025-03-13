import os
import json
import tempfile
import numpy as np
import datetime as dt
import bittensor as bt
from datetime import datetime
from typing import Dict, List, Tuple, Literal, Any, Union
from huggingface_hub import HfApi, hf_hub_download


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)


class DatasetCardGenerator:
    """
    A class to generate, update, and save dataset cards.
    """

    def __init__(self, miner_hotkey: str, repo_id: str, token: str):
        """
        Initialize the DatasetCardGenerator.

        Args:
            miner_hotkey (str): The miner's hotkey.
            repo_id (str): The Hugging Face repository ID.
            token (str): Hugging Face API token.
        """
        self.miner_hotkey = miner_hotkey
        self.repo_id = repo_id
        self.hf_api = HfApi(token=token)
        self.hf_api.whoami()

    def generate_card(self, platform: str) -> str:
        """
        Generate a dataset card for the specified platform.

        Args:
            platform (str): The platform for which to generate the card ('reddit' or 'x').

        Returns:
            str: The generated dataset card.
        """
        yaml_metadata = self.generate_yaml_metadata()
        if platform == 'reddit':
            card_content = self._generate_reddit_card()
        elif platform == 'x':
            card_content = self._generate_x_card()
        else:
            raise ValueError(f"Unsupported platform: {platform}")

        return f"{yaml_metadata}\n\n{card_content}"

    def generate_yaml_metadata(self):
        return """---
license: mit
multilinguality:
  - multilingual
source_datasets:
  - original
task_categories:
  - text-classification
  - token-classification
  - question-answering
  - summarization
  - text-generation
task_ids:
  - sentiment-analysis
  - topic-classification
  - named-entity-recognition
  - language-modeling
  - text-scoring
  - multi-class-classification
  - multi-label-classification
  - extractive-qa
  - news-articles-summarization
---
"""

    def _generate_reddit_card(self) -> str:
        # Template with placeholders
        return f"""# Bittensor Subnet 13 Reddit Dataset

<center>
    <img src="https://huggingface.co/datasets/macrocosm-os/images/resolve/main/bittensor.png" alt="Data-universe: The finest collection of social media data the web has to offer">
</center>

<center>
    <img src="https://huggingface.co/datasets/macrocosm-os/images/resolve/main/macrocosmos-black.png" alt="Data-universe: The finest collection of social media data the web has to offer">
</center>


## Dataset Description

- **Repository:** {self.repo_id}
- **Subnet:** Bittensor Subnet 13
- **Miner Hotkey:** {self.miner_hotkey}

### Miner Data Compliance Agreement 

In uploading this dataset, I am agreeing to the [Macrocosmos Miner Data Compliance Policy](https://github.com/macrocosm-os/data-universe/blob/add-miner-policy/docs/miner_policy.md). 

### Dataset Summary

This dataset is part of the Bittensor Subnet 13 decentralized network, containing preprocessed Reddit data. The data is continuously updated by network miners, providing a real-time stream of Reddit content for various analytical and machine learning tasks.
For more information about the dataset, please visit the [official repository](https://github.com/macrocosm-os/data-universe).

### Supported Tasks

The versatility of this dataset allows researchers and data scientists to explore various aspects of social media dynamics and develop innovative applications. Users are encouraged to leverage this data creatively for their specific research or business needs.
For example:

- Sentiment Analysis
- Topic Modeling
- Community Analysis
- Content Categorization

### Languages

Primary language: Datasets are mostly English, but can be multilingual due to decentralized ways of creation.

## Dataset Structure

### Data Instances

Each instance represents a single Reddit post or comment with the following fields:


### Data Fields

- `text` (string): The main content of the Reddit post or comment.
- `label` (string): Sentiment or topic category of the content.
- `dataType` (string): Indicates whether the entry is a post or a comment.
- `communityName` (string): The name of the subreddit where the content was posted.
- `datetime` (string): The date when the content was posted or commented.
- `username_encoded` (string): An encoded version of the username to maintain user privacy.
- `url_encoded` (string): An encoded version of any URLs included in the content.

### Data Splits

This dataset is continuously updated and does not have fixed splits. Users should create their own splits based on their requirements and the data's timestamp.

## Dataset Creation

### Source Data

Data is collected from public posts and comments on Reddit, adhering to the platform's terms of service and API usage guidelines.

### Personal and Sensitive Information

All usernames and URLs are encoded to protect user privacy. The dataset does not intentionally include personal or sensitive information.

## Considerations for Using the Data

### Social Impact and Biases

Users should be aware of potential biases inherent in Reddit data, including demographic and content biases. This dataset reflects the content and opinions expressed on Reddit and should not be considered a representative sample of the general population.

### Limitations

- Data quality may vary due to the nature of media sources.
- The dataset may contain noise, spam, or irrelevant content typical of social media platforms.
- Temporal biases may exist due to real-time collection methods.
- The dataset is limited to public subreddits and does not include private or restricted communities.

## Additional Information

### Licensing Information

The dataset is released under the MIT license. The use of this dataset is also subject to Reddit Terms of Use.

### Citation Information

If you use this dataset in your research, please cite it as follows:

```
{self._generate_citation()}
```

### Contributions

To report issues or contribute to the dataset, please contact the miner or use the Bittensor Subnet 13 governance mechanisms.

## Dataset Statistics

[This section is automatically updated]

- **Total Instances:** [TOTAL_INSTANCES]
- **Date Range:** [START_DATE] to [END_DATE]
- **Last Updated:** [LAST_UPDATE_DATE]

### Data Distribution

- Posts: [POSTS_PERCENTAGE]%
- Comments: [COMMENTS_PERCENTAGE]%

### Top 10 Subreddits

For full statistics, please refer to the `stats.json` file in the repository.

[TOP_SUBREDDITS]

## Update History

| Date | New Instances | Total Instances |
|------|---------------|-----------------|
[UPDATE_HISTORY]
"""

    def _generate_x_card(self) -> str:
        # Template with placeholders
        return f"""# Bittensor Subnet 13 X (Twitter) Dataset

<center>
    <img src="https://huggingface.co/datasets/macrocosm-os/images/resolve/main/bittensor.png" alt="Data-universe: The finest collection of social media data the web has to offer">
</center>

<center>
    <img src="https://huggingface.co/datasets/macrocosm-os/images/resolve/main/macrocosmos-black.png" alt="Data-universe: The finest collection of social media data the web has to offer">
</center>


## Dataset Description

- **Repository:** {self.repo_id}
- **Subnet:** Bittensor Subnet 13
- **Miner Hotkey:** {self.miner_hotkey}

### Miner Data Compliance Agreement 

In uploading this dataset, I am agreeing to the [Macrocosmos Miner Data Compliance Policy](https://github.com/macrocosm-os/data-universe/blob/add-miner-policy/docs/miner_policy.md). 


### Dataset Summary

This dataset is part of the Bittensor Subnet 13 decentralized network, containing preprocessed data from X (formerly Twitter). The data is continuously updated by network miners, providing a real-time stream of tweets for various analytical and machine learning tasks.
For more information about the dataset, please visit the [official repository](https://github.com/macrocosm-os/data-universe).

### Supported Tasks

The versatility of this dataset allows researchers and data scientists to explore various aspects of social media dynamics and develop innovative applications. Users are encouraged to leverage this data creatively for their specific research or business needs.
For example: 
- Sentiment Analysis
- Trend Detection
- Content Analysis
- User Behavior Modeling

### Languages

Primary language: Datasets are mostly English, but can be multilingual due to decentralized ways of creation.

## Dataset Structure

### Data Instances

Each instance represents a single tweet with the following fields:


### Data Fields

- `text` (string): The main content of the tweet.
- `label` (string): Sentiment or topic category of the tweet.
- `tweet_hashtags` (list): A list of hashtags used in the tweet. May be empty if no hashtags are present.
- `datetime` (string): The date when the tweet was posted.
- `username_encoded` (string): An encoded version of the username to maintain user privacy.
- `url_encoded` (string): An encoded version of any URLs included in the tweet. May be empty if no URLs are present.

### Data Splits

This dataset is continuously updated and does not have fixed splits. Users should create their own splits based on their requirements and the data's timestamp.

## Dataset Creation

### Source Data

Data is collected from public tweets on X (Twitter), adhering to the platform's terms of service and API usage guidelines.

### Personal and Sensitive Information

All usernames and URLs are encoded to protect user privacy. The dataset does not intentionally include personal or sensitive information.

## Considerations for Using the Data

### Social Impact and Biases

Users should be aware of potential biases inherent in X (Twitter) data, including demographic and content biases. This dataset reflects the content and opinions expressed on X and should not be considered a representative sample of the general population.

### Limitations

- Data quality may vary due to the decentralized nature of collection and preprocessing.
- The dataset may contain noise, spam, or irrelevant content typical of social media platforms.
- Temporal biases may exist due to real-time collection methods.
- The dataset is limited to public tweets and does not include private accounts or direct messages.
- Not all tweets contain hashtags or URLs.

## Additional Information

### Licensing Information

The dataset is released under the MIT license. The use of this dataset is also subject to X Terms of Use.

### Citation Information

If you use this dataset in your research, please cite it as follows:

```
{self._generate_citation()}
```

### Contributions

To report issues or contribute to the dataset, please contact the miner or use the Bittensor Subnet 13 governance mechanisms.

## Dataset Statistics

[This section is automatically updated]

- **Total Instances:** [TOTAL_INSTANCES]
- **Date Range:** [START_DATE] to [END_DATE]
- **Last Updated:** [LAST_UPDATE_DATE]

### Data Distribution

- Tweets with hashtags: [TWEETS_WITH_HASHTAGS_PERCENTAGE]%
- Tweets without hashtags: [TWEETS_WITHOUT_HASHTAGS_PERCENTAGE]%

### Top 10 Hashtags

For full statistics, please refer to the `stats.json` file in the repository.

[TOP_HASHTAGS]

## Update History

| Date | New Instances | Total Instances |
|------|---------------|-----------------|
[UPDATE_HISTORY]
"""

    def _generate_citation(self):
        current_year = dt.datetime.now().year
        miner_username = self.repo_id.split('/')[0]  # Assuming repo_id is in the format "username/dataset-name"
        repo_url = f"https://huggingface.co/datasets/{self.repo_id}"

        misc = f"{miner_username}{current_year}datauniverse{self.repo_id.split('/')[1]}"

        return f"""@misc{{{misc},
        title={{The Data Universe Datasets: The finest collection of social media data the web has to offer}},
        author={{{miner_username}}},
        year={{{current_year}}},
        url={{{repo_url}}},
        }}"""

    def update_statistics(self, card: str, stats: Dict[str, Any], platform: str) -> str:
        """
        Replace placeholders in the card with actual statistics.

        Args:
            card (str): The dataset card template with placeholders.
            stats (Dict[str, Any]): The statistics from stats.json.
            platform (str): The platform ('reddit' or 'x').

        Returns:
            str: The updated card with placeholders replaced.
        """
        summary = stats.get('summary', {})
        metadata = summary.get('metadata', {})
        topics = stats.get('topics', [])

        card = card.replace("[TOTAL_INSTANCES]", str(summary.get('total_rows', 0)))
        card = card.replace("[START_DATE]", summary.get('start_dt', ''))
        card = card.replace("[END_DATE]", summary.get('end_dt', ''))
        card = card.replace("[LAST_UPDATE_DATE]", summary.get('last_update_dt', ''))

        if platform == 'reddit':
            posts_percentage = metadata.get('posts_percentage', 0)
            comments_percentage = metadata.get('comments_percentage', 0)
            card = card.replace("[POSTS_PERCENTAGE]", f"{posts_percentage:.2f}")
            card = card.replace("[COMMENTS_PERCENTAGE]", f"{comments_percentage:.2f}")
            top_subreddits = self._format_top_items(topics, 'subreddit')
            card = card.replace("[TOP_SUBREDDITS]", top_subreddits)
        elif platform == 'x':
            tweets_with_hashtags_percentage = metadata.get('tweets_with_hashtags_percentage', 0)
            tweets_without_hashtags_percentage = 100 - tweets_with_hashtags_percentage
            card = card.replace("[TWEETS_WITH_HASHTAGS_PERCENTAGE]", f"{tweets_with_hashtags_percentage:.2f}")
            card = card.replace("[TWEETS_WITHOUT_HASHTAGS_PERCENTAGE]", f"{tweets_without_hashtags_percentage:.2f}")
            top_hashtags = self._format_top_items(topics, 'hashtag')
            card = card.replace("[TOP_HASHTAGS]", top_hashtags)

        return card

    def _format_top_items(self, topics: List[Dict[str, Any]], topic_type: str, limit: int = 10) -> str:
        """
        Format the top topics into a markdown table.

        Args:
            topics (List[Dict[str, Any]]): List of topics with total counts.
            topic_type (str): Type of topics ('subreddit' or 'hashtag').
            limit (int): Number of top items to include.

        Returns:
            str: Markdown table of top topics.
        """
        # Filter topics of the specified type
        filtered_topics = [topic for topic in topics if topic['topic_type'] == topic_type]

        # Sort topics by total count
        sorted_topics = sorted(filtered_topics, key=lambda x: x['total_count'], reverse=True)[:limit]

        # Create markdown table
        table = "| Rank | Topic | Total Count | Percentage |\n|------|-------|-------------|-------------|\n"
        for i, topic in enumerate(sorted_topics, 1):
            table += f"| {i} | {topic['topic']} | {topic['total_count']} | {topic['total_percentage']:.2f}% |\n"

        return table
    def update_history(self, card: str, history: List[Tuple[str, int, int]]) -> str:
        """
        Replace the [UPDATE_HISTORY] placeholder with the update history table.

        Args:
            card (str): The dataset card content.
            history (List[Tuple[str, int, int]]): List of tuples (date, new_instances, total_instances).

        Returns:
            str: The updated card with the update history included.
        """
        history_rows = "\n".join([f"| {date} | {new_instances} | {total_instances} |" for date, new_instances, total_instances in history])
        return card.replace("[UPDATE_HISTORY]", history_rows)

    def update_or_create_card(self, stats: Dict[str, Any], history: List[Tuple[str, int, int]]):
        """
        Update or create a dataset card with the latest statistics and history.

        Args:
            stats (Dict[str, Any]): The updated statistics.
            history (List[Tuple[str, int, int]]): Update history.
        """
        platform = stats["data_source"]

        existing_card = self.generate_card(platform)
        updated_card = self.update_statistics(existing_card, stats, platform)
        updated_card = self.update_history(updated_card, history)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
            temp_file.write(updated_card)

        self.hf_api.upload_file(
            path_or_fileobj=temp_file.name,
            path_in_repo="README.md",
            repo_id=self.repo_id,
            repo_type="dataset",
            commit_message="Update README.md with latest statistics",
        )

        os.unlink(temp_file.name)  # Remove the temporary file after uploading

        bt.logging.info(f"Dataset card for {platform} updated successfully.")

