import os
import json
import tempfile
import numpy as np
import datetime as dt
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
    A class to generate, update, and save dataset cards for Bittensor Subnet 13.

    This class creates and manages dataset cards for Reddit and X (Twitter) datasets,
    allowing for easy generation and updating of statistics and update history.
    """

    def __init__(self, miner_hotkey: str, repo_id: str, token: str):
        """
        Initialize the DatasetCardGenerator.

        Args:
            miner_hotkey (str): The miner's hotkey.
            repo_id (str): The Hugging Face repository ID (e.g., 'username/dataset-name').
            token (str): Hugging Face API token for authentication.
        """
        self.miner_hotkey = miner_hotkey
        self.repo_id = repo_id
        self.hf_api = HfApi(token=token)
        self.hf_api.whoami()

    def _generate_reddit_card(self) -> str:
        """Generate the base dataset card for Reddit data."""
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

- Posts: [POST_COUNT]
- Comments: [COMMENT_COUNT]

### Top 10 Subreddits

For full statistics, please refer to the `reddit_stats.json` file in the repository.

[TOP_SUBREDDITS]


## Update History

| Date | New Instances | Total Instances |
|------|---------------|-----------------|
[UPDATE_HISTORY]
"""

    def _generate_x_card(self) -> str:
        """Generate the base dataset card for X (Twitter) data."""
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

- Tweets with hashtags: [TWEETS_WITH_HASHTAGS]%
- Tweets without hashtags: [TWEETS_WITHOUT_HASHTAGS]%

### Top 10 Hashtags

For full statistics, please refer to the `x_stats.json` file in the repository.

[TOP_HASHTAGS]

## Update History

| Date | New Instances | Total Instances |
|------|---------------|-----------------|
[UPDATE_HISTORY]
"""

    def generate_card(self, platform: str) -> str:
        """
        Generate a dataset card for the specified platform.

        Args:
            platform (Literal['reddit', 'x']): The platform for which to generate the card.

        Returns:
            str: The generated dataset card.

        Raises:
            ValueError: If an unsupported platform is specified.
        """
        yaml_metadata = self.generate_yaml_metadata()
        card_content = self._generate_reddit_card() if platform == 'reddit' else self._generate_x_card()
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
        card = card.replace("[TOTAL_INSTANCES]", str(stats.get('total_rows', 0)))
        card = card.replace("[START_DATE]", stats.get('start_date', ''))
        card = card.replace("[END_DATE]", stats.get('end_date', ''))
        card = card.replace("[LAST_UPDATE_DATE]", stats.get('last_update', ''))

        if platform == 'reddit':
            posts_percentage = stats.get('posts_percentage', 0)
            comments_percentage = stats.get('comments_percentage', 0)
            card = card.replace("[POST_COUNT]", f"{posts_percentage:.2f}%")
            card = card.replace("[COMMENT_COUNT]", f"{comments_percentage:.2f}%")
            top_subreddits = self._format_top_items(stats.get('subreddits', {}), 10)
            card = card.replace("[TOP_SUBREDDITS]", top_subreddits)
        elif platform == 'x':
            tweets_with_hashtags_percentage = stats.get('tweets_with_hashtags_percentage', 0)
            card = card.replace("[TWEETS_WITH_HASHTAGS]", f"{tweets_with_hashtags_percentage:.2f}")
            card = card.replace("[TWEETS_WITHOUT_HASHTAGS]", f"{(100 - tweets_with_hashtags_percentage):.2f}")
            top_hashtags = self._format_top_items(stats.get('hashtags', {}), 10)
            card = card.replace("[TOP_HASHTAGS]", top_hashtags)

        return card

    def _format_top_items(self, items: Dict[str, Dict[str, Union[int, float]]], limit: int = 10) -> str:
        sorted_items = sorted(items.items(), key=lambda x: x[1]['percentage'], reverse=True)[:limit]
        table = "| Rank | Item | Percentage |\n|------|------|------------|\n"
        for i, (item, data) in enumerate(sorted_items, 1):
            table += f"| {i} | {item} | {data['percentage']:.2f}% |\n"
        return table

    def update_history(self, card: str, history: List[Tuple[str, int, int]]) -> str:
        history_rows = "\n".join([f"| {date} | {new} | {total} |" for date, new, total in history])
        return card.replace("[UPDATE_HISTORY]", history_rows)

    def save_stats_json(self, stats: Dict[str, Any], platform: str):
        filename = f"{platform}_stats.json"

        print(self.hf_api.whoami())
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            json.dump(stats, temp_file, indent=2, cls=NumpyEncoder)

        self.hf_api.upload_file(
            path_or_fileobj=temp_file.name,
            path_in_repo=filename,
            repo_id=self.repo_id,
            repo_type="dataset",
            commit_message=f"Update {filename} with latest statistics",
        )

        os.unlink(temp_file.name)  # Remove the temporary file after uploading

    def update_or_create_card(self, platform: str, stats: Dict[str, Any],
                              history: List[Tuple[str, int, int]]):

        """
            Update or create a dataset card for the specified platform with the latest statistics and history.

            This method generates a new dataset card or updates an existing one with the provided statistics
            and update history. It then uploads the updated card to the Hugging Face repository and saves
            the statistics as a separate JSON file.

            Args:
                platform (str): The platform for which to update or create the card ('reddit' or 'x').
                stats (Dict[str, Any]): A dictionary containing the latest statistics for the dataset.
                    The structure of this dictionary should match the expected fields for the specified platform.
                history (List[Tuple[str, int, int]]): A list of tuples representing the update history.
                    Each tuple should contain (date, new_instances, total_instances).

            Raises:
                ValueError: If an unsupported platform is specified.

            The method performs the following steps:
            1. Generates a new dataset card for the specified platform.
            2. Updates the card with the provided statistics.
            3. Adds the update history to the card.
            4. Saves the updated card as README.md in the Hugging Face repository.
            5. Saves the statistics as a separate JSON file in the repository.

            Note:
            - This method requires proper authentication with the Hugging Face API.
            - It uses temporary files for handling the card and statistics data.
            - Existing README.md and platform-specific stats JSON files in the repository will be overwritten.
            """

        existing_card = self.generate_card(platform)

        updated_card = self.update_statistics(existing_card, stats, platform)
        updated_card = self.update_history(updated_card, history)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
            temp_file.write(updated_card)

        self.hf_api.delete_file(
            repo_type='dataset',
            repo_id=self.repo_id,
            path_in_repo="README.md"
        )
        self.hf_api.upload_file(
            path_or_fileobj=temp_file.name,
            path_in_repo="README.md",
            repo_id=self.repo_id,
            repo_type="dataset",
            commit_message="Update README.md with latest statistics",
        )

        os.unlink(temp_file.name)  # Remove the temporary file after uploading

        self.save_stats_json(stats, platform)

        print(f"Dataset card for {platform} updated successfully.")





