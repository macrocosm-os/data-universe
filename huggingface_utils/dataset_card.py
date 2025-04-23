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
        return f"""# Template
"""

    def _generate_x_card(self) -> str:
        # Template with placeholders
        return f"""# Template
"""

    def _generate_citation(self):
        current_year = dt.datetime.now().year
        miner_username = self.repo_id.split('/')[0]  # Assuming repo_id is in the format "username/dataset-name"
        repo_url = f"https://huggingface.co/datasets/{self.repo_id}"

        misc = f"{miner_username}{current_year}datauniverse{self.repo_id.split('/')[1]}"

        return f"""@misc{{{misc},
        title={{The Lovely Boy Dataset}},
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

