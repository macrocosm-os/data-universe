"""
Utility functions for on-demand processing.

Contains helper functions moved from scraping/x/model.py
to facilitate the EnhancedXContent to XContent transition for OnDemand.
"""

import datetime as dt
import json
import re
from typing import Optional, List
from common.data import DataEntity


def is_nested_format(data_entity: DataEntity) -> bool:
    """Check if a DataEntity contains legacy nested format content."""
    try:
        content_str = data_entity.content.decode("utf-8")
        content_dict = json.loads(content_str)
        return "user" in content_dict or "tweet" in content_dict
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False


def from_enhanced_nested_format(data_entity: DataEntity) -> dict:
    """Convert from legacy EnhancedXContent nested format to unified XContent format.

    Returns a dictionary of fields that can be used to create an XContent instance.
    """
    # Parse the content from the data entity
    content_str = data_entity.content.decode("utf-8")
    content_dict = json.loads(content_str)

    base_fields = {
        "username": content_dict.get("username"),
        "text": content_dict.get("text"),
        "url": content_dict.get("url") or data_entity.uri,
        "timestamp": data_entity.datetime,  # Use precise timestamp from DataEntity (to_data_entity will obfuscate for content)
        "tweet_hashtags": content_dict.get("tweet_hashtags", []),
        "media": extract_media_urls(content_dict.get("media")),
    }

    # Extract user fields from nested user object
    user_dict = content_dict.get("user", {})
    if user_dict:
        base_fields.update(
            {
                "user_id": user_dict.get("id"),
                "user_display_name": user_dict.get("display_name"),
                "user_verified": user_dict.get("verified"),
                "user_followers_count": user_dict.get("followers_count"),
                "user_following_count": user_dict.get("following_count"),
                "username": user_dict.get("username", base_fields["username"]),
            }
        )

    # Extract tweet fields from nested tweet object
    tweet_dict = content_dict.get("tweet", {})
    if tweet_dict:
        base_fields.update(
            {
                "tweet_id": tweet_dict.get("id"),
                "is_reply": tweet_dict.get("is_reply"),
                "is_quote": tweet_dict.get("is_quote"),
                "conversation_id": tweet_dict.get("conversation_id"),
                "like_count": tweet_dict.get("like_count"),
                "retweet_count": tweet_dict.get("retweet_count"),
                "reply_count": tweet_dict.get("reply_count"),
                "quote_count": tweet_dict.get("quote_count"),
            }
        )

        # Handle in_reply_to nested object
        in_reply_to = tweet_dict.get("in_reply_to")
        if in_reply_to and isinstance(in_reply_to, dict):
            base_fields["in_reply_to_user_id"] = in_reply_to.get("user_id")

    # Handle direct top-level fields that might exist in legacy format
    for field in [
        "tweet_id",
        "user_id",
        "like_count",
        "retweet_count",
        "reply_count",
        "quote_count",
        "is_reply",
        "is_quote",
        "conversation_id",
        "user_display_name",
        "user_verified",
        "user_followers_count",
        "user_following_count",
    ]:
        if field in content_dict and base_fields.get(field) is None:
            base_fields[field] = content_dict[field]

    return base_fields


def extract_media_urls(media_data) -> Optional[List[str]]:
    """Extract media URLs from nested format media data."""
    if not media_data:
        return None

    media_urls = []
    if isinstance(media_data, list):
        for item in media_data:
            if isinstance(item, dict) and "url" in item:
                # Extract URL from {"url": "...", "type": "..."} format
                media_urls.append(item["url"])
            elif isinstance(item, str):
                # Already a URL string
                media_urls.append(item)

    return media_urls if media_urls else None
