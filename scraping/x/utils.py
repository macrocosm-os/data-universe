import bittensor as bt
import re
import traceback
from typing import Optional
from urllib.parse import urlparse
from common.data import DataEntity
from scraping.scraper import ValidationResult

from scraping.x.model import XContent


def is_valid_twitter_url(url: str) -> bool:
    """Verifies a URL is both a valid URL and is for twitter.com."""
    if not url:
        return False

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and "twitter.com" in result.netloc
    except ValueError:
        return False


def get_user_from_twitter_url(url: str) -> Optional[str]:
    match = re.search(r"https://twitter.com/([^/]+)/status/", url)

    if match:
        return "@" + match.group(1)
    else:
        return None


def validate_tweet_content(
    actual_tweet: XContent, entity: DataEntity
) -> ValidationResult:
    """Validates the tweet is valid by the definition provided by entity."""
    tweet_to_verify = None
    try:
        tweet_to_verify = XContent.from_data_entity(entity)
    except Exception:
        bt.logging.error(
            f"Failed to decode XContent from data entity bytes: {traceback.format_exc()}."
        )
        return ValidationResult(
            is_valid=False,
            reason="Failed to decode data entity",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Previous scrapers would only get to the minute granularity.
    if actual_tweet.timestamp != tweet_to_verify.timestamp:
        actual_tweet.timestamp = actual_tweet.timestamp.replace(second=0).replace(
            microsecond=0
        )
        tweet_to_verify.timestamp = tweet_to_verify.timestamp.replace(second=0).replace(
            microsecond=0
        )
        # Also reduce entity granularity for the check below.
        entity.datetime = entity.datetime.replace(second=0).replace(microsecond=0)

    if tweet_to_verify != actual_tweet:
        bt.logging.info(f"Tweets do not match: {tweet_to_verify} != {actual_tweet}.")
        return ValidationResult(
            is_valid=False,
            reason="Tweet does not match",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Wahey! A valid Tweet.
    # One final check. Does the tweet content match the data entity information?
    try:
        tweet_entity = XContent.to_data_entity(actual_tweet)
        if not DataEntity.are_non_content_fields_equal(tweet_entity, entity):
            return ValidationResult(
                is_valid=False,
                reason="The DataEntity fields are incorrect based on the tweet.",
                content_size_bytes_validated=entity.content_size_bytes,
            )
    except Exception:
        # This shouldn't really happen, but let's safeguard against it anyway to avoid us somehow accepting
        # corrupted or malformed data.
        bt.logging.error(
            f"Failed to convert XContent to DataEntity: {traceback.format_exc()}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Failed to convert XContent to DataEntity.",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # At last, all checks have passed. The DataEntity is indeed valid. Nice work!
    return ValidationResult(
        is_valid=True,
        reason="Good job, you honest miner!",
        content_size_bytes_validated=entity.content_size_bytes,
    )
