import bittensor as bt
import re
import traceback
from typing import Dict, List
from urllib.parse import urlparse
from common.data import DataEntity
from scraping import utils
from scraping.scraper import ValidationResult

from scraping.x.model import XContent

from datadog import statsd


def _validate_model_config(model_config: Dict[str, str]) -> bool:
    """Validates that extra content isn't stowed away in the 'model_config'

    Args:
        model_config (Dict[str, str]): Dict to validate.

    Returns:
        bool: True if the model configuration is valid.
    """
    # The model_config must either be empty, or contain only the 'extra' key with the value 'ignore'.
    return model_config is None or (
        len(model_config) == 1 and model_config.get("extra") == "ignore"
    )


def is_valid_twitter_url(url: str) -> bool:
    """Verifies a URL is both a valid URL and is for twitter.com."""
    if not url:
        return False

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and "twitter.com" in result.netloc
    except ValueError:
        return False


def extract_user(url: str) -> str:
    """Extracts the twitter user from the URL and returns it in the expected format."""
    pattern = r"https://twitter.com/(\w+)/status/.*"
    if re.match(pattern, url):
        return f"@{re.match(pattern, url).group(1)}"
    raise ValueError(f"Unable to extract user from {url}")


# Note this doesn't handle cases with punctuation at the beginning or end of a tag.
# Note this over-eagerly matches things like $10 and uses $ for cashtags not #.
def extract_hashtags(text: str) -> List[str]:
    """Given a tweet, extracts the hashtags in the order they appear in the tweet."""
    hashtags = []
    for word in text.split():
        if word.startswith("#"):
            hashtags.append(word)
        if word.startswith("$"):
            # For backwards compatibility, we also extract "cashtags" as a hashtag.
            hashtags.append(f"#{word[1:]}")
    # As of python 3.7 dictionary key order is maintained, so we can use this to remove duplicates.
    return list(dict.fromkeys(hashtags))


def sanitize_scraped_tweet(text: str) -> str:
    """Removes leading @user mentions from the tweet."""
    # First, strip the @user from the beginning of the tweet, which is added by the actor when the
    # tweet is in response to another.
    pattern = r"^@\w+\s+"
    while re.match(pattern, text):
        text = re.sub(pattern, "", text, count=1)

    pattern = r"\s*https://t.co/[^\s]+\s*"  # Matches any link to a twitter image
    return re.sub(pattern, "", text)


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

    # Check Tweet username
    if tweet_to_verify.username != actual_tweet.username:
        bt.logging.info(
            f"Tweet usernames do not match: {tweet_to_verify} != {actual_tweet}."
        )
        return ValidationResult(
            is_valid=False,
            reason="Tweet usernames do not match",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Check Tweet text
    if tweet_to_verify.text != actual_tweet.text:
        bt.logging.info(
            f"Tweet texts do not match: {tweet_to_verify} != {actual_tweet}."
        )
        return ValidationResult(
            is_valid=False,
            reason="Tweet texts do not match",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Check Tweet url
    if tweet_to_verify.url != actual_tweet.url:
        bt.logging.info(
            f"Tweet urls do not match: {tweet_to_verify} != {actual_tweet}."
        )
        return ValidationResult(
            is_valid=False,
            reason="Tweet urls do not match",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Timestamps on the contents within the entities must be obfuscated to the minute.
    actual_tweet_obfuscated_timestamp = utils.obfuscate_datetime_to_minute(
        actual_tweet.timestamp
    )
    if tweet_to_verify.timestamp != actual_tweet_obfuscated_timestamp:
        # Check if this is specifically because the entity was not obfuscated.
        if tweet_to_verify.timestamp == actual_tweet.timestamp:
            bt.logging.info(
                f"Provided tweet content datetime was not obfuscated to the minute as required. {tweet_to_verify.timestamp} != {actual_tweet_obfuscated_timestamp}"
            )
            return ValidationResult(
                is_valid=False,
                reason="Provided tweet content datetime was not obfuscated to the minute as required",
                content_size_bytes_validated=entity.content_size_bytes,
            )
        else:
            bt.logging.info(
                f"Tweet timestamps do not match to the minute: {tweet_to_verify} != {actual_tweet}."
            )
            return ValidationResult(
                is_valid=False,
                reason="Tweet timestamps do not match to the minute",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    # Check Tweet hashtags.
    if tweet_to_verify.tweet_hashtags != actual_tweet.tweet_hashtags:
        bt.logging.info(
            f"Tweet hashtags do not match: {tweet_to_verify} != {actual_tweet}."
        )
        return ValidationResult(
            is_valid=False,
            reason="Tweet hashtags do not match",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Validate the model_config.
    if not _validate_model_config(tweet_to_verify.model_config):
        bt.logging.info(
            f"Tweet content contains an invalid model_config: {tweet_to_verify.model_config}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Tweet content contains an invalid model_config",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Wahey! A valid Tweet.
    # One final check. Does the tweet content match the data entity information?
    try:
        tweet_entity = XContent.to_data_entity(content=actual_tweet)

        # Extra check that the content size is reasonably close to what we expect.
        # Allow a 10 byte difference to account for timestamp serialization differences.
        byte_difference_allowed = 10
        # The entity generated here will never have a model config, so add that in as buffer if included.
        if tweet_to_verify.model_config:
            byte_difference_allowed += len('"model_config":{"extra": "ignore"}"')

        if (
            entity.content_size_bytes - tweet_entity.content_size_bytes
        ) > byte_difference_allowed:
            return ValidationResult(
                is_valid=False,
                reason="The claimed bytes are too big compared to the actual tweet.",
                content_size_bytes_validated=entity.content_size_bytes,
            )

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
