import datetime as dt
import re
import traceback
import bittensor as bt
from typing import Dict, List
from urllib.parse import urlparse
from common.data import DataEntity
from common.constants import NO_TWITTER_URLS_DATE, MEDIA_REQUIRED_DATE
from scraping import utils
from scraping.scraper import ValidationResult
from scraping.x.model import XContent


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


def normalize_url(url: str) -> str:
    """Normalizes URLs for comparison while maintaining original domain."""
    # After deadline, no normalization needed since we only accept x.com
    if dt.datetime.now(dt.timezone.utc) >= NO_TWITTER_URLS_DATE:
        return url
    # Before deadline, normalize twitter.com to x.com for comparison
    return url.replace("twitter.com/", "x.com/")


def is_valid_twitter_url(url: str) -> bool:
    """Verifies a URL is both a valid URL and is for x.com."""
    if not url:
        return False

    try:
        result = urlparse(url)
        # After deadline, only accept x.com
        if dt.datetime.now(dt.timezone.utc) >= NO_TWITTER_URLS_DATE:
            return all([result.scheme, result.netloc]) and "x.com" in result.netloc
        # Before deadline, accept both
        return all([result.scheme, result.netloc]) and (
                "twitter.com" in result.netloc or "x.com" in result.netloc
        )
    except ValueError:
        return False


def remove_at_sign_from_username(username: str) -> str:
    if username.startswith('@'):
        return username[1:]
    return username


def extract_user(url: str) -> str:
    """Extracts the twitter user from the URL and returns it in the expected format."""
    pattern = r"https://(?:twitter|x).com/(\w+)/status/.*"
    if re.match(pattern, url):
        return f"@{re.match(pattern, url).group(1)}"
    raise ValueError(f"Unable to extract user from {url}")


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


def are_hashtags_valid(tweet_to_verify_hashtags: List, actual_tweet_hashtags: List) -> bool:
    """
    Check if hashtags from tweet_to_verify are valid compared to actual_tweet.
    :param tweet_to_verify_hashtags: List of hashtags from the tweet submitted by the miner
    :param actual_tweet_hashtags: List of hashtags from the tweet scraped by the validator
    :return: Boolean indicating if the hashtags are valid
    """
    return set(tweet_to_verify_hashtags).issubset(set(actual_tweet_hashtags)) and \
        len(tweet_to_verify_hashtags) <= 2.5 * len(actual_tweet_hashtags)


def hf_tweet_validation(validation_results: List[ValidationResult]):
    total_count = len(validation_results)
    true_count = sum(1 for item in validation_results if item.is_valid)
    true_percentage = (true_count / total_count) * 100

    return true_percentage >= 50, true_percentage


def validate_hf_retrieved_tweet(actual_tweet: Dict, tweet_to_verify: Dict) -> ValidationResult:
    """Validates the tweet based on URL, text, and date."""
    # Check URL
    if not is_valid_twitter_url(tweet_to_verify.get('url')):
        return ValidationResult(is_valid=False, reason="Invalid URL", content_size_bytes_validated=0)

    if normalize_url(tweet_to_verify.get('url')) != normalize_url(actual_tweet.get('url')):
        return ValidationResult(is_valid=False, reason="Tweet URLs do not match", content_size_bytes_validated=0)

    # Check text
    if tweet_to_verify.get('text') != actual_tweet.get('text'):
        return ValidationResult(is_valid=False, reason="Tweet texts do not match", content_size_bytes_validated=0)

    # If we're after the media required date, validate media content
    now = dt.datetime.now(dt.timezone.utc)
    if now >= MEDIA_REQUIRED_DATE:
        actual_media = actual_tweet.get('media', [])
        verify_media = tweet_to_verify.get('media', [])

        # Check if both have media or both don't have media
        if bool(actual_media) != bool(verify_media):
            return ValidationResult(
                is_valid=False,
                reason="Media presence mismatch - one has media while the other doesn't",
                content_size_bytes_validated=0
            )

        # If both have media, check that they match
        if actual_media and verify_media:
            if len(actual_media) != len(verify_media):
                return ValidationResult(
                    is_valid=False,
                    reason=f"Media count mismatch - expected {len(actual_media)}, got {len(verify_media)}",
                    content_size_bytes_validated=0
                )

    return ValidationResult(is_valid=True, reason="Tweet is valid", content_size_bytes_validated=0)


def validate_tweet_content(
        actual_tweet: XContent, entity: DataEntity, is_retweet: bool
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
    if remove_at_sign_from_username(tweet_to_verify.username) != remove_at_sign_from_username(actual_tweet.username):
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
    if normalize_url(tweet_to_verify.url) != normalize_url(actual_tweet.url):
        bt.logging.info(
            f"Tweet urls do not match: {tweet_to_verify} != {actual_tweet}."
        )
        return ValidationResult(
            is_valid=False,
            reason="Tweet urls do not match",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # After deadline, reject twitter.com URLs
    if dt.datetime.now(dt.timezone.utc) >= NO_TWITTER_URLS_DATE:
        if "twitter.com" in tweet_to_verify.url or "twitter.com" in actual_tweet.url:
            bt.logging.info(
                f"Twitter.com URLs are not accepted after December 27, 2024"
            )
            return ValidationResult(
                is_valid=False,
                reason="Only x.com URLs are accepted after December 27, 2024",
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
    if not are_hashtags_valid(tweet_to_verify.tweet_hashtags, actual_tweet.tweet_hashtags):
        bt.logging.info(
            f"Tweet hashtags do not match: {tweet_to_verify.tweet_hashtags} not subset of {actual_tweet.tweet_hashtags}."
        )
        return ValidationResult(
            is_valid=False,
            reason="Tweet hashtags do not match",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # If we're after the media requirement date, check for media
    # Get the current date/time
    now = dt.datetime.now(dt.timezone.utc)

    # After deadline: Check if media is required but missing
    if now >= MEDIA_REQUIRED_DATE:
        # Check if the actual tweet has media but the verified one doesn't
        if actual_tweet.media and not tweet_to_verify.media:
            bt.logging.info(f"Tweet is missing required media content.")
            return ValidationResult(
                is_valid=False,
                reason="Tweet is missing required media content",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    # ALWAYS validate: If miner claims to have media, validate it's legitimate
    if tweet_to_verify.media:
        # If miner claims media but actual tweet has none, reject it
        if not actual_tweet.media:
            bt.logging.info(f"Miner included media but the tweet has none")
            return ValidationResult(
                is_valid=False,
                reason="Miner included fake media for a tweet with no media",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Sort the URLs for consistent comparison
        actual_urls = sorted(actual_tweet.media)
        miner_urls = sorted(tweet_to_verify.media)

        # Simple check: URLs must match exactly
        if actual_urls != miner_urls:
            bt.logging.info(f"Tweet media URLs don't match")
            return ValidationResult(
                is_valid=False,
                reason="Tweet media URLs don't match actual content",
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

    if is_retweet:
        return ValidationResult(
            is_valid=False,
            reason="Retweets are no longer eligible after July 6, 2024.",
            content_size_bytes_validated=entity.content_size_bytes
        )

    # Create DataEntity instances with normalization for comparison
    tweet_entity = XContent.to_data_entity(content=actual_tweet)

    # Create normalized copies for comparison
    normalized_tweet_entity = DataEntity(
        uri=normalize_url(tweet_entity.uri),
        datetime=tweet_entity.datetime,
        source=tweet_entity.source,
        label=tweet_entity.label,
        content=tweet_entity.content,
        content_size_bytes=tweet_entity.content_size_bytes
    )

    normalized_entity = DataEntity(
        uri=normalize_url(entity.uri),
        datetime=entity.datetime,
        source=entity.source,
        label=entity.label,
        content=entity.content,
        content_size_bytes=entity.content_size_bytes
    )

    # Allow a 10 byte difference to account for timestamp serialization differences.
    byte_difference_allowed = 10

    if (
            entity.content_size_bytes - tweet_entity.content_size_bytes
    ) > byte_difference_allowed:
        return ValidationResult(
            is_valid=False,
            reason="The claimed bytes are too big compared to the actual tweet.",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    if not DataEntity.are_non_content_fields_equal(normalized_tweet_entity, normalized_entity):
        return ValidationResult(
            is_valid=False,
            reason="The DataEntity fields are incorrect based on the tweet.",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    return ValidationResult(
        is_valid=True,
        reason="Good job, you honest miner!",
        content_size_bytes_validated=entity.content_size_bytes,
    )