import bittensor as bt
import re
import traceback
from typing import Dict, List
from urllib.parse import urlparse
from common.data import DataEntity
from common.constants import NO_IS_RETWEET_AND_MODEL_BYTES_DATE
import datetime as dt
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


def is_valid_twitter_url(url: str) -> bool:
    """Verifies a URL is both a valid URL and is for twitter.com."""
    if not url:
        return False

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and (
                "twitter.com" in result.netloc or "x.com" in result.netloc
        )
    except ValueError:
        return False


def remove_at_sign_from_username(username: str) -> str:
    if username.startswith('@'):
        return username[1:]
    return username


def normalize_url(url: str) -> str:
    """Normalizes a twitter URL to the twitter.com domain."""
    # We normalize to the twitter.com domain because that is what was historically used.
    # This will ensure the miner's DB deduplicates correctly.
    return url.replace("x.com/", "twitter.com/")


def extract_user(url: str) -> str:
    """Extracts the twitter user from the URL and returns it in the expected format."""
    pattern = r"https://(?:twitter|x).com/(\w+)/status/.*"
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


def are_hashtags_valid(tweet_to_verify_hashtags: List, actual_tweet_hashtags: List) -> bool:
    """
    Check if hashtags from tweet_to_verify are valid compared to actual_tweet.
    :param tweet_to_verify_hashtags: List of hashtags from the tweet submitted by the miner
    :param actual_tweet_hashtags: List of hashtags from the tweet scraped by the validator
    :return: Boolean indicating if the hashtags are valid
    """
    return set(tweet_to_verify_hashtags).issubset(set(actual_tweet_hashtags)) and \
        len(tweet_to_verify_hashtags) <= 2.5 * len(actual_tweet_hashtags)


def hf_tweet_validation(validation_results: List[ValidationResult]) -> bool:
    total_count = len(validation_results)
    true_count = sum(1 for item in validation_results if item.is_valid)

    true_percentage = (true_count / total_count) * 100

    return true_percentage >= 50


def validate_hf_retrieved_tweet(actual_tweet: Dict, tweet_to_verify: Dict) -> ValidationResult:
    """Validates the tweet based on URL, text, and date."""
    # Check URL
    if not is_valid_twitter_url(tweet_to_verify.get('url')):
        return ValidationResult(is_valid=False, reason="Invalid Twitter URL", content_size_bytes_validated=0)

    if normalize_url(tweet_to_verify.get('url')) != normalize_url(actual_tweet.get('url')):
        return ValidationResult(is_valid=False, reason="Tweet URLs do not match", content_size_bytes_validated=0)


    # Check text
    if tweet_to_verify.get('text') != actual_tweet.get('text'):
        return ValidationResult(is_valid=False, reason="Tweet texts do not match", content_size_bytes_validated=0)

    # Check date (without time) TODO obfuscate and validate.
    # try:
    #     actual_date = dt.datetime.strptime(actual_tweet.get('datetime'), "%Y-%m-%d %H:%M:%S").date()
    #     verify_date = dt.datetime.strptime(tweet_to_verify.get('datetime'), "%Y-%m-%d").date() # TODO
    #
    #     if actual_date != verify_date:
    #         return {"is_valid": False, "reason": "Tweet dates do not match"}
    # except ValueError:
    #     return {"is_valid": False, "reason": "Invalid date format"}

    # All checks passed
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

    # Wahey! A valid Tweet.
    # One final check. Does the tweet content match the data entity information?
    try:
        tweet_entity = XContent.to_data_entity(content=actual_tweet)

        # Allow a 10 byte difference to account for timestamp serialization differences.
        byte_difference_allowed = 10
        # The entity generated here will never have a model config, so add that in as buffer if included.
        if dt.datetime.now(dt.timezone.utc) < NO_IS_RETWEET_AND_MODEL_BYTES_DATE:
            if tweet_to_verify.model_config:
                byte_difference_allowed += len('"model_config":{"extra": "ignore"}"')

            byte_difference_allowed += len("is_retweet=False")

        if (
                entity.content_size_bytes - tweet_entity.content_size_bytes
        ) > byte_difference_allowed:
            return ValidationResult(
                is_valid=False,
                reason="The claimed bytes are too big compared to the actual tweet.",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Create new instances with normalized URIs instead of modifying frozen ones
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

        if not DataEntity.are_non_content_fields_equal(normalized_tweet_entity, normalized_entity):
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
