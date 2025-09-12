import re
import math
import traceback
import datetime as dt
import bittensor as bt
from typing import Dict, List, Optional
from urllib.parse import urlparse
from common.data import DataEntity
from common.constants import NO_TWITTER_URLS_DATE
from scraping import utils
from scraping.scraper import ValidationResult
from scraping.x.model import XContent

# Validation fields
REQUIRED_FIELDS = [
    "username",
    "text",
    "url",
    "tweet_hashtags",
]

OPTIONAL_FIELDS = [
    "user_id",
    "user_display_name",
    "user_verified",
    "tweet_id",
    "is_reply",
    "is_quote",
    "conversation_id",
    "in_reply_to_user_id",
    # ===== NEW STATIC FIELDS =====
    "language",
    "in_reply_to_username",
    "quoted_tweet_id",
    # ===== NEW USER PROFILE FIELDS =====
    "user_blue_verified",
    "user_description",
    "user_location",
    "profile_image_url",
    "cover_picture_url",
]

# Dynamic fields that require special validation (like Reddit score/comment count)
DYNAMIC_FIELDS = [
    "like_count",
    "retweet_count",
    "reply_count",
    "quote_count",
    "view_count",
    "bookmark_count",
    # User profile metrics that change frequently
    "user_followers_count",
    "user_following_count",
]


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
    if username.startswith("@"):
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


def are_hashtags_valid(
    tweet_to_verify_hashtags: List, actual_tweet_hashtags: List
) -> bool:
    """
    Check if hashtags from tweet_to_verify are valid compared to actual_tweet.
    :param tweet_to_verify_hashtags: List of hashtags from the tweet submitted by the miner
    :param actual_tweet_hashtags: List of hashtags from the tweet scraped by the validator
    :return: Boolean indicating if the hashtags are valid
    """
    return set(tweet_to_verify_hashtags).issubset(set(actual_tweet_hashtags)) and len(
        tweet_to_verify_hashtags
    ) <= 2.5 * len(actual_tweet_hashtags)


def validate_tweet_fields(
    tweet_to_verify: XContent, actual_tweet: XContent, entity: DataEntity
) -> Optional[ValidationResult]:
    """Validate all tweet fields between submitted and actual tweet data.

    Returns:
        ValidationResult if validation fails, None if all validations pass
    """
    # Validate REQUIRED fields - these must never be None
    for field_name in REQUIRED_FIELDS:
        submitted_value = getattr(tweet_to_verify, field_name, None)
        actual_value = getattr(actual_tweet, field_name, None)

        # REQUIRED fields cannot be None
        if submitted_value is None:
            bt.logging.info(
                f"Required field {field_name} is missing from submitted tweet"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"Required field '{field_name}' is missing",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        if actual_value is None:
            bt.logging.info(
                f"Required field {field_name} is missing from actual tweet - this shouldn't happen"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"Required field '{field_name}' missing from actual tweet",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Apply field-specific validation logic for required fields
        if field_name == "username":
            is_valid = remove_at_sign_from_username(
                submitted_value
            ) == remove_at_sign_from_username(actual_value)
        elif field_name == "url":
            is_valid = normalize_url(submitted_value) == normalize_url(actual_value)
        elif field_name == "tweet_hashtags":
            is_valid = are_hashtags_valid(submitted_value, actual_value)
        else:
            is_valid = submitted_value == actual_value

        if not is_valid:
            bt.logging.info(
                f"Required field {field_name} do not match: {submitted_value} != {actual_value}"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"Field: {field_name} do not match",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    # Validate OPTIONAL fields - skip if either is None
    for field_name in OPTIONAL_FIELDS:
        submitted_value = getattr(tweet_to_verify, field_name, None)
        actual_value = getattr(actual_tweet, field_name, None)

        # Skip validation if either value is None (this is OK for optional fields)
        if submitted_value is None or actual_value is None:
            continue

        # Both values exist, so validate them
        is_valid = submitted_value == actual_value

        if not is_valid:
            bt.logging.info(
                f"Optional field {field_name} do not match: {submitted_value} != {actual_value}"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"Field: {field_name} do not match",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    return None


def validate_timestamp(
    tweet_to_verify: XContent, actual_tweet: XContent, entity: DataEntity
) -> Optional[ValidationResult]:
    """Validate tweet timestamp with obfuscation logic."""
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
    return None


def validate_twitter_url_deadline(
    tweet_to_verify: XContent, actual_tweet: XContent, entity: DataEntity
) -> Optional[ValidationResult]:
    """Validate twitter.com URL deadline restriction."""
    if dt.datetime.now(dt.timezone.utc) >= NO_TWITTER_URLS_DATE:
        if "twitter.com" in tweet_to_verify.url or "twitter.com" in actual_tweet.url:
            bt.logging.info("Twitter.com URLs are not accepted after December 27, 2024")
            return ValidationResult(
                is_valid=False,
                reason="Only x.com URLs are accepted after December 27, 2024",
                content_size_bytes_validated=entity.content_size_bytes,
            )
    return None


def validate_media_content(
    tweet_to_verify: XContent, actual_tweet: XContent, entity: DataEntity
) -> Optional[ValidationResult]:
    """Validate media content requirements and matching."""

    # After deadline: Check if media is required but missing
    if actual_tweet.media and not tweet_to_verify.media:
        bt.logging.info("Tweet is missing required media content.")
        return ValidationResult(
            is_valid=False,
            reason="Tweet is missing required media content",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # ALWAYS validate: If miner claims to have media, validate it's legitimate
    if tweet_to_verify.media:
        # If miner claims media but actual tweet has none, reject it
        if not actual_tweet.media:
            bt.logging.info("Miner included media but the tweet has none")
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
            bt.logging.info("Tweet media URLs don't match")
            return ValidationResult(
                is_valid=False,
                reason="Tweet media URLs don't match actual content",
                content_size_bytes_validated=entity.content_size_bytes,
            )
    return None


def validate_data_entity_fields(
    actual_tweet: XContent, entity: DataEntity
) -> ValidationResult:
    """Validate DataEntity fields against the actual tweet."""
    # Create DataEntity instances with normalization for comparison
    tweet_entity = XContent.to_data_entity(content=actual_tweet)

    # Create normalized copies for comparison
    normalized_tweet_entity = DataEntity(
        uri=normalize_url(tweet_entity.uri),
        datetime=tweet_entity.datetime,
        source=tweet_entity.source,
        label=tweet_entity.label,
        content=tweet_entity.content,
        content_size_bytes=tweet_entity.content_size_bytes,
    )

    normalized_entity = DataEntity(
        uri=normalize_url(entity.uri),
        datetime=entity.datetime,
        source=entity.source,
        label=entity.label,
        content=entity.content,
        content_size_bytes=entity.content_size_bytes,
    )

    byte_difference_allowed = 0

    if (
        entity.content_size_bytes - tweet_entity.content_size_bytes
    ) > byte_difference_allowed:
        return ValidationResult(
            is_valid=False,
            reason="The claimed bytes must not exceed the actual tweet size.",
            content_size_bytes_validated=entity.content_size_bytes,  # Only validate actual bytes
        )

    if not DataEntity.are_non_content_fields_equal(
        normalized_tweet_entity, normalized_entity
    ):
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


def is_spam_account(author_data: dict) -> bool:
    """Check if an account exhibits spam characteristics.

    Args:
        author_data: Author data from actor response

    Returns:
        True if account should be filtered as spam
    """

    if not isinstance(author_data, dict):
        return True

    # Check minimum followers (50+)
    followers = author_data.get("followers", 0)
    if followers < 50:
        return True

    # Check account age (30+ days)
    created_at_str = author_data.get("createdAt")
    if created_at_str:
        try:
            created_at = dt.datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
            account_age = dt.datetime.now(dt.timezone.utc) - created_at
            if account_age.days < 30:
                return True
        except (ValueError, TypeError):
            # If we can't parse the date, be conservative and reject
            return True
    else:
        # No creation date = suspicious
        return True

    return False


def is_low_engagement_tweet(tweet_data: dict) -> bool:
    """Check if a tweet has suspiciously low engagement.

    Args:
        tweet_data: Tweet data from actor response

    Returns:
        True if tweet should be filtered for low engagement
    """

    if not isinstance(tweet_data, dict):
        return True

    # Check minimum views (50+)
    view_count = tweet_data.get("viewCount", 0)
    if view_count < 50:
        return True

    return False


def validate_tweet_content(
    actual_tweet: XContent,
    entity: DataEntity,
    is_retweet: bool,
    author_data: dict = None,
    view_count: int = None,
) -> ValidationResult:
    """Validates the tweet is valid by the definition provided by entity."""
    # Decode tweet from entity
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

    # Check for retweets early
    if is_retweet:
        return ValidationResult(
            is_valid=False,
            reason="Retweets are no longer eligible after July 6, 2024.",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Validate all basic fields using the unified function
    field_validation_result = validate_tweet_fields(
        tweet_to_verify, actual_tweet, entity
    )
    if field_validation_result is not None:
        return field_validation_result

    # Validate timestamp with special obfuscation logic
    timestamp_validation_result = validate_timestamp(
        tweet_to_verify, actual_tweet, entity
    )
    if timestamp_validation_result is not None:
        return timestamp_validation_result

    # Validate twitter.com URL deadline
    url_deadline_result = validate_twitter_url_deadline(
        tweet_to_verify, actual_tweet, entity
    )
    if url_deadline_result is not None:
        return url_deadline_result

    # Validate media content
    media_validation_result = validate_media_content(
        tweet_to_verify, actual_tweet, entity
    )
    if media_validation_result is not None:
        return media_validation_result

    # Validate the model_config
    if not _validate_model_config(tweet_to_verify.model_config):
        bt.logging.info(
            f"Tweet content contains an invalid model_config: {tweet_to_verify.model_config}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Tweet content contains an invalid model_config",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Validate dynamic engagement metrics (following Reddit pattern)
    engagement_validation_result = validate_engagement_metrics(
        tweet_to_verify, actual_tweet, entity
    )
    if engagement_validation_result is not None:
        return engagement_validation_result

    # Final DataEntity validation
    return validate_data_entity_fields(actual_tweet, entity)


def validate_engagement_metrics(
    submitted_tweet: XContent, actual_tweet: XContent, entity: DataEntity
) -> Optional[ValidationResult]:
    """
    Validate dynamic engagement metrics with time-based tolerance and anti-cheating mechanisms.
    Backward compatible - only validates fields that miners provide.

    Args:
        submitted_tweet: Tweet submitted by miner
        actual_tweet: Actual tweet from API
        entity: DataEntity being validated

    Returns:
        ValidationResult if validation fails, None if all validations pass
    """
    # Calculate tweet age for validation logic
    now = dt.datetime.now(dt.timezone.utc)
    tweet_age = now - submitted_tweet.timestamp

    # Dynamic engagement fields to validate - use the DYNAMIC_FIELDS constant
    dynamic_fields = DYNAMIC_FIELDS

    for field_name in dynamic_fields:
        submitted_value = getattr(submitted_tweet, field_name, None)
        actual_value = getattr(actual_tweet, field_name, None)

        # Skip validation if miner didn't provide field (backward compatibility)
        if submitted_value is None:
            continue

        # Validate individual engagement metric
        field_validation_result = _validate_engagement_field(
            field_name, submitted_value, actual_value, tweet_age, entity
        )
        if field_validation_result is not None:
            return field_validation_result

    return None


def _validate_engagement_field(
    field_name: str,
    submitted_value: int,
    actual_value: int,
    tweet_age: dt.timedelta,
    entity: DataEntity,
) -> Optional[ValidationResult]:
    """
    Validate a single engagement field with tolerance and anti-cheating.


    Args:
        field_name: Name of the engagement field
        submitted_value: Value submitted by miner
        actual_value: Actual value from API
        tweet_age: Age of the tweet
        entity: DataEntity being validated

    Returns:
        ValidationResult if validation fails, None if validation passes
    """
    # Basic sanity checks
    if submitted_value < 0:
        bt.logging.info(f"Invalid negative {field_name}: {submitted_value}")
        return ValidationResult(
            is_valid=False,
            reason=f"Invalid negative {field_name}: {submitted_value}",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Use percentage-based validation for follower counts since we have exact current values
    if field_name in ["user_followers_count", "user_following_count"]:
        return _validate_follower_count_percentage(
            field_name, submitted_value, actual_value, tweet_age, entity
        )

    # For regular engagement metrics, use the original logic
    # Anti-cheating: Maximum reasonable values based on tweet age and research data
    max_reasonable_value = _calculate_max_reasonable_engagement(field_name, tweet_age)
    if submitted_value > max_reasonable_value:
        bt.logging.info(
            f"Submitted {field_name} {submitted_value} exceeds reasonable maximum {max_reasonable_value} for tweet age {tweet_age}"
        )
        return ValidationResult(
            is_valid=False,
            reason=f"{field_name} {submitted_value} is unreasonably high for tweet of this age",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # Calculate age-based tolerance
    tolerance = _calculate_engagement_tolerance(
        field_name, actual_value or 0, tweet_age
    )

    # Engagement metrics can only increase or stay the same (Twitter doesn't remove likes typically)
    # Allow very small decreases for edge cases (deleted retweets, etc.)
    max_allowed_value = (actual_value or 0) + tolerance
    min_allowed_value = max(
        0, (actual_value or 0) - min(tolerance // 10, 3)
    )  # Very strict on decreases

    # Validate engagement is within reasonable bounds - binary pass/fail
    if not (min_allowed_value <= submitted_value <= max_allowed_value):
        bt.logging.info(
            f"{field_name} validation failed: submitted={submitted_value}, "
            f"actual={actual_value}, allowed range=[{min_allowed_value}, {max_allowed_value}]"
        )
        return ValidationResult(
            is_valid=False,
            reason=f"{field_name} {submitted_value} is outside acceptable range [{min_allowed_value}, {max_allowed_value}]",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    return None


def _validate_follower_count_percentage(
    field_name: str,
    submitted_value: int,
    actual_value: int,
    tweet_age: dt.timedelta,
    entity: DataEntity,
) -> Optional[ValidationResult]:
    """
    Validate follower counts using smart percentage-based tolerance with age scaling.
    Uses logarithmic scaling - smaller accounts have higher percentage tolerance.
    Older scraped tweets get more tolerance to handle viral growth scenarios.

    Args:
        field_name: Name of the follower field
        submitted_value: Value submitted by miner
        actual_value: Actual current value from API
        tweet_age: Age of the scraped tweet (affects tolerance)
        entity: DataEntity being validated

    Returns:
        ValidationResult if validation fails, None if validation passes
    """
    # If we don't have an actual value, we can't validate percentage-wise
    if actual_value is None or actual_value <= 0:
        return None

    # Smart tolerance calculation using logarithmic decay
    # Formula: max_percentage = base_percentage / log10(max(actual_value, 10))
    # This gives smaller accounts higher percentage tolerance naturally

    base_percentage = 200.0  # Starting percentage for very small accounts
    log_factor = math.log10(max(actual_value, 10))  # Prevent log(0)
    max_percentage = min(base_percentage / log_factor, 50.0)  # Cap at 50%

    # Age-based multiplier to handle viral growth scenarios
    # Older scraped data gets more tolerance since accounts can grow significantly
    age_hours = max(tweet_age.total_seconds() / 3600, 0.1)  # Minimum 0.1 hours

    if age_hours < 24:
        # Fresh data (< 1 day): standard tolerance
        age_multiplier = 1.0
    elif age_hours < 168:  # < 1 week
        # Recent data: moderate increase in tolerance
        age_multiplier = 1.5
    elif age_hours < 720:  # < 1 month
        # Older data: higher tolerance for viral growth
        age_multiplier = 2.5
    else:
        # Very old data: maximum tolerance
        age_multiplier = 4.0

    # Apply age multiplier to percentage tolerance
    max_percentage = min(
        max_percentage * age_multiplier, 500.0
    )  # Cap at 500% (5x growth)

    # Minimum absolute tolerance scales with account size (square root for gentle scaling)
    min_absolute = max(int(math.sqrt(actual_value) * 10), 50)

    # Calculate tolerance
    percentage_tolerance = int(actual_value * max_percentage / 100)
    final_tolerance = max(percentage_tolerance, min_absolute)

    # Follower counts can go up or down
    max_allowed = actual_value + final_tolerance
    min_allowed = max(0, actual_value - final_tolerance)

    # Validate range
    if not (min_allowed <= submitted_value <= max_allowed):
        diff_percentage = abs(submitted_value - actual_value) / actual_value * 100
        bt.logging.info(
            f"{field_name} validation failed: submitted={submitted_value}, "
            f"actual={actual_value}, diff={diff_percentage:.1f}%, "
            f"max_allowed={max_percentage:.1f}%, tolerance=±{final_tolerance}"
        )
        return ValidationResult(
            is_valid=False,
            reason=f"{field_name} {submitted_value} differs too much from current value {actual_value} ({diff_percentage:.1f}% > {max_percentage:.1f}%)",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    return None


def _calculate_engagement_tolerance(
    field_name: str, base_value: int, tweet_age: dt.timedelta
) -> int:
    """
    Calculate sophisticated tolerance for engagement metric changes based on Twitter research data.
    Research shows: median engagement is 0, but viral tweets can get 1K-100K+ engagement.

    Args:
        field_name: Name of the engagement field
        base_value: Current value of the engagement metric
        tweet_age: Age of the tweet

    Returns:
        Engagement tolerance (absolute number)
    """
    # Age-based tolerance - newer tweets have higher engagement velocity
    if tweet_age < dt.timedelta(hours=1):
        # Very fresh: high engagement velocity (tweets can go viral quickly)
        age_tolerance_percent = 1.0  # 100% tolerance for very fresh tweets
        min_tolerance = 20
    elif tweet_age < dt.timedelta(hours=6):
        # Recent: moderate engagement velocity
        age_tolerance_percent = 0.75  # 75% tolerance
        min_tolerance = 15
    elif tweet_age < dt.timedelta(days=1):
        # Day-old: slowing down but still active
        age_tolerance_percent = 0.50  # 50% tolerance
        min_tolerance = 10
    elif tweet_age < dt.timedelta(days=7):
        # Week-old: much slower growth
        age_tolerance_percent = 0.30  # 30% tolerance
        min_tolerance = 5
    else:
        # Old: very slow growth
        age_tolerance_percent = 0.20  # 20% tolerance
        min_tolerance = 3

    # Field-specific multipliers based on research (views are most volatile)
    field_multipliers = {
        "like_count": 1.0,  # Baseline - most common engagement
        "retweet_count": 1.3,  # Higher tolerance - can spike with shareability
        "reply_count": 1.5,  # High tolerance - can spike with controversy
        "quote_count": 1.4,  # Moderate-high tolerance - quote tweets perform well
        "view_count": 3.0,  # Highest tolerance - most volatile, views-to-likes ~10-100:1
        "bookmark_count": 0.8,  # Lower tolerance - more private/stable action
        # User profile metrics - change more slowly but can fluctuate
        "user_followers_count": 2.0,  # High tolerance - can spike with viral content
        "user_following_count": 1.5,  # Moderate tolerance - users follow/unfollow
    }

    multiplier = field_multipliers.get(field_name, 1.0)

    # Calculate final tolerance
    base_tolerance = max(int(base_value * age_tolerance_percent), min_tolerance)
    final_tolerance = int(base_tolerance * multiplier)

    return final_tolerance


def _calculate_max_reasonable_engagement(
    field_name: str, tweet_age: dt.timedelta
) -> int:
    """
    Calculate maximum reasonable engagement values based on Twitter research data.
    Research shows: viral threshold ~1K-2K likes, mega-viral can reach 100K-3M+ likes.

    Args:
        field_name: Name of the engagement field
        tweet_age: Age of the tweet

    Returns:
        Maximum reasonable engagement value for this metric
    """
    # Base rates per hour for different engagement types (based on research)
    base_hourly_rates = {
        "like_count": 10000,  # Max 10K likes per hour (viral tweets can spike quickly)
        "retweet_count": 3000,  # Max 3K retweets per hour
        "reply_count": 1000,  # Max 1K replies per hour
        "quote_count": 800,  # Max 800 quotes per hour
        "view_count": 100000,  # Max 100K views per hour (research shows ~2K impressions avg)
        "bookmark_count": 2000,  # Max 2K bookmarks per hour
    }

    # Absolute maximums for mega-viral content (based on record holders from research)
    absolute_maximums = {
        "like_count": 5000000,  # 5M likes max (research shows 3.3M+ record)
        "retweet_count": 4000000,  # 4M retweets max (research shows 3.8M+ record)
        "reply_count": 500000,  # 500K replies max
        "quote_count": 200000,  # 200K quotes max
        "view_count": 500000000,  # 500M views max (research shows billions of daily views)
        "bookmark_count": 1000000,  # 1M bookmarks max
    }

    base_hourly_rate = base_hourly_rates.get(field_name, 5000)
    max_absolute = absolute_maximums.get(field_name, 1000000)

    # Calculate time-based maximum
    age_hours = max(tweet_age.total_seconds() / 3600, 0.1)  # Minimum 0.1 hours
    time_based_max = int(base_hourly_rate * age_hours)

    # Apply diminishing returns for older content (engagement peaks then stabilizes)
    if age_hours > 24:
        # After 24 hours, growth slows significantly
        time_based_max = int(time_based_max * 0.6)
    elif age_hours > 168:  # 1 week
        # After 1 week, very little growth
        time_based_max = int(time_based_max * 0.3)

    return min(time_based_max, max_absolute)
