import bittensor as bt
import traceback
import datetime as dt
import random

from typing import List
from urllib.parse import urlparse
from scraping import utils
from scraping.scraper import ValidationResult
from scraping.reddit.model import RedditContent, RedditDataType
from common.data import DataEntity, DataLabel
from common.constants import REDDIT_MEDIA_REQUIRED_DATE


def is_valid_reddit_url(url: str) -> bool:
    """Verifies a URL is both a valid URL and is for reddit.com."""
    if not url:
        return False

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and "reddit.com" in result.netloc
    except ValueError:
        return False


def validate_reddit_content(
        actual_content: RedditContent,
        entity_to_validate: DataEntity,
) -> ValidationResult:
    """Verifies the RedditContent is valid by the definition provided by entity."""
    content_to_validate = None
    try:
        content_to_validate = RedditContent.from_data_entity(entity_to_validate)
    except Exception:
        bt.logging.error(
            f"Failed to decode RedditContent from data entity bytes: {traceback.format_exc()}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Failed to decode data entity",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Check Reddit id
    if content_to_validate.id != actual_content.id:
        bt.logging.info(
            f"Reddit ids do not match: {actual_content} != {content_to_validate}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Reddit ids do not match",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Check Reddit url
    if content_to_validate.url != actual_content.url:
        bt.logging.info(
            f"Reddit urls do not match: {actual_content} != {content_to_validate}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Reddit urls do not match",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Check Reddit username
    if content_to_validate.username != actual_content.username:
        bt.logging.info(
            f"Reddit usernames do not match: {actual_content} != {content_to_validate}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Reddit usernames do not match",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Check Reddit community
    if content_to_validate.community != actual_content.community:
        bt.logging.info(
            f"Reddit communities do not match: {actual_content} != {content_to_validate}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Reddit communities do not match",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Check Reddit body
    if content_to_validate.body != actual_content.body:
        bt.logging.info(
            f"Reddit bodies do not match: {actual_content} != {content_to_validate}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Reddit bodies do not match",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Timestamps on the contents within the entities must be obfuscated to the minute.
    # If checking an data entity with obfuscated content we compare to the entity directly instead.
    actual_content_obfuscated = utils.obfuscate_datetime_to_minute(
        actual_content.created_at
    )
    if content_to_validate.created_at != actual_content_obfuscated:
        if content_to_validate.created_at == actual_content.created_at:
            bt.logging.info(
                f"Provided Reddit content datetime was not obfuscated to the minute as required: {actual_content} != {content_to_validate}"
            )
            return ValidationResult(
                is_valid=False,
                reason="Provided Reddit content datetime was not obfuscated to the minute as required",
                content_size_bytes_validated=entity_to_validate.content_size_bytes,
            )
        else:
            bt.logging.info(
                f"Reddit timestamps do not match: {actual_content} != {content_to_validate}"
            )
            return ValidationResult(
                is_valid=False,
                reason="Reddit timestamps do not match",
                content_size_bytes_validated=entity_to_validate.content_size_bytes,
            )

    # Check Reddit data_type
    if content_to_validate.data_type != actual_content.data_type:
        bt.logging.info(
            f"Reddit data types do not match: {actual_content} != {content_to_validate}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Reddit data types do not match",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Post Only Fields
    # Check Reddit Title
    if content_to_validate.title != actual_content.title:
        bt.logging.info(
            f"Reddit titles do not match: {actual_content} != {content_to_validate}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Reddit titles do not match",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Comment Only Fields
    # Check Reddit Parent Id
    # Ignore exact parent id here until all scraped data has been scraped with correct parent id (~30 days):
    # Since the mistake was to assign the submission id which is always earlier and therefore smaller we can check that
    # length of the claimed is always less than or equal to that of the real entity.
    if (
            actual_content.parent_id is not None
            and content_to_validate.parent_id is not None
    ):
        if len(content_to_validate.parent_id) > len(actual_content.parent_id):
            bt.logging.info(
                f"RedditContent parent id size too large: claimed {content_to_validate.parent_id} vs actual {actual_content.parent_id}."
            )
            return ValidationResult(
                is_valid=False,
                reason="Parent id size too large",
                content_size_bytes_validated=entity_to_validate.content_size_bytes,
            )
        elif content_to_validate.parent_id != actual_content.parent_id:
            # Only None out for posts that had non-matching but otherwise valid parent ids.
            bt.logging.trace(
                f"RedditContent had non-matching but otherwise valid parent id: claimed {content_to_validate.parent_id} vs actual {actual_content.parent_id}."
            )
            actual_content.parent_id = None
            content_to_validate.parent_id = None

    if content_to_validate.parent_id != actual_content.parent_id:
        bt.logging.info(
            f"Reddit parent ids do not match: {actual_content} != {content_to_validate}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Reddit parent ids do not match",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Wahey! The content is valid.
    # One final check. Does the Reddit content match the data entity information?
    try:
        actual_entity = RedditContent.to_data_entity(content=actual_content)

        # Extra check that the content size is reasonably close to what we expect.
        # Allow a 10 byte difference to account for timestamp serialization differences.
        byte_difference_allowed = 0

        if (
                entity_to_validate.content_size_bytes - actual_entity.content_size_bytes
        ) > byte_difference_allowed:
            return ValidationResult(
                is_valid=False,
                reason="The claimed bytes are too big compared to the actual Reddit content",
                content_size_bytes_validated=entity_to_validate.content_size_bytes,
            )

        if not DataEntity.are_non_content_fields_equal(
                actual_entity, entity_to_validate
        ):
            return ValidationResult(
                is_valid=False,
                reason="The DataEntity fields are incorrect based on the Reddit content",
                content_size_bytes_validated=entity_to_validate.content_size_bytes,
            )
    except Exception:
        # This shouldn't really happen, but let's safeguard against it anyway to avoid us somehow accepting
        # corrupted or malformed data.
        bt.logging.error(
            f"Failed to convert RedditContent to DataEntity: {traceback.format_exc()}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Failed to convert RedditContent to DataEntity",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Score validation (with smart time-based tolerance)
    score_validation_result = validate_score_content(content_to_validate, actual_content, entity_to_validate)
    if not score_validation_result.is_valid:
        return score_validation_result

    # Comment count validation (with sophisticated growth modeling)
    comment_validation_result = validate_comment_count(content_to_validate, actual_content, entity_to_validate)
    if not comment_validation_result.is_valid:
        return comment_validation_result

    # At last, all checks have passed. The DataEntity is indeed valid. Nice work!
    return ValidationResult(
        is_valid=True,
        reason="Good job, you honest miner!",
        content_size_bytes_validated=entity_to_validate.content_size_bytes,
    )


def get_time_input(datetime: dt.datetime) -> str:
    """Returns the value of the 'time' key for a run input based on the targetted scrape time"""
    now = dt.datetime.now(tz=dt.timezone.utc)
    # For scraping requests that are almost in the past hour, look in the past 1 hour.
    if now - datetime < dt.timedelta(minutes=90):
        return "hour"
    if now - datetime < dt.timedelta(days=1):
        return "day"
    if now - datetime < dt.timedelta(days=7):
        return "week"
    if now - datetime < dt.timedelta(days=30):
        return "month"
    return "year"


def get_sort_input(datetime: dt.datetime) -> str:
    """Returns the sort to use for a scrape query based on the targeted timestamp."""
    # We are unable to scrape reddit with any date filters.
    # So instead, we'll use the "sort" field to help increase the chances that we get some data
    # from our targetted time window.
    now = dt.datetime.now(tz=dt.timezone.utc)
    if now - datetime < dt.timedelta(minutes=90):
        return "new"

    # For all other time-windows, we randomly pick one of the sort options. This in combination
    # with the chosen "time" input, should help get us data spread over time.
    return random.choice(["top", "hot", "relevance", "comments", "new"])


def get_custom_sort_input(datetime: dt.datetime) -> str:
    """Returns the sort to use for a scrape query based on the targeted timestamp."""
    # We are unable to scrape reddit with any date filters.
    # So instead, we'll use the "sort" field to help increase the chances that we get some data
    # from our targetted time window.
    now = dt.datetime.now(tz=dt.timezone.utc)
    if now - datetime < dt.timedelta(minutes=90):
        return "new"

    # For all other time-windows, we randomly pick one of the sort options. This in combination
    # with the chosen "time" input, should help get us data spread over time.
    return random.choice(["top", "hot", "new"])


def normalize_label(label: DataLabel) -> str:
    """Returns the datalabel value without the 'r/' prefix."""
    return label.value.removeprefix("r/")


def normalize_permalink(permalink: str) -> str:
    "Ensures that the reddit permalink always starts with '/r/' prefix (including a leading /)"
    if permalink.startswith("/"):
        return permalink
    else:
        return "/" + permalink


def validate_media_content(submitted_content: RedditContent, actual_content: RedditContent,
                           entity: DataEntity) -> ValidationResult:
    """
    Validate media content to prevent exploitation - follows X/Twitter validation pattern.
    Backward compatible: only validates if miner provided media field.

    Args:
        submitted_content: Content submitted by miner
        actual_content: Actual content from Reddit API
        entity: DataEntity being validated

    Returns:
        ValidationResult indicating if media is valid
    """
    # Skip validation if miner didn't provide media field (backward compatibility)
    if submitted_content.media is None:
        return ValidationResult(
            is_valid=True,
            reason="Media validation skipped - field not provided (backward compatibility)",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    now = dt.datetime.now(dt.timezone.utc)

    # After REDDIT_MEDIA_REQUIRED_DATE: Check if media is required but missing
    if now >= REDDIT_MEDIA_REQUIRED_DATE:
        if actual_content.media and not submitted_content.media:
            bt.logging.info("Reddit post is missing required media content.")
            return ValidationResult(
                is_valid=False,
                reason="Reddit post is missing required media content",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    # If miner provided media field, validate it strictly
    if submitted_content.media:
        # If miner claims media but actual post has none, reject it
        if not actual_content.media:
            bt.logging.info("Miner included media but the Reddit post has none")
            return ValidationResult(
                is_valid=False,
                reason="Miner included fake media for a Reddit post with no media",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Sort the URLs for consistent comparison (same as X validation)
        actual_urls = sorted(actual_content.media)
        miner_urls = sorted(submitted_content.media)

        # Strict check: URLs must match exactly (prevent any fake media URLs)
        if actual_urls != miner_urls:
            bt.logging.info("Reddit post media URLs don't match")
            return ValidationResult(
                is_valid=False,
                reason="Reddit post media URLs don't match actual content",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    return ValidationResult(
        is_valid=True,
        reason="Media content is valid",
        content_size_bytes_validated=entity.content_size_bytes,
    )


def validate_nsfw_content(submitted_content: RedditContent, actual_content: RedditContent,
                          entity: DataEntity) -> ValidationResult:
    """
    Validate NSFW content rules.
    Backward compatible: only validates if miner provided is_nsfw field.
    NO DATE RESTRICTIONS - applies universally when field is present.

    Args:
        submitted_content: Content submitted by miner
        actual_content: Actual content from Reddit API
        entity: DataEntity being validated

    Returns:
        ValidationResult indicating if NSFW content is valid
    """
    # Skip validation if miner didn't provide is_nsfw field (backward compatibility)
    if submitted_content.is_nsfw is None:
        return ValidationResult(
            is_valid=True,
            reason="NSFW validation skipped - field not provided (backward compatibility)",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # If miner provided is_nsfw field, validate it strictly

    # Validate NSFW flag accuracy
    if actual_content.is_nsfw and not submitted_content.is_nsfw:
        bt.logging.info("Miner submitted NSFW content but marked it as safe")
        return ValidationResult(
            is_valid=False,
            reason="NSFW content incorrectly marked as safe",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    if not actual_content.is_nsfw and submitted_content.is_nsfw:
        bt.logging.info("Miner incorrectly marked safe content as NSFW")
        return ValidationResult(
            is_valid=False,
            reason="Safe content incorrectly marked as NSFW",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    # ALWAYS validate: NSFW content with media is never valid for the subnet
    # NO DATE RESTRICTIONS - this rule applies universally
    if submitted_content.is_nsfw and submitted_content.media:
        bt.logging.info("NSFW content with media is not valid for the subnet")
        return ValidationResult(
            is_valid=False,
            reason="NSFW content with media is not valid for the subnet",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    if actual_content.is_nsfw and actual_content.media:
        bt.logging.info("NSFW content with media detected and rejected")
        return ValidationResult(
            is_valid=False,
            reason="NSFW content with media is not valid for the subnet",
            content_size_bytes_validated=entity.content_size_bytes,
        )

    return ValidationResult(
        is_valid=True,
        reason="NSFW validation passed",
        content_size_bytes_validated=entity.content_size_bytes,
    )


def extract_media_urls(submission) -> List[str]:
    """
    Extract media URLs from a Reddit submission following X/Twitter pattern.

    Args:
        submission: Reddit submission object from asyncpraw

    Returns:
        List[str]: List of media URLs found in the submission
    """
    media_urls = []

    try:
        # 1. Direct URL (for image/video posts) - prioritize original URLs
        if hasattr(submission, 'url') and submission.url:
            url = submission.url
            # Check if it's a direct media URL or Reddit media domain
            if (any(url.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.webm']) or
                    any(domain in url for domain in ['i.redd.it', 'v.redd.it'])):
                # Clean URL parameters to get original
                clean_url = url.split('?')[0]
                media_urls.append(clean_url)

        # 2. Preview images (only if no direct URL found, and clean parameters)
        if hasattr(submission, 'preview') and submission.preview:
            preview_data = submission.preview
            if isinstance(preview_data, dict) and 'images' in preview_data:
                for image in preview_data['images']:
                    if 'source' in image and 'url' in image['source']:
                        # Clean URL parameters to prevent gaming with extra bytes
                        clean_url = image['source']['url'].split('?')[0]
                        # Convert preview URLs to original i.redd.it URLs when possible
                        if 'preview.redd.it' in clean_url:
                            original_url = clean_url.replace('preview.redd.it', 'i.redd.it')
                            media_urls.append(original_url)
                        else:
                            media_urls.append(clean_url)

        # 3. Gallery media - clean URLs and get originals
        if hasattr(submission, 'media_metadata') and submission.media_metadata:
            if isinstance(submission.media_metadata, dict):
                for media_id, media_data in submission.media_metadata.items():
                    if isinstance(media_data, dict) and 's' in media_data:
                        source = media_data['s']
                        if 'u' in source:
                            # Decode HTML entities and clean parameters
                            url = source['u'].replace('&amp;', '&').split('?')[0]
                            # Convert preview URLs to original i.redd.it URLs
                            if 'preview.redd.it' in url:
                                original_url = url.replace('preview.redd.it', 'i.redd.it')
                                media_urls.append(original_url)
                            else:
                                media_urls.append(url)

    except Exception as e:
        bt.logging.warning(f"Error extracting media URLs from submission: {e}")

    # Clean all URLs by removing parameters and duplicates
    clean_media_urls = []
    seen_urls = set()

    for url in media_urls:
        # Remove all parameters after ? to eliminate auto=webp&s=... stuff
        clean_url = url.split('?')[0]

        # Skip if we've already seen this clean URL
        if clean_url in seen_urls:
            continue

        seen_urls.add(clean_url)
        clean_media_urls.append(clean_url)

    return clean_media_urls


def validate_score_content(submitted_content: RedditContent, actual_content: RedditContent,
                          entity: DataEntity) -> ValidationResult:
    """
    Validate score content with smart time-based tolerance and anti-cheating mechanisms.
    Backward compatible: only validates if miner provided score fields.
    
    Args:
        submitted_content: Content submitted by miner
        actual_content: Actual content from Reddit API
        entity: DataEntity being validated
        
    Returns:
        ValidationResult indicating if score content is valid
    """
    # Skip validation if miner didn't provide score field (backward compatibility)
    if submitted_content.score is None:
        return ValidationResult(
            is_valid=True,
            reason="Score validation skipped - field not provided (backward compatibility)",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    # Calculate content age from creation time
    now = dt.datetime.now(dt.timezone.utc)
    content_age = now - submitted_content.created_at
    
    # Define age-based score tolerance thresholds
    # Newer content has tighter tolerance, older content allows more variance
    if content_age < dt.timedelta(hours=1):
        # Very fresh content: allow small changes
        score_tolerance_percent = 0.15  # 15%
        min_tolerance = 5
    elif content_age < dt.timedelta(hours=6):
        # Recent content: moderate tolerance
        score_tolerance_percent = 0.25  # 25%
        min_tolerance = 10
    elif content_age < dt.timedelta(days=1):
        # Day-old content: higher tolerance
        score_tolerance_percent = 0.40  # 40%
        min_tolerance = 20
    elif content_age < dt.timedelta(days=7):
        # Week-old content: even higher tolerance
        score_tolerance_percent = 0.60  # 60%
        min_tolerance = 30
    else:
        # Old content: highest tolerance
        score_tolerance_percent = 1.0   # 100%
        min_tolerance = 50
    
    # Anti-cheating mechanism: prevent unreasonably high scores
    max_reasonable_score = _calculate_max_reasonable_score(submitted_content, content_age)
    if submitted_content.score > max_reasonable_score:
        bt.logging.info(f"Submitted score {submitted_content.score} exceeds reasonable maximum {max_reasonable_score} for content age {content_age}")
        return ValidationResult(
            is_valid=False,
            reason=f"Score {submitted_content.score} is unreasonably high for content of this age",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    # Calculate allowed score variance
    base_score = abs(actual_content.score) if actual_content.score else 0
    tolerance = max(int(base_score * score_tolerance_percent), min_tolerance)
    
    # Score can only increase or stay the same (Reddit doesn't remove upvotes typically)
    # Allow some decrease for edge cases but be more strict about it
    max_allowed_score = actual_content.score + tolerance
    min_allowed_score = actual_content.score - min(tolerance // 2, 10)  # Less tolerance for decreases
    
    # Validate score is within reasonable bounds - use penalty system for outliers
    if not (min_allowed_score <= submitted_content.score <= max_allowed_score):
        # Calculate how far off the score is (penalty factor)
        if submitted_content.score > max_allowed_score:
            score_deviation = (submitted_content.score - max_allowed_score) / max(max_allowed_score, 1)
        else:
            score_deviation = (min_allowed_score - submitted_content.score) / max(actual_content.score, 1)
        
        # For very extreme outliers (>500% off), still reject to prevent major cheating
        if score_deviation > 5.0:  # More than 5x off
            bt.logging.info(
                f"Score validation failed - extreme outlier: submitted={submitted_content.score}, "
                f"actual={actual_content.score}, deviation={score_deviation:.2f}x"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"Score {submitted_content.score} is extremely unrealistic (deviation: {score_deviation:.2f}x)",
                content_size_bytes_validated=entity.content_size_bytes,
            )
        
        # For moderate outliers, apply penalty but still validate
        penalty_factor = min(0.5, score_deviation * 0.1)                    # Max 50% penalty, scales with deviation
        penalized_bytes = int(entity.content_size_bytes * penalty_factor)   # lower penalties for less score deviation
        
        bt.logging.info(
            f"Score validation passed with penalty: submitted={submitted_content.score}, "
            f"actual={actual_content.score}, deviation={score_deviation:.2f}x, penalty={penalty_factor:.2f}"
        )
        return ValidationResult(
            is_valid=False,
            reason=f"Score validation failed with adjusted penalty for viral outlier (deviation: {score_deviation:.2f}x)",
            content_size_bytes_validated=penalized_bytes,
        )
    
    # Validate upvote_ratio if provided (submissions only)
    if submitted_content.upvote_ratio is not None:
        if actual_content.upvote_ratio is None:
            bt.logging.info("Miner provided upvote_ratio but actual content has none (likely a comment)")
            return ValidationResult(
                is_valid=False,
                reason="Upvote ratio provided for content that doesn't support it",
                content_size_bytes_validated=entity.content_size_bytes,
            )
        
        # Upvote ratio should be between 0.0 and 1.0
        if not (0.0 <= submitted_content.upvote_ratio <= 1.0):
            bt.logging.info(f"Invalid upvote_ratio: {submitted_content.upvote_ratio}")
            return ValidationResult(
                is_valid=False,
                reason=f"Invalid upvote_ratio {submitted_content.upvote_ratio}, must be between 0.0 and 1.0",
                content_size_bytes_validated=entity.content_size_bytes,
            )
        
        # Allow some tolerance for upvote_ratio changes (typically more stable than raw scores)
        # Increased tolerance to account for Reddit's vote fuzzing and natural variance
        ratio_tolerance = 0.12  # 12% tolerance (was 5% - too strict)
        if abs(submitted_content.upvote_ratio - actual_content.upvote_ratio) > ratio_tolerance:
            bt.logging.info(
                f"Upvote ratio validation failed: submitted={submitted_content.upvote_ratio}, "
                f"actual={actual_content.upvote_ratio}, tolerance={ratio_tolerance}"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"Upvote ratio {submitted_content.upvote_ratio} differs too much from actual {actual_content.upvote_ratio}",
                content_size_bytes_validated=entity.content_size_bytes,
            )
    
    return ValidationResult(
        is_valid=True,
        reason="Score validation passed",
        content_size_bytes_validated=entity.content_size_bytes,
    )


def _calculate_max_reasonable_score(content: RedditContent, content_age: dt.timedelta) -> int:
    """
    Calculate maximum reasonable score based on content age and type to prevent cheating.
    
    Args:
        content: The Reddit content
        content_age: Age of the content
        
    Returns:
        Maximum reasonable score for this content
    """
    # Base limits by content type
    if content.data_type == RedditDataType.POST:
        # Posts can get much higher scores
        base_hourly_rate = 500  # Max 500 upvotes per hour for posts
        max_absolute = 50000     # Absolute maximum for posts
    else:
        # Comments typically get lower scores
        base_hourly_rate = 200  # Max 200 upvotes per hour for comments  
        max_absolute = 10000    # Absolute maximum for comments
    
    # Calculate time-based maximum
    age_hours = max(content_age.total_seconds() / 3600, 0.1)  # Minimum 0.1 hours
    time_based_max = int(base_hourly_rate * age_hours)
    
    # Apply diminishing returns for older content (viral content peaks then stabilizes)
    if age_hours > 24:
        # After 24 hours, growth slows significantly
        time_based_max = int(time_based_max * 0.7)
    if age_hours > 168:  # 1 week
        # After 1 week, very little growth
        time_based_max = int(time_based_max * 0.5)
    
    # Return the minimum of time-based and absolute maximum
    return min(time_based_max, max_absolute)


def validate_comment_count(submitted_content: RedditContent, actual_content: RedditContent,
                          entity: DataEntity) -> ValidationResult:
    """
    Validate comment count with sophisticated growth modeling and anti-cheating mechanisms.
    Comments grow differently than scores - they can only increase and follow predictable patterns.
    Backward compatible: only validates if miner provided num_comments field.
    
    Args:
        submitted_content: Content submitted by miner
        actual_content: Actual content from Reddit API
        entity: DataEntity being validated
        
    Returns:
        ValidationResult indicating if comment count is valid
    """
    # Skip validation if miner didn't provide num_comments field (backward compatibility)
    if submitted_content.num_comments is None:
        return ValidationResult(
            is_valid=True,
            reason="Comment count validation skipped - field not provided (backward compatibility)",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    # Comment count only applies to posts, not comments
    if submitted_content.data_type != RedditDataType.POST:
        if submitted_content.num_comments is not None:
            bt.logging.info("Miner provided num_comments for a comment (should only be for posts)")
            return ValidationResult(
                is_valid=False,
                reason="Comment count provided for content that doesn't support it (comments don't have comment counts)",
                content_size_bytes_validated=entity.content_size_bytes,
            )
        return ValidationResult(
            is_valid=True,
            reason="Comment count validation skipped - not applicable to comments",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    # Basic sanity check: comment count must be non-negative
    if submitted_content.num_comments < 0:
        bt.logging.info(f"Invalid negative comment count: {submitted_content.num_comments}")
        return ValidationResult(
            is_valid=False,
            reason=f"Invalid negative comment count: {submitted_content.num_comments}",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    # Calculate content age for validation logic
    now = dt.datetime.now(dt.timezone.utc)
    content_age = now - submitted_content.created_at
    
    # Anti-cheating: Maximum reasonable comment count based on content age and viral potential
    max_reasonable_comments = _calculate_max_reasonable_comment_count(submitted_content, content_age)
    if submitted_content.num_comments > max_reasonable_comments:
        bt.logging.info(f"Submitted comment count {submitted_content.num_comments} exceeds reasonable maximum {max_reasonable_comments} for content age {content_age}")
        return ValidationResult(
            is_valid=False,
            reason=f"Comment count {submitted_content.num_comments} is unreasonably high for content of this age",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    # Sophisticated tolerance based on comment growth patterns
    comment_tolerance = _calculate_comment_count_tolerance(submitted_content, actual_content, content_age)
    
    # Comments can ONLY increase over time (Reddit doesn't delete comments automatically)
    # Allow very small decreases only for edge cases (deleted spam comments)
    max_allowed_comments = actual_content.num_comments + comment_tolerance
    min_allowed_comments = max(0, actual_content.num_comments - min(comment_tolerance // 10, 2))  # Very strict on decreases
    
    # Special case: If actual count is 0, allow some initial growth
    if actual_content.num_comments == 0:
        min_allowed_comments = 0
        # Fresh posts can get initial comments quickly
        max_allowed_comments = max(comment_tolerance, 10)
    
    # Validate comment count is within sophisticated bounds - use penalty system for viral outliers
    if not (min_allowed_comments <= submitted_content.num_comments <= max_allowed_comments):
        # Calculate how far off the comment count is
        if submitted_content.num_comments > max_allowed_comments:
            comment_deviation = (submitted_content.num_comments - max_allowed_comments) / max(max_allowed_comments, 1)
        else:
            comment_deviation = (min_allowed_comments - submitted_content.num_comments) / max(actual_content.num_comments, 1)
        
        # For very extreme outliers (>800% off), still reject to prevent major cheating
        if comment_deviation > 8.0:  # More than 8x off (comments can be more viral than scores)
            bt.logging.info(
                f"Comment count validation failed - extreme outlier: submitted={submitted_content.num_comments}, "
                f"actual={actual_content.num_comments}, deviation={comment_deviation:.2f}x"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"Comment count {submitted_content.num_comments} is extremely unrealistic (deviation: {comment_deviation:.2f}x)",
                content_size_bytes_validated=entity.content_size_bytes,
            )
        
        # For moderate outliers, apply penalty but still validate
        penalty_factor = min(0.4, comment_deviation * 0.08)  # Max 40% penalty for comments, scales with deviation
        penalized_bytes = int(entity.content_size_bytes * (1.0 - penalty_factor))
        
        bt.logging.info(
            f"Comment count validation passed with penalty: submitted={submitted_content.num_comments}, "
            f"actual={actual_content.num_comments}, deviation={comment_deviation:.2f}x, penalty={penalty_factor:.2f}"
        )
        return ValidationResult(
            is_valid=True,
            reason=f"Comment count validation passed with penalty for viral outlier (deviation: {comment_deviation:.2f}x)",
            content_size_bytes_validated=penalized_bytes,
        )
    
    # Advanced validation: Check comment-to-score ratio for suspicious patterns
    suspicious_ratio_result = _validate_comment_score_ratio(submitted_content, actual_content, entity)
    if not suspicious_ratio_result.is_valid:
        return suspicious_ratio_result
    
    return ValidationResult(
        is_valid=True,
        reason="Comment count validation passed",
        content_size_bytes_validated=entity.content_size_bytes,
    )


def _calculate_comment_count_tolerance(submitted_content: RedditContent, actual_content: RedditContent, 
                                     content_age: dt.timedelta) -> int:
    """
    Calculate sophisticated tolerance for comment count changes based on content patterns.
    
    Args:
        submitted_content: Content submitted by miner
        actual_content: Actual content from Reddit API
        content_age: Age of the content
        
    Returns:
        Comment count tolerance (absolute number)
    """
    base_count = actual_content.num_comments or 0
    
    # Age-based tolerance - newer content has more comment activity
    if content_age < dt.timedelta(hours=1):
        # Very fresh: high comment velocity
        age_tolerance_percent = 0.50  # 50% tolerance
        min_tolerance = 5
    elif content_age < dt.timedelta(hours=6):
        # Recent: moderate comment velocity
        age_tolerance_percent = 0.35  # 35% tolerance
        min_tolerance = 3
    elif content_age < dt.timedelta(days=1):
        # Day-old: slowing down
        age_tolerance_percent = 0.25  # 25% tolerance
        min_tolerance = 2
    elif content_age < dt.timedelta(days=7):
        # Week-old: much slower
        age_tolerance_percent = 0.15  # 15% tolerance
        min_tolerance = 1
    else:
        # Old: very slow growth
        age_tolerance_percent = 0.10  # 10% tolerance
        min_tolerance = 1
    
    # Score-based adjustment: higher scoring posts get more comments
    score_multiplier = 1.0
    if submitted_content.score and submitted_content.score > 100:
        # High-scoring posts attract more comments
        score_multiplier = min(2.0, 1.0 + (submitted_content.score / 1000))
    
    # Calculate final tolerance
    base_tolerance = max(int(base_count * age_tolerance_percent), min_tolerance)
    final_tolerance = int(base_tolerance * score_multiplier)
    
    return final_tolerance


def _calculate_max_reasonable_comment_count(content: RedditContent, content_age: dt.timedelta) -> int:
    """
    Calculate maximum reasonable comment count to prevent extreme cheating.
    
    Args:
        content: The Reddit content
        content_age: Age of the content
        
    Returns:
        Maximum reasonable comment count for this content
    """
    # Base comment velocity (comments per hour) - Reddit can get MASSIVELY viral
    base_hourly_rate = 200  # Max 200 comments per hour for normal posts (increased from 50)
    max_absolute = 100000   # Absolute maximum comments (increased from 5000 to handle mega-viral posts like COVID threads, AMAs, breaking news)
    
    # Viral content multiplier based on score - handle mega-viral events
    viral_multiplier = 1.0
    if content.score and content.score > 10000:
        # Mega-viral posts (COVID, breaking news, major AMAs) can get 50x+ comments
        viral_multiplier = min(50.0, content.score / 1000)  # Up to 50x for 50k+ score posts
    elif content.score and content.score > 1000:
        # Very high scoring posts can get many more comments
        viral_multiplier = min(20.0, content.score / 1000)  # Up to 20x for 20k+ score posts
    elif content.score and content.score > 100:
        # Moderately high scoring posts get modest boost
        viral_multiplier = min(5.0, 1.0 + (content.score / 500))  # Up to 5x
    
    # Calculate time-based maximum with viral adjustment
    age_hours = max(content_age.total_seconds() / 3600, 0.1)  # Minimum 0.1 hours
    time_based_max = int(base_hourly_rate * viral_multiplier * age_hours)
    
    # Apply diminishing returns for very old content
    if age_hours > 48:  # After 48 hours, comment growth slows significantly
        time_based_max = int(time_based_max * 0.6)
    if age_hours > 168:  # After 1 week, very little new comment activity
        time_based_max = int(time_based_max * 0.3)
    
    return min(time_based_max, max_absolute)


def _validate_comment_score_ratio(submitted_content: RedditContent, actual_content: RedditContent,
                                entity: DataEntity) -> ValidationResult:
    """
    Advanced validation: Check for suspicious comment-to-score ratios that might indicate cheating.
    
    Args:
        submitted_content: Content submitted by miner
        actual_content: Actual content from Reddit API
        entity: DataEntity being validated
        
    Returns:
        ValidationResult indicating if the comment-score ratio is reasonable
    """
    # Skip if we don't have both score and comment data
    if not (submitted_content.score and submitted_content.num_comments):
        return ValidationResult(
            is_valid=True,
            reason="Comment-score ratio validation skipped - insufficient data",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    # Calculate comment-to-score ratio
    if submitted_content.score <= 0:
        # Edge case: negative or zero score posts can still have comments
        if submitted_content.num_comments > 50:  # But not too many
            return ValidationResult(
                is_valid=False,
                reason=f"Suspicious: {submitted_content.num_comments} comments on post with score {submitted_content.score}",
                content_size_bytes_validated=entity.content_size_bytes,
            )
        return ValidationResult(
            is_valid=True,
            reason="Comment-score ratio acceptable for low/negative score post",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    # Normal case: positive score
    comment_to_score_ratio = submitted_content.num_comments / submitted_content.score
    
    # Typical Reddit patterns:
    # - Most posts: 0.1-2.0 comments per upvote (10-200 comments per 100 score)
    # - Controversial posts: higher ratio (lots of arguing)
    # - Simple memes: lower ratio (just upvote and move on)
    # - Low-score posts (1-5 points): Often have 6-10 comments per point due to discussions
    
    # Dynamic threshold based on score - low-score posts naturally have higher ratios
    if submitted_content.score <= 3:
        max_reasonable_ratio = 10.0  # Low-score posts can have high engagement
    elif submitted_content.score <= 10:
        max_reasonable_ratio = 8.0   # Moderate adjustment for medium-low scores
    else:
        max_reasonable_ratio = 5.0   # Keep original threshold for higher scores
    
    if comment_to_score_ratio > max_reasonable_ratio:
        bt.logging.info(f"Suspicious comment-to-score ratio: {comment_to_score_ratio:.2f} (comments={submitted_content.num_comments}, score={submitted_content.score})")
        return ValidationResult(
            is_valid=False,
            reason=f"Suspicious comment-to-score ratio: {comment_to_score_ratio:.2f} exceeds reasonable maximum {max_reasonable_ratio}",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    return ValidationResult(
        is_valid=True,
        reason="Comment-score ratio is reasonable",
        content_size_bytes_validated=entity.content_size_bytes,
    )
