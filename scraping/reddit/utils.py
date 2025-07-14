import bittensor as bt
import traceback
import datetime as dt
import random

from typing import List
from urllib.parse import urlparse
from scraping import utils
from scraping.scraper import ValidationResult
from scraping.reddit.model import RedditContent
from common.data import DataEntity, DataLabel
from common.constants import BYTE_ALLOWANCE_DATE, REDDIT_MEDIA_REQUIRED_DATE


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
        byte_difference_allowed = 10
        if dt.datetime.now(dt.timezone.utc) >= BYTE_ALLOWANCE_DATE:
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
