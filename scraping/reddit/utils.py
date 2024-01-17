from urllib.parse import urlparse
from scraping.scraper import ValidationResult
from scraping.reddit.model import RedditContent
from common.data import DataEntity, DataLabel
import bittensor as bt
import traceback
import datetime as dt
import random


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
    actual_content: RedditContent, entity_to_validate: DataEntity
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

    if actual_content != content_to_validate:
        bt.logging.info(
            f"RedditContent does not match: {actual_content} != {content_to_validate}"
        )
        return ValidationResult(
            is_valid=False,
            reason="Content does not match",
            content_size_bytes_validated=entity_to_validate.content_size_bytes,
        )

    # Wahey! The content is valid.
    # One final check. Does the Reddit content match the data entity information?
    try:
        actual_entity = RedditContent.to_data_entity(actual_content)
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
