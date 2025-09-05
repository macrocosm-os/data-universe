import re
from urllib.parse import urlparse, parse_qs
import datetime as dt
import bittensor as bt
from scraping import utils
from scraping.scraper import ValidationResult
from common.data import DataEntity
from common.constants import YOUTUBE_TIMESTAMP_OBFUSCATION_REQUIRED_DATE
from .model import YouTubeContent
from .model import normalize_channel_name


def extract_video_id(url: str) -> str:
    """
    Extracts the YouTube video ID from a YouTube URL.

    Args:
        url: The YouTube video URL.

    Returns:
        The YouTube video ID or an empty string if no ID could be extracted.
    """
    if not url:
        return ""

    # Standard YouTube URLs like https://www.youtube.com/watch?v=dQw4w9WgXcQ
    parsed_url = urlparse(url)
    if parsed_url.netloc in ('youtube.com', 'www.youtube.com'):
        query_params = parse_qs(parsed_url.query)
        if 'v' in query_params:
            return query_params['v'][0]

    # Short YouTube URLs like https://youtu.be/dQw4w9WgXcQ
    if parsed_url.netloc == 'youtu.be':
        return parsed_url.path.strip('/')

    # Embedded YouTube URLs like https://www.youtube.com/embed/dQw4w9WgXcQ
    if parsed_url.netloc in ('youtube.com', 'www.youtube.com') and '/embed/' in parsed_url.path:
        return parsed_url.path.split('/embed/')[1].split('/')[0].split('?')[0]

    # Try to find a video ID pattern in the URL
    video_id_pattern = r'(?:v=|v\/|embed\/|youtu\.be\/|\/v\/|\/e\/|watch\?v=|youtube.com\/v\/|youtube.com\/embed\/|youtu.be\/|v=|e=|u\/\w+\/|embed\?video_id=|\/videos\/|\/embed\/|\/v\/|watch\?.*v=|youtube.com\/embed\/)([\w-]{11})'
    match = re.search(video_id_pattern, url)
    if match:
        return match.group(1)

    return ""


def normalize_youtube_url(url: str) -> str:
    """
    Normalizes a YouTube URL to a standard form.

    Args:
        url: The YouTube URL to normalize.

    Returns:
        The normalized URL or the original if no normalization is possible.
    """
    video_id = extract_video_id(url)
    if video_id:
        return f"https://www.youtube.com/watch?v={video_id}"
    return url


def validate_youtube_content(actual_content, entity_to_validate, threshold=0.8):
    """
    Validates a YouTube content entity against an actual content.

    Args:
        actual_content: The actual YouTube content from the API.
        entity_to_validate: The entity that needs validation.
        threshold: The similarity threshold for text comparison.

    Returns:
        A tuple (is_valid, reason) where is_valid is a boolean and reason is a string.
    """
    # Check if the video IDs match
    if actual_content.video_id != entity_to_validate.video_id:
        return False, "Video IDs do not match"

    # Check if the upload dates are within a reasonable range
    # (YouTube may show slightly different timestamps depending on time zones)
    date_difference = abs((actual_content.upload_date - entity_to_validate.upload_date).total_seconds())
    if date_difference > 86400:  # More than 24 hours difference
        return False, "Upload dates do not match"

    # Check if the titles are similar enough
    if not texts_are_similar(actual_content.title, entity_to_validate.title, threshold):
        return False, "Titles do not match"

    # Check if the transcripts are similar enough
    if not transcripts_are_similar(actual_content.transcript, entity_to_validate.transcript, threshold):
        return False, "Transcripts do not match"

    return True, "Content is valid"


def texts_are_similar(text1, text2, threshold=0.8):
    """
    Check if two texts are similar enough.

    Args:
        text1: First text.
        text2: Second text.
        threshold: Similarity threshold (0-1).

    Returns:
        True if the texts are similar enough, False otherwise.
    """
    if not text1 or not text2:
        return text1 == text2

    # Simple approach: check if enough words from one text appear in the other
    words1 = set(text1.lower().split())
    words2 = set(text2.lower().split())

    # Calculate overlap ratio
    overlap = len(words1.intersection(words2))
    similarity = overlap / max(len(words1), len(words2))

    return similarity >= threshold


def transcripts_are_similar(transcript1, transcript2, threshold=0.8):
    """
    Check if two transcripts are similar enough.

    Args:
        transcript1: First transcript (list of dicts with 'text' keys).
        transcript2: Second transcript (list of dicts with 'text' keys).
        threshold: Similarity threshold (0-1).

    Returns:
        True if the transcripts are similar enough, False otherwise.
    """
    if not transcript1 or not transcript2:
        return transcript1 == transcript2

    # Extract text from both transcripts
    text1 = " ".join([item.get('text', '') for item in transcript1])
    text2 = " ".join([item.get('text', '') for item in transcript2])

    return texts_are_similar(text1, text2, threshold)


def validate_youtube_timestamp(stored_content, actual_content, entity: DataEntity) -> ValidationResult:
    """
    Validate YouTube timestamp with obfuscation logic, following X and Reddit pattern.
    Only enforces obfuscation after YOUTUBE_TIMESTAMP_OBFUSCATION_REQUIRED_DATE.
    
    Args:
        stored_content: YouTubeContent submitted by miner
        actual_content: Actual upload date from YouTube API
        entity: DataEntity being validated
        
    Returns:
        ValidationResult indicating if timestamp is valid
    """
    now = dt.datetime.now(dt.timezone.utc)
    
    # Before the deadline: Allow both obfuscated and non-obfuscated timestamps
    if now < YOUTUBE_TIMESTAMP_OBFUSCATION_REQUIRED_DATE:
        # Check if either exact match OR obfuscated match is valid
        actual_obfuscated_timestamp = utils.obfuscate_datetime_to_minute(actual_content)
        
        if stored_content.upload_date == actual_content or stored_content.upload_date == actual_obfuscated_timestamp:
            return ValidationResult(
                is_valid=True,
                reason="YouTube timestamp validation passed (before obfuscation deadline)",
                content_size_bytes_validated=entity.content_size_bytes,
            )
        else:
            bt.logging.info(
                f"YouTube timestamps do not match: stored={stored_content.upload_date}, actual={actual_content}, actual_obfuscated={actual_obfuscated_timestamp}"
            )
            return ValidationResult(
                is_valid=False,
                reason="YouTube timestamps do not match",
                content_size_bytes_validated=entity.content_size_bytes,
            )
    
    # After the deadline: Strict obfuscation required (same as X and Reddit)
    actual_obfuscated_timestamp = utils.obfuscate_datetime_to_minute(actual_content)
    
    if stored_content.upload_date != actual_obfuscated_timestamp:
        # Check if this is specifically because the entity was not obfuscated.
        if stored_content.upload_date == actual_content:
            bt.logging.info(
                f"Provided YouTube content datetime was not obfuscated to the minute as required: {stored_content.upload_date} != {actual_obfuscated_timestamp}"
            )
            return ValidationResult(
                is_valid=False,
                reason="Provided YouTube content datetime was not obfuscated to the minute as required",
                content_size_bytes_validated=entity.content_size_bytes,
            )
        else:
            bt.logging.info(
                f"YouTube timestamps do not match: stored={stored_content.upload_date}, actual_obfuscated={actual_obfuscated_timestamp}"
            )
            return ValidationResult(
                is_valid=False,
                reason="YouTube timestamps do not match",
                content_size_bytes_validated=entity.content_size_bytes,
            )
    
    return ValidationResult(
        is_valid=True,
        reason="YouTube timestamp validation passed",
        content_size_bytes_validated=entity.content_size_bytes,
    )


def validate_youtube_data_entity_fields(actual_content: YouTubeContent, entity: DataEntity) -> ValidationResult:
    """
    Validate DataEntity fields against the actual YouTube content.
    Replicates the pattern from X and Reddit validation using DataEntity.are_non_content_fields_equal().
    
    Args:
        actual_content: YouTubeContent with actual data from YouTube API
        entity: DataEntity submitted by miner
        
    Returns:
        ValidationResult indicating if DataEntity fields are valid
    """
    # Create DataEntity from actual content for comparison
    actual_entity = YouTubeContent.to_data_entity(content=actual_content)
    
    # Validate content size (prevent claiming more bytes than actual)
    byte_difference_allowed = 0
    
    if (entity.content_size_bytes - actual_entity.content_size_bytes) > byte_difference_allowed:
        return ValidationResult(
            is_valid=False,
            reason="The claimed bytes must not exceed the actual YouTube content size.",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    # Use the same DataEntity field equality check as X and Reddit
    if not DataEntity.are_non_content_fields_equal(actual_entity, entity):
        return ValidationResult(
            is_valid=False,
            reason="The DataEntity fields are incorrect based on the YouTube content.",
            content_size_bytes_validated=entity.content_size_bytes,
        )
    
    return ValidationResult(
        is_valid=True,
        reason="Good job, you honest miner!",
        content_size_bytes_validated=entity.content_size_bytes,
    )