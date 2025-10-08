import re
from urllib.parse import urlparse, parse_qs
import datetime as dt
import bittensor as bt
from typing import Optional, List, Dict
from scraping import utils
from scraping.scraper import ValidationResult
from common.data import DataEntity
from common.constants import YOUTUBE_TIMESTAMP_OBFUSCATION_REQUIRED_DATE
from .model import YouTubeContent
from .model import normalize_channel_name

# Fields that can only increase (engagement metrics)
INCREASING_ONLY_FIELDS = [
    "view_count",
    "like_count",
]

# Fields that can increase or decrease (subscriber metrics)
BI_DIRECTIONAL_FIELDS = [
    "subscriber_count",
]


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


def generate_thumbnails(video_id: str):
    """get thumbnails"""
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


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


def texts_are_similar(text1, text2, threshold=0.9):
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

    # Obfuscate the actual_entity datetime to match the original entity's obfuscated datetime
    actual_entity = actual_entity.model_copy(update={
        'datetime': utils.obfuscate_datetime_to_minute(actual_entity.datetime)
    })
    
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


def validate_transcript_timing(
    transcript: List[Dict],
    video_duration_seconds: int,
    entity: DataEntity
) -> Optional[ValidationResult]:
    """
    Validate transcript timing to prevent miners from submitting fake timing data.

    Checks:
    1. Start/end times are sequential and non-negative
    2. Segment durations are positive and reasonable
    3. Total transcript duration roughly matches video duration

    Args:
        transcript: List of transcript segments with 'start' and 'end' fields (per model spec)
        video_duration_seconds: Total video duration
        entity: DataEntity being validated

    Returns:
        ValidationResult if validation fails, None if validation passes
    """
    if not transcript or len(transcript) == 0:
        # Empty transcript is allowed (some videos have no transcript)
        return None

    prev_end_time = 0.0
    total_duration = 0.0

    for i, segment in enumerate(transcript):
        # Check segment has required fields: 'start' and 'end'
        has_end = 'end' in segment
        has_start = 'start' in segment

        if not has_start:
            bt.logging.info(f"Transcript segment {i} missing 'start' field")
            return ValidationResult(
                is_valid=False,
                reason=f"Transcript segment {i} missing required 'start' field",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        if not has_end:
            bt.logging.info(f"Transcript segment {i} missing 'end' field")
            return ValidationResult(
                is_valid=False,
                reason=f"Transcript segment {i} missing required 'end' field",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        start = float(segment.get('start', 0))
        end = float(segment.get('end', 0))

        duration = end - start

        # Check start time is non-negative
        if start < 0:
            bt.logging.info(f"Transcript segment {i} has negative start time: {start}")
            return ValidationResult(
                is_valid=False,
                reason=f"Transcript segment {i} has invalid negative start time",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Check end time is after start time and duration is reasonable (max 5 minutes per segment)
        if end <= start or duration > 300:
            bt.logging.info(f"Transcript segment {i} has invalid timing: start={start}, end={end}, duration={duration}")
            return ValidationResult(
                is_valid=False,
                reason=f"Transcript segment {i} has invalid timing (end must be > start, duration max 300s)",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Check timing is sequential (allow gaps/overlaps - videos can have pauses, scene changes)
        # Only reject if current segment starts way before previous segment ended (backwards in time)
        if i > 0:
            if start < prev_end_time - 5.0:  # Allow 5s overlap tolerance for subtitle timing quirks
                bt.logging.info(
                    f"Transcript segment {i} goes backwards in time: "
                    f"start={start}, prev_end={prev_end_time}"
                )
                return ValidationResult(
                    is_valid=False,
                    reason=f"Transcript timing goes backwards at segment {i}",
                    content_size_bytes_validated=entity.content_size_bytes,
                )

        prev_end_time = end
        total_duration = max(total_duration, end)

    # Check total duration roughly matches video duration (allow 10% tolerance)
    if video_duration_seconds > 0:
        duration_diff = abs(total_duration - video_duration_seconds)
        tolerance = video_duration_seconds * 0.10  # 10% tolerance

        if duration_diff > tolerance:
            bt.logging.info(
                f"Transcript total duration {total_duration}s differs significantly from "
                f"video duration {video_duration_seconds}s (diff={duration_diff}s, tolerance={tolerance}s)"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"Transcript duration mismatch: {total_duration}s vs video {video_duration_seconds}s",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    return None  # Validation passed


def validate_youtube_data_entities(
    entity_to_validate: DataEntity,
    actual_entity: DataEntity
) -> ValidationResult:
    """
    Unified YouTube validation function comparing two DataEntity objects.
    Both entities should have obfuscated datetime fields.

    Args:
        entity_to_validate: DataEntity from miner to validate
        actual_entity: DataEntity from validator's fresh scrape

    Returns:
        ValidationResult indicating if the entity is valid
    """
    try:
        # Step 1: Decode content from both entities
        content_to_validate = YouTubeContent.from_data_entity(entity_to_validate)
        actual_content = YouTubeContent.from_data_entity(actual_entity)

        bt.logging.info(f"Validating video {content_to_validate.video_id} in language: {content_to_validate.language}")

        # Step 2: Validate timestamp with obfuscation
        timestamp_validation = validate_youtube_timestamp(
            content_to_validate, actual_content.upload_date, entity_to_validate
        )
        if not timestamp_validation.is_valid:
            return timestamp_validation

        # Step 3: Validate video ID match
        if actual_content.video_id != content_to_validate.video_id:
            return ValidationResult(
                is_valid=False,
                reason=f"Video ID mismatch: expected {content_to_validate.video_id}, got {actual_content.video_id}",
                content_size_bytes_validated=entity_to_validate.content_size_bytes
            )

        # Step 4: Validate title similarity
        if not texts_are_similar(actual_content.title, content_to_validate.title, threshold=0.8):
            return ValidationResult(
                is_valid=False,
                reason="Title does not match current video title",
                content_size_bytes_validated=entity_to_validate.content_size_bytes
            )

        # Step 5: Validate transcript similarity
        if not transcripts_are_similar(actual_content.transcript, content_to_validate.transcript, threshold=0.7):
            return ValidationResult(
                is_valid=False,
                reason="Transcript does not match current video transcript",
                content_size_bytes_validated=entity_to_validate.content_size_bytes
            )

        # Step 5.5: Validate transcript timing structure (anti-cheating)
        timing_validation = validate_transcript_timing(
            content_to_validate.transcript,
            content_to_validate.duration_seconds,
            entity_to_validate
        )
        if timing_validation is not None:
            return timing_validation

        # Step 6: Ensure both DataEntity datetime fields are obfuscated before comparison
        entity_to_validate_obfuscated = entity_to_validate.model_copy(update={
            'datetime': utils.obfuscate_datetime_to_minute(entity_to_validate.datetime)
        })

        actual_entity_obfuscated = actual_entity.model_copy(update={
            'datetime': utils.obfuscate_datetime_to_minute(actual_entity.datetime)
        })

        # Step 7: Use DataEntity field equality check (like X and Reddit)
        if not DataEntity.are_non_content_fields_equal(actual_entity_obfuscated, entity_to_validate_obfuscated):
            bt.logging.info(f"DataEntity field mismatch detected")
            bt.logging.info(f"Actual: URI={actual_entity_obfuscated.uri}, DateTime={actual_entity_obfuscated.datetime}, Source={actual_entity_obfuscated.source}, Label={actual_entity_obfuscated.label}")
            bt.logging.info(f"Expected: URI={entity_to_validate_obfuscated.uri}, DateTime={entity_to_validate_obfuscated.datetime}, Source={entity_to_validate_obfuscated.source}, Label={entity_to_validate_obfuscated.label}")
            return ValidationResult(
                is_valid=False,
                reason="The DataEntity fields are incorrect based on the YouTube content.",
                content_size_bytes_validated=entity_to_validate.content_size_bytes,
            )

        # Step 7.5: Validate content size (prevent byte inflation with extra fields)
        byte_difference_allowed = 20  # Allow small differences for encoding/formatting variations
        if (entity_to_validate.content_size_bytes - actual_entity.content_size_bytes) > byte_difference_allowed:
            return ValidationResult(
                is_valid=False,
                reason=f"Claimed bytes ({entity_to_validate.content_size_bytes}) exceed actual content size ({actual_entity.content_size_bytes}) by more than {byte_difference_allowed} bytes",
                content_size_bytes_validated=entity_to_validate.content_size_bytes,
            )

        # Step 8: Validate optional description field
        description_result = validate_youtube_description(content_to_validate, actual_content, entity_to_validate)
        if description_result is not None:
            return description_result

        # Step 9: Validate optional thumbnails field
        thumbnails_result = validate_youtube_thumbnails(content_to_validate, actual_content, entity_to_validate)
        if thumbnails_result is not None:
            return thumbnails_result

        # Step 10: Validate dynamic engagement fields (view_count, like_count, subscriber_count)
        dynamic_fields = INCREASING_ONLY_FIELDS + BI_DIRECTIONAL_FIELDS

        # Calculate video age for engagement validation
        now = dt.datetime.now(dt.timezone.utc)
        video_age = now - content_to_validate.upload_date

        for field_name in dynamic_fields:
            submitted_value = getattr(content_to_validate, field_name, None)
            actual_value = getattr(actual_content, field_name, None)

            # Skip validation if miner didn't provide field (backward compatibility)
            if submitted_value is None:
                continue

            # Validate individual engagement metric
            field_validation_result = _validate_youtube_engagement_field(
                field_name, submitted_value, actual_value, video_age, entity_to_validate
            )
            if field_validation_result is not None:
                return field_validation_result

        # Step 11: All validations passed!
        return ValidationResult(
            is_valid=True,
            reason="YouTube validation passed",
            content_size_bytes_validated=entity_to_validate.content_size_bytes
        )

    except Exception as e:
        bt.logging.error(f"YouTube validation error: {str(e)}")
        return ValidationResult(
            is_valid=False,
            reason=f"Validation failed due to error: {str(e)}",
            content_size_bytes_validated=entity_to_validate.content_size_bytes
        )


def validate_youtube_description(
    submitted_content: YouTubeContent,
    actual_content: YouTubeContent,
    entity: DataEntity
) -> Optional[ValidationResult]:
    """
    Validate YouTube description field.
    Backward compatible: only validates if miner provided description.

    Args:
        submitted_content: Content submitted by miner
        actual_content: Actual content from YouTube API
        entity: DataEntity being validated

    Returns:
        ValidationResult if validation fails, None if validation passes
    """
    # Skip validation if miner didn't provide description (backward compatibility)
    if submitted_content.description is None:
        return None

    # If miner provided description, validate it strictly
    if submitted_content.description:
        # Check length bounds (YouTube limit is 5000 characters)
        if len(submitted_content.description) > 5000:
            bt.logging.info(f"Description exceeds maximum length: {len(submitted_content.description)}")
            return ValidationResult(
                is_valid=False,
                reason=f"Description exceeds YouTube maximum length (5000 characters, got {len(submitted_content.description)})",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # If miner claims description but actual video has none, reject it
        if not actual_content.description:
            bt.logging.info("Miner included description but the video has none")
            return ValidationResult(
                is_valid=False,
                reason="Miner included fake description for a video with no description",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Validate description similarity (strict 95% threshold)
        if not texts_are_similar(submitted_content.description, actual_content.description, threshold=0.95):
            bt.logging.info("Description does not match actual content")
            return ValidationResult(
                is_valid=False,
                reason="Description does not match current video description (95% similarity required)",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    return None  # Validation passed


def validate_youtube_thumbnails(
    submitted_content: YouTubeContent,
    actual_content: YouTubeContent,
    entity: DataEntity
) -> Optional[ValidationResult]:
    """
    Validate YouTube thumbnails URL field.
    Backward compatible: only validates if miner provided thumbnails.
    Simple equality check: miner's thumbnail must match validator's.

    Args:
        submitted_content: Content submitted by miner
        actual_content: Actual content from YouTube API
        entity: DataEntity being validated

    Returns:
        ValidationResult if validation fails, None if validation passes
    """
    # Skip validation if miner didn't provide thumbnails (backward compatibility)
    if submitted_content.thumbnails is None:
        return None

    # If miner provided thumbnails, validate exact match
    if submitted_content.thumbnails:
        # Both should use youtube_utils.generate_thumbnails(video_id)
        # So they should be identical
        if actual_content.thumbnails is None:
            bt.logging.info("Miner included thumbnails but validator has none")
            return ValidationResult(
                is_valid=False,
                reason="Miner included thumbnails but validator could not generate thumbnail URL",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        if submitted_content.thumbnails != actual_content.thumbnails:
            bt.logging.info(
                f"Thumbnail URL mismatch: miner={submitted_content.thumbnails}, validator={actual_content.thumbnails}"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"Thumbnail URL does not match (expected: {actual_content.thumbnails})",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    return None  # Validation passed


def _validate_youtube_engagement_field(
    field_name: str,
    submitted_value: int,
    actual_value: int,
    video_age: dt.timedelta,
    entity: DataEntity,
) -> Optional[ValidationResult]:
    """
    Validate a single engagement field with tolerance and anti-cheating.
    Following X validation pattern exactly.

    Args:
        field_name: Name of the engagement field
        submitted_value: Value submitted by miner
        actual_value: Actual value from API
        video_age: Age of the video
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

    # Use percentage-based validation for subscriber counts since we have exact current values
    if field_name in BI_DIRECTIONAL_FIELDS:
        return _validate_subscriber_count_percentage(
            field_name, submitted_value, actual_value, video_age, entity
        )

    # For increasing-only engagement metrics (view_count, like_count)
    # Calculate tolerance first to determine what small decreases are acceptable
    tolerance = _calculate_engagement_tolerance(field_name, submitted_value, video_age)

    # Dynamic small tolerance for decreases based on engagement size
    # Allow 0.5% decrease with min/max bounds appropriate to the metric
    if field_name == "view_count":
        small_tolerance = max(int(submitted_value * 0.005), 10)  # 0.5% with min 10 views
    else:  # like_count
        small_tolerance = max(int(submitted_value * 0.005), 3)   # 0.5% with min 3 likes

    # Allow small decreases for edge cases (spam removal, etc.)
    if actual_value is not None:
        max_allowed_decrease = small_tolerance
        if submitted_value > actual_value + max_allowed_decrease:
            bt.logging.info(
                f"{field_name} validation failed: submitted value {submitted_value} > actual value {actual_value} + tolerance {max_allowed_decrease} (impossible decrease for increasing-only metric)"
            )
            return ValidationResult(
                is_valid=False,
                reason=f"{field_name} decreased too much: submitted {submitted_value} > actual {actual_value} + {max_allowed_decrease}",
                content_size_bytes_validated=entity.content_size_bytes,
            )

    min_allowed_value = max(0, submitted_value - small_tolerance)
    max_allowed_value = submitted_value + tolerance

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


def _validate_subscriber_count_percentage(
    field_name: str,
    submitted_value: int,
    actual_value: int,
    video_age: dt.timedelta,
    entity: DataEntity,
) -> Optional[ValidationResult]:
    """
    Validate subscriber counts using smart percentage-based tolerance with age scaling.
    Uses logarithmic scaling - smaller channels have higher percentage tolerance.

    Args:
        field_name: Name of the subscriber field
        submitted_value: Value submitted by miner
        actual_value: Actual current value from API
        video_age: Age of the video (affects tolerance)
        entity: DataEntity being validated

    Returns:
        ValidationResult if validation fails, None if validation passes
    """
    import math

    # If we don't have an actual value, we can't validate percentage-wise
    if actual_value is None or actual_value <= 0:
        return None

    # Smart tolerance calculation using logarithmic decay
    base_percentage = 200.0  # Starting percentage for very small channels
    log_factor = math.log10(max(actual_value, 10))  # Prevent log(0)
    max_percentage = min(base_percentage / log_factor, 50.0)  # Cap at 50%

    # Age-based multiplier to handle viral growth scenarios
    age_hours = max(video_age.total_seconds() / 3600, 0.1)  # Minimum 0.1 hours

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
    max_percentage = min(max_percentage * age_multiplier, 500.0)  # Cap at 500%

    # Minimum absolute tolerance scales with channel size
    min_absolute = max(int(math.sqrt(actual_value) * 10), 50)

    # Calculate tolerance
    percentage_tolerance = int(actual_value * max_percentage / 100)
    final_tolerance = max(percentage_tolerance, min_absolute)

    # Subscriber counts can go up or down
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
    field_name: str, base_value: int, video_age: dt.timedelta
) -> int:
    """
    Calculate tolerance for engagement metric changes based on YouTube patterns.

    Args:
        field_name: Name of the engagement field
        base_value: Current value of the engagement metric
        video_age: Age of the video

    Returns:
        Engagement tolerance (absolute number)
    """
    # Age-based tolerance - newer videos have higher engagement velocity
    if video_age < dt.timedelta(hours=1):
        # Very fresh: high engagement velocity
        age_tolerance_percent = 1.0  # 100% tolerance
        min_tolerance = 20
    elif video_age < dt.timedelta(hours=6):
        # Recent: moderate engagement velocity
        age_tolerance_percent = 0.75  # 75% tolerance
        min_tolerance = 15
    elif video_age < dt.timedelta(days=1):
        # Day-old: slowing down but still active
        age_tolerance_percent = 0.50  # 50% tolerance
        min_tolerance = 10
    elif video_age < dt.timedelta(days=7):
        # Week-old: much slower growth
        age_tolerance_percent = 0.30  # 30% tolerance
        min_tolerance = 5
    else:
        # Old: very slow growth
        age_tolerance_percent = 0.20  # 20% tolerance
        min_tolerance = 3

    # Field-specific multipliers
    field_multipliers = {
        "view_count": 3.0,  # Highest tolerance - most volatile
        "like_count": 1.0,  # Baseline
    }

    multiplier = field_multipliers.get(field_name, 1.0)

    # Calculate final tolerance
    base_tolerance = max(int(base_value * age_tolerance_percent), min_tolerance)
    final_tolerance = int(base_tolerance * multiplier)

    return final_tolerance
