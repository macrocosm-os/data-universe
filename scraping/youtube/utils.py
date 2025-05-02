import re
from urllib.parse import urlparse, parse_qs


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