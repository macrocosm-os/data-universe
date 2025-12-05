import datetime as dt
import hashlib
import re
import unicodedata
from typing import Dict, List, Optional
from pydantic.v1 import BaseModel, Field
from common.data import DataEntity, DataLabel, DataSource
from scraping import utils


def normalize_channel_name(name: str, max_len: int = 50) -> str:
    """
    Normalize channel name to a lowercase ASCII slug with fallback for non-ASCII content.
    
    Handles:
    - Pure emoji channels: 😀🎮🔥 → chan-a1b2c3d4 (deterministic hash)
    - Non-English languages: 中文频道 → chan-a1b2c3d4 (deterministic hash)
    - Mixed content: Gaming 🎮 → gaming-chan-a1b2c3d4 (ASCII part + hash)
    - Regular ASCII: Fireship → fireship (unchanged)
    
    Args:
        name: Original channel name
        max_len: Maximum length of output slug
        
    Returns:
        Normalized slug that's always ASCII-safe and deterministic
    """
    if not name or not name.strip():
        return "unknown"
    
    name = name.strip()
    
    # First, try to extract ASCII content
    ascii_text = (
        unicodedata
        .normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode("utf-8")
    )
    
    # Create ASCII slug from available ASCII characters
    ascii_slug = ascii_text.lower()
    ascii_slug = re.sub(r"[^\w\s-]", "", ascii_slug)
    # Convert both spaces AND hyphens to underscores for consistency
    ascii_slug = re.sub(r"[\s-]+", "_", ascii_slug).strip("_")
    
    # If we have a good ASCII slug (3+ chars), use it
    if ascii_slug and len(ascii_slug) >= 3:
        return ascii_slug[:max_len]
    
    # Fallback: create deterministic hash for non-ASCII content
    # Use first 8 characters of SHA256 for deterministic short hash
    hash_suffix = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
    
    # If we have some ASCII content, combine it with hash
    if ascii_slug:
        combined = f"{ascii_slug}-chan-{hash_suffix}"
        return combined[:max_len]
    
    # Pure non-ASCII case: use chan- prefix with hash
    return f"chan-{hash_suffix}"


class YouTubeContent(BaseModel):
    """The content model for YouTube transcripts with language support."""

    class Config:
        extra = "forbid"

    video_id: str = Field(description="The YouTube video ID (e.g., 'dQw4w9WgXcQ')")
    title: str = Field(description="The title of the YouTube video")
    channel_name: str = Field(description="The name of the YouTube channel")

    upload_date: dt.datetime = Field(description="The date the video was uploaded")
    transcript: List[Dict] = Field(
        description="The transcript of the video, as a list of dictionaries with 'text', 'start', and 'end' keys",
        default_factory=list
    )
    url: str = Field(description="The URL of the YouTube video")
    duration_seconds: int = Field(
        description="The duration of the video in seconds",
        default=0
    )
    language: str = Field(
        description="The transcript language in ISO 639-1 format (e.g., 'en' for English, 'fr' for French)",
        default="en"
    )
    thumbnails: str = Field(description='Image url that serves as a visual preview of a video.')
    view_count: int = Field(description='The view count of a video.')
    # some channels hide this number so it's optional
    description: Optional[str] = Field(description='Description of the video. Some videos may not have descriptions.', default=None)
    like_count: Optional[int] = Field(description='The like count of a video.')
    subscriber_count: Optional[int] = Field(description='Subscriber count of a video.')

    @classmethod
    def to_data_entity(cls, content: "YouTubeContent") -> DataEntity:
        """Converts the YouTubeContent to a DataEntity with normalized channel label."""
        label_value = f"#ytc_c_{normalize_channel_name(content.channel_name)}"
        label = DataLabel(value=label_value)

        entity_timestamp = content.upload_date
        content.upload_date = utils.obfuscate_datetime_to_minute(entity_timestamp)
        content_bytes = content.json(exclude_none=True).encode("utf-8")

        return DataEntity(
            uri=content.url,
            datetime=entity_timestamp,
            source=DataSource.YOUTUBE,
            label=label,
            content=content_bytes,
            content_size_bytes=len(content_bytes),
        )

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "YouTubeContent":
        """Converts a DataEntity to a YouTubeContent."""
        content_str = data_entity.content.decode("utf-8")
        return YouTubeContent.parse_raw(content_str)

    @staticmethod
    def create_channel_label(channel_identifier: str) -> str:
        """Create a label from a channel identifier (slug or raw name)."""
        if channel_identifier.startswith('@'):
            channel_identifier = channel_identifier[1:]
        return f"#ytc_c_{normalize_channel_name(channel_identifier)}"

    @staticmethod
    def parse_channel_label(label_value: str) -> Optional[str]:
        """Parse a label and extract the channel slug."""
        match = re.match(r'^#ytc_c_([a-zA-Z0-9_-]+)$', label_value)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def normalize_label_for_comparison(label_value: str) -> str:
        """Normalize a YouTube channel label for backwards-compatible comparison.

        Converts hyphens to underscores so that labels created with either
        separator will match. This ensures miners with old hyphen-based
        labels won't fail validation.

        Example:
            '#ytc_c_wicked-tuna' -> '#ytc_c_wicked_tuna'
            '#ytc_c_wicked_tuna' -> '#ytc_c_wicked_tuna'
        """
        if label_value and label_value.startswith('#ytc_c_'):
            # Extract the channel slug part after the prefix
            prefix = '#ytc_c_'
            slug = label_value[len(prefix):]
            # Normalize hyphens to underscores in the slug
            normalized_slug = slug.replace('-', '_')
            return f"{prefix}{normalized_slug}"
        return label_value

    @staticmethod
    def labels_match(label1: str, label2: str) -> bool:
        """Check if two YouTube channel labels match, accounting for underscore/hyphen differences.

        This provides backwards compatibility for miners who have labels with
        underscores vs hyphens.
        """
        return YouTubeContent.normalize_label_for_comparison(label1) == YouTubeContent.normalize_label_for_comparison(label2)

    def get_transcript_text(self) -> str:
        """Extract the full transcript text."""
        return " ".join([segment.get('text', '') for segment in self.transcript]) if self.transcript else ""

    def get_transcript_duration(self) -> float:
        """Calculate the total duration of the transcript."""
        last_end = 0.0
        for segment in self.transcript:
            if 'end' in segment:
                last_end = max(last_end, float(segment['end']))
            elif 'start' in segment and 'duration' in segment:
                end_time = float(segment['start']) + float(segment['duration'])
                last_end = max(last_end, end_time)
        return last_end

    def compress_transcript(self, max_segments: int = 100) -> "YouTubeContent":
        """Create a compressed version of the transcript."""
        if not self.transcript or len(self.transcript) <= max_segments:
            return self

        compression_ratio = len(self.transcript) / max_segments
        compressed_transcript = []

        for i in range(0, len(self.transcript), int(compression_ratio)):
            if len(compressed_transcript) >= max_segments:
                break
            compressed_transcript.append(self.transcript[i])

        content_dict = self.dict()
        content_dict['transcript'] = compressed_transcript
        return YouTubeContent(**content_dict)
