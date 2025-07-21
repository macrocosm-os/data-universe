import datetime as dt
import re
from typing import Dict, List, Optional
from pydantic.v1 import BaseModel, Field
from common.data import DataEntity, DataLabel, DataSource


class YouTubeContent(BaseModel):
    """The content model for YouTube transcripts with language support.

    This model standardizes how YouTube transcript data is stored.
    """

    class Config:
        extra = "forbid"

    video_id: str = Field(
        description="The YouTube video ID (e.g., 'dQw4w9WgXcQ')"
    )

    title: str = Field(
        description="The title of the YouTube video"
    )

    channel_id: str = Field(
        description="The YouTube channel ID or normalized identifier"
    )

    channel_name: str = Field(
        description="The name of the YouTube channel"
    )

    upload_date: dt.datetime = Field(
        description="The date the video was uploaded"
    )

    transcript: List[Dict] = Field(
        description="The transcript of the video, as a list of dictionaries with 'text', 'start', and 'end' keys",
        default_factory=list
    )

    url: str = Field(
        description="The URL of the YouTube video"
    )

    duration_seconds: int = Field(
        description="The duration of the video in seconds",
        default=0
    )

    language: str = Field(
        description="The transcript language in ISO 639-1 format (e.g., 'en' for English, 'fr' for French)",
        default="en"
    )

    @classmethod
    def to_data_entity(cls, content: "YouTubeContent") -> DataEntity:
        """Converts the YouTubeContent to a DataEntity with channel-based labels.

        Args:
            content: The YouTubeContent object to convert

        Returns:
            A DataEntity with the channel label format: #ytc_c_{channel_id}
        """
        entity_timestamp = content.upload_date
        content_bytes = content.json(exclude_none=True).encode("utf-8")

        # Use simple channel-based format (no language in label)
        label = DataLabel(value=f"#ytc_c_{content.channel_id}")

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
        """Create a channel-based label for scraping videos from a channel.

        Args:
            channel_identifier: Channel handle (e.g., 'fireship') or channel ID

        Returns:
            Channel label string: #ytc_c_{channel_identifier}
        """
        # Normalize channel identifier (remove @ if present, convert to lowercase)
        if channel_identifier.startswith('@'):
            channel_identifier = channel_identifier[1:]
        channel_identifier = channel_identifier.lower()

        return f"#ytc_c_{channel_identifier}"

    @staticmethod
    def parse_channel_label(label_value: str) -> Optional[str]:
        """Parse a channel label to extract channel identifier.

        Args:
            label_value: Channel label string to parse

        Returns:
            Channel identifier string, or None if invalid
        """
        # Pattern: #ytc_c_{channel_identifier}
        match = re.match(r'^#ytc_c_([a-zA-Z0-9_-]+)$', label_value)
        if match:
            return match.group(1)

        return None

    def get_transcript_text(self) -> str:
        """Extract the full transcript text as a single string."""
        if not self.transcript:
            return ""

        return " ".join([segment.get('text', '') for segment in self.transcript])

    def get_transcript_duration(self) -> float:
        """Calculate the total duration covered by the transcript."""
        if not self.transcript:
            return 0.0

        # Find the last segment with an end time
        last_end = 0.0
        for segment in self.transcript:
            if 'end' in segment:
                last_end = max(last_end, float(segment['end']))
            elif 'start' in segment and 'duration' in segment:
                # Handle segments with start + duration instead of end
                end_time = float(segment['start']) + float(segment['duration'])
                last_end = max(last_end, end_time)

        return last_end

    def compress_transcript(self, max_segments: int = 100) -> "YouTubeContent":
        """Create a compressed version of the transcript to reduce storage size.

        Args:
            max_segments: Maximum number of transcript segments to keep

        Returns:
            New YouTubeContent object with compressed transcript
        """
        if not self.transcript or len(self.transcript) <= max_segments:
            return self

        # Calculate compression ratio
        compression_ratio = len(self.transcript) / max_segments

        compressed_transcript = []
        for i in range(0, len(self.transcript), int(compression_ratio)):
            if len(compressed_transcript) >= max_segments:
                break

            # Take segments at regular intervals
            segment = self.transcript[i]
            compressed_transcript.append(segment)

        # Create new content object with compressed transcript
        content_dict = self.dict()
        content_dict['transcript'] = compressed_transcript

        return YouTubeContent(**content_dict)