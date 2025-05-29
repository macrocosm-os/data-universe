import datetime as dt
from typing import Dict, List, Optional
from pydantic.v1 import BaseModel, Field
from common.data import DataEntity, DataLabel, DataSource


class YouTubeContent(BaseModel):
    """The content model for YouTube transcripts.

    This model standardizes how YouTube transcript data is stored,
    regardless of how it was scraped.
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
        description="The YouTube channel ID"
    )

    channel_name: str = Field(
        description="The name of the YouTube channel"
    )

    upload_date: dt.datetime = Field(
        description="The date the video was uploaded"
    )

    transcript: List[Dict] = Field(
        description="The transcript of the video, as a list of dictionaries with 'text', 'start', and 'duration' keys",
        default_factory=list
    )

    url: str = Field(
        description="The URL of the YouTube video"
    )

    language: str = Field(
        description="The language of the transcript",
        default="en"
    )

    duration_seconds: int = Field(
        description="The duration of the video in seconds",
        default=0
    )

    @classmethod
    def to_data_entity(cls, content: "YouTubeContent", original_label: Optional[str] = None) -> DataEntity:
        """Converts the YouTubeContent to a DataEntity.

        Args:
            content: The YouTubeContent object to convert
            original_label: The original label type that was used for scraping (optional)

        Returns:
            A DataEntity with the appropriate label
        """
        entity_timestamp = content.upload_date
        content_bytes = content.json(exclude_none=True).encode("utf-8")

        # Create a DataLabel - ALWAYS use NEW format for output, but check BOTH old and new for input
        if original_label and (original_label.startswith('#youtube_v_') or original_label.startswith('#ytc_v_')):
            # If scraped with a video label, use NEW video label format
            label = DataLabel(value=f"#ytc_v_{content.video_id}")
        else:
            # Default to NEW channel label format
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