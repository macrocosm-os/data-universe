import datetime as dt
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field
import json
import base64

from common import constants
from common.data import DataEntity, DataLabel, DataSource
from scraping import utils


class TumblrDataType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"
    TEXT = "text"


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return obj.isoformat()
        return super().default(obj)


class TumblrContent(BaseModel):
    """The content model for Tumblr image data."""

    class Config:
        extra = "forbid"
        arbitrary_types_allowed = True
        json_encoders = {dt.datetime: lambda v: v.isoformat()}

    timestamp: dt.datetime = Field(description="The date and time the post was published")
    image_bytes: bytes = Field(description="The image data in bytes")
    image_format: str = Field(description='the image format')
    tags: List[str] = Field(description="List of tags associated with the post")
    description: str = Field(description="A short summary or caption of the post")
    post_url: str = Field(description="The full URL of the post")
    creator: str = Field(description="The name of the Tumblr blog creator")

    @classmethod
    def to_data_entity(cls, content: "TumblrContent") -> DataEntity:
        """Converts the TumblrContent to a DataEntity."""
        entity_created_at = content.timestamp
        content_dict = content.dict(exclude={'image_bytes'})
        content_dict['timestamp'] = utils.obfuscate_datetime_to_minute(entity_created_at)

        # Convert image_bytes to base64 string for JSON serialization
        content_dict['image_bytes'] = base64.b64encode(content.image_bytes).decode('utf-8')

        content_bytes = json.dumps(content_dict, cls=DateTimeEncoder).encode("utf-8")

        return DataEntity(
            uri=content.post_url,
            datetime=entity_created_at,
            source=DataSource.TUMBLR,
            label=DataLabel(
                value=content.creator.lower()[: constants.MAX_LABEL_LENGTH]
            ),
            content=content_bytes,
            content_size_bytes=len(content_bytes),
        )

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "TumblrContent":
        """Converts a DataEntity to a TumblrContent."""
        content_dict = json.loads(data_entity.content.decode("utf-8"))

        # Convert base64 string back to bytes
        content_dict['image_bytes'] = base64.b64decode(content_dict['image_bytes'])

        # Convert ISO format string back to datetime
        content_dict['timestamp'] = dt.datetime.fromisoformat(content_dict['timestamp'])

        return TumblrContent(**content_dict)



if __name__ == '__main__':
    # Example usage
    tumblr_data = {
        "timestamp": dt.datetime.now(dt.timezone.utc),
        "image_bytes": b"example_image_bytes",
        "tags": ["Ukraine", "Independence Day", "photo"],
        "description": "A beautiful image from Ukrainian Independence Day celebrations",
        "post_url": "https://example.tumblr.com/post/123456789/example-image-post",
        "creator": "example-blog"
    }

    # Create a TumblrContent instance
    tumblr_content = TumblrContent(**tumblr_data)

    # Convert to DataEntity
    data_entity = TumblrContent.to_data_entity(tumblr_content)

    print("DataEntity created:")
    print(f"URI: {data_entity.uri}")
    print(f"DateTime: {data_entity.datetime}")
    print(f"Source: {data_entity.source}")
    print(f"Label: {data_entity.label.value}")
    print(f"Content Size: {data_entity.content_size_bytes} bytes")

    # Convert back to TumblrContent
    reconstructed_content = TumblrContent.from_data_entity(data_entity)

    print("\nReconstructed TumblrContent:")
    print(reconstructed_content)
    print(f"Original timestamp: {tumblr_content.timestamp}")
    print(f"Reconstructed timestamp: {reconstructed_content.timestamp}")
    print(f"Original image bytes: {tumblr_content.image_bytes}")
    print(f"Reconstructed image bytes: {reconstructed_content.image_bytes}")
    print(f"Image bytes are identical: {tumblr_content.image_bytes == reconstructed_content.image_bytes}")