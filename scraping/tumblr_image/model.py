import datetime as dt
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field
import json

from common import constants
from common.data import DataEntity, DataLabel, DataSource
from scraping import utils


class TumblrDataType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"
    TEXT = "text"


class TumblrContent(BaseModel):
    """The content model for Tumblr image data."""

    class Config:
        extra = "forbid"

    id: int = Field(description="The unique ID of the post")
    url: str = Field(description="URL of the post")
    blog_name: str = Field(alias="blogName", description="The name of the Tumblr blog")
    post_url: str = Field(alias="postUrl", description="The full URL of the post")
    type: TumblrDataType = Field(description="The type of the Tumblr post")
    date: dt.datetime = Field(description="The date and time the post was published")
    tags: List[str] = Field(description="List of tags associated with the post")
    summary: str = Field(description="A short summary or caption of the post")

    # Image-specific fields
    image_url: str = Field(alias="imageUrl", description="URL of the image")
    image_width: int = Field(alias="imageWidth", description="Width of the image")
    image_height: int = Field(alias="imageHeight", description="Height of the image")

    @classmethod
    def to_data_entity(cls, content: "TumblrContent") -> DataEntity:
        """Converts the TumblrContent to a DataEntity."""
        entity_created_at = content.date
        content.date = utils.obfuscate_datetime_to_minute(entity_created_at)
        content_bytes = content.json(by_alias=True).encode("utf-8")

        return DataEntity(
            uri=content.post_url,
            datetime=entity_created_at,
            source=DataSource.TUMBLR,
            label=DataLabel(
                value=content.blog_name.lower()[: constants.MAX_LABEL_LENGTH]
            ),
            content=content_bytes,
            content_size_bytes=len(content_bytes),
        )

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "TumblrContent":
        """Converts a DataEntity to a TumblrContent."""
        return TumblrContent.parse_raw(data_entity.content.decode("utf-8"))


# # Example usage
# tumblr_data = {
#     "id": 3,
#     "url": "https://example.tumblr.com/post/123456789",
#     "blogName": "example-blog",
#     "postUrl": "https://example.tumblr.com/post/123456789/example-image-post",
#     "type": "image",
#     "date": "2024-08-24T17:53:01+00:00",
#     "tags": ["Ukraine", "Independence Day", "photo"],
#     "summary": "A beautiful image from Ukrainian Independence Day celebrations",
#     "imageUrl": "https://64.media.tumblr.com/123456789abcdef/s1080x1080/image.jpg",
#     "imageWidth": 1080,
#     "imageHeight": 1080
# }
#
# # Create a TumblrContent instance
# tumblr_content = TumblrContent(**tumblr_data)
#
# # Convert to DataEntity
# data_entity = TumblrContent.to_data_entity(tumblr_content)
#
# print("DataEntity created:")
# print(f"URI: {data_entity.uri}")
# print(f"DateTime: {data_entity.datetime}")
# print(f"Source: {data_entity.source}")
# print(f"Label: {data_entity.label.value}")
# print(f"Content Size: {data_entity.content_size_bytes} bytes")
#
# # Convert back to TumblrContent
# reconstructed_content = TumblrContent.from_data_entity(data_entity)
#
# print("\nReconstructed TumblrContent:")
# # print(json.dumps(reconstructed_content.dict(by_alias=True), indent=2))
# print(reconstructed_content )