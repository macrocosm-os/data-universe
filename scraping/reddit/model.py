import datetime as dt
from enum import Enum
from typing import Optional

# Use v1 for these models to keep serialization consistent.
# Pydantic v2 doesn't include spaces in its serialization.
from pydantic.v1 import BaseModel, Field


from common import constants
from common.data import DataEntity, DataLabel, DataSource
from scraping import utils

# The username used for deleted users.
# This is the value returned by the Apify lite scraper.
# Other scrapers may need to adapt their code to use this value.
DELETED_USER = "[deleted]"


class RedditDataType(str, Enum):
    POST = "post"
    COMMENT = "comment"


class RedditContent(BaseModel):
    """The content model for Reddit data.

    Useful to standardize the representation of Reddit data, that could be scraped from different sources.
    """

    class Config:
        extra = "forbid"

    id: str = Field(description="The unique ID of the post/comment")
    url: str = Field(
        description="URL of the post/comment",
    )
    username: str
    community: str = Field(
        alias="communityName", description="The subreddit. Includes the 'r/' prefix"
    )
    body: str = Field()
    created_at: dt.datetime = Field(alias="createdAt")
    data_type: RedditDataType = Field(alias="dataType")

    # Post-only fields.
    title: Optional[str] = Field(
        description="Title of the post. Empty for comments", default=None
    )

    # Comment-only fields.
    parent_id: Optional[str] = Field(
        description="The ID of the parent comment. Only applicable to comments.",
        alias="parentId",
        default=None,
    )

    @classmethod
    def to_data_entity(cls, content: "RedditContent") -> DataEntity:
        """Converts the RedditContent to a DataEntity."""
        entity_created_at = content.created_at
        content.created_at = utils.obfuscate_datetime_to_minute(entity_created_at)
        content_bytes = content.json(by_alias=True).encode("utf-8")

        return DataEntity(
            uri=content.url,
            datetime=entity_created_at,
            source=DataSource.REDDIT,
            label=DataLabel(
                value=content.community.lower()[: constants.MAX_LABEL_LENGTH]
            ),
            content=content_bytes,
            content_size_bytes=len(content_bytes),
        )

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "RedditContent":
        """Converts a DataEntity to a RedditContent."""

        return RedditContent.parse_raw(data_entity.content.decode("utf-8"))
