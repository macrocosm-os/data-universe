import datetime as dt
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field

from common.data import DataEntity, DataLabel, DataSource


class RedditDataType(str, Enum):
    POST = "post"
    COMMENT = "comment"


class RedditContent(BaseModel):
    """The content model for Reddit data.

    Useful to standardize the representation of Reddit data, that could be scraped from different sources.
    """

    id: str = Field(description="The unique ID of the post/comment")
    url: str = Field(
        description="URL of the post/comment",
    )
    username: str
    community: str = Field(
        alias="communityName", description="The subreddit. Includes the 'r/' prefix"
    )
    body: str = Field()
    upvotes: int = Field(alias="upVotes", ge=0)
    created_at: dt.datetime = Field(alias="createdAt")
    data_type: RedditDataType = Field(alias="dataType")

    # Post-only fields.
    title: Optional[str] = Field(
        description="Title of the post. Empty for comments", default=None
    )
    number_of_comments: Optional[int] = Field(
        alias="numberOfComments", ge=0, default=None
    )

    # Comment-only fields.
    parent_id: Optional[str] = Field(
        description="The ID of the parent comment. Only applicable to comments.",
        alias="parentId",
        default=None,
    )
    number_of_replies: Optional[int] = Field(
        ge=0, alias="numberOfreplies", default=None
    )

    def to_data_entity(self) -> DataEntity:
        """Converts the RedditContent to a DataEntity."""

        content_bytes = self.json(by_alias=True).encode("utf-8")
        return DataEntity(
            uri=self.url,
            datetime=self.created_at,
            source=DataSource.REDDIT,
            label=DataLabel(value=self.community),
            content=content_bytes,
            content_size_bytes=len(content_bytes),
        )

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "RedditContent":
        """Converts a DataEntity to a RedditContent."""

        return RedditContent.parse_raw(data_entity.content.decode("utf-8"))

    def is_equivalent_to(self, other: "RedditContent") -> bool:
        """Returns whether this content is equivalent to another content, for the purposes of data correctness.

        This check excludes dynamic fields that may have changed between data scraping and validation.
        """

        if not other:
            return False

        return (
            other.id == self.id
            and other.url == self.url
            and other.username == self.username
            and other.community == self.community
            and other.body == self.body
            and other.created_at == self.created_at
            and other.data_type == self.data_type
            and other.title == self.title
            and other.parent_id == self.parent_id
            and other.url == self.url
        )
