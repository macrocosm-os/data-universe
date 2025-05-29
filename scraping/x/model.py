import datetime as dt
import json
from typing import Dict, List, Optional
# Use v1 for these models to keep serialization consistent.
# Pydantic v2 doesn't include spaces in its serialization.
from pydantic.v1 import BaseModel, Field

from common import constants
from common.data import DataEntity, DataLabel, DataSource
from scraping import utils


class XContent(BaseModel):
    """The content model for tweets.

    The model helps standardize the data format for tweets, even if they're scraped using different methods.
    """

    class Config:
        extra = "forbid"

    # model_config should NOT be set by Miners.
    # In the near future, Validators will penalized Miners who set this field.
    model_config: Dict[str, str] = Field(default=None)

    username: str
    text: str
    url: str
    timestamp: dt.datetime
    tweet_hashtags: List[str] = Field(
        default_factory=list,
        description="A list of hashtags associated with the tweet, in order they appear in the tweet. Note: it's critical this ordering is respected as the first tag is used as the DataLabel for the index.",
    )
    media: Optional[List[str]] = Field(
        default=None,
        description="A list of media URLs associated with the tweet. Can be None if no media is present.",
    )

    # Enhanced fields
    user_id: Optional[str] = None
    user_display_name: Optional[str] = None
    user_verified: Optional[bool] = None

    # Non-dynamic tweet metadata
    tweet_id: Optional[str] = None
    is_reply: Optional[bool] = None
    is_quote: Optional[bool] = None

    # Additional metadata
    conversation_id: Optional[str] = None
    in_reply_to_user_id: Optional[str] = None

    @classmethod
    def to_data_entity(cls, content: "XContent") -> DataEntity:
        """Converts the XContent to a DataEntity."""
        entity_timestamp = content.timestamp
        content.timestamp = utils.obfuscate_datetime_to_minute(entity_timestamp)
        content_bytes = content.json(exclude_none=True).encode("utf-8")

        return DataEntity(
            uri=content.url,
            datetime=entity_timestamp,
            source=DataSource.X,
            label=(
                DataLabel(
                    value=content.tweet_hashtags[0].lower()[
                        : constants.MAX_LABEL_LENGTH
                    ]
                )
                if content.tweet_hashtags
                else None
            ),
            content=content_bytes,
            content_size_bytes=len(content_bytes),
        )

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "XContent":
        """Converts a DataEntity to an XContent."""
        content_str = data_entity.content.decode("utf-8")
        return XContent.parse_raw(content_str)