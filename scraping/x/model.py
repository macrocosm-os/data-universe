import datetime as dt
import json
from typing import Any, Dict, List, Optional

# Use v1 for these models to keep serialization consistent.
# Pydantic v2 doesn't include spaces in its serialization.
from pydantic.v1 import BaseModel, Field

from common import constants
from common.constants import X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE
from common.data import DataEntity, DataLabel, DataSource
from scraping import utils
from vali_utils.on_demand import utils as on_demand_utils


class XContent(BaseModel):
    """The content model for tweets.

    The model helps standardize the data format for tweets, even if they're scraped using different methods.
    """

    class Config:
        extra = "forbid"

    # model_config should NOT be set by Miners.
    # In the near future, Validators will penalized Miners who set this field.
    model_config: dict[str, str] = Field(default=None)

    username: str
    text: str
    url: str
    timestamp: dt.datetime
    tweet_hashtags: list[str] = Field(
        default_factory=list,
        description="A list of hashtags associated with the tweet, in order they appear in the tweet. Note: it's critical this ordering is respected as the first tag is used as the DataLabel for the index.",
    )
    media: list[str] | None = Field(
        default=None,
        description="A list of media URLs associated with the tweet. Can be None if no media is present.",
    )

    # Enhanced fields
    user_id: str | None = None
    user_display_name: str | None = None
    user_verified: bool | None = None

    # Non-dynamic tweet metadata
    tweet_id: str | None = None
    is_reply: bool | None = None
    is_quote: bool | None = None

    # Additional metadata
    conversation_id: str | None = None
    in_reply_to_user_id: str | None = None

    # ===== NEW FIELDS =====
    # Static tweet metadata
    language: str | None = None
    in_reply_to_username: str | None = None
    quoted_tweet_id: str | None = None

    # Dynamic engagement metrics
    like_count: int | None = None
    retweet_count: int | None = None
    reply_count: int | None = None
    quote_count: int | None = None
    view_count: int | None = None
    bookmark_count: int | None = None

    # User profile data
    user_blue_verified: bool | None = None
    user_description: str | None = None
    user_location: str | None = None
    profile_image_url: str | None = None
    cover_picture_url: str | None = None
    user_followers_count: int | None = None
    user_following_count: int | None = None

    # Scrape tracking
    scraped_at: dt.datetime | None = None

    @classmethod
    def to_data_entity(cls, content: "XContent") -> DataEntity:
        """Converts the XContent to a DataEntity."""
        entity_timestamp = content.timestamp
        content.timestamp = utils.obfuscate_datetime_to_minute(entity_timestamp)

        if content.scraped_at is not None:
            content.scraped_at = utils.obfuscate_datetime_to_minute(content.scraped_at)

        content_bytes = content.json(exclude_none=True).encode("utf-8")

        return DataEntity(
            uri=content.url,
            datetime=entity_timestamp,
            source=DataSource.X,
            label=(
                DataLabel(value=content.tweet_hashtags[0].lower()[: constants.MAX_LABEL_LENGTH])
                if content.tweet_hashtags
                else None
            ),
            content=content_bytes,
            content_size_bytes=len(content_bytes),
        )

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "XContent":
        """Converts a DataEntity to an XContent with backward compatibility for nested format."""
        content_str = data_entity.content.decode("utf-8")

        # Check if this is the legacy nested format and we're still in compatibility period
        if (
            on_demand_utils.is_nested_format(data_entity)
            and dt.datetime.now(dt.timezone.utc) < X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE
        ):
            # Handle legacy nested format
            base_fields = on_demand_utils.from_enhanced_nested_format(data_entity)
            return cls(**base_fields)
        else:
            return XContent.parse_raw(content_str)
