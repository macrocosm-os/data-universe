import datetime as dt
from typing import Dict, List, Optional, Any
import json

# Use v1 for these models to keep serialization consistent.
# Pydantic v2 doesn't include spaces in its serialization.
from pydantic.v1 import BaseModel, Field

from common import constants
from common.constants import X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE
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

    # ===== NEW FIELDS =====
    # Static tweet metadata
    language: Optional[str] = None
    in_reply_to_username: Optional[str] = None
    quoted_tweet_id: Optional[str] = None

    # Dynamic engagement metrics
    like_count: Optional[int] = None
    retweet_count: Optional[int] = None
    reply_count: Optional[int] = None
    quote_count: Optional[int] = None
    view_count: Optional[int] = None
    bookmark_count: Optional[int] = None

    # User profile data
    user_blue_verified: Optional[bool] = None
    user_description: Optional[str] = None
    user_location: Optional[str] = None
    profile_image_url: Optional[str] = None
    cover_picture_url: Optional[str] = None
    user_followers_count: Optional[int] = None
    user_following_count: Optional[int] = None

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
    def is_nested_format(cls, data_entity: DataEntity) -> bool:
        """Check if a DataEntity contains legacy nested format content."""
        try:
            content_str = data_entity.content.decode("utf-8")
            content_dict = json.loads(content_str)
            return "user" in content_dict or "tweet" in content_dict
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False
    
    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "XContent":
        """Converts a DataEntity to an XContent with backward compatibility for nested format."""
        content_str = data_entity.content.decode("utf-8")
        
        # Check if this is the legacy nested format and we're still in compatibility period
        if cls.is_nested_format(data_entity) and dt.datetime.now(dt.timezone.utc) < X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE:
            # Handle legacy nested format
            return cls._from_enhanced_nested_format(data_entity)
        else:
            return XContent.parse_raw(content_str)
    
    @classmethod
    def _from_enhanced_nested_format(cls, data_entity: "DataEntity") -> "XContent":
        """Convert from legacy EnhancedXContent nested format to unified XContent format."""
        # Parse the content from the data entity
        content_str = data_entity.content.decode("utf-8")
        content_dict = json.loads(content_str)
        
        base_fields = {
            "username": content_dict.get("username"),
            "text": content_dict.get("text"),
            "url": content_dict.get("url") or data_entity.uri,
            "timestamp": data_entity.datetime,  # Use precise timestamp from DataEntity (to_data_entity will obfuscate for content)
            "tweet_hashtags": content_dict.get("tweet_hashtags", []),
            "media": cls._extract_media_urls(content_dict.get("media"))
        }
        
        # Extract user fields from nested user object
        user_dict = content_dict.get("user", {})
        if user_dict:
            base_fields.update({
                "user_id": user_dict.get("id"),
                "user_display_name": user_dict.get("display_name"),
                "user_verified": user_dict.get("verified"),
                "user_followers_count": user_dict.get("followers_count"),
                "user_following_count": user_dict.get("following_count"),
                "username": user_dict.get("username", base_fields["username"])
            })
        
        # Extract tweet fields from nested tweet object
        tweet_dict = content_dict.get("tweet", {})
        if tweet_dict:
            base_fields.update({
                "tweet_id": tweet_dict.get("id"),
                "is_reply": tweet_dict.get("is_reply"),
                "is_quote": tweet_dict.get("is_quote"),
                "conversation_id": tweet_dict.get("conversation_id"),
                "like_count": tweet_dict.get("like_count"),
                "retweet_count": tweet_dict.get("retweet_count"),
                "reply_count": tweet_dict.get("reply_count"),
                "quote_count": tweet_dict.get("quote_count")
            })
            
            # Handle in_reply_to nested object
            in_reply_to = tweet_dict.get("in_reply_to")
            if in_reply_to and isinstance(in_reply_to, dict):
                base_fields["in_reply_to_user_id"] = in_reply_to.get("user_id")
        
        # Handle direct top-level fields that might exist in legacy format
        for field in ["tweet_id", "user_id", "like_count", "retweet_count", "reply_count", "quote_count", 
                     "is_reply", "is_quote", "conversation_id", "user_display_name", "user_verified",
                     "user_followers_count", "user_following_count"]:
            if field in content_dict and base_fields.get(field) is None:
                base_fields[field] = content_dict[field]
        
        return cls(**base_fields)
    
    @classmethod
    def _extract_media_urls(cls, media_data) -> Optional[List[str]]:
        """Extract media URLs from nested format media data."""
        if not media_data:
            return None
            
        media_urls = []
        if isinstance(media_data, list):
            for item in media_data:
                if isinstance(item, dict) and "url" in item:
                    # Extract URL from {"url": "...", "type": "..."} format
                    media_urls.append(item["url"])
                elif isinstance(item, str):
                    # Already a URL string
                    media_urls.append(item)
        
        return media_urls if media_urls else None