import datetime as dt
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
import json
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from common.constants import X_ON_DEMAND_CONTENT_EXPIRATION_DATE
from scraping import utils
from scraping.x.model import XContent


class EnhancedXContent(BaseModel):
    """Enhanced content model for tweets with additional metadata.

    The model helps standardize the data format for tweets, even if they're scraped using different methods,
    and provides additional metadata for API responses.
    """

    class Config:
        extra = "forbid"

    # Basic fields (same as original XContent)
    username: str
    text: str
    url: str
    timestamp: dt.datetime
    tweet_hashtags: List[str] = Field(
        default_factory=list,
        description="A list of hashtags associated with the tweet, in order they appear in the tweet.",
    )

    # Enhanced fields - "user"
    user_id: Optional[str] = None
    user_display_name: Optional[str] = None
    user_verified: Optional[bool] = None
    user_followers_count: Optional[int] = None
    user_following_count: Optional[int] = None

    # Tweet metadata - "tweet"
    tweet_id: Optional[str] = None
    like_count: Optional[int] = None
    retweet_count: Optional[int] = None
    reply_count: Optional[int] = None
    quote_count: Optional[int] = None
    is_retweet: Optional[bool] = None
    is_reply: Optional[bool] = None
    is_quote: Optional[bool] = None

    # Media content - "media"
    media_urls: List[str] = Field(default_factory=list)
    media_types: List[str] = Field(default_factory=list)

    # Additional metadata
    conversation_id: Optional[str] = None
    in_reply_to_user_id: Optional[str] = None


    @classmethod
    def from_apify_response(cls, data: Dict[str, Any]) -> "EnhancedXContent":
        """Construct EnhancedXContent from raw Apify response data."""
        from scraping.x import utils as x_utils

        # Extract basic fields
        text = data.get('text', '')

        # Extract hashtags and symbols
        hashtags = data.get('entities', {}).get('hashtags', [])
        cashtags = data.get('entities', {}).get('symbols', [])

        # Combine hashtags and cashtags into one list and sort them by their first index
        sorted_tags = sorted(hashtags + cashtags, key=lambda x: x['indices'][0])

        # Create a list of formatted tags with prefixes
        tags = ["#" + item['text'] for item in sorted_tags]

        # Get user info
        author = data.get('author', {})
        user_id = author.get('userId', None)
        username = author.get('userName', None)
        display_name = author.get('displayName', None)
        verified = author.get('verified', False)
        followers = author.get('followersCount', None)
        following = author.get('followingCount', None)

        # Tweet metadata
        tweet_id = data.get('id', None)
        url = data.get('url', '')
        like_count = data.get('likeCount', None)
        retweet_count = data.get('retweetCount', None)
        reply_count = data.get('replyCount', None)
        quote_count = data.get('quoteCount', None)
        is_retweet = data.get('isRetweet', False)
        is_reply = data.get('isReply', False)
        is_quote = data.get('isQuote', False)
        conversation_id = data.get('conversationId', None)

        # Reply info
        in_reply_to_user_id = data.get('inReplyToUserId', None)

        # Media content
        media_urls = []
        media_types = []

        # Extract media if present
        if 'media' in data:
            for media_item in data.get('media', []):
                media_url = media_item.get('media_url_https', None)
                if media_url:
                    media_urls.append(media_url)
                    media_types.append(media_item.get('type', 'unknown'))

        # Create timestamp from createdAt
        timestamp = None
        if 'createdAt' in data:
            try:
                timestamp = dt.datetime.strptime(
                    data["createdAt"], "%a %b %d %H:%M:%S %z %Y"
                )
            except ValueError:
                # Try alternative formats if the first one fails
                try:
                    timestamp = dt.datetime.fromisoformat(data["createdAt"])
                except ValueError:
                    timestamp = dt.datetime.now(dt.timezone.utc)

        return cls(
            username=f"@{username}" if username else "",
            text=x_utils.sanitize_scraped_tweet(text),
            url=url,
            timestamp=timestamp,
            tweet_hashtags=tags,
            user_id=user_id,
            user_display_name=display_name,
            user_verified=verified,
            user_followers_count=followers,
            user_following_count=following,
            tweet_id=tweet_id,
            like_count=like_count,
            retweet_count=retweet_count,
            reply_count=reply_count,
            quote_count=quote_count,
            is_retweet=is_retweet,
            is_reply=is_reply,
            is_quote=is_quote,
            media_urls=media_urls,
            media_types=media_types,
            conversation_id=conversation_id,
            in_reply_to_user_id=in_reply_to_user_id
        )

    def to_api_response(self) -> Dict[str, Any]:
        """Convert to a dictionary suitable for API responses."""
        result = {
            'uri': self.url,
            'datetime': self.timestamp.isoformat() if self.timestamp else None,
            'source': 'X',
            'label': self.tweet_hashtags[0] if self.tweet_hashtags else None,
            'text': self.text,
            'user': {
                'username': self.username,
                'display_name': self.user_display_name,
                'id': self.user_id,
                'verified': self.user_verified,
                'followers_count': self.user_followers_count,
                'following_count': self.user_following_count
            },
            'tweet': {
                'id': self.tweet_id,
                'like_count': self.like_count,
                'retweet_count': self.retweet_count,
                'reply_count': self.reply_count,
                'quote_count': self.quote_count,
                'hashtags': self.tweet_hashtags,
                'is_retweet': self.is_retweet,
                'is_reply': self.is_reply,
                'is_quote': self.is_quote,
                'conversation_id': self.conversation_id
            }
        }

        # Add media info if available
        if self.media_urls:
            result['media'] = [
                {'url': url, 'type': type}
                for url, type in zip(self.media_urls, self.media_types)
            ]

        # Add reply info if available
        if self.is_reply and self.in_reply_to_user_id:
            result['tweet']['in_reply_to'] = {
                'user_id': self.in_reply_to_user_id
            }

        return result


    @classmethod
    def to_data_entity(cls, content: "EnhancedXContent", enhanced: bool = False) -> DataEntity:
        """Converts the EnhancedXContent to a DataEntity.
        
        Args:
            content: The EnhancedXContent instance to convert
            enhanced: If True, uses the enhanced API response format.
                            If False, uses the basic XContent format for validation compatibility.
        """
        entity_timestamp = content.timestamp
        obfuscated_timestamp = utils.obfuscate_datetime_to_minute(entity_timestamp)
        
        if enhanced:
            # Use the enhanced API response format
            api_response = content.to_api_response()
            api_response['datetime'] = obfuscated_timestamp.isoformat() if obfuscated_timestamp else None
            content_bytes = json.dumps(api_response, ensure_ascii=False).encode("utf-8")
        else:
            # Use basic XContent format for validation compatibility
            basic_content = XContent(
                username=content.username,
                text=content.text,
                url=content.url,
                timestamp=obfuscated_timestamp,
                tweet_hashtags=content.tweet_hashtags,
                media=content.media_urls if content.media_urls else None
            )
            content_bytes = basic_content.json(exclude_none=True).encode("utf-8")

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
    def to_enhanced_data_entity(cls, content: "EnhancedXContent") -> DataEntity:
        """Converts the EnhancedXContent to a DataEntity with enhanced format.
        
        This is a convenience method that calls to_data_entity with enhanced=True.
        """
        return cls.to_data_entity(content, enhanced=True)
    

    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "EnhancedXContent":
        """Converts a DataEntity to an EnhancedXContent."""
        
        # Decode the content - this should be the new X API format
        content_str = data_entity.content.decode("utf-8")  
        content_dict = json.loads(content_str)
        
        # Extract data from the new API structure
        user_info = content_dict.get("user", {})
        tweet_info = content_dict.get("tweet", {})
        media_info = content_dict.get("media", [])
        
        # Map to EnhancedXContent fields
        username = user_info.get("username")
        if username and not username.startswith("@"):
            username = f"@{username}"
            
        text = content_dict.get("text")
        now = dt.datetime.now(dt.timezone.utc)
        if now <= X_ON_DEMAND_CONTENT_EXPIRATION_DATE:
            if not text:
                # Using 'content' as fallback for compatibility until Aug 25 2025
                text = content_dict.get("content")
        if not text:
            text = "" 
        url = content_dict.get("uri")
        
        # Handle timestamp - could be in content_dict or data_entity
        timestamp = data_entity.datetime
        
        # Extract hashtags from tweet info
        hashtags = tweet_info.get("hashtags", [])
        
        # Extract media URLs and types
        media_urls = []
        media_types = []
        for media_item in media_info:
            media_urls.append(media_item.get("url"))
            media_types.append(media_item.get("type", "unknown"))
        
        return cls(
            username=username,
            text=text,
            url=url,
            timestamp=timestamp,
            tweet_hashtags=hashtags,
            user_id=user_info.get("id"),
            user_display_name=user_info.get("display_name"),
            user_verified=user_info.get("verified"),
            user_followers_count=user_info.get("followers_count"),
            user_following_count=user_info.get("following_count"),
            tweet_id=tweet_info.get("id"),
            like_count=tweet_info.get("like_count"),
            retweet_count=tweet_info.get("retweet_count"),
            reply_count=tweet_info.get("reply_count"),
            quote_count=tweet_info.get("quote_count"),
            is_retweet=tweet_info.get("is_retweet"),
            is_reply=tweet_info.get("is_reply"),
            is_quote=tweet_info.get("is_quote"),
            media_urls=media_urls,
            media_types=media_types,
            conversation_id=tweet_info.get("conversation_id"),
            in_reply_to_user_id=tweet_info.get("in_reply_to", {}).get("user_id") if tweet_info.get("in_reply_to") else None
        )
