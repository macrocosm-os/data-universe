"""
Output models for organic query responses.

These Pydantic models define the structured output format for each data source
in organic query responses, matching the Go struct definitions exactly.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Union, Dict
import json
from common.data import DataEntity, DataSource


class UserInfo(BaseModel):
    """User information for X posts."""
    display_name: str
    followers_count: float
    verified: bool
    id: str
    following_count: float
    username: str


class TweetInfo(BaseModel):
    """Tweet metadata for X posts."""
    quote_count: float
    id: str
    retweet_count: float
    like_count: float
    is_reply: bool
    hashtags: List[str]
    conversation_id: str
    is_quote: bool
    in_reply_to: Optional[dict] = None
    reply_count: float
    is_retweet: bool


class MediaItem(BaseModel):
    """Media item for X posts."""
    url: str
    type: str


class TranscriptSegment(BaseModel):
    """Transcript segment for YouTube videos."""
    text: str
    start: float
    duration: float


class XOrganicOutput(BaseModel):
    """Output model for X (Twitter) content matching the example JSON structure."""
    
    text: str
    datetime: str
    uri: str
    source: str
    tweet: TweetInfo
    content_size_bytes: float
    user: UserInfo
    label: Optional[str] = None
    
    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "XOrganicOutput":
        """Create XOrganicOutput from DataEntity"""
        from scraping.x.model import XContent
        
        x_content = XContent.from_data_entity(data_entity)
        
        # Create nested user and tweet structures
        user_info = UserInfo(
            display_name=x_content.user_display_name or "",
            followers_count=float(x_content.user_followers_count or 0),
            verified=x_content.user_verified or False,
            id=x_content.user_id or "",
            following_count=float(x_content.user_following_count or 0),
            username=x_content.username or ""
        )
        
        tweet_info = TweetInfo(
            quote_count=float(x_content.quote_count or 0),
            id=x_content.tweet_id or "",
            retweet_count=float(x_content.retweet_count or 0),
            like_count=float(x_content.like_count or 0),
            is_reply=x_content.is_reply or False,
            hashtags=x_content.tweet_hashtags or [],
            conversation_id=x_content.conversation_id or "",
            is_quote=x_content.is_quote or False,
            in_reply_to={"user_id": x_content.in_reply_to_user_id} if x_content.in_reply_to_user_id else None,
            reply_count=float(x_content.reply_count or 0),
            is_retweet=getattr(x_content, 'is_retweet', False)
        )
        
        return cls(
            text=x_content.text or "",
            datetime=data_entity.datetime.isoformat() if data_entity.datetime else "",
            uri=data_entity.uri or "",
            source="X",
            tweet=tweet_info,
            content_size_bytes=float(data_entity.content_size_bytes or 0),
            user=user_info,
            label=data_entity.label.value if data_entity.label else None
        )


class RedditOrganicOutput(BaseModel):
    """Output model for Reddit content matching Go RedditPost struct."""
    
    uri: str
    datetime: str
    source: str
    label: Optional[str] = None
    content_size_bytes: int
    id: str
    url: str
    username: str
    communityName: str
    body: str
    createdAt: str
    dataType: str
    title: Optional[str] = None
    parentId: Optional[str] = None
    media: Optional[List[str]] = None
    is_nsfw: bool
    
    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "RedditOrganicOutput":
        """Create RedditOrganicOutput from DataEntity"""
        # Import here to avoid circular imports
        from scraping.reddit.model import RedditContent
        
        reddit_content = RedditContent.from_data_entity(data_entity)
        
        return cls(
            uri=data_entity.uri or "",
            datetime=data_entity.datetime.isoformat() if data_entity.datetime else "",
            source="Reddit",
            label=data_entity.label.value if data_entity.label else None,
            content_size_bytes=data_entity.content_size_bytes or 0,
            id=getattr(reddit_content, 'post_id', '') or "",
            url=data_entity.uri or "",
            username=reddit_content.username or "",
            communityName=reddit_content.communityName or "",
            body=reddit_content.body or "",
            createdAt=data_entity.datetime.isoformat() if data_entity.datetime else "",
            dataType=getattr(reddit_content, 'data_type', '') or "post",
            title=reddit_content.title,
            parentId=getattr(reddit_content, 'parent_id', None),
            media=getattr(reddit_content, 'media_urls', None),
            is_nsfw=getattr(reddit_content, 'is_nsfw', False)
        )


class YouTubeOrganicOutput(BaseModel):
    """Output model for YouTube content matching Go YouTubePost struct."""
    
    uri: str
    datetime: str
    source: str
    label: Optional[str] = None
    content_size_bytes: int
    video_id: str
    title: str
    channel_name: str
    upload_date: str
    transcript: List[TranscriptSegment]
    url: str
    duration_seconds: int
    language: str
    
    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "YouTubeOrganicOutput":
        """Create YouTubeOrganicOutput from DataEntity"""
        # Import here to avoid circular imports
        from scraping.youtube.model import YouTubeContent
        
        youtube_content = YouTubeContent.from_data_entity(data_entity)
        
        return cls(
            uri=data_entity.uri or "",
            datetime=data_entity.datetime.isoformat() if data_entity.datetime else "",
            source="YouTube",
            label=data_entity.label.value if data_entity.label else None,
            content_size_bytes=data_entity.content_size_bytes or 0,
            video_id=getattr(youtube_content, 'video_id', '') or "",
            title=youtube_content.title or "",
            channel_name=youtube_content.channel_name or "",
            upload_date=data_entity.datetime.isoformat() if data_entity.datetime else "",
            transcript=getattr(youtube_content, 'transcript', []) or [],
            url=data_entity.uri or "",
            duration_seconds=getattr(youtube_content, 'duration_seconds', 0) or 0,
            language=getattr(youtube_content, 'language', '') or ""
        )


OrganicOutput = Union[XOrganicOutput, RedditOrganicOutput, YouTubeOrganicOutput]


def create_organic_output_dict(data_entity: DataEntity) -> Dict:
    """
    Create output dictionary using source-specific Pydantic models.
    
    Args:
        data_entity: DataEntity to convert to output format
        
    Returns:
        Dictionary representation of the appropriate output model
    """
    source = DataSource(data_entity.source).name
    
    if source.upper() == "X":
        return XOrganicOutput.from_data_entity(data_entity).dict()
    elif source.upper() == "REDDIT":
        return RedditOrganicOutput.from_data_entity(data_entity).dict()
    elif source.upper() == "YOUTUBE":
        return YouTubeOrganicOutput.from_data_entity(data_entity).dict()
    else:
        raise ValueError(f"Unknown source: {source}")