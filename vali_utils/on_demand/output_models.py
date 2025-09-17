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
    username: str
    user_id: str
    user_display_name: str
    user_verified: bool
    user_blue_verified: Optional[bool] = None
    user_description: Optional[str] = None
    user_location: Optional[str] = None
    profile_image_url: Optional[str] = None
    cover_picture_url: Optional[str] = None
    user_followers_count: int
    user_following_count: int


class TweetInfo(BaseModel):
    """Tweet metadata for X posts."""
    like_count: Optional[int] = None
    retweet_count: Optional[int] = None
    reply_count: Optional[int] = None
    quote_count: Optional[int] = None
    view_count: Optional[int] = None
    bookmark_count: Optional[int] = None
    language: Optional[str] = None
    in_reply_to_username: Optional[str] = None
    quoted_tweet_id: Optional[str] = None
    conversation_id: Optional[str] = None
    in_reply_to_user_id: Optional[str] = None
    tweet_id: str
    is_reply: bool
    is_quote: bool


class MediaItem(BaseModel):
    """Media item for X posts."""
    url: str
    type: str


class TranscriptSegment(BaseModel):
    """Transcript segment for YouTube videos."""
    text: str
    start: float
    end: float


class XOrganicOutput(BaseModel):
    """Output model for X (Twitter) content matching the example JSON structure."""
    
    text: str
    datetime: str
    uri: str
    source: str
    tweet: TweetInfo
    content_size_bytes: int
    user: UserInfo
    label: Optional[str] = None
    
    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "XOrganicOutput":
        """Create XOrganicOutput from DataEntity"""
        from scraping.x.model import XContent
        
        x_content = XContent.from_data_entity(data_entity)
        
        # Create nested user and tweet structures
        user_info = UserInfo(
            username=x_content.username,
            user_id=x_content.user_id or "",
            user_display_name=x_content.user_display_name or "",
            user_verified=x_content.user_verified or False,
            user_blue_verified=x_content.user_blue_verified,
            user_description=x_content.user_description,
            user_location=x_content.user_location,
            profile_image_url=x_content.profile_image_url,
            cover_picture_url=x_content.cover_picture_url,
            user_followers_count=int(x_content.user_followers_count or 0),
            user_following_count=int(x_content.user_following_count or 0)
        )
        
        tweet_info = TweetInfo(
            like_count=x_content.like_count,
            retweet_count=x_content.retweet_count,
            reply_count=x_content.reply_count,
            quote_count=x_content.quote_count,
            view_count=x_content.view_count,
            bookmark_count=x_content.bookmark_count,
            language=x_content.language,
            in_reply_to_username=x_content.in_reply_to_username,
            quoted_tweet_id=x_content.quoted_tweet_id,
            conversation_id=x_content.conversation_id,
            in_reply_to_user_id=x_content.in_reply_to_user_id,
            tweet_id=x_content.tweet_id or "",
            is_reply=x_content.is_reply or False,
            is_quote=x_content.is_quote or False
        )
        
        return cls(
            text=x_content.text or "",
            datetime=data_entity.datetime.isoformat() if data_entity.datetime else "",
            uri=data_entity.uri or "",
            source="X",
            tweet=tweet_info,
            content_size_bytes=int(data_entity.content_size_bytes or 0),
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
            datetime=data_entity.datetime.isoformat(),
            source="REDDIT",
            label=data_entity.label.value if data_entity.label else None,
            content_size_bytes=data_entity.content_size_bytes,
            id=reddit_content.id,
            url=reddit_content.url,
            username=reddit_content.username,
            communityName=reddit_content.community,
            body=reddit_content.body,
            createdAt=reddit_content.created_at.isoformat(),
            dataType=reddit_content.data_type,
            title=reddit_content.title,
            parentId=reddit_content.parent_id,
            media=reddit_content.media,
            is_nsfw=reddit_content.is_nsfw,
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
        
        # Convert transcript data to TranscriptSegment objects
        transcript_segments = []
        if youtube_content.transcript:
            for segment_dict in youtube_content.transcript:
                if isinstance(segment_dict, dict) and 'text' in segment_dict:
                    transcript_segments.append(TranscriptSegment(
                        text=segment_dict.get('text'),
                        start=float(segment_dict.get('start')),
                        end=float(segment_dict.get('end'))
                    ))
        
        return cls(
            uri=data_entity.uri or "",
            datetime=data_entity.datetime.isoformat(),
            source="YOUTUBE",
            label=data_entity.label.value if data_entity.label else None,
            content_size_bytes=data_entity.content_size_bytes,
            video_id=youtube_content.video_id,
            title=youtube_content.title,
            channel_name=youtube_content.channel_name,
            upload_date=youtube_content.upload_date.isoformat(),
            transcript=transcript_segments,
            url=youtube_content.url,
            duration_seconds=youtube_content.duration_seconds,
            language=youtube_content.language,
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
        return XOrganicOutput.from_data_entity(data_entity).model_dump()
    elif source.upper() == "REDDIT":
        return RedditOrganicOutput.from_data_entity(data_entity).model_dump()
    elif source.upper() == "YOUTUBE":
        return YouTubeOrganicOutput.from_data_entity(data_entity).model_dump()
    else:
        raise ValueError(f"Unknown source: {source}")