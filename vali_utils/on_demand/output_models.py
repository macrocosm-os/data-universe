"""
Output models for organic query responses.

These Pydantic models define the structured output format for each data source
in organic query responses, matching the Go struct definitions exactly.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Union, Dict
import json
import datetime as dt
import bittensor as bt
from common.data import DataEntity, DataSource
from common.constants import ENHANCED_METADATA_REQUIRED_DATE


class UserInfo(BaseModel):
    """User information for X posts."""
    # Required fields
    display_name: str
    followers_count: int
    verified: bool
    id: str
    following_count: int
    username: str
    # Optional fields
    user_blue_verified: Optional[bool] = None
    user_description: Optional[str] = None
    user_location: Optional[str] = None
    profile_image_url: Optional[str] = None
    cover_picture_url: Optional[str] = None


class TweetInfo(BaseModel):
    """Tweet metadata for X posts."""
    # Required fields
    quote_count: int
    id: str
    retweet_count: int
    like_count: int
    is_reply: bool
    hashtags: List[str]
    conversation_id: str
    is_quote: bool
    reply_count: int
    is_retweet: bool = False
    # Optional fields until ENHANCED_METADATA_REQUIRED_DATE
    view_count: Optional[int] = None
    bookmark_count: Optional[int] = None
    language: Optional[str] = None
    in_reply_to_username: Optional[str] = None
    quoted_tweet_id: Optional[str] = None
    in_reply_to_user_id: Optional[str] = None
    in_reply_to: Optional[dict] = None


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
            id=x_content.user_id or "",
            display_name=x_content.user_display_name or "",
            verified=x_content.user_verified or False,
            user_blue_verified=x_content.user_blue_verified,
            user_description=x_content.user_description,
            user_location=x_content.user_location,
            profile_image_url=x_content.profile_image_url,
            cover_picture_url=x_content.cover_picture_url,
            followers_count=int(x_content.user_followers_count or 0),
            following_count=int(x_content.user_following_count or 0)
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
            id=x_content.tweet_id or "",
            is_reply=x_content.is_reply or False,
            is_quote=x_content.is_quote or False,
            hashtags=x_content.tweet_hashtags or []
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
    score: Optional[int] = None
    upvote_ratio: Optional[float] = None
    num_comments: Optional[int] = None
    
    @classmethod
    def from_data_entity(cls, data_entity: DataEntity) -> "RedditOrganicOutput":
        """Create RedditOrganicOutput from DataEntity"""
        # Import here to avoid circular imports
        from scraping.reddit.model import RedditContent
        
        reddit_content = RedditContent.from_data_entity(data_entity)
        
        return cls(
            uri=data_entity.uri,
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
            score=reddit_content.score,
            upvote_ratio=reddit_content.upvote_ratio,
            num_comments=reddit_content.num_comments,
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


def validate_metadata_completeness(data_entity: DataEntity) -> tuple[bool, List[str]]:
    """
    Validate that a DataEntity has all required metadata fields for its output model.
    Uses the Pydantic output models to determine required fields automatically.
    
    Args:
        data_entity: DataEntity to validate
        
    Returns:
        Tuple of (is_valid, missing_fields) where:
        - is_valid: True if all required fields are present
        - missing_fields: List of missing field names (empty if is_valid=True)
    """
    try:
        source = DataSource(data_entity.source).name.upper()
        
        if source == "X":
            return _validate_x_metadata_completeness(data_entity)
        elif source == "REDDIT":
            return _validate_reddit_metadata_completeness(data_entity)
        elif source == "YOUTUBE":
            return _validate_youtube_metadata_completeness(data_entity)
        else:
            bt.logging.warning(f"Unknown source for metadata validation: {source}")
            return False, [f"Unknown source: {source}"]
            
    except Exception as e:
        bt.logging.error(f"Error validating metadata completeness: {str(e)}")
        return False, [f"Validation error: {str(e)}"]


def _validate_x_metadata_completeness(data_entity: DataEntity) -> tuple[bool, List[str]]:
    """Validate X content metadata completeness using XOrganicOutput model."""
    try:
        from scraping.x.model import XContent
        x_content = XContent.from_data_entity(data_entity)
        missing_fields = []
        
        now = dt.datetime.now(dt.timezone.utc)
        enhanced_validation_required = now >= ENHANCED_METADATA_REQUIRED_DATE
        
        # Core XOrganicOutput fields (required immediately)
        core_base_required_fields = [
            ('text', x_content.text),
        ]
        
        # Core UserInfo fields (required immediately)
        core_user_required_fields = [
            ('display_name', x_content.user_display_name),
            ('followers_count', x_content.user_followers_count),
            ('verified', x_content.user_verified),
            ('id', x_content.user_id),
            ('following_count', x_content.user_following_count),
            ('username', x_content.username),
        ]
        
        # Core TweetInfo fields (required immediately)
        core_tweet_required_fields = [
            ('quote_count', x_content.quote_count),
            ('id', x_content.tweet_id),
            ('retweet_count', x_content.retweet_count),
            ('like_count', x_content.like_count),
            ('is_reply', x_content.is_reply),
            ('hashtags', x_content.tweet_hashtags),
            ('conversation_id', x_content.conversation_id),
            ('is_quote', x_content.is_quote),
            ('reply_count', x_content.reply_count),
            ('is_retweet', getattr(x_content, 'is_retweet', False)),
        ]
        
        # Optional fields that become required after the deadline
        optional_fields_after_deadline = [
            ('user.user_blue_verified', x_content.user_blue_verified),
            ('user.user_description', x_content.user_description),
            ('user.user_location', x_content.user_location),
            ('user.profile_image_url', x_content.profile_image_url),
            ('user.cover_picture_url', x_content.cover_picture_url),
            ('tweet.view_count', x_content.view_count),
            ('tweet.bookmark_count', x_content.bookmark_count),
            ('tweet.language', x_content.language),
            ('tweet.in_reply_to_username', x_content.in_reply_to_username),
            ('tweet.quoted_tweet_id', x_content.quoted_tweet_id),
            ('tweet.in_reply_to_user_id', x_content.in_reply_to_user_id),
        ]
        
        # Check core base required fields (always required)
        for field_name, field_value in core_base_required_fields:
            if field_value is None:
                missing_fields.append(field_name)
        
        # Check core user required fields (always required)
        for field_name, field_value in core_user_required_fields:
            if field_value is None:
                missing_fields.append(f"user.{field_name}")
        
        # Check core tweet required fields (always required)
        for field_name, field_value in core_tweet_required_fields:
            if field_value is None:
                missing_fields.append(f"tweet.{field_name}")
        
        # Check optional fields only if deadline has passed
        if enhanced_validation_required:
            for field_name, field_value in optional_fields_after_deadline:
                if field_value is None:
                    missing_fields.append(field_name)
        
        if missing_fields:
            if enhanced_validation_required:
                bt.logging.debug(f"X metadata validation failed (enhanced validation required). Missing fields: {missing_fields}")
            else:
                bt.logging.debug(f"X metadata validation failed (core validation only). Missing fields: {missing_fields}")
            return False, missing_fields
        
        return True, []
        
    except Exception as e:
        bt.logging.error(f"Error validating X metadata: {str(e)}")
        return False, [f"X validation error: {str(e)}"]


def _validate_reddit_metadata_completeness(data_entity: DataEntity) -> tuple[bool, List[str]]:
    """Validate Reddit content metadata completeness using RedditOrganicOutput model."""
    try:
        from scraping.reddit.model import RedditContent
        reddit_content = RedditContent.from_data_entity(data_entity)
        missing_fields = []
        
        now = dt.datetime.now(dt.timezone.utc)
        enhanced_validation_required = now >= ENHANCED_METADATA_REQUIRED_DATE
        
        # Always required fields (core fields that have been required for a while)
        core_required_fields = [
            ('id', reddit_content.id),
            ('url', reddit_content.url),
            ('username', reddit_content.username),
            ('communityName', reddit_content.community),
            ('body', reddit_content.body),
            ('createdAt', reddit_content.created_at),
            ('dataType', reddit_content.data_type),
            ('is_nsfw', reddit_content.is_nsfw),
        ]
        
        # Enhanced fields (required after ENHANCED_METADATA_REQUIRED_DATE)
        enhanced_required_fields = [
            ('score', reddit_content.score),
            ('upvote_ratio', reddit_content.upvote_ratio),
            ('num_comments', reddit_content.num_comments),
        ]
        
        # Check core required fields (always required)
        for field_name, field_value in core_required_fields:
            if field_value is None:
                missing_fields.append(field_name)
        
        # Check enhanced fields only if deadline has passed
        if enhanced_validation_required:
            for field_name, field_value in enhanced_required_fields:
                if field_value is None:
                    missing_fields.append(field_name)
        
        if missing_fields:
            if enhanced_validation_required:
                bt.logging.debug(f"Reddit metadata validation failed (enhanced validation required). Missing fields: {missing_fields}")
            else:
                bt.logging.debug(f"Reddit metadata validation failed (core validation only). Missing fields: {missing_fields}")
            return False, missing_fields
        
        return True, []
        
    except Exception as e:
        bt.logging.error(f"Error validating Reddit metadata: {str(e)}")
        return False, [f"Reddit validation error: {str(e)}"]


def _validate_youtube_metadata_completeness(data_entity: DataEntity) -> tuple[bool, List[str]]:
    """Validate YouTube content metadata completeness using YouTubeOrganicOutput model."""
    try:
        from scraping.youtube.model import YouTubeContent
        youtube_content = YouTubeContent.from_data_entity(data_entity)
        missing_fields = []

        required_fields = [
            ('video_id', youtube_content.video_id),
            ('title', youtube_content.title),
            ('channel_name', youtube_content.channel_name),
            ('upload_date', youtube_content.upload_date),
            ('transcript', youtube_content.transcript),
            ('url', youtube_content.url),
            ('duration_seconds', youtube_content.duration_seconds),
            ('language', youtube_content.language),
        ]
        
        for field_name, field_value in required_fields:
            if field_value is None:
                missing_fields.append(field_name)
        
        if missing_fields:
            bt.logging.debug(f"YouTube metadata validation failed. Missing fields: {missing_fields}")
            return False, missing_fields
        
        return True, []
        
    except Exception as e:
        bt.logging.error(f"Error validating YouTube metadata: {str(e)}")
        return False, [f"YouTube validation error: {str(e)}"]