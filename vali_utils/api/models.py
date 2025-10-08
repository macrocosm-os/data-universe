from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Optional, Dict, Any
import datetime as dt
import bittensor as bt
from common.data import DataSource, StrictBaseModel
from common.protocol import KeywordMode


class DesirabilityRequest(BaseModel):
    desirabilities: List[Dict[str, Any]] = Field(
        description="List of source items with label weights"
    )


class QueryRequest(StrictBaseModel):
    """Request model for data queries"""
    source: str = Field(
        ...,  # Required field
        description="Data source (x, reddit, or youtube)"
    )
    usernames: List[str] = Field(
        default_factory=list,
        description="List of usernames to fetch data from",
        max_length=10
    )
    keywords: List[str] = Field(
        default_factory=list,
        description="List of keywords to search for",
        max_length=5
    )
    # Change to optional strings for ISO format
    start_date: Optional[str] = Field(
        default=None,
        description="Start date (ISO format)"
    )
    end_date: Optional[str] = Field(
        default=None,
        description="End date (ISO format)"
    )
    limit: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Maximum number of items to return"
    )
    keyword_mode: KeywordMode = Field(
        default="all",
        description="Keyword matching mode: 'any' (if any keyword is present) or 'all' (all keywords must be present)"
    )

    @field_validator('source')
    @classmethod
    def validate_source(cls, v: str) -> str:
        try:
            source = DataSource[v.upper()]
            if source.weight == 0:  # Check if it's an active source
                raise ValueError(f"Source {v} is not currently active")
            return v.upper()  # Return uppercase to match enum
        except KeyError:
            valid_sources = [s.name.lower() for s in DataSource if s.weight > 0]
            raise ValueError(f"Invalid source. Must be one of: {valid_sources}")

    @field_validator('usernames')
    @classmethod
    def validate_usernames(cls, v: List[str], info) -> List[str]:
        source = info.data.get('source', '').upper()
        
        # Clean usernames by removing empty/whitespace-only strings
        cleaned_usernames = [username.strip() for username in v if username and username.strip()]
        
        # Source-specific validation
        if source == 'YOUTUBE':
            if len(cleaned_usernames) != 1:
                raise ValueError("YouTube requests must have exactly one non-empty username")
        
        if len(cleaned_usernames) < len(v):
            removed_count = len(v) - len(cleaned_usernames)
            bt.logging.warning(f"Filtered out {removed_count} empty username(s) from request")
        
        return cleaned_usernames
    
    @model_validator(mode='after')
    def validate_x_requirements(self):
        """Validate that X requests have either usernames or keywords"""
        if self.source.upper() == 'X':
            if not self.usernames and not self.keywords:
                raise ValueError("X requests must have either usernames or keywords (or both)")
        return self

    @field_validator('keywords')
    @classmethod
    def validate_keywords_for_youtube(cls, v: List[str], info) -> List[str]:
        # Get the source from the model context
        source = info.data.get('source', '').upper()
        
        if source == 'YOUTUBE':
            if len(v) > 0:
                raise ValueError("YouTube requests cannot have keywords")
        
        return v


class QueryResponse(StrictBaseModel):
    """Response model for data queries"""
    status: str = Field(description="Request status (success/error)")
    data: List[Dict[str, Any]] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata about the request"
    )


class DataItem(StrictBaseModel):
    """Single data item in response"""
    content: bytes
    datetime: dt.datetime
    uri: str
    source: DataSource
    label: Optional[str] = None


class HealthResponse(StrictBaseModel):
    """Response model for health check"""
    status: str = Field(description="Service status")
    timestamp: dt.datetime = Field(description="Current UTC timestamp")
    miners_available: int = Field(description="Number of available miners")
    version: str = Field(default="1.0.0", description="API version")
    netuid: int = Field(description="Network UID")
    hotkey: str = Field(description="Validator hotkey address")


class MinerInfo(BaseModel):
    """Information about a miner's current data"""
    hotkey: str
    credibility: float
    bucket_count: int
    content_size_bytes_reddit: int
    content_size_bytes_twitter: int
    last_updated: dt.datetime


class LabelSize(BaseModel):
    """Content size information for a specific label"""
    label_value: str
    content_size_bytes: int
    adj_content_size_bytes: int


class AgeSize(BaseModel):
    """Content size information for a specific time bucket"""
    time_bucket_id: int
    content_size_bytes: int
    adj_content_size_bytes: int


class LabelBytes(BaseModel):
    """Byte size information for a particular label"""
    label: str
    total_bytes: int
    adj_total_bytes: float
