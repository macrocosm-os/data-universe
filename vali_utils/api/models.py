from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any
import datetime as dt
from common.data import DataSource, StrictBaseModel


class DesirabilityRequest(BaseModel):
    desirabilities: List[Dict[str, Any]] = Field(
        description="List of source items with label weights"
    )


class QueryRequest(StrictBaseModel):
    """Request model for data queries"""
    source: str = Field(
        ...,  # Required field
        description="Data source (x or reddit)"
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


class HfReposResponse(BaseModel):
    count: int
    repo_names: List[str]


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
