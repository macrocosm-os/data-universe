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
        ...,
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

    start_date: Optional[dt.datetime] = Field(
        default=None,
        description="Start date for data collection (UTC)"
    )

    end_date: Optional[dt.datetime] = Field(
        default=None,
        description="End date for data collection (UTC)"
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

            return v.lower()
        except KeyError:
            valid_sources = [s.name.lower() for s in DataSource if s.weight > 0]
            raise ValueError(f"Invalid source. Must be one of: {valid_sources}")

    @field_validator('end_date')
    @classmethod
    def validate_dates(cls, v: Optional[dt.datetime], values: Dict) -> Optional[dt.datetime]:
        if v and values.get('start_date'):
            if v < values['start_date']:
                raise ValueError("end_date must be after start_date")

        return v


class DataItem(StrictBaseModel):
    """Single data item in response"""
    content: bytes
    datetime: dt.datetime
    uri: str
    source: DataSource
    label: Optional[str] = None


class QueryResponse(StrictBaseModel):
    """Response model for data queries"""
    status: str = Field(description="Request status (success/error)")
    data: List[DataItem] = Field(default_factory=list)
    meta: Dict = Field(
        default_factory=dict,
        description="Additional metadata about the request"
    )


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