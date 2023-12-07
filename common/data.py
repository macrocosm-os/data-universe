import dataclasses

from . import utils
import datetime as dt
from enum import Enum, auto
from typing import List, Type, Optional
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, validator


class TimeBucket(BaseModel):
    """Represents a specific time bucket in the linear flow of time."""

    # Makes the object "Immutable" once created.
    model_config = ConfigDict(frozen=True)

    id: PositiveInt = Field(description="Monotonically increasing value idenitifying the given time bucket")

    def id(self) -> int:
        return self.id

    @classmethod
    def from_datetime(cls, datetime: dt.datetime) -> Type["TimeBucket"]:
        """Creates a TimeBucket from the provided datetime.

        Args:
            datetime (datetime.datetime): A datetime object, assumed to be in UTC.
        """
        return TimeBucket(id=utils.seconds_to_hours(datetime.timestamp()))


class DataSource(Enum):
    """The source of data. This will be expanded over time as we increase the types of data we collect."""

    REDDIT = auto()
    X = auto()


class DataLabel(BaseModel):
    """An optional label to classify a data entity. Each data source will have its own definition and interpretation of labels.

    For example, in Reddit a label is the subreddit. For a stock price, it'll be the ticker symbol.
    """

    class Config:
        frozen = True

    value: str = Field(
        max_length=32,
        description="The label. E.g. a subreddit for Reddit data.",
    )

    @validator("value")
    @classmethod
    def lower_case_value(cls, value: str) -> str:
        """Converts the value to lower case to consistent casing throughout the system."""
        return value.lower()


class DataEntity(BaseModel):
    """A logical unit of data that has been scraped. E.g. a Reddit post"""

    # Makes the object "Immutable" once created.
    model_config = ConfigDict(frozen=True)

    # Path from which the entity was generated.
    uri: str
    # The datetime of the data entity, usually its creation time.
    # Should be in UTC.
    datetime: dt.datetime
    source: DataSource
    label: Optional[DataLabel]
    content: bytes
    content_size_bytes: int = Field(ge=0)


class DataChunkSummary(BaseModel):
    """Summarizes a chunk of data stored by a miner.

    Each chunk is uniquely identified by the time bucket, source, and label and it must be complete. i.e. a miner should never report
    multiple chunks for the same time bucket, source, and label.

    A single chunk is limited to 128MBs to ensure requests sent over the network aren't too large.
    """

    time_bucket: TimeBucket
    source: DataSource
    label: Optional[DataLabel]
    size_bytes: int = Field(ge=0, le=utils.mb_to_bytes(mb=128))


class ScorableDataChunkSummary(DataChunkSummary):
    """A DataChunkSummary that contains additional information required for scoring."""

    # Scorable bytes are the bytes that can be credited to this miner for scoring.
    # This is always less than or equal to the total size of the chunk.
    # This scorable bytes are computed as:
    # 1 byte for every byte in size_bytes that no other miner has in their index.
    # 1 byte / # of miners that have this chunk in their index for every byte in size_bytes that at least one other miner has in their index.
    scorable_bytes: int = Field(ge=0, le=utils.mb_to_bytes(mb=128))


class ScorableMinerIndex(BaseModel):
    """The Miner index, with additional information required for scoring."""

    chunks: List[ScorableDataChunkSummary]
    # Time last updated in UTC.
    last_updated: dt.datetime
