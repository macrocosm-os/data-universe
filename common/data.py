import dataclasses

from . import utils
import datetime as dt
from enum import Enum, auto
from typing import Type, Optional
from pydantic import BaseModel, ConfigDict, Field


@dataclasses.dataclass(frozen=True)
class Hour:
    """Represents an hour period from the :00 to :59.99..."""

    def __init__(self, hours_since_epoch: int):
        self.hour = hours_since_epoch

    def get_hours_since_epoch(self) -> int:
        return self.hour

    @classmethod
    def from_datetime(cls, datetime: dt.datetime) -> Type["Hour"]:
        """Creates an Hour from the provided datetime.

        Args:
            datetime (datetime.datetime): A datetime object, assumed to be in UTC.
        """
        return Hour(hours_since_epoch=utils.seconds_to_hours(datetime.timestamp()))


class DataSource(Enum):
    """The source of data. This will be expanded over time as we increase the types of data we collect."""

    # Placeholder for now.
    UNKNOWN = auto()


class DataLabel(BaseModel):
    """An optional label to classify a data entity. Each data source will have its own definition and interpretation of labels.

    For example, in Reddit a label is the subreddit. For a stock price, it'll be the ticker symbol.
    """

    model_config = ConfigDict(frozen=True)

    value: str = Field(max_length=32)


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

    Each chunk is uniquely identified by the hour, source, and label and it must be complete. i.e. a miner should never report
    multiple chunks for the same hour, source, and label.

    A single chunk is limited to 128MBs to ensure requests sent over the network aren't too large.
    """

    hour: Hour
    source: DataSource
    label: Optional[DataLabel]
    size_bytes: int = Field(ge=0, le=utils.mb_to_bytes(mb=128))
