import dataclasses
from common import constants
from . import utils
import datetime as dt
from enum import Enum, auto
from typing import List, Type, Optional
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, validator


class StrictBaseModel(BaseModel):
    """A BaseModel that enforces stricter validation constraints"""

    class Config:
        extra = "forbid"

        # JSON serialization doesn't seem to work correctly without
        # enabling `use_enum_values`. It's possible this isn't an
        # issue with newer version of pydantic, which we can't use.
        use_enum_values = True


@dataclasses.dataclass(frozen=True)
class DateRange:
    """Represents a specific time range from start time inclusive to end time exclusive."""

    # The start time inclusive of the time range.
    start: dt.datetime

    # The end time exclusive of the time range.
    end: dt.datetime

    def contains(self, datetime: dt.datetime) -> bool:
        """Returns True if the provided datetime is within this DateRange."""
        return self.start <= datetime < self.end


class TimeBucket(StrictBaseModel):
    """Represents a specific time bucket in the linear flow of time."""

    # Makes the object "Immutable" once created.
    model_config = ConfigDict(frozen=True)

    id: PositiveInt = Field(
        description="Monotonically increasing value idenitifying the given time bucket"
    )

    @classmethod
    def from_datetime(cls, datetime: dt.datetime) -> Type["TimeBucket"]:
        """Creates a TimeBucket from the provided datetime.

        Args:
            datetime (datetime.datetime): A datetime object, assumed to be in UTC.
        """
        datetime.astimezone(dt.timezone.utc)
        return TimeBucket(
            id=utils.seconds_to_hours(
                datetime.astimezone(tz=dt.timezone.utc).timestamp()
            )
        )

    @classmethod
    def to_date_range(cls, bucket: "TimeBucket") -> DateRange:
        """Returns the date range for this time bucket."""
        return DateRange(
            start=utils.datetime_from_hours_since_epoch(bucket.id),
            end=utils.datetime_from_hours_since_epoch(bucket.id + 1),
        )


class DataSource(Enum):
    """The source of data. This will be expanded over time as we increase the types of data we collect."""

    REDDIT = 1
    X = 2


class DataLabel(StrictBaseModel):
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


class DataEntity(StrictBaseModel):
    """A logical unit of data that has been scraped. E.g. a Reddit post"""

    # Makes the object "Immutable" once created.
    model_config = ConfigDict(frozen=True)

    # Path from which the entity was generated.
    uri: str
    # The datetime of the data entity, usually its creation time.
    # Should be in UTC.
    datetime: dt.datetime
    source: DataSource
    label: Optional[DataLabel] = Field(
        default=None,
    )
    content: bytes
    content_size_bytes: int = Field(ge=0)

    @classmethod
    def are_non_content_fields_equal(
        cls, this: "DataEntity", other: "DataEntity"
    ) -> bool:
        """Returns whether this entity matches the non-content fields of another entity."""
        return (
            this.uri == other.uri
            and this.datetime == other.datetime
            and this.source == other.source
            and this.label == other.label
        )


class DataEntityBucketId(StrictBaseModel):
    """Uniquely identifies a bucket to group DataEntities by time bucket, source, and label."""

    time_bucket: TimeBucket
    source: DataSource = Field()
    label: Optional[DataLabel] = Field(
        default=None,
    )


class DataEntityBucket(StrictBaseModel):
    """Summarizes a group of data entities stored by a miner.

    Each bucket is uniquely identified by the time bucket, source, and label and it must be complete. i.e. a mine
    should never report multiple chunks for the same time bucket, source, and label.

    A single bucket is limited to 128MBs to ensure requests sent over the network aren't too large.
    """

    id: DataEntityBucketId = Field(
        description="Identifies the qualities by which this bucket is grouped."
    )
    size_bytes: int = Field(ge=0, le=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES)


class ScorableDataEntityBucket(StrictBaseModel):
    """Composes both a DataEntityBucket and additional information required for scoring."""

    data_entity_bucket: DataEntityBucket = Field(
        description="The DataEntityBucket that has additional information attached."
    )
    # Scorable bytes are the bytes that can be credited to this miner for scoring.
    # This is always less than or equal to the total size of the chunk.
    # This scorable bytes are computed as:
    # 1 byte for every byte in size_bytes that no other miner has in their index.
    # 1 byte / # of miners that have this chunk in their index for every byte in size_bytes that at least one other miner has in their index.
    scorable_bytes: int = Field(ge=0, le=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES)


class MinerIndex(StrictBaseModel):
    """The Miner index."""

    hotkey: str = Field(min_length=1, description="ss58_address of the miner's hotkey.")
    data_entity_buckets: List[DataEntityBucket] = Field(
        description="Buckets the miner is serving.",
        max_items=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX,
    )


class ScorableMinerIndex(StrictBaseModel):
    """The Miner index, with additional information required for scoring."""

    hotkey: str = Field(min_length=1, description="ss58_address of the miner's hotkey.")
    scorable_data_entity_buckets: List[ScorableDataEntityBucket] = Field(
        description="DataEntityBuckets the miner is serving, scored on uniqueness.",
        max_items=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX,
    )
    last_updated: dt.datetime = Field(description="Time last updated in UTC.")
