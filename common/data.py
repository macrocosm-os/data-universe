import dataclasses
import time
from common import constants
from common.date_range import DateRange
from . import utils
import datetime as dt
from enum import IntEnum
from typing import Any, Dict, List, Type, Optional
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    validator,
)


class StrictBaseModel(BaseModel):
    """A BaseModel that enforces stricter validation constraints"""

    class Config:
        # JSON serialization doesn't seem to work correctly without
        # enabling `use_enum_values`. It's possible this isn't an
        # issue with newer version of pydantic, which we can't use.
        use_enum_values = True


class TimeBucket(StrictBaseModel):
    """Represents a specific time bucket in the linear flow of time."""

    # Makes the object "Immutable" once created.
    class Config:
        frozen = True

    id: PositiveInt = Field(
        description="Monotonically increasing value idenitifying the given time bucket"
    )

    # Manually define a hash function to handle TimeBucket not being seen as hashable by pydantic.
    def __hash__(self) -> int:
        return hash(int(self.id))

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


class DataSource(IntEnum):
    """The source of data. This will be expanded over time as we increase the types of data we collect."""

    REDDIT = 1
    X = 2
    # Additional enum values reserved for yet to be implemented sources.
    UNKNOWN_3 = 3
    UNKNOWN_4 = 4
    UNKNOWN_5 = 5
    UNKNOWN_6 = 6
    UNKNOWN_7 = 7


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
        # See reply on https://stackoverflow.com/questions/28695245/can-a-string-ever-get-shorter-when-converted-to-upper-lowercase.
        if len(value.lower()) > 32:
            raise ValueError(
                f"Label: {value} when is over 32 characters when .lower() is applied: {value.lower()}."
            )
        return value.lower()


class DataEntity(StrictBaseModel):
    """A logical unit of data that has been scraped. E.g. a Reddit post"""

    # Makes the object "Immutable" once created.
    class Config:
        frozen = True

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

    # Makes the object "Immutable" once created.
    class Config:
        frozen = True

    time_bucket: TimeBucket
    source: DataSource = Field()
    label: Optional[DataLabel] = Field(
        default=None,
    )

    # Manually define a hash function to handle TimeBucket not being seen as hashable by pydantic.
    def __hash__(self) -> int:
        return hash(hash(self.time_bucket) + hash(self.source) + hash(self.label))


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


# For the Compressed data classes, we intentionally avoid using nested classes (particularly
# nested pydantic classes) to avoid the performance hit of the extra validation.
@dataclasses.dataclass()
class CompressedEntityBucket:
    """A compressed version of the DataEntityBucket to reduce bytes sent on the wire."""

    # The label of the bucket.
    label: Optional[str] = None

    # A list of time bucket ids for this label and source. Must match the length of sizes_bytes.
    time_bucket_ids: List[int] = dataclasses.field(
        default_factory=list,
    )

    # A list of sizes in bytes for each time bucket where sizes_bytes[i] is the size_bytes for time_bucket_ids[i].",
    sizes_bytes: List[int] = dataclasses.field(
        default_factory=list,
    )


class CompressedMinerIndex(BaseModel):
    """A compressed version of the MinerIndex to reduce bytes sent on the wire."""

    # A map from source to a list of compressed buckets.
    sources: Dict[int, List[CompressedEntityBucket]]

    @validator("sources")
    @classmethod
    def validate_index_size(
        cls, sources: Dict[int, List[CompressedEntityBucket]]
    ) -> Dict[int, List[CompressedEntityBucket]]:
        """Converts the value to lower case for consistent casing throughout the system."""
        size = sum(
            len(compressed_bucket.time_bucket_ids)
            for compressed_buckets in sources.values()
            for compressed_bucket in compressed_buckets
        )
        if size > constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4:
            raise ValueError(
                f"Compressed index is too large. {size} buckets > {constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4}"
            )
        return sources

    @classmethod
    def bucket_count(cls, index: "CompressedMinerIndex") -> int:
        """Returns the number of buckets in a compressed index."""
        return sum(
            len(compressed_bucket.time_bucket_ids)
            for compressed_buckets in index.sources.values()
            for compressed_bucket in compressed_buckets
        )

    @classmethod
    def size_bytes(cls, index: "CompressedMinerIndex") -> int:
        """Returns the total size in bytes in a compressed index."""
        return sum(
            size_byte
            for compressed_buckets in index.sources.values()
            for compressed_bucket in compressed_buckets
            for size_byte in compressed_bucket.sizes_bytes
        )
