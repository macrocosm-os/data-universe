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
    field_validator,  # Changed from validator
)


class StrictBaseModel(BaseModel):
    """A BaseModel that enforces stricter validation constraints"""

    model_config = ConfigDict(
        use_enum_values=True
    )


class TimeBucket(StrictBaseModel):
    """Represents a specific time bucket in the linear flow of time."""

    model_config = ConfigDict(frozen=True)

    id: PositiveInt = Field(
        description="Monotonically increasing value idenitifying the given time bucket"
    )

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
    YOUTUBE = 3
    UNKNOWN_4 = 4
    UNKNOWN_5 = 5
    UNKNOWN_6 = 6
    UNKNOWN_7 = 7

    @property
    def weight(self):
        weights = {
            DataSource.REDDIT: 0.55,
            DataSource.X: 0.35,
            DataSource.YOUTUBE: 0.1,
            DataSource.UNKNOWN_4: 0,
            DataSource.UNKNOWN_5: 0,
            DataSource.UNKNOWN_6: 0,
            DataSource.UNKNOWN_7: 0
        }
        return weights[self]


class DataLabel(StrictBaseModel):
    """An optional label to classify a data entity."""

    model_config = ConfigDict(frozen=True)

    value: str = Field(
        max_length=140,
        description="The label. E.g. a subreddit for Reddit data.",
    )


    @field_validator("value")  # Changed from validator
    @classmethod
    def lower_case_value(cls, value: str) -> str:
        """Handles value casing based on the type of label.

        YouTube channel and video IDs are preserved in their original case,
        while other labels are converted to lowercase.
        """
        # Special cases where we need to preserve the original case
        if value.startswith('#ytc_c_') or value.startswith('#ytc_v_'):
            # Extract the prefix and the ID parts
            parts = value.split('_', 2)
            if len(parts) < 3:
                return value  # Return as is if the format is unexpected

            prefix = '_'.join(parts[:2]).lower()  # '#youtube_c' or '#youtube_v' in lowercase
            id_part = parts[2]  # Keep the ID with original case

            # Reassemble with lowercase prefix but preserve ID case
            return f"{prefix}_{id_part}"

        # For all other labels, convert to lowercase as before
        if len(value.lower()) > 140:
            raise ValueError(
                f"Label: {value} is over 140 characters when .lower() is applied: {value.lower()}."
            )
        return value.lower()


class DataEntity(StrictBaseModel):
    """A logical unit of data that has been scraped. E.g. a Reddit post"""

    model_config = ConfigDict(frozen=True)

    uri: str
    datetime: dt.datetime
    source: DataSource
    label: Optional[DataLabel] = Field(default=None)
    content: bytes
    content_size_bytes: int = Field(ge=0)

    @classmethod
    def are_non_content_fields_equal(
            cls, this: "DataEntity", other: "DataEntity"
    ) -> bool:
        return (
                this.uri == other.uri
                and this.datetime == other.datetime
                and this.source == other.source
                and this.label == other.label
        )


class HuggingFaceMetadata(StrictBaseModel):
    repo_name: str
    source: DataSource
    updated_at: dt.datetime
    encoding_key: Optional[str] = None

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_encoders={
            dt.datetime: lambda v: v.isoformat(),
        }
    )


class DataEntityBucketId(StrictBaseModel):
    """Uniquely identifies a bucket to group DataEntities by time bucket, source, and label."""

    model_config = ConfigDict(frozen=True)

    time_bucket: TimeBucket
    source: DataSource = Field()
    label: Optional[DataLabel] = Field(default=None)

    def __hash__(self) -> int:
        return hash(hash(self.time_bucket) + hash(self.source) + hash(self.label))


class DataEntityBucket(StrictBaseModel):
    """Summarizes a group of data entities stored by a miner."""

    id: DataEntityBucketId = Field(
        description="Identifies the qualities by which this bucket is grouped."
    )
    size_bytes: int = Field(ge=0, le=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES)


@dataclasses.dataclass()
class CompressedEntityBucket:
    """A compressed version of the DataEntityBucket to reduce bytes sent on the wire."""

    label: Optional[str] = None
    time_bucket_ids: List[int] = dataclasses.field(default_factory=list)
    sizes_bytes: List[int] = dataclasses.field(default_factory=list)


class CompressedMinerIndex(BaseModel):
    """A compressed version of the MinerIndex to reduce bytes sent on the wire."""

    sources: Dict[int, List[CompressedEntityBucket]]

    @field_validator("sources")  # Changed from validator
    @classmethod
    def validate_index_size(
            cls, sources: Dict[int, List[CompressedEntityBucket]]
    ) -> Dict[int, List[CompressedEntityBucket]]:
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
        return sum(
            len(compressed_bucket.time_bucket_ids)
            for compressed_buckets in index.sources.values()
            for compressed_bucket in compressed_buckets
        )

    @classmethod
    def size_bytes(cls, index: "CompressedMinerIndex") -> int:
        return sum(
            size_byte
            for compressed_buckets in index.sources.values()
            for compressed_bucket in compressed_buckets
            for size_byte in compressed_bucket.sizes_bytes
        )