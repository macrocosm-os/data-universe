from collections import defaultdict
import dataclasses

from common import constants
from common.pydantic_dict_encoder import PydanticDictEncodersMixin
from . import utils
import datetime as dt
from enum import Enum, IntEnum
from typing import Any, Dict, List, Type, Optional
from pydantic import (
    BaseModel,
)


class StrictBaseModel(BaseModel):
    """A BaseModel that enforces stricter validation constraints"""

    class Config:
        extra = "forbid"

        # JSON serialization doesn't seem to work correctly without
        # enabling `use_enum_values`. It's possible this isn't an
        # issue with newer version of pydantic, which we can't use.
        use_enum_values = True

        arbitrary_types_allowed = True


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


@dataclasses.dataclass(frozen=True)
class TimeBucket:
    """Represents a specific time bucket in the linear flow of time."""

    # Monotonically increasing value idenitifying the given time bucket
    id: int

    @classmethod
    def todict(cls, v):
        return {
            "id": v.id,
        }

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


@dataclasses.dataclass(frozen=True)
class DataLabel:
    """An optional label to classify a data entity. Each data source will have its own definition and interpretation of labels.

    For example, in Reddit a label is the subreddit. For a stock price, it'll be the ticker symbol.
    """

    value: str

    @classmethod
    def todict(cls, v):
        return {
            "value": v.value,
        }

    def __getattribute__(self, prop):
        if prop == "value":
            v = super().__getattribute__(prop)
            return v.lower() if v else None
        return super().__getattribute__(prop)

    def __hash__(self):
        return hash(self.value.lower() if self.value else None)

    def __eq__(self, other):
        if isinstance(other, DataLabel):
            this_value = self.value.lower() if self.value else None
            other_value = other.value.lower() if other.value else None
            return this_value == other_value
        return False


@dataclasses.dataclass(frozen=True)
class DataEntity:
    """A logical unit of data that has been scraped. E.g. a Reddit post"""

    # Path from which the entity was generated.
    uri: str
    # The datetime of the data entity, usually its creation time.
    # Should be in UTC.
    datetime: dt.datetime
    source: DataSource
    content: bytes
    content_size_bytes: int
    label: Optional[DataLabel] = None

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

    @classmethod
    def todict(cls, v):
        return {
            "uri": v.uri,
            "datetime": v.datetime.isoformat(),
            "source": int(v.source),
            "content": v.content,
            "content_size_bytes": v.content_size_bytes,
            "label": DataLabel.todict(v.label) if v.label else None,
        }


@dataclasses.dataclass(frozen=True)
class DataEntityBucketId:
    """Uniquely identifies a bucket to group DataEntities by time bucket, source, and label."""

    time_bucket: TimeBucket
    source: DataSource
    label: Optional[DataLabel] = None

    @classmethod
    def todict(cls, v):
        return {
            "time_bucket": TimeBucket.todict(v.time_bucket),
            "source": int(v.source),
            "label": DataLabel.todict(v.label) if v.label else None,
        }


@dataclasses.dataclass(frozen=True)
class DataEntityBucket:
    """Summarizes a group of data entities stored by a miner.

    Each bucket is uniquely identified by the time bucket, source, and label and it must be complete. i.e. a mine
    should never report multiple chunks for the same time bucket, source, and label.

    A single bucket is limited to 128MBs to ensure requests sent over the network aren't too large.
    """

    # Identifies the qualities by which this bucket is grouped.
    id: DataEntityBucketId
    size_bytes: int

    @classmethod
    def todict(cls, v):
        return {
            "id": DataEntityBucketId.todict(v.id),
            "size_bytes": v.size_bytes,
        }


@dataclasses.dataclass(frozen=True)
class ScorableDataEntityBucket:
    """Composes both a DataEntityBucket and additional information required for scoring."""

    # The DataEntityBucket that has additional information attached.
    data_entity_bucket: DataEntityBucket

    # Scorable bytes are the bytes that can be credited to this miner for scoring.
    # This is always less than or equal to the total size of the chunk.
    # This scorable bytes are computed as:
    # 1 byte for every byte in size_bytes that no other miner has in their index.
    # 1 byte / # of miners that have this chunk in their index for every byte in size_bytes that at least one other miner has in their index.
    scorable_bytes: int


@dataclasses.dataclass(frozen=True)
class MinerIndex:
    """The Miner index."""

    # ss58_address of the miner's hotkey.
    hotkey: str

    # Buckets the miner is serving.
    data_entity_buckets: List[DataEntityBucket]

    @classmethod
    def compress(cls, index: "MinerIndex") -> "CompressedMinerIndex":
        """Compresses a MinerIndex to reduce bytes sent on the wire."""
        sources_to_buckets = defaultdict(dict)

        # Iterate through the buckets and aggregate them by source and label.
        for bucket in index.data_entity_buckets:
            labels_to_buckets = sources_to_buckets[bucket.id.source]

            # The label may be None, but this is a valid key in a dict.
            if bucket.id.label in labels_to_buckets:
                labels_to_buckets[bucket.id.label].time_bucket_ids.append(
                    bucket.id.time_bucket.id
                )
                labels_to_buckets[bucket.id.label].sizes_bytes.append(bucket.size_bytes)
            else:
                labels_to_buckets[bucket.id.label] = CompressedEntityBucket(
                    label=bucket.id.label,
                    time_bucket_ids=[bucket.id.time_bucket.id],
                    sizes_bytes=[bucket.size_bytes],
                )

        return CompressedMinerIndex(
            sources={
                source: list(labels_to_buckets.values())
                for source, labels_to_buckets in sources_to_buckets.items()
            },
        )


@dataclasses.dataclass(frozen=True)
class ScorableMinerIndex:
    """The Miner index, with additional information required for scoring."""

    # ss58_address of the miner's hotkey.
    hotkey: str

    # DataEntityBuckets the miner is serving, scored on uniqueness.
    scorable_data_entity_buckets: List[ScorableDataEntityBucket]

    # Time last updated in UTC.
    last_updated: dt.datetime


@dataclasses.dataclass()
class CompressedEntityBucket:
    """A compressed version of the DataEntityBucket to reduce bytes sent on the wire."""

    # The label of the bucket.
    label: Optional[DataLabel] = None

    # A list of time bucket ids for this label and source. Must match the length of sizes_bytes.
    time_bucket_ids: List[int] = dataclasses.field(
        default_factory=list,
    )

    # A list of sizes in bytes for each time bucket where sizes_bytes[i] is the size_bytes for time_bucket_ids[i].",
    sizes_bytes: List[int] = dataclasses.field(
        default_factory=list,
    )


@dataclasses.dataclass(frozen=True)
class CompressedMinerIndex:
    """A compressed version of the MinerIndex to reduce bytes sent on the wire."""

    # A map from source to a list of compressed buckets.
    sources: Dict[DataSource, List[CompressedEntityBucket]]

    @classmethod
    def decompress(
        cls, compressed_index: "CompressedMinerIndex", hotkey: str
    ) -> MinerIndex:
        """Decompresses a compressed index."""
        data_entity_buckets = []

        for source, compressed_buckets in compressed_index.sources.items():
            for compressed_bucket in compressed_buckets:
                for time_bucket_id, size_bytes in zip(
                    compressed_bucket.time_bucket_ids, compressed_bucket.sizes_bytes
                ):
                    data_entity_buckets.append(
                        DataEntityBucket(
                            id=DataEntityBucketId(
                                time_bucket=TimeBucket(id=time_bucket_id),
                                source=source,
                                label=compressed_bucket.label,
                            ),
                            size_bytes=size_bytes,
                        )
                    )

        return MinerIndex(hotkey=hotkey, data_entity_buckets=data_entity_buckets)
