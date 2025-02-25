"""data.py contains the original data structures used for the project.

data_v2.py contains the newer data structures used, which are more performant.

From the original data structures we learned:
1. Pydantic adds a huge overhead for performance, particularly when creating > 1M objects
2. Object nesting has notable performance overhead

Hence, with the V2 models, we make trade-off the nicer coding symantics in exchange for better performance.

If a class needs to be included as a Field in a pydantic BaseModel, it should be a dataclass (which adds a small overhead),
because pydantic know how to serialize dataclasses, as long as all fields are themselves JSON serializable. 

As a rule of thumb:
1. If the class needs to perform validation on fields, use a class with a custom __init__, __eq__, and __hash__.
2. Always use __slots__.
"""

import datetime as dt
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional

from common import constants
from common.data import (
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
)


class ScorableDataEntityBucket:
    """Composes both a DataEntityBucket and additional information required for scoring.

    Attributes:
        scorable_bytes: Scorable bytes are the bytes that can be credited to this miner for scoring.
        This is always less than or equal to the total size of the chunk.
        This scorable bytes are computed as:
            1 byte for every byte in size_bytes that no other miner has in their index.
            1 byte / # of miners that have this chunk in their index for every byte in size_bytes
            that at least one other miner has in their index.
    """

    __slots__ = "time_bucket_id", "source", "label", "size_bytes", "scorable_bytes"

    def __init__(
        self,
        time_bucket_id: int,
        source: DataSource,
        label: Optional[str],
        size_bytes: int,
        scorable_bytes: int,
    ):
        if label and len(label) > constants.MAX_LABEL_LENGTH:
            raise ValueError("Label value cannot be longer than 140 characters.")
        if not 0 <= size_bytes <= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES:
            raise ValueError(
                f"Size must be between 0 and {constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES}."
            )
        if not 0 <= scorable_bytes <= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES:
            raise ValueError(
                f"Scorable bytes must be between 0 and {constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES}."
            )
        if scorable_bytes > size_bytes:
            raise ValueError(
                f"Scorable bytes cannot be greater than size bytes. Scorable bytes: {scorable_bytes}, size bytes: {size_bytes}."
            )

        self.time_bucket_id = time_bucket_id
        self.source = source
        self.label = label.casefold() if label else None
        self.size_bytes = size_bytes
        self.scorable_bytes = scorable_bytes

    def __repr__(self):
        return f"ScorableDataEntityBucket(time_bucket_id={self.time_bucket_id}, source={self.source}, label={self.label}, size_bytes={self.size_bytes}, scorable_bytes={self.scorable_bytes})"

    def __eq__(self, other):
        return (
            self.time_bucket_id == other.time_bucket_id
            and self.source == other.source
            and self.label == other.label
            and self.size_bytes == other.size_bytes
            and self.scorable_bytes == other.scorable_bytes
        )

    def __hash__(self):
        return hash(
            (
                self.time_bucket_id,
                self.source,
                self.label,
                self.size_bytes,
                self.scorable_bytes,
            )
        )

    def to_data_entity_bucket(self) -> DataEntityBucket:
        return DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket(id=self.time_bucket_id),
                source=self.source,
                label=DataLabel(value=self.label) if self.label else None,
            ),
            size_bytes=self.size_bytes,
        )


class ScorableMinerIndex(BaseModel):
    """The Miner index, with additional information required for scoring.

    Use a pydantic model for this class, because we only create 1 per miner,
    so the additional overhead is acceptable.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        frozen=True
    )

    scorable_data_entity_buckets: List[ScorableDataEntityBucket] = Field(
        description="DataEntityBuckets the miner is serving, scored on uniqueness.",
        max_length=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4,
    )
    last_updated: dt.datetime = Field(description="Time last updated in UTC.")