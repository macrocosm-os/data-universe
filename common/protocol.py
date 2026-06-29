from typing import Dict, List, Literal, Optional, Tuple

import bittensor as bt
from pydantic import ConfigDict, Field, field_validator

from common.data import (
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataSource,
)

KeywordMode = Literal["any", "all"]


class BaseProtocol(bt.Synapse):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)

    version: int | None = Field(description="Protocol version", default=None)


class GetMinerIndex(BaseProtocol):
    """
    Protocol by which Validators can retrieve the Index from a Miner.

    Attributes:
    - data_entity_buckets: A list of DataEntityBucket objects that the Miner can serve.
    """

    # We opt to send the compressed index in pre-serialized form to have full control
    # over serialization and deserialization, rather than relying on fastapi and bittensors
    # interactions with pydantic serialization, which can be problematic for certain types.
    compressed_index_serialized: str | None = Field(
        description="The compressed index of the Miner of type CompressedMinerIndex.",
        frozen=False,
        repr=False,
        default=None,
    )


class GetDataEntityBucket(BaseProtocol):
    """
    Protocol by which Validators can retrieve the DataEntities of a Bucket from a Miner.

    Attributes:
    - bucket_id: The id of the bucket that the requester is asking for.
    - data_entities: A list of DataEntity objects that make up the requested DataEntityBucket.
    """

    data_entity_bucket_id: DataEntityBucketId | None = Field(
        title="data_entity_bucket_id",
        description="The identifier for the requested DataEntityBucket.",
        frozen=True,
        repr=False,
        default=None,
    )

    data_entities: list[DataEntity] = Field(
        title="data_entities",
        description="All of the data that makes up the requested DataEntityBucket.",
        frozen=False,
        repr=False,
        default_factory=list,
    )


class GetContentsByBuckets(BaseProtocol):
    """
    Protocol by which Validators can retrieve contents from one or more Miner Buckets.
    After March 1st all contents have their creation timestamp obfuscated to the minute.

    Attributes:
    - bucket_ids: The ids of the buckets that the requester is asking for.
    - bucket_ids_to_contents: A dict of DataEntityBucketId objects to a list of contained contents.
    """

    data_entity_bucket_ids: list[DataEntityBucketId] | None = Field(
        title="data_entity_bucket_ids",
        description="The identifiers for the requested DataEntityBuckets.",
        frozen=True,
        repr=False,
        default=None,
    )

    bucket_ids_to_contents: list[tuple[DataEntityBucketId, list[bytes]]] = Field(
        title="bucket_ids_to_contents",
        description="A list of bucket ids to the contents contained by that bucket. Each DataEntityBucketId appears at most once. This is just a flattened dictionary.",
        frozen=False,
        repr=False,
        default_factory=list,
    )


class OnDemandRequest(BaseProtocol):
    """Protocol for on-demand data retrieval requests"""

    # Request parameters
    source: DataSource | None = Field(default=None, description="Source to query (X or Reddit)")

    usernames: list[str] = Field(default_factory=list, description="Usernames to fetch data from", max_length=10)

    keywords: list[str] = Field(default_factory=list, description="Keywords/hashtags to search for", max_length=5)

    url: str | None = Field(default=None, description="Single URL for URL search mode (X or YouTube)")

    keyword_mode: KeywordMode = Field(default="all", description="Keyword matching mode: 'any' or 'all'")

    start_date: str | None = Field(default=None, description="Start date (ISO format)")

    end_date: str | None = Field(default=None, description="End date (ISO format)")

    limit: int = Field(default=100, ge=1, le=1000, description="Maximum items to return")

    # Response fields
    data: list[DataEntity] = Field(default_factory=list, description="Retrieved data")

    version: int | None = Field(default=None, description="Protocol version")


# How many times validators can send requests per validation period.
REQUEST_LIMIT_BY_TYPE_PER_PERIOD = {
    GetMinerIndex: 1,
    GetDataEntityBucket: 1,
    GetContentsByBuckets: 5,
}
