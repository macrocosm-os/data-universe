import bittensor as bt
from pydantic import Field, ConfigDict, field_validator
from common.data import (
    DataEntityBucket,
    DataEntity,
    DataEntityBucketId,
    HuggingFaceMetadata
)
from typing import Dict, List, Optional, Tuple


class BaseProtocol(bt.Synapse):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True
    )

    version: Optional[int] = Field(
        description="Protocol version", 
        default=None
    )


class GetMinerIndex(BaseProtocol):
    """
    Protocol by which Validators can retrieve the Index from a Miner.

    Attributes:
    - data_entity_buckets: A list of DataEntityBucket objects that the Miner can serve.
    """

    # We opt to send the compressed index in pre-serialized form to have full control
    # over serialization and deserialization, rather than relying on fastapi and bittensors
    # interactions with pydantic serialization, which can be problematic for certain types.
    compressed_index_serialized: Optional[str] = Field(
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

    data_entity_bucket_id: Optional[DataEntityBucketId] = Field(
        title="data_entity_bucket_id",
        description="The identifier for the requested DataEntityBucket.",
        frozen=True,
        repr=False,
        default=None,
    )

    data_entities: List[DataEntity] = Field(
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

    data_entity_bucket_ids: Optional[List[DataEntityBucketId]] = Field(
        title="data_entity_bucket_ids",
        description="The identifiers for the requested DataEntityBuckets.",
        frozen=True,
        repr=False,
        default=None,
    )

    bucket_ids_to_contents: List[Tuple[DataEntityBucketId, List[bytes]]] = Field(
        title="bucket_ids_to_contents",
        description="A list of bucket ids to the contents contained by that bucket. Each DataEntityBucketId appears at most once. This is just a flattened dictionary.",
        frozen=False,
        repr=False,
        default_factory=list,
    )


class GetHuggingFaceMetadata(BaseProtocol):
    """
    Protocol by which Validators can retrieve HuggingFace metadata from a Miner.
    """

    metadata: List[HuggingFaceMetadata] = Field(
        title="metadata",
        description="List of HuggingFace metadata entries.",
        default_factory=list
    )


class DecodeURLRequest(BaseProtocol):
    """
    Protocol by which Validators can request URL decoding from a Miner.

    Attributes:
    - encoded_urls: A list of encoded URL strings to be decoded
    - decoded_urls: A list of decoded URL strings returned by the miner
    """

    encoded_urls: List[str] = Field(
        title="encoded_urls",
        description="List of encoded URLs that need to be decoded",
        frozen=True,
        repr=False,
        default_factory=list,
        max_length=10  # Changed from validator to direct Field constraint
    )

    decoded_urls: List[str] = Field(
        title="decoded_urls",
        description="List of decoded URLs corresponding to the encoded URLs",
        frozen=False,
        repr=False,
        default_factory=list,
    )


# How many times validators can send requests per validation period.
REQUEST_LIMIT_BY_TYPE_PER_PERIOD = {
    GetMinerIndex: 1,
    GetDataEntityBucket: 1,
    GetContentsByBuckets: 5,
    DecodeURLRequest: 2,
    GetHuggingFaceMetadata: 1,  # New entry for HuggingFace metadata requests
}