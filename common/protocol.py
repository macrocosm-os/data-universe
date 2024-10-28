# The MIT License (MIT)
# Copyright © 2023 data-universe

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import bittensor as bt
import pydantic
from common.data import (
    DataEntityBucket,
    DataEntity,
    DataEntityBucketId,
    HuggingFaceMetadata
)
from typing import Dict, List, Optional, Tuple


class BaseProtocol(bt.Synapse):
    class Config:
        arbitrary_types_allowed = True
        validate_assignment = True

    version: Optional[int] = pydantic.Field(
        description="Protocol version", default=None
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
    compressed_index_serialized: Optional[str] = pydantic.Field(
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

    # Required request input, filled by sending dendrite caller.
    data_entity_bucket_id: Optional[DataEntityBucketId] = pydantic.Field(
        title="data_entity_bucket_id",
        description="The identifier for the requested DataEntityBucket.",
        frozen=True,
        repr=False,
        default=None,
    )

    # Required request output, filled by recieving axon.
    data_entities: List[DataEntity] = pydantic.Field(
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

    # Required request input, filled by sending dendrite caller.
    data_entity_bucket_ids: Optional[List[DataEntityBucketId]] = pydantic.Field(
        title="data_entity_bucket_ids",
        description="The identifiers for the requested DataEntityBuckets.",
        frozen=True,
        repr=False,
        default=None,
    )

    # Required request output, filled by receiving axon.
    # Note a List of Tuples is used because a Dict with a dataclass as the key cannot be serialized properly by pydantic.
    bucket_ids_to_contents: List[Tuple[DataEntityBucketId, List[bytes]]] = (
        pydantic.Field(
            title="bucket_ids_to_contents",
            description="A list of bucket ids to the contents contained by that bucket. Each DataEntityBucketId appears at most once. This is just a flattened dictionary.",
            frozen=False,
            repr=False,
            default_factory=list,
        )
    )


class GetHuggingFaceMetadata(BaseProtocol):
    """
    Protocol by which Validators can retrieve HuggingFace metadata from a Miner.
    """

    # Required request output, filled by receiving axon.
    metadata: List[HuggingFaceMetadata] = pydantic.Field(
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

    # Required request input, filled by sending dendrite caller
    encoded_urls: List[str] = pydantic.Field(
        title="encoded_urls",
        description="List of encoded URLs that need to be decoded",
        max_length=10,  # Limit to 10 URLs per request
        frozen=True,
        repr=False,
        default_factory=list,
    )

    # Required request output, filled by receiving axon
    decoded_urls: List[str] = pydantic.Field(
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
