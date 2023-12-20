"""This file contains the previous versions of our data models.

This is kept around to verify backwards compatibility of the wire protocol."""

import bittensor as bt
import pydantic
from common import constants
from common.old_data import DataEntityBucket, DataEntity, DataEntityBucketId
from typing import List, Optional


class GetMinerIndex(bt.Synapse):
    """
    Protocol by which Validators can retrieve the Index from a Miner.

    Attributes:
    - data_entity_buckets: A list of DataEntityBucket objects that the Miner can serve.
    """

    # Required request output, filled by receiving axon.
    data_entity_buckets: List[DataEntityBucket] = pydantic.Field(
        title="data_entity_buckets",
        description="All of the data entity buckets that a Miner can serve.",
        frozen=False,
        repr=False,
        max_items=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX,
        default_factory=list,
    )


class GetDataEntityBucket(bt.Synapse):
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


# TODO Protocol for Users to Query Data which will accept query parameters such as a startDatetime, endDatetime.
