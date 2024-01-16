import json
import os
from pathlib import Path
from typing import Type
import unittest
import datetime as dt
import bittensor as bt
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
)
from common import old_protocol
from scraping.reddit.model import RedditContent, RedditDataType
from scraping.x.model import XContent
from tests import utils

from common.protocol import GetDataEntityBucket, GetMinerIndex

DATA_DIR = os.path.join(Path(__file__).parent, "data")


def serialize_like_dendrite(synapse: bt.Synapse) -> str:
    """Serializes a synapse like a Dendrite would."""
    d = synapse.model_dump()
    return json.dumps(d)


def serialize_like_axon(synapse: bt.Synapse) -> str:
    """Serializes a synapse like an Axon would."""
    return serialize_like_dendrite(synapse)


def deserialize(json_str: str, cls: Type) -> bt.Synapse:
    """Deserializes the same way a dendrite/axon does."""
    d = json.loads(json_str)
    return cls(**d)


class TestGetMinerIndex(unittest.TestCase):
    # Don't truncate diffs.
    maxDiff = None

    def test_get_miner_index_old_format_round_trip(self):
        """Tests that the old miner index format can be serialized/deserialized for transport."""
        request = GetMinerIndex()
        json = request.model_dump_json()
        print(json)
        deserialized = GetMinerIndex.model_validate_json(json)
        self.assertEqual(request, deserialized)

        # Also check that the headers can be constructed.
        request.to_headers()

        # Now construct a response and check it.
        response = GetMinerIndex(
            data_entity_buckets=[
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=5),
                        label=DataLabel(value="r/bittensor_"),
                        source=DataSource.REDDIT,
                    ),
                    size_bytes=100,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=6),
                        source=DataSource.X,
                    ),
                    size_bytes=200,
                ),
            ]
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, GetMinerIndex)
        self.assertEqual(response, deserialized)

    def test_get_miner_index_new_format_round_trip(self):
        """Tests that the compressed miner index can be serialized/deserialized for transport."""

        request = GetMinerIndex()

        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, GetMinerIndex)
        self.assertEqual(request, deserialized)

        # Also check that the headers can be constructed.
        request.to_headers()

        # Now construct a response and check it.
        response = GetMinerIndex(
            compressed_index_serialized=CompressedMinerIndex(
                sources={
                    DataSource.REDDIT.value: [
                        CompressedEntityBucket(
                            label="r/bittensor_",
                            time_bucket_ids=[5, 6],
                            sizes_bytes=[100, 200],
                        )
                    ],
                    DataSource.X.value: [
                        CompressedEntityBucket(
                            time_bucket_ids=[10, 11, 12], sizes_bytes=[300, 400, 500]
                        ),
                        CompressedEntityBucket(
                            label="#bittensor", time_bucket_ids=[5], sizes_bytes=[100]
                        ),
                    ],
                }
            ).model_dump_json()
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, GetMinerIndex)
        self.assertEqual(response, deserialized)

    def test_old_miner_new_vali_round_trip(self):
        """Verifies round trip communication betwee a new vali and an old miner"""
        request = GetMinerIndex()

        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, old_protocol.GetMinerIndex)
        # We can't check equality because the new GetMinerIndex includes a version field.

        # Now construct a response from the old miner and deserialize it into the new format.
        response = old_protocol.GetMinerIndex(
            data_entity_buckets=[
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=5),
                        label=DataLabel(value="r/bittensor_"),
                        source=DataSource.REDDIT,
                    ),
                    size_bytes=100,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=6),
                        source=DataSource.X,
                    ),
                    size_bytes=200,
                ),
            ]
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, GetMinerIndex)
        self.assertEqual(2, len(deserialized.data_entity_buckets))
        self.assertEqual(
            DataEntityBucket(
                id=DataEntityBucketId(
                    time_bucket=TimeBucket(id=5),
                    label=DataLabel(value="r/bittensor_"),
                    source=DataSource.REDDIT,
                ),
                size_bytes=100,
            ),
            deserialized.data_entity_buckets[0],
        )
        self.assertEqual(
            DataEntityBucket(
                id=DataEntityBucketId(
                    time_bucket=TimeBucket(id=6),
                    source=DataSource.X,
                ),
                size_bytes=200,
            ),
            deserialized.data_entity_buckets[1],
        )

    def test_new_miner_old_vali_round_trip(self):
        """Verifies round trip communication betwee a new miner and an old vali"""
        request = old_protocol.GetMinerIndex()

        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, GetMinerIndex)
        # We can't check equality because the new GetMinerIndex includes a version field.

        # Now construct a response from the old miner and deserialize it into the new format.
        response = GetMinerIndex(
            data_entity_buckets=[
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=5),
                        label=DataLabel(value="r/bittensor_"),
                        source=DataSource.REDDIT,
                    ),
                    size_bytes=100,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        time_bucket=TimeBucket(id=6),
                        source=DataSource.X,
                    ),
                    size_bytes=200,
                ),
            ]
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, old_protocol.GetMinerIndex)
        self.assertEqual(2, len(deserialized.data_entity_buckets))
        self.assertEqual(
            DataEntityBucket(
                id=DataEntityBucketId(
                    time_bucket=TimeBucket(id=5),
                    label=DataLabel(value="r/bittensor_"),
                    source=DataSource.REDDIT,
                ),
                size_bytes=100,
            ),
            deserialized.data_entity_buckets[0],
        )
        self.assertEqual(
            DataEntityBucket(
                id=DataEntityBucketId(
                    time_bucket=TimeBucket(id=6),
                    source=DataSource.X,
                ),
                size_bytes=200,
            ),
            deserialized.data_entity_buckets[1],
        )

    def test_parse_miner_index_from_v1_pydantic(self):
        """Tests that deserializing a GetMinerIndex response from V1 pydantic works
        with pydanitc V2."""
        expected_index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT.value: [
                    CompressedEntityBucket(
                        label="r/bittensor_",
                        time_bucket_ids=[1, 2, 3],
                        sizes_bytes=[100, 200, 300],
                    )
                ],
                DataSource.X.value: [
                    CompressedEntityBucket(
                        time_bucket_ids=[3, 4],
                        sizes_bytes=[123, 234],
                    ),
                    CompressedEntityBucket(
                        label="#bittensor",
                        time_bucket_ids=[5, 4],
                        sizes_bytes=[321, 99],
                    ),
                ],
            }
        )

        with open(os.path.join(DATA_DIR, "miner_index.json"), "r") as f:
            raw_str = f.read()
            proto = GetMinerIndex.model_validate_json(raw_str)
            index = CompressedMinerIndex.model_validate_json(
                proto.compressed_index_serialized
            )
            self.assertTrue(utils.are_compressed_indexes_equal(index, expected_index))


class TestGetDataEntityBucket(unittest.TestCase):
    maxDiff = None

    def test_synapse_serialization(self):
        """Tests that the protocol messages can be serialized/deserialized for transport."""
        request = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(dt.datetime.utcnow()),
                label=DataLabel(value="r/bittensor_"),
                source=DataSource.REDDIT,
            )
        )
        json = request.model_dump_json()
        print(json)
        deserialized = GetDataEntityBucket.model_validate_json(json)
        self.assertEqual(request, deserialized)

        # Check that the enum is deserialized correctly
        self.assertEqual(deserialized.data_entity_bucket_id.source, DataSource.REDDIT)

        # Also check that the headers can be constructed.
        request.to_headers()

        # TODO: Add a test for the response.

    def test_parse_reddit_entity_buckets(self):
        """Tests that deserializing a GetDataEntityBucket response from V1 pydantic works
        with pydanitc V2."""
        t = dt.datetime(2024, 1, 15, 17, 8, 29, 651126)

        expected = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(t),
                label=DataLabel(value="r/bittensor_"),
                source=DataSource.REDDIT,
            ),
            data_entities=[
                RedditContent.to_data_entity(
                    RedditContent(
                        id="entity1",
                        url="http://www.reddit.com/entityt1",
                        username="username",
                        communityName="r/bittensor_",
                        body="Some insightful text should be here. But it is not...",
                        createdAt=t,
                        dataType=RedditDataType.POST,
                        title="Bittensor is wow.",
                    )
                ),
                RedditContent.to_data_entity(
                    RedditContent(
                        id="commentId",
                        url="http://www.reddit.com/entityt1/commentId",
                        username="other-username",
                        communityName="r/bittensor_",
                        body="Much wow",
                        createdAt=t,
                        dataType=RedditDataType.COMMENT,
                        parentId="entity1",
                    )
                ),
            ],
        )

        with open(os.path.join(DATA_DIR, "reddit_data_entities.json"), "r") as f:
            raw_str = f.read()
            response = GetDataEntityBucket.model_validate_json(raw_str)
            self.assertEqual(
                expected.data_entity_bucket_id, response.data_entity_bucket_id
            )
            self.assertEqual(expected.data_entities, response.data_entities)
            self.assertEqual(response, expected)

    def test_parse_x_entity_buckets(self):
        """Tests that deserializing a GetDataEntityBucket response from V1 pydantic works
        with pydanitc V2."""
        t = dt.datetime(2024, 1, 15, 17, 8, 31, 246220)

        expected = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(t),
                label=DataLabel(value="#bittensor"),
                source=DataSource.X,
            ),
            data_entities=[
                XContent.to_data_entity(
                    XContent(
                        url="http://www.reddit.com/entityt1",
                        username="username",
                        text="Some insightful text should be here. But it is not...",
                        timestamp=t,
                        tweet_hashtags=["#bittensor", "#tao"],
                    )
                ),
            ],
        )

        with open(os.path.join(DATA_DIR, "x_data_entities.json"), "r") as f:
            raw_str = f.read()
            response = GetDataEntityBucket.model_validate_json(raw_str)
            self.assertEqual(
                expected.data_entity_bucket_id, response.data_entity_bucket_id
            )
            self.assertEqual(expected.data_entities, response.data_entities)
            self.assertEqual(expected, response)

    def test_parse_x_no_label_entity_buckets(self):
        """Tests that deserializing a GetDataEntityBucket response from V1 pydantic works
        with pydanitc V2."""
        t = dt.datetime(2024, 1, 15, 17, 8, 31, 643653)

        expected = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(t),
                label=None,
                source=DataSource.X,
            ),
            data_entities=[
                XContent.to_data_entity(
                    XContent(
                        url="http://www.reddit.com/entityt1",
                        username="username",
                        text="Some insightful text should be here. But it is not...",
                        timestamp=t,
                    )
                ),
            ],
        )

        with open(os.path.join(DATA_DIR, "x_no_label_data_entities.json"), "r") as f:
            raw_str = f.read()
            response = GetDataEntityBucket.model_validate_json(raw_str)
            self.assertEqual(
                expected.data_entity_bucket_id, response.data_entity_bucket_id
            )
            self.assertEqual(expected.data_entities, response.data_entities)
            self.assertEqual(response, expected)


if __name__ == "__main__":
    unittest.main()
