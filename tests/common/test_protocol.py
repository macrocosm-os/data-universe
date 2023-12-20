import json
import time
from typing import Type
import unittest
import datetime as dt
import bittensor as bt
from h11 import Data
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
from common import old_data, old_protocol

from common.protocol import GetDataEntityBucket, GetMinerIndex


def serialize_like_dendrite(synapse: bt.Synapse) -> str:
    """Serializes a synapse like a Dendrite would."""
    d = synapse.dict()
    return json.dumps(d)


def serialize_like_axon(synapse: bt.Synapse) -> str:
    """Serializes a synapse like an Axon would."""
    return synapse.json()


def deserialize(json_str: str, cls: Type) -> bt.Synapse:
    """Deserializes the same way a dendrite/axon does."""
    d = json.loads(json_str)
    return cls(**d)


class TestGetMinerIndex(unittest.TestCase):
    def test_get_miner_index_old_format_round_trip(self):
        """Tests that the protocol messages can be serialized/deserialized for transport."""
        request = GetMinerIndex()

        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, GetMinerIndex)
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
        """Tests that the protocol messages can be serialized/deserialized for transport."""

        request = GetMinerIndex()

        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, GetMinerIndex)
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

    def test_old_miner_new_vali_round_trip(self):
        """Verifies round trip communication betwee a new vali and an old miner"""
        request = GetMinerIndex()

        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, old_protocol.GetMinerIndex)
        # We can't check equality because the new GetMinerIndex includes a version field.

        # Now construct a response from the old miner and deserialize it into the new format.
        response = old_protocol.GetMinerIndex(
            data_entity_buckets=[
                old_data.DataEntityBucket(
                    id=old_data.DataEntityBucketId(
                        time_bucket=old_data.TimeBucket(id=5),
                        label=old_data.DataLabel(value="r/bittensor_"),
                        source=old_data.DataSource.REDDIT,
                    ),
                    size_bytes=100,
                ),
                old_data.DataEntityBucket(
                    id=old_data.DataEntityBucketId(
                        time_bucket=old_data.TimeBucket(id=6),
                        source=old_data.DataSource.X,
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
            old_data.DataEntityBucket(
                id=old_data.DataEntityBucketId(
                    time_bucket=old_data.TimeBucket(id=5),
                    label=old_data.DataLabel(value="r/bittensor_"),
                    source=old_data.DataSource.REDDIT,
                ),
                size_bytes=100,
            ),
            deserialized.data_entity_buckets[0],
        )
        self.assertEqual(
            old_data.DataEntityBucket(
                id=old_data.DataEntityBucketId(
                    time_bucket=old_data.TimeBucket(id=6),
                    source=old_data.DataSource.X,
                ),
                size_bytes=200,
            ),
            deserialized.data_entity_buckets[1],
        )


class TestGetDataEntityBucket(unittest.TestCase):
    def test_get_data_entity_bucket_round_trip(self):
        """Tests that the protocol messages can be serialized/deserialized for transport."""
        request = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(dt.datetime.utcnow()),
                label=DataLabel(value="r/bittensor_"),
                source=DataSource.REDDIT,
            )
        )
        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, GetDataEntityBucket)
        self.assertEqual(request, deserialized)

        # Check that the enum is deserialized correctly
        self.assertEqual(deserialized.data_entity_bucket_id.source, DataSource.REDDIT)

        # Also check that the headers can be constructed.
        request.to_headers()

        # Now check the response serialization and deserialization.
        response = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket(id=500),
                label=DataLabel(value="r/bittensor_"),
                source=DataSource.REDDIT,
            ),
            data_entities=[
                DataEntity(
                    uri="http://reddit.com/r/bittensor_/post/1",
                    datetime=dt.datetime.utcnow(),
                    source=DataSource.REDDIT,
                    content=b"abc123",
                    content_size_bytes=6,
                ),
                DataEntity(
                    uri="http://reddit.com/r/bittensor_/post/2",
                    datetime=dt.datetime.utcnow(),
                    source=DataSource.REDDIT,
                    label=DataLabel(value="r/bittensor_"),
                    content=b"edf",
                    content_size_bytes=3,
                ),
            ],
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, GetDataEntityBucket)
        self.assertEqual(response, deserialized)

    def test_get_data_entity_bucket_new_miner_old_vali(self):
        """Tests the wire protocol between a new miner and an old vali."""
        request = old_protocol.GetDataEntityBucket(
            data_entity_bucket_id=old_data.DataEntityBucketId(
                time_bucket=old_data.TimeBucket(id=500),
                label=old_data.DataLabel(value="r/bittensor_"),
                source=old_data.DataSource.REDDIT,
            )
        )
        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, GetDataEntityBucket)
        self.assertEqual(
            GetDataEntityBucket(
                data_entity_bucket_id=DataEntityBucketId(
                    time_bucket=TimeBucket(500),
                    label=DataLabel(value="r/bittensor_"),
                    source=DataSource.REDDIT,
                )
            ),
            deserialized,
        )

        # Check that the enum is deserialized correctly
        self.assertEqual(deserialized.data_entity_bucket_id.source, DataSource.REDDIT)

        # Now check the response serialization and deserialization.
        now = dt.datetime.now(tz=dt.timezone.utc)
        response = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket(id=500),
                label=DataLabel(value="r/bittensor_"),
                source=DataSource.REDDIT,
            ),
            data_entities=[
                DataEntity(
                    uri="http://reddit.com/r/bittensor_/post/1",
                    datetime=now,
                    source=DataSource.REDDIT,
                    content=b"abc123",
                    content_size_bytes=6,
                ),
                DataEntity(
                    uri="http://reddit.com/r/bittensor_/post/2",
                    datetime=now,
                    source=DataSource.REDDIT,
                    label=DataLabel(value="r/bittensor_"),
                    content=b"edf",
                    content_size_bytes=3,
                ),
            ],
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, old_protocol.GetDataEntityBucket)
        self.assertEqual(
            old_data.DataEntityBucketId(
                time_bucket=old_data.TimeBucket(id=500),
                label=old_data.DataLabel(value="r/bittensor_"),
                source=old_data.DataSource.REDDIT,
            ),
            deserialized.data_entity_bucket_id,
        )
        self.assertEqual(
            old_data.DataEntity(
                uri="http://reddit.com/r/bittensor_/post/1",
                datetime=now,
                source=DataSource.REDDIT,
                content=b"abc123",
                content_size_bytes=6,
            ),
            deserialized.data_entities[0],
        )
        self.assertEqual(
            old_data.DataEntity(
                uri="http://reddit.com/r/bittensor_/post/2",
                datetime=now,
                source=DataSource.REDDIT,
                label=old_data.DataLabel(value="r/bittensor_"),
                content=b"edf",
                content_size_bytes=3,
            ),
            deserialized.data_entities[1],
        )

    def test_get_data_entity_bucket_old_miner_new_vali(self):
        """Tests the wire protocol between a old miner and an old vali."""
        request = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket(id=500),
                label=DataLabel(value="r/bittensor_"),
                source=DataSource.REDDIT,
            )
        )
        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, old_protocol.GetDataEntityBucket)
        self.assertEqual(
            old_protocol.GetDataEntityBucket(
                data_entity_bucket_id=old_data.DataEntityBucketId(
                    time_bucket=old_data.TimeBucket(id=500),
                    label=old_data.DataLabel(value="r/bittensor_"),
                    source=old_data.DataSource.REDDIT,
                )
            ),
            deserialized,
        )

        # Check that the enum is deserialized correctly
        self.assertEqual(deserialized.data_entity_bucket_id.source, DataSource.REDDIT)

        # Now check the response serialization and deserialization.
        now = dt.datetime.now(tz=dt.timezone.utc)
        response = old_protocol.GetDataEntityBucket(
            data_entity_bucket_id=old_data.DataEntityBucketId(
                time_bucket=old_data.TimeBucket(id=500),
                label=old_data.DataLabel(value="r/bittensor_"),
                source=old_data.DataSource.REDDIT,
            ),
            data_entities=[
                old_data.DataEntity(
                    uri="http://reddit.com/r/bittensor_/post/1",
                    datetime=now,
                    source=old_data.DataSource.REDDIT,
                    content=b"abc123",
                    content_size_bytes=6,
                ),
                old_data.DataEntity(
                    uri="http://reddit.com/r/bittensor_/post/2",
                    datetime=now,
                    source=old_data.DataSource.REDDIT,
                    label=old_data.DataLabel(value="r/bittensor_"),
                    content=b"edf",
                    content_size_bytes=3,
                ),
            ],
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, GetDataEntityBucket)
        self.assertEqual(
            DataEntityBucketId(
                time_bucket=TimeBucket(id=500),
                label=DataLabel(value="r/bittensor_"),
                source=DataSource.REDDIT,
            ),
            deserialized.data_entity_bucket_id,
        )
        self.assertEqual(
            DataEntity(
                uri="http://reddit.com/r/bittensor_/post/1",
                datetime=now,
                source=DataSource.REDDIT,
                content=b"abc123",
                content_size_bytes=6,
            ),
            deserialized.data_entities[0],
        )
        self.assertEqual(
            DataEntity(
                uri="http://reddit.com/r/bittensor_/post/2",
                datetime=now,
                source=DataSource.REDDIT,
                label=DataLabel(value="r/bittensor_"),
                content=b"edf",
                content_size_bytes=3,
            ),
            deserialized.data_entities[1],
        )

    def test_get_data_entity_bucket_old_miner_new_vali_no_label(self):
        """Tests the wire protocol between a old miner and an new vali with no label."""
        request = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket(id=500),
                source=DataSource.REDDIT,
            )
        )
        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, old_protocol.GetDataEntityBucket)
        self.assertEqual(
            old_protocol.GetDataEntityBucket(
                data_entity_bucket_id=old_data.DataEntityBucketId(
                    time_bucket=old_data.TimeBucket(id=500),
                    source=old_data.DataSource.REDDIT,
                )
            ),
            deserialized,
        )

        # Check that the enum is deserialized correctly
        self.assertEqual(deserialized.data_entity_bucket_id.source, DataSource.REDDIT)

        # Now check the response serialization and deserialization.
        now = dt.datetime.now(tz=dt.timezone.utc)
        response = old_protocol.GetDataEntityBucket(
            data_entity_bucket_id=old_data.DataEntityBucketId(
                time_bucket=old_data.TimeBucket(id=500),
                source=old_data.DataSource.REDDIT,
            ),
            data_entities=[
                old_data.DataEntity(
                    uri="http://reddit.com/r/bittensor_/post/1",
                    datetime=now,
                    source=old_data.DataSource.REDDIT,
                    content=b"abc123",
                    content_size_bytes=6,
                ),
                old_data.DataEntity(
                    uri="http://reddit.com/r/bittensor_/post/2",
                    datetime=now,
                    source=old_data.DataSource.REDDIT,
                    content=b"edf",
                    content_size_bytes=3,
                ),
            ],
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, GetDataEntityBucket)
        self.assertEqual(
            DataEntityBucketId(
                time_bucket=TimeBucket(id=500),
                source=DataSource.REDDIT,
            ),
            deserialized.data_entity_bucket_id,
        )
        self.assertEqual(
            DataEntity(
                uri="http://reddit.com/r/bittensor_/post/1",
                datetime=now,
                source=DataSource.REDDIT,
                content=b"abc123",
                content_size_bytes=6,
            ),
            deserialized.data_entities[0],
        )
        self.assertEqual(
            DataEntity(
                uri="http://reddit.com/r/bittensor_/post/2",
                datetime=now,
                source=DataSource.REDDIT,
                content=b"edf",
                content_size_bytes=3,
            ),
            deserialized.data_entities[1],
        )


if __name__ == "__main__":
    unittest.main()
