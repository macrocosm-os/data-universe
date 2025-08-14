import json
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

from common.protocol import GetDataEntityBucket, GetMinerIndex


def serialize_like_dendrite(synapse: bt.Synapse) -> str:
    """Serializes a synapse like a Dendrite would."""
    d = synapse.dict()
    return json.dumps(d)


def serialize_like_axon(synapse: bt.Synapse) -> str:
    """Serializes a synapse like an Axon would."""
    return serialize_like_dendrite(synapse)


def deserialize(json_str: str, cls: Type) -> bt.Synapse:
    """Deserializes the same way a dendrite/axon does."""
    d = json.loads(json_str)
    return cls(**d)


class TestGetMinerIndex(unittest.TestCase):
    def test_get_miner_index_old_format_round_trip(self):
        """Tests that the old miner index format can be serialized/deserialized for transport."""
        request = GetMinerIndex()
        json = request.json()
        print(json)
        deserialized = GetMinerIndex.parse_raw(json)
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
            ).json()
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, GetMinerIndex)
        self.assertEqual(response, deserialized)


class TestGetDataEntityBucket(unittest.TestCase):
    def test_synapse_serialization(self):
        """Tests that the protocol messages can be serialized/deserialized for transport."""
        request = GetDataEntityBucket(
            data_entity_bucket_id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(dt.datetime.utcnow()),
                label=DataLabel(value="r/bittensor_"),
                source=DataSource.REDDIT,
            )
        )
        json = request.json()
        print(json)
        deserialized = GetDataEntityBucket.parse_raw(json)
        self.assertEqual(request, deserialized)

        # Check that the enum is deserialized correctly
        self.assertEqual(deserialized.data_entity_bucket_id.source, DataSource.REDDIT)

        # Also check that the headers can be constructed.
        request.to_headers()

        # TODO: Add a test for the response.


if __name__ == "__main__":
    unittest.main()
