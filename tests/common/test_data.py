import datetime as dt
import time
from pydantic import ValidationError

from common import constants, utils

from common.data import (
    CompressedMinerIndex,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    MinerIndex,
    TimeBucket,
)
import unittest

from common.protocol import GetMinerIndex


class TestData(unittest.TestCase):
    def test_time_bucket_to_date_range(self):
        """Tests a Timebucket's date range function"""

        # Create a datetime that should align with the start of a time bucket.
        datetime = dt.datetime.fromtimestamp(36000, tz=dt.timezone.utc)
        time_bucket = TimeBucket.from_datetime(datetime)

        date_range = TimeBucket.to_date_range(time_bucket)

        for i in range(0, 60):
            self.assertTrue(date_range.contains(datetime + dt.timedelta(minutes=i)))

        self.assertFalse(date_range.contains(datetime + dt.timedelta(minutes=60)))

    def test_data_source_init(self):
        """Tests that the data source enum can be initialized"""
        source = 1
        self.assertEqual(DataSource.REDDIT, DataSource(source))

    def test_index_compression(self):
        """Tests that the index compression works"""

        index = MinerIndex(
            hotkey="hotkey",
            data_entity_buckets=[
                DataEntityBucket(
                    id=DataEntityBucketId(
                        source=DataSource.REDDIT,
                        label=DataLabel(value="r/bittensor_"),
                        time_bucket=TimeBucket(id=5),
                    ),
                    size_bytes=100,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        source=DataSource.REDDIT,
                        label=DataLabel(value="r/bittensor_"),
                        time_bucket=TimeBucket(id=6),
                    ),
                    size_bytes=250,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        source=DataSource.REDDIT,
                        time_bucket=TimeBucket(id=5),
                    ),
                    size_bytes=10,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        source=DataSource.X,
                        # It's unrealistic, but share a label and time bucket with an entity from a different source.
                        label=DataLabel(value="r/bittensor_"),
                        time_bucket=TimeBucket(id=5),
                    ),
                    size_bytes=100,
                ),
                DataEntityBucket(
                    id=DataEntityBucketId(
                        source=DataSource.X,
                        time_bucket=TimeBucket(id=5),
                    ),
                    size_bytes=10,
                ),
            ],
        )

        compressed_index = MinerIndex.compress(index)
        self.assertEqual(
            index, CompressedMinerIndex.decompress(compressed_index, "hotkey")
        )

    def test_deserialize_from_old_model(self):
        """Verifies the current data models can deserialize from the old pydantic model form, that used
        to have an extra model_config arg."""
        GetMinerIndex.parse_raw(
            '{"name": "GetMinerIndex", "timeout": 12.0, "total_size": 0, "header_size": 0, "data_entity_buckets": [{"id": {"time_bucket": {"id": 5, "model_config": {"frozen": true}}, "source": 1, "label": {"value": "r/BitTensor"}}, "size_bytes": 100}]}'
        )

    def test_label_is_lower_cased(self):
        """Tests that the label is lower cased when even if the raw json uses upper case."""
        index = GetMinerIndex.parse_raw(
            '{"name": "GetMinerIndex", "timeout": 12.0, "total_size": 0, "header_size": 0, "data_entity_buckets": [{"id": {"time_bucket": {"id": 5}, "source": 1, "label": {"value": "r/BitTensor"}}, "size_bytes": 100}]}'
        )

        self.assertEqual(index.data_entity_buckets[0].id.label.value, "r/bittensor")

    def test_get_miner_index_performs_validation(self):
        """Tests that basic validation is performed when deserializing a GetMinerIndex."""
        with self.assertRaises(ValidationError):
            # Parse with an invalid data source.
            GetMinerIndex.parse_raw(
                '{"name": "GetMinerIndex", "timeout": 12.0, "total_size": 0, "header_size": 0, "data_entity_buckets": [{"id": {"time_bucket": {"id": 5}, "source": 5, "label": {"value": "r/BitTensor"}}, "size_bytes": 100}]}'
            )

    def test_compression_supports_max_index(self):
        """Tests that the compressed version of the maximal Miner index is under our response size limit."""

        target_buckets = constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX

        # Figure out how many time buckets and labels we need to fill the index.
        buckets_per_source = target_buckets // len(DataSource)
        num_time_buckets = constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 7 * 24
        num_labels = buckets_per_source // num_time_buckets

        # Double check the math
        total_buckets = len(DataSource) * num_time_buckets * num_labels
        self.assertAlmostEqual(
            target_buckets,
            total_buckets,
            delta=target_buckets * 0.05,
        )

        entity_buckets = [None] * total_buckets
        i = 0
        start = time.time()
        for source in list(DataSource):
            for label_id in range(0, num_labels):
                for time_bucket_id in range(1, num_time_buckets + 1):
                    entity_buckets[i] = DataEntityBucket(
                        id=DataEntityBucketId(
                            source=source,
                            label=DataLabel(value=f"label_{label_id}"),
                            time_bucket=TimeBucket(id=time_bucket_id),
                        ),
                        size_bytes=100,
                    )
                    i += 1

        print(f"Time to create index: {time.time() - start}")
        maximal_index = MinerIndex(
            hotkey="hotkey",
            data_entity_buckets=entity_buckets,
        )

        start = time.time()
        compressed_index = MinerIndex.compress(maximal_index)
        print(f"Time to compress index: {time.time() - start}")

        start = time.time()
        message_new = GetMinerIndex(compressed_index=compressed_index)
        print(f"Time to create synapse: {time.time() - start}")

        start = time.time()
        compressed_json = message_new.json()
        print(f"Time to serialize synapse: {time.time() - start}")
        print(f"Compressed index size: {len(compressed_json)}")
        self.assertLess(len(compressed_json), utils.mb_to_bytes(mb=128))


if __name__ == "__main__":
    unittest.main()
