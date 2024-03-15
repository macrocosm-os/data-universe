import datetime as dt
import random
import string
import time

from common import constants, utils

from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataLabel,
    DataSource,
    TimeBucket,
)
import unittest

from common.protocol import GetMinerIndex
from pydantic import ValidationError


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

    def test_compressed_index_bucket_count(self):
        """Tests that the compressed version of Miner index can get bucket count."""
        # Make 5 compressed buckets per source, each containing 5 unique time bucket ids of size 10.
        sources = {}
        for source in [DataSource.REDDIT, DataSource.X]:
            compressed_buckets = [None] * 5
            for label_i in range(0, 5):
                label = "label" + str(label_i)
                compressed_buckets[label_i] = CompressedEntityBucket(
                    label=label,
                    time_bucket_ids=[i for i in range(1, 6)],
                    sizes_bytes=[10 for i in range(1, 6)],
                )
            sources[int(source)] = compressed_buckets

        index = CompressedMinerIndex(sources=sources)

        self.assertEqual(CompressedMinerIndex.bucket_count(index), 50)

    def test_compressed_index_size_bytes(self):
        """Tests that the compressed version of Miner index can get size in bytes."""
        # Make 5 compressed buckets per source, each containing 5 unique time bucket ids of size 10.
        sources = {}
        for source in [DataSource.REDDIT, DataSource.X]:
            compressed_buckets = [None] * 5
            for label_i in range(0, 5):
                label = "label" + str(label_i)
                compressed_buckets[label_i] = CompressedEntityBucket(
                    label=label,
                    time_bucket_ids=[i for i in range(1, 6)],
                    sizes_bytes=[10 for i in range(1, 6)],
                )
            sources[int(source)] = compressed_buckets

        index = CompressedMinerIndex(sources=sources)

        self.assertEqual(CompressedMinerIndex.bucket_count(index), 50)

    def test_compressed_index_supports_max_index(self):
        """Tests that the compressed version of the maximal Miner index is under our response size limit."""

        target_buckets = (
            constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4
        )

        # Figure out how many time buckets and labels we need to fill the index.
        buckets_per_source = target_buckets // 2  # Twitter/Reddit
        num_time_buckets = constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 7 * 24
        num_labels = buckets_per_source // num_time_buckets

        # Double check the math
        total_buckets = 2 * num_time_buckets * num_labels
        self.assertAlmostEqual(
            target_buckets,
            total_buckets,
            delta=target_buckets * 0.05,
        )

        start = time.time()
        sources = {}

        def generate_random_string(length):
            # Combine letters and digits for the random string
            characters = string.ascii_letters + string.digits
            return "".join(random.choice(characters) for _ in range(length))

        for source in [DataSource.REDDIT, DataSource.X]:
            compressed_buckets = [None] * num_labels
            for label_i in range(0, num_labels):
                label = generate_random_string(random.randint(4, 32))
                compressed_buckets[label_i] = CompressedEntityBucket(
                    label=label,
                    time_bucket_ids=[i for i in range(1, num_time_buckets + 1)],
                    sizes_bytes=[
                        random.randint(1, 112345678)
                        for i in range(1, num_time_buckets + 1)
                    ],
                )
            sources[int(source)] = compressed_buckets

        print(f"Time to create index: {time.time() - start}")
        maximal_index = CompressedMinerIndex(
            sources=sources,
        )

        start = time.time()
        serialized_compressed_index = maximal_index.json()
        print(f"Time to serialize index: {time.time() - start}")

        start = time.time()
        get_miner_index = GetMinerIndex(
            compressed_index_serialized=serialized_compressed_index
        )
        print(f"Time to create synapse: {time.time() - start}")

        start = time.time()
        compressed_json = get_miner_index.json()
        print(f"Time to serialize synapse: {time.time() - start}")
        print(f"Compressed index size: {len(compressed_json)}")
        self.assertLess(len(compressed_json), utils.mb_to_bytes(mb=128))

        start = time.time()
        deserialized_index = GetMinerIndex.parse_raw(compressed_json)
        deserialized_compressed_index = CompressedMinerIndex.parse_raw(
            deserialized_index.compressed_index_serialized
        )
        print(f"Time to deserialize synapse: {time.time() - start}")

        # Verify the deserialized form is as expected.
        self.assertEqual(deserialized_compressed_index, maximal_index)

    def test_data_label_lower_validation(self):
        """Tests that the data label value is checked to be <32 characters even after .lower()."""
        with self.assertRaises(ValidationError):
            bad_label = DataLabel(value="#İsrailleTicaretFilistineİhanet")


if __name__ == "__main__":
    unittest.main()
