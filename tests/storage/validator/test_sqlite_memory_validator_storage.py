from collections import defaultdict
import random
import time
import unittest
import os

from pytz import NonExistentTimeError
from common import constants, utils
from common.constants import DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntityBucket,
    DataEntity,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    MinerIndex,
    TimeBucket,
)
import datetime as dt
from common.data_v2 import ScorableDataEntityBucket, ScorableMinerIndex
from storage.validator.sqlite_memory_validator_storage import (
    SqliteMemoryValidatorStorage,
)


class TestSqliteMemoryValidatorStorage(unittest.TestCase):
    def setUp(self):
        self.test_storage = SqliteMemoryValidatorStorage()
        # When test_storage goes out of scope the in memory db automatically gets deleted.

    def test_upsert_miner(self):
        """Tests that we can store a newly encountered miner."""
        # Upsert a miner.
        now = dt.datetime.utcnow()
        miner_id = self.test_storage._upsert_miner("test_hotkey", now, credibility=1.0)

        self.assertEqual(miner_id, 1)

        # Check inserting the same miner again returns the same id.
        next_id = self.test_storage._upsert_miner(
            "test_hotkey", dt.datetime.utcnow(), credibility=0.5
        )
        self.assertEqual(next_id, miner_id)

        # Finally, insert a new miner and make sure it has a new id.
        other_id = self.test_storage._upsert_miner(
            "other_hotkey", dt.datetime.utcnow(), credibility=0.5
        )
        self.assertNotEqual(other_id, miner_id)

    def test_insert_labels(self):
        """Tests that we can store a label."""
        # Insert a label.
        # self.test_storage._insert_labels(set("test_label_value"))
        # label_value_to_id = self.test_storage._get_label_value_to_id_dict(set("test_label_value"))
        # id = label_value_to_id[]

        # # Store it again and ensure we get the same id.
        # next_id = self.test_storage._insert_labels(set("test_label_value"))
        # self.assertEqual(next_id, label_id)

        # # Insert a new label and ensure we get a new id.
        # other_id = self.test_storage._insert_labels(None)
        # self.assertNotEqual(other_id, label_id)

    def test_upsert_special_labels(self):
        """Tests that we can store labels with special characters."""
        self.test_storage._insert_labels(set("#mineria", "#minería", "#🌌"))

        self.assertTrue(label_id1 != label_id2 != label_id3)

    def test_upsert_bucket(self):
        """Tests upserting a bucket"""
        bucket_ids = set()
        for time_bucket_id in [100, 200]:
            for source in [1, 2]:
                for label_id in [5, 6]:
                    # Upsert twice. Both should produce the same bucket ID.
                    bucket_ids.add(
                        self.test_storage._upsert_bucket(
                            source, time_bucket_id, label_id
                        )
                    )
                    bucket_ids.add(
                        self.test_storage._upsert_bucket(
                            source, time_bucket_id, label_id
                        )
                    )

        self.assertEqual(8, len(bucket_ids))

    def test_upsert_miner_index_insert_index(self):
        """Tests that we can insert a miner index, including a label with a special character."""
        # Create two DataEntityBuckets for the index.
        now = dt.datetime.utcnow()
        time_bucket = TimeBucket.from_datetime(now)
        bucket_1 = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=time_bucket,
                source=DataSource.REDDIT,
                label=DataLabel(value="#𝙅𝙚𝙬𝙚𝙡𝙧𝙮"),
            ),
            size_bytes=10,
        )

        bucket_2 = DataEntityBucket(
            id=DataEntityBucketId(time_bucket=time_bucket, source=DataSource.X),
            size_bytes=50,
        )

        # Create the index containing the buckets.
        index = MinerIndex(hotkey="hotkey1", data_entity_buckets=[bucket_1, bucket_2])

        # Store the index.
        self.test_storage.upsert_miner_index(index, credibility=1.0)

        # Confirm we get back the expected index.
        index = self.test_storage.read_miner_index("hotkey1")

        expected_scorable_index = ScorableMinerIndex(
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=time_bucket.id,
                    source=DataSource.REDDIT,
                    label="#𝙅𝙚𝙬𝙚𝙡𝙧𝙮",
                    size_bytes=10,
                    scorable_bytes=10,
                ),
                ScorableDataEntityBucket(
                    time_bucket_id=time_bucket.id,
                    source=DataSource.X,
                    label=None,
                    size_bytes=50,
                    scorable_bytes=50,
                ),
            ],
            last_updated=now,
        )
        self.assertEqual(
            index.scorable_data_entity_buckets,
            expected_scorable_index.scorable_data_entity_buckets,
        )
        self.assertTrue(
            index.last_updated - expected_scorable_index.last_updated
            < dt.timedelta(seconds=5)
        )

    # TODO: Add a test that inserts 1 miner index, then inserts a bunch of random indexes, deletes some, and verifies the final index is correct.

    # TODO: Add a test to insert millions of rows across many miners and check the total memory size.

    def test_upsert_miner_index_insert_index_with_duplicates(self):
        """Tests that we can insert a miner index with duplicates, taking the last one."""
        # Create two identical DataEntityBuckets for the index.
        now = dt.datetime.utcnow()
        time_bucket = TimeBucket.from_datetime(now)
        bucket_1 = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=time_bucket,
                source=DataSource.REDDIT,
                label=DataLabel(value="label_1"),
            ),
            size_bytes=10,
        )

        # Create the index containing the buckets.
        index = MinerIndex(hotkey="hotkey1", data_entity_buckets=[bucket_1, bucket_1])

        # Store the index.
        self.test_storage.upsert_miner_index(index, credibility=1.0)

        # Confirm we have one row in the index.
        index = self.test_storage.read_miner_index("hotkey1")
        expected_buckets = [
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket.id,
                source=DataSource.REDDIT,
                label="label_1",
                size_bytes=10,
                scorable_bytes=10,
            ),
        ]
        self.assertEqual(
            index.scorable_data_entity_buckets,
            expected_buckets,
        )

    def test_upsert_miner_index_update_index(self):
        """Tests that we can update a miner index"""
        # Create three DataEntityBuckets for the index.
        now = dt.datetime.utcnow()
        time_bucket = TimeBucket.from_datetime(now)
        time_bucket2 = TimeBucket.from_datetime(now + dt.timedelta(days=1))
        bucket_1 = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=time_bucket,
                source=DataSource.REDDIT,
                label=DataLabel(value="label_1"),
            ),
            size_bytes=10,
        )

        bucket_2 = DataEntityBucket(
            id=DataEntityBucketId(time_bucket=time_bucket, source=DataSource.X),
            size_bytes=50,
        )

        bucket_3 = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=time_bucket2,
                source=DataSource.X,
                label=DataLabel(value="label_2"),
            ),
            size_bytes=100,
        )

        # Create an index containing the first two buckets.
        index_1 = MinerIndex(hotkey="hotkey1", data_entity_buckets=[bucket_1, bucket_2])

        # Create an index containing the last two buckets.
        index_2 = MinerIndex(hotkey="hotkey1", data_entity_buckets=[bucket_2, bucket_3])

        # Store the first index.
        self.test_storage.upsert_miner_index(index_1, credibility=1.0)

        # Store the second index.
        self.test_storage.upsert_miner_index(index_2, credibility=1.0)

        # Confirm we have only the last two buckets in the index.
        index = self.test_storage.read_miner_index("hotkey1")
        expected_buckets = [
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket.id,
                source=DataSource.X,
                label=None,
                size_bytes=50,
                scorable_bytes=50,
            ),
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket2.id,
                source=DataSource.X,
                label="label_2",
                size_bytes=100,
                scorable_bytes=100,
            ),
        ]
        self.assertEqual(
            index.scorable_data_entity_buckets,
            expected_buckets,
        )

    def test_upsert_compressed_miner_index_update_index(self):
        """Tests that we can update a compressed miner index"""
        # Create three DataEntityBuckets for the index.
        now = dt.datetime.utcnow()
        time_bucket = TimeBucket.from_datetime(now)
        time_bucket2 = TimeBucket.from_datetime(now + dt.timedelta(days=1))
        compressed_index_1 = CompressedMinerIndex(
            sources={
                int(DataSource.REDDIT): [
                    CompressedEntityBucket(
                        label="label_1",
                        time_bucket_ids=[time_bucket.id],
                        sizes_bytes=[10],
                    )
                ],
                int(DataSource.X): [
                    CompressedEntityBucket(
                        label=None,
                        time_bucket_ids=[time_bucket.id, time_bucket2.id],
                        sizes_bytes=[50, 100],
                    )
                ],
            }
        )
        compressed_index_2 = CompressedMinerIndex(
            sources={
                int(DataSource.X): [
                    CompressedEntityBucket(
                        label=None,
                        time_bucket_ids=[time_bucket.id, time_bucket2.id],
                        sizes_bytes=[50, 100],
                    )
                ],
            }
        )

        # Store the first index.
        self.test_storage.upsert_compressed_miner_index(
            compressed_index_1, "hotkey1", credibility=1.0
        )

        # Store the second index.
        self.test_storage.upsert_compressed_miner_index(
            compressed_index_2, "hotkey1", credibility=1.0
        )

        # Confirm we have only the last two buckets in the index.
        index = self.test_storage.read_miner_index("hotkey1")
        expected_buckets = [
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket.id,
                source=DataSource.X,
                label=None,
                size_bytes=50,
                scorable_bytes=50,
            ),
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket2.id,
                source=DataSource.X,
                label=None,
                size_bytes=100,
                scorable_bytes=100,
            ),
        ]
        self.assertEqual(
            index.scorable_data_entity_buckets,
            expected_buckets,
        )

    def test_roundtrip_large_miner_index(self):
        """Tests that we can roundtrip a large index via a compressed index."""

        labels = ["label1", "label2", "label3", "label4", None]

        # Create the DataEntityBuckets for the index.
        sources = [int(DataSource.REDDIT), int(DataSource.X.value)]
        num_time_buckets = (
            constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX
            // len(sources)
            // len(labels)
        )
        time_bucket_ids = list(range(1, num_time_buckets + 1))

        start = time.time()
        buckets_by_source = defaultdict(list)
        for source in sources:
            for label in labels:
                buckets_by_source[source].append(
                    CompressedEntityBucket(
                        label=label,
                        time_bucket_ids=time_bucket_ids[:],
                        sizes_bytes=[100 for i in range(len(time_bucket_ids))],
                    )
                )

        # Create the index containing the buckets.
        index = CompressedMinerIndex(sources=buckets_by_source)
        print(f"Created index in {time.time() - start} seconds")

        # Store the index.
        start = time.time()
        self.test_storage.upsert_compressed_miner_index(
            index, "hotkey1", credibility=1.0
        )
        print(f"Stored index in {time.time() - start} seconds")

        # Confirm we can read the index.
        start = time.time()
        scorable_index = self.test_storage.read_miner_index("hotkey1")
        print(f"Read index in {time.time() - start} seconds")

        self.assertIsNotNone(scorable_index)

        expected_index = ScorableMinerIndex.construct(
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=time_bucket_id,
                    source=source,
                    label=label,
                    size_bytes=100,
                    scorable_bytes=100,
                )
                for source in sources
                for label in labels
                for time_bucket_id in time_bucket_ids
            ],
            last_updated=dt.datetime.utcnow(),
        )

        # We can't assert equality because of the last_updated time.
        # Verify piece-wise equality instead.
        def sort_key(entity):
            return (
                entity.source,
                entity.label if entity.label else "NULL",
                entity.time_bucket_id,
            )

        sorted_got = sorted(scorable_index.scorable_data_entity_buckets, key=sort_key)
        sorted_expected = sorted(
            expected_index.scorable_data_entity_buckets, key=sort_key
        )

        # Using assertListEqual is very slow, so we perform our own element-wise comparison.
        for got, expected in zip(sorted_got, sorted_expected):
            self.assertEqual(got, expected)

        self.assertTrue(
            scorable_index.last_updated
            > (dt.datetime.utcnow() - dt.timedelta(minutes=1))
        )

    def test_many_large_indexes_perf(self):
        labels = [f"label{i}" for i in range(500000)]
        time_buckets = [i for i in range(1000, 10000)]
        miners = [f"hotkey{i}" for i in range(200)]

        for miner in miners:
            buckets_by_source = {
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label=label,
                        time_bucket_ids=random.sample(time_buckets, 1),
                        sizes_bytes=[i for i in range(1)],
                    )
                    for label in labels
                ],
                DataSource.X: [
                    CompressedEntityBucket(
                        label=label,
                        time_bucket_ids=random.sample(time_buckets, 1),
                        sizes_bytes=[i for i in range(1)],
                    )
                    for label in labels
                ],
            }
            index = CompressedMinerIndex(sources=buckets_by_source)
            start = time.time()
            self.test_storage.upsert_compressed_miner_index(
                index, miner, credibility=random.random()
            )
            print(f"Inserted index for miner {miner} in {time.time() - start}")

        for i in range(10):
            start = time.time()
            miner = random.choice(miners)
            index = self.test_storage.read_miner_index(miner)
            print(f"Read index for miner {miner} in {time.time() - start}")

    # def test_read_miner_index_with_duplicate_data_entity_buckets(self):
    #     """Tests that we can read (and score) a miner index when other miners have duplicate buckets."""
    #     # Create two DataEntityBuckets for the index.
    #     now = dt.datetime.utcnow()
    #     bucket_1_miner_1 = DataEntityBucket(
    #         id=DataEntityBucketId(
    #             time_bucket=TimeBucket.from_datetime(now),
    #             source=DataSource.REDDIT,
    #             label=DataLabel(value="label_1"),
    #         ),
    #         size_bytes=10,
    #     )

    #     bucket_1_miner_2 = DataEntityBucket(
    #         id=DataEntityBucketId(
    #             time_bucket=TimeBucket.from_datetime(now),
    #             source=DataSource.REDDIT,
    #             label=DataLabel(value="label_1"),
    #         ),
    #         size_bytes=40,
    #     )

    #     expected_bucket_1 = ScorableDataEntityBucket(
    #         time_bucket_id=utils.time_bucket_id_from_datetime(now),
    #         source=DataSource.REDDIT,
    #         label="label_1",
    #         size_bytes=10,
    #         scorable_bytes=2,
    #     )

    #     # Create the indexes containing the bucket.
    #     index_1 = MinerIndex(hotkey="hotkey1", data_entity_buckets=[bucket_1_miner_1])
    #     index_2 = MinerIndex(hotkey="hotkey2", data_entity_buckets=[bucket_1_miner_2])

    #     # Store the indexes.
    #     self.test_storage.upsert_miner_index(index_1)
    #     self.test_storage.upsert_miner_index(index_2)

    #     # Read the index.
    #     scored_index = self.test_storage.read_miner_index(
    #         "hotkey1", set(["hotkey1", "hotkey2"])
    #     )

    #     # Confirm the scored index matches expectations.
    #     self.assertEqual(
    #         scored_index.scorable_data_entity_buckets[0], expected_bucket_1
    #     )

    # def test_read_non_existing_miner_index(self):
    #     """Tests that we correctly return none for a non existing miner index."""
    #     # Read the index.
    #     scored_index = self.test_storage.read_miner_index("hotkey1", set(["hotkey1"]))

    #     # Confirm the scored_index is None.
    #     self.assertEqual(scored_index, None)

    # def test_delete_miner_index(self):
    #     """Tests that we can delete a miner index."""
    #     # Create two DataEntityBuckets for the indexes.
    #     now = dt.datetime.utcnow()
    #     bucket_1 = DataEntityBucket(
    #         id=DataEntityBucketId(
    #             time_bucket=TimeBucket.from_datetime(now),
    #             source=DataSource.REDDIT,
    #             label=DataLabel(value="label_1"),
    #         ),
    #         size_bytes=10,
    #     )

    #     bucket_2 = DataEntityBucket(
    #         id=DataEntityBucketId(
    #             time_bucket=TimeBucket.from_datetime(now), source=DataSource.X
    #         ),
    #         size_bytes=50,
    #     )

    #     # Create three indexes containing the buckets.
    #     index_1 = MinerIndex(hotkey="hotkey1", data_entity_buckets=[bucket_1, bucket_2])
    #     index_2 = MinerIndex(hotkey="hotkey2", data_entity_buckets=[bucket_1, bucket_2])
    #     index_3 = MinerIndex(hotkey="hotkey3", data_entity_buckets=[bucket_1, bucket_2])

    #     # Store the indexes.
    #     self.test_storage.upsert_miner_index(index_1)
    #     self.test_storage.upsert_miner_index(index_2)
    #     self.test_storage.upsert_miner_index(index_3)

    #     # Delete one index.
    #     self.test_storage._delete_miner_index("hotkey2")

    #     # Confirm we have four rows in the index.
    #     cursor = self.test_storage.connection.cursor(dictionary=True, buffered=True)
    #     cursor.execute("SELECT * FROM MinerIndex")
    #     self.assertEqual(cursor.rowcount, 4)

    # def test_read_miner_last_updated(self):
    #     """Tests getting the last time a miner was updated."""
    #     # Insert a miner
    #     now = dt.datetime.utcnow()
    #     now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    #     self.test_storage._upsert_miner("test_hotkey", now_str)

    #     # Get the last updated
    #     last_updated = self.test_storage.read_miner_last_updated("test_hotkey")

    #     # Confirm the last updated is None.
    #     self.assertEqual(now, last_updated)

    # def test_read_miner_last_updated_never_updated(self):
    #     """Tests getting the last time a miner was updated when it has never been updated."""
    #     # Insert a miner
    #     now = dt.datetime.utcnow()
    #     now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    #     self.test_storage._upsert_miner("test_hotkey", now_str)

    #     # Get the last updated
    #     last_updated = self.test_storage.read_miner_last_updated("test_hotkey2")

    #     # Confirm the last updated is None.
    #     self.assertEqual(None, last_updated)


if __name__ == "__main__":
    unittest.main()
