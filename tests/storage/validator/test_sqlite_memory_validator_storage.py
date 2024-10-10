from collections import defaultdict
import contextlib
import random
import time
from typing import Dict, List
import unittest
import concurrent

from common import constants, utils
from common.constants import DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntityBucket,
    DataEntity,
    DataEntityBucketId,
    DataLabel,
    DataSource,
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

        with contextlib.closing(self.test_storage._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT credibility FROM MINER WHERE hotkey='test_hotkey'")
            credibility = cursor.fetchone()[0]
            self.assertEqual(credibility, 0.5)

        # Finally, insert a new miner and make sure it has a new id.
        other_id = self.test_storage._upsert_miner(
            "other_hotkey", dt.datetime.utcnow(), credibility=0.5
        )
        self.assertNotEqual(other_id, miner_id)

    def test_upsert_compressed_miner_index_insert_index_special_characters(self):
        """Tests that we can insert a miner index, including a label with a special character."""
        # Create 3 DataEntityBuckets for the index.
        now = dt.datetime.utcnow()
        time_bucket = TimeBucket.from_datetime(now)

        # Create the index containing the buckets.
        hotkey = "hotkey1"
        index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT.value: [
                    CompressedEntityBucket(
                        label="#𝙅𝙚𝙬𝙚𝙡𝙧𝙮",
                        time_bucket_ids=[time_bucket.id],
                        sizes_bytes=[10],
                    )
                ],
                DataSource.X.value: [
                    CompressedEntityBucket(
                        label=None,
                        time_bucket_ids=[time_bucket.id],
                        sizes_bytes=[50],
                    ),
                    CompressedEntityBucket(
                        label="#🌌",
                        time_bucket_ids=[time_bucket.id],
                        sizes_bytes=[200],
                    ),
                ],
            }
        )

        # Store the index.
        self.test_storage.upsert_compressed_miner_index(index, hotkey, credibility=1.0)

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
                ScorableDataEntityBucket(
                    time_bucket_id=time_bucket.id,
                    source=DataSource.X,
                    label="#🌌",
                    size_bytes=200,
                    scorable_bytes=200,
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

    # TODO: Add a test to insert millions of rows across many miners and check the total memory size.

    def test_upsert_compressed_miner_index_insert_index_with_duplicates(self):
        """Tests that we can insert a miner index with duplicates, taking the last one."""
        # Create two identical DataEntityBuckets for the index.
        now = dt.datetime.utcnow()
        time_bucket = TimeBucket.from_datetime(now)
        bucket_1 = CompressedEntityBucket(
            label="label_1",
            time_bucket_ids=[time_bucket.id],
            sizes_bytes=[10],
        )

        # Create the index containing the buckets.
        hotkey = "hotkey1"
        index = CompressedMinerIndex(
            sources={DataSource.REDDIT.value: [bucket_1, bucket_1]}
        )

        # Store the index.
        self.test_storage.upsert_compressed_miner_index(index, hotkey, credibility=1.0)

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
            constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4
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

    def test_read_miner_index_with_duplicate_data_entity_buckets(self):
        """Tests that we can read (and score) a miner index when other miners have duplicate buckets."""
        # Create two DataEntityBuckets for the index.
        now = dt.datetime.utcnow()
        hotkey1 = "hotkey1"
        hotkey2 = "hotkey2"
        bucket_1_miner_1 = CompressedEntityBucket(
            label="label_1",
            time_bucket_ids=[TimeBucket.from_datetime(now).id],
            sizes_bytes=[10],
        )
        bucket_1_miner_2 = CompressedEntityBucket(
            label="label_1",
            time_bucket_ids=[TimeBucket.from_datetime(now).id],
            sizes_bytes=[40],
        )

        expected_bucket_1 = ScorableDataEntityBucket(
            time_bucket_id=utils.time_bucket_id_from_datetime(now),
            source=DataSource.REDDIT,
            label="label_1",
            size_bytes=10,
            scorable_bytes=2,
        )

        # Create the indexes containing the bucket.
        index_1 = CompressedMinerIndex(
            sources={DataSource.REDDIT.value: [bucket_1_miner_1]}
        )
        index_2 = CompressedMinerIndex(
            sources={DataSource.REDDIT.value: [bucket_1_miner_2]}
        )

        # Store the indexes.
        self.test_storage.upsert_compressed_miner_index(index_1, hotkey1, 1)
        self.test_storage.upsert_compressed_miner_index(index_2, hotkey2, 1)

        # Read the index.
        scored_index = self.test_storage.read_miner_index(hotkey1)

        # Confirm the scored index matches expectations.
        self.assertEqual(
            scored_index.scorable_data_entity_buckets[0], expected_bucket_1
        )

    def test_read_non_existing_miner_index(self):
        """Tests that we correctly return none for a non existing miner index."""
        # Read the index.
        scored_index = self.test_storage.read_miner_index("hotkey1")

        # Confirm the scored_index is None.
        self.assertEqual(scored_index, None)

    def test_delete_miner(self):
        """Tests that we can delete a miner."""
        # Create two DataEntityBuckets for the indexes.
        now = dt.datetime.utcnow()
        index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT.value: [
                    CompressedEntityBucket(
                        label="label_1",
                        time_bucket_ids=[TimeBucket.from_datetime(now).id],
                        sizes_bytes=[10],
                    )
                ],
                DataSource.X.value: [
                    CompressedEntityBucket(
                        label=None,
                        time_bucket_ids=[TimeBucket.from_datetime(now).id],
                        sizes_bytes=[50],
                    )
                ],
            }
        )

        # Store the indexes.
        self.test_storage.upsert_compressed_miner_index(index, "hotkey1", 1)
        self.test_storage.upsert_compressed_miner_index(index, "hotkey2", 1)
        self.test_storage.upsert_compressed_miner_index(index, "hotkey3", 1)

        # Delete one miner.
        self.test_storage.delete_miner("hotkey2")

        # Confirm we have only two indexes left.
        self.assertIsNotNone(self.test_storage.read_miner_index("hotkey1"))
        self.assertIsNone(self.test_storage.read_miner_index("hotkey2"))
        self.assertIsNotNone(self.test_storage.read_miner_index("hotkey3"))

    def test_read_miner_last_updated(self):
        """Tests getting the last time a miner was updated."""
        # Insert a miner
        now = dt.datetime.utcnow()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        self.test_storage._upsert_miner("test_hotkey", now_str, 1)

        # Get the last updated
        last_updated = self.test_storage.read_miner_last_updated("test_hotkey")

        # Confirm the last updated is None.
        self.assertEqual(now, last_updated)

    def test_read_miner_last_updated_never_updated(self):
        """Tests getting the last time a miner was updated when it has never been updated."""
        # Insert a miner
        now = dt.datetime.utcnow()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        self.test_storage._upsert_miner("test_hotkey", now_str, 1)

        # Get the last updated
        last_updated = self.test_storage.read_miner_last_updated("test_hotkey2")

        # Confirm the last updated is None.
        self.assertEqual(None, last_updated)

    @unittest.skip("Skip the multi threaded test by default.")
    def test_multithreaded_inserts(self):
        """In a multi-threaded environment, insert 5 indexes for 5 miners, then read them back and verify they're correct."""

        def _create_index(
            labels: List[str], time_buckets: List[int]
        ) -> CompressedMinerIndex:
            """Creates a MinerIndex with the given labels and time buckets."""
            return CompressedMinerIndex(
                sources={
                    DataSource.REDDIT: [
                        CompressedEntityBucket(
                            label=label,
                            time_bucket_ids=time_buckets,
                            sizes_bytes=[100 for i in range(len(time_buckets))],
                        )
                        for label in labels
                    ],
                    DataSource.X: [
                        CompressedEntityBucket(
                            label=label,
                            time_bucket_ids=time_buckets,
                            sizes_bytes=[50 for i in range(len(time_buckets))],
                        )
                        for label in labels
                    ],
                }
            )

        # Create a large index to be shared by 2 miners.
        labels1 = [None] + [f"label{i}" for i in range(4_999)]
        time_buckets1 = [i for i in range(1, 101)]
        index1 = _create_index(labels1, time_buckets1)

        # Create a second large index to be shared by 3 miners.
        labels2 = [f"label{i}" for i in range(5_000, 10_000)]
        time_buckets2 = [i for i in range(1, 101)]
        index2 = _create_index(labels2, time_buckets2)

        index1_miners = [f"hotkey{i}" for i in range(5)]
        index2_miners = [f"hotkey{i}" for i in range(5, 8)]

        # On 3 threads, insert the 5 indexes, twice each.
        def _insert_index(index: CompressedMinerIndex, hotkey: str):
            self.test_storage.upsert_compressed_miner_index(
                index, hotkey, credibility=1.0
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for miner in index1_miners:
                futures.append(executor.submit(_insert_index, index1, miner))
                futures.append(executor.submit(_insert_index, index1, miner))
            for miner in index2_miners:
                futures.append(executor.submit(_insert_index, index2, miner))
                futures.append(executor.submit(_insert_index, index2, miner))

            concurrent.futures.wait(futures)
            for future in futures:
                if future.exception() is not None:
                    raise future.exception()

        # Construct the expected indexes. The first index is shared by 2 miners, the second by 3.
        expected_index1 = [
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket_id,
                source=DataSource.REDDIT,
                label=label,
                size_bytes=100,
                scorable_bytes=20,
            )
            for time_bucket_id in time_buckets1
            for label in labels1
        ] + [
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket_id,
                source=DataSource.X,
                label=label,
                size_bytes=50,
                scorable_bytes=10,
            )
            for time_bucket_id in time_buckets1
            for label in labels1
        ]
        expected_index2 = [
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket_id,
                source=DataSource.REDDIT,
                label=label,
                size_bytes=100,
                scorable_bytes=33,
            )
            for time_bucket_id in time_buckets2
            for label in labels2
        ] + [
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket_id,
                source=DataSource.X,
                label=label,
                size_bytes=50,
                scorable_bytes=16,
            )
            for time_bucket_id in time_buckets2
            for label in labels2
        ]

        def _is_index_as_expected(
            index: ScorableMinerIndex, expected_index: List[ScorableDataEntityBucket]
        ):
            """Returns True if the index contains the expected buckets, False otherwise."""

            # We can't assert equality because of the last_updated time.
            # Verify piece-wise equality instead.
            def sort_key(entity):
                return (
                    entity.source,
                    entity.label if entity.label else "NULL",
                    entity.time_bucket_id,
                )

            sorted_got = sorted(index.scorable_data_entity_buckets, key=sort_key)
            sorted_expected = sorted(expected_index, key=sort_key)
            for got, expected in zip(sorted_got, sorted_expected):
                if got != expected:
                    print(f"Got: {got} != Expected: {expected}")
                    return False
            return True

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for miner in index1_miners:
                futures.append(
                    executor.submit(
                        self.assertTrue(
                            _is_index_as_expected(
                                self.test_storage.read_miner_index(miner),
                                expected_index1,
                            )
                        )
                    )
                )
            for miner in index2_miners:
                futures.append(
                    executor.submit(
                        self.assertTrue(
                            _is_index_as_expected(
                                self.test_storage.read_miner_index(miner),
                                expected_index2,
                            )
                        )
                    )
                )

            print("Waiting to verify indexes...")
            concurrent.futures.wait(futures)
            print("Verification complete")

    @unittest.skip("Skip the large index test by default.")
    def test_many_large_indexes_perf(self):
        """Inserts 200 miners with maximal indexes and reads them back."""
        max_buckets = DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4

        labels = [f"label{i}" for i in range(100_000)]
        time_buckets = [i for i in range(1000, 10_000)]
        miners = [f"hotkey{i}" for i in range(200)]

        for miner in miners:
            # Split max buckets equaly between sources with reddit having 100 time buckets and x having 500.
            buckets_by_source = {
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label=label,
                        time_bucket_ids=random.sample(time_buckets, 100),
                        sizes_bytes=[i for i in range(100)],
                    )
                    for label in random.sample(labels, int(max_buckets / 2 / 100))
                ],
                DataSource.X: [
                    CompressedEntityBucket(
                        label=label,
                        time_bucket_ids=random.sample(time_buckets, 500),
                        sizes_bytes=[i for i in range(500)],
                    )
                    for label in random.sample(labels, int(max_buckets / 2 / 500))
                ],
            }
            index = CompressedMinerIndex(sources=buckets_by_source)
            start = time.time()
            self.test_storage.upsert_compressed_miner_index(
                index, miner, credibility=random.random()
            )
            print(
                f"Inserted index of {CompressedMinerIndex.bucket_count(index)} buckets for miner {miner} in {time.time() - start}"
            )

        for i in range(10):
            start = time.time()
            miner = random.choice(miners)
            index = self.test_storage.read_miner_index(miner)
            print(f"Read index for miner {miner} in {time.time() - start}")


if __name__ == "__main__":
    unittest.main()
