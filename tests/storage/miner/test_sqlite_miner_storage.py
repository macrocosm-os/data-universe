import contextlib
import time
import unittest
import os

from common import constants
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
import datetime as dt
import pytz

from tests import utils

from storage.miner.sqlite_miner_storage import SqliteMinerStorage


class TestSqliteMinerStorage(unittest.TestCase):
    def setUp(self):
        # Make a test database for the test to operate against.
        self.test_storage = SqliteMinerStorage(
            "TestDb.sqlite", max_database_size_gb_hint=1
        )

    def tearDown(self):
        # Clean up the test database.
        os.remove(self.test_storage.database)

    def test_instantiate_sqlite_miner_storage(self):
        # Just ensure the setUp/tearDown methods work.
        self.assertTrue(True)

    def test_store_entities(self):
        """Tests that we can successfully store entities"""
        now = dt.datetime.now()
        # Create an entity without a label.
        entity1 = DataEntity(
            uri="test_entity_1",
            datetime=now,
            source=DataSource.REDDIT,
            content=bytes(10),
            content_size_bytes=10,
        )

        # Create an entity with a label.
        entity2 = DataEntity(
            uri="test_entity_2",
            datetime=now,
            source=DataSource.X,
            content=bytes(20),
            content_size_bytes=20,
        )

        # Store the entities.
        self.test_storage.store_data_entities([entity1, entity2])

        # Confirm the entities were stored.
        with contextlib.closing(self.test_storage._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM DataEntity")
            rows = cursor.fetchone()[0]
            self.assertEqual(rows, 2)

    def test_store_identical_entities(self):
        """Tests that we can handle attempts to store the same entity"""
        now = dt.datetime.now()
        # Create an entity without a label.
        entity1 = DataEntity(
            uri="test_entity_1",
            datetime=now,
            source=DataSource.REDDIT,
            content=bytes(10),
            content_size_bytes=10,
        )

        # Create an entity with a label.
        entity2 = DataEntity(
            uri="test_entity_2",
            datetime=now,
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(20),
            content_size_bytes=20,
        )

        # Store the entities.
        self.test_storage.store_data_entities([entity1, entity2])

        # Update the contents
        entity1.content = bytes(50)
        entity1.content_size_bytes = 50
        entity2.content = bytes(100)
        entity2.content_size_bytes = 100

        # Store the entities again.
        self.test_storage.store_data_entities([entity1, entity2])

        # Confirm that only one set of entities were stored and the content matches the latest.
        with contextlib.closing(self.test_storage._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT SUM(contentSizeBytes) FROM DataEntity")
            size = cursor.fetchone()[0]
            self.assertEqual(size, 150)

    # TODO Consider storing what we can and discarding the rest.
    def test_store_over_max_content_size_fail(self):
        """Tests that we except on attempts to store entities larger than maximum storage in one store call"""
        now = dt.datetime.now()
        # Create an entity that is too large to store.
        large_entity = DataEntity(
            uri="large_entity",
            datetime=now,
            source=DataSource.REDDIT,
            content=bytes(1000),
            content_size_bytes=1.1 * 1024 * 1024 * 1024,
        )

        # Attempt to store the entity.
        with self.assertRaises(ValueError):
            self.test_storage.store_data_entities([large_entity])

    def test_store_over_max_content_size_succeeds(self):
        """Tests that we succeed on clearing space when going over maximum configured content storage."""
        now = dt.datetime.now()
        mb_400 = 400 * 1024 * 1024
        # Create three entities such that only two fit the maximum allowed content size.
        entity1 = DataEntity(
            uri="test_entity_1",
            datetime=now,
            source=DataSource.REDDIT,
            content=b"entity1",
            content_size_bytes=mb_400,
        )
        entity2 = DataEntity(
            uri="test_entity_2",
            datetime=now + dt.timedelta(hours=1),
            source=DataSource.REDDIT,
            content=b"entity2",
            content_size_bytes=mb_400,
        )
        entity3 = DataEntity(
            uri="test_entity_3",
            datetime=now + dt.timedelta(hours=2),
            source=DataSource.REDDIT,
            content=b"entity3",
            content_size_bytes=mb_400,
        )

        # Store two entities that take up all but 100 bytes of the maximum allowed.
        self.test_storage.store_data_entities([entity1, entity2])
        # Store the third entity that would go over the maximum allowed.
        self.test_storage.store_data_entities([entity3])
        # Confirm the oldest entity was deleted to make room.
        with contextlib.closing(self.test_storage._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT uri FROM DataEntity")
            uris = []
            for row in cursor:
                uris.append(row["uri"])

            self.assertEqual(uris, ["test_entity_2", "test_entity_3"])

    def test_get_compressed_index(self):
        """Tests that we can get the compressed miner index from storage."""
        now = dt.datetime.now()
        # Create an entity for bucket 1.
        bucket1_datetime = now
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=bytes(10),
            content_size_bytes=10,
        )

        # Create two entities for bucket 2.
        bucket2_datetime = now + dt.timedelta(hours=1)
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(20),
            content_size_bytes=20,
        )

        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(30),
            content_size_bytes=30,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Get the index.
        index = self.test_storage.get_compressed_index()

        expected_index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label="label_1",
                        time_bucket_ids=[TimeBucket.from_datetime(bucket1_datetime).id],
                        sizes_bytes=[10],
                    )
                ],
                DataSource.X: [
                    CompressedEntityBucket(
                        label="label_2",
                        time_bucket_ids=[TimeBucket.from_datetime(bucket2_datetime).id],
                        sizes_bytes=[50],
                    )
                ],
            }
        )

        # Confirm we get back the expected summary.
        self.assertTrue(utils.are_compressed_indexes_equal(index, expected_index))

    def test_get_compressed_index_no_labels(self):
        """Tests that we can get a compressed index with no labels from storage."""
        now = dt.datetime.now()
        # Create an entity for bucket 1.
        bucket1_datetime = now
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            content=bytes(10),
            content_size_bytes=10,
        )

        # Create two entities for bucket 2.
        bucket2_datetime = now + dt.timedelta(hours=1)
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            content=bytes(20),
            content_size_bytes=20,
        )

        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            content=bytes(30),
            content_size_bytes=30,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Get the index.
        index = self.test_storage.get_compressed_index()

        expected_index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label=None,
                        time_bucket_ids=[TimeBucket.from_datetime(bucket1_datetime).id],
                        sizes_bytes=[10],
                    )
                ],
                DataSource.X: [
                    CompressedEntityBucket(
                        label=None,
                        time_bucket_ids=[TimeBucket.from_datetime(bucket2_datetime).id],
                        sizes_bytes=[50],
                    )
                ],
            }
        )

        # Confirm we get back the expected summary.
        self.assertTrue(utils.are_compressed_indexes_equal(index, expected_index))

    def test_get_compressed_index_multiple_bucket_per_label(self):
        """Tests that we can get the compressed miner index when there are multiple buckets for a single label."""

        datetime = dt.datetime.now()

        # Store the entities, that share the same label across 3 different timeBucketIds
        self.test_storage.store_data_entities(
            [
                DataEntity(
                    uri="test_entity_1",
                    datetime=datetime,
                    source=DataSource.REDDIT,
                    label=DataLabel(value="label_1"),
                    content=bytes(10),
                    content_size_bytes=10,
                ),
                DataEntity(
                    uri="test_entity_2",
                    datetime=(datetime + dt.timedelta(minutes=61)),
                    source=DataSource.REDDIT,
                    label=DataLabel(value="label_1"),
                    content=bytes(20),
                    content_size_bytes=20,
                ),
                DataEntity(
                    uri="test_entity_3",
                    datetime=(datetime + dt.timedelta(minutes=122)),
                    source=DataSource.REDDIT,
                    label=DataLabel(value="label_1"),
                    content=bytes(30),
                    content_size_bytes=30,
                ),
            ]
        )

        # Get the index and check it's as expected.
        index = self.test_storage.get_compressed_index()
        expected_index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label="label_1",
                        time_bucket_ids=[
                            TimeBucket.from_datetime(
                                datetime + dt.timedelta(hours=i)
                            ).id
                            # Expect the buckets in size order descending.
                            for i in range(2, -1, -1)
                        ],
                        sizes_bytes=[30, 20, 10],
                    )
                ],
            }
        )

        # Confirm we get back the expected summary.
        self.assertTrue(utils.are_compressed_indexes_equal(index, expected_index))

    def test_get_compressed_index_buckets_too_old(self):
        """Tests that we can list the compressed index from storage, discarding out of date ones."""
        now = dt.datetime.now()
        # Create an entity for bucket 1.
        bucket1_datetime = now
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=bytes(10),
            content_size_bytes=10,
        )

        # Create two entities for bucket 2.
        bucket2_datetime = now - dt.timedelta(
            days=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS + 1
        )
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(20),
            content_size_bytes=20,
        )

        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(30),
            content_size_bytes=30,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Get the index.
        index = self.test_storage.get_compressed_index()

        expected_index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label="label_1",
                        time_bucket_ids=[TimeBucket.from_datetime(bucket1_datetime).id],
                        sizes_bytes=[10],
                    )
                ],
            }
        )

        # Confirm we get back the expected summary.
        self.assertTrue(utils.are_compressed_indexes_equal(index, expected_index))

    def test_get_compressed_index_empty_storage(self):
        """Tests that we can get a compressed index, when storage is empty."""
        index = self.test_storage.get_compressed_index()
        self.assertEqual(index, CompressedMinerIndex(sources={}))

    def test_list_entities_in_data_entity_bucket(self):
        """Tests that we can get all the enities in a data entity bucket"""
        # Create an entity for bucket 1.
        bucket1_datetime = dt.datetime(
            2023, 12, 12, 1, 30, 0, 1000, tzinfo=dt.timezone.utc
        )
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=bytes(10),
            content_size_bytes=10,
        )

        # Create two entities for bucket 2.
        bucket2_datetime = dt.datetime(
            2023,
            12,
            12,
            2,
            30,
            0,
            1000,
            tzinfo=pytz.timezone("America/Los_Angeles"),
        )
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(20),
            content_size_bytes=20,
        )

        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(30),
            content_size_bytes=30,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Create the DataEntityBucketId to query by.
        bucket2_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket2_datetime),
            source=DataSource.X,
            label=DataLabel(value="label_2"),
        )

        # Get the entities by the bucket
        data_entities = self.test_storage.list_data_entities_in_data_entity_bucket(
            bucket2_id
        )

        # Confirm we get back the expected data entities.
        self.assertEqual(data_entities, [bucket2_entity1, bucket2_entity2])

    def test_list_entities_in_data_entity_bucket_over_max_size(self):
        """Tests that we can get enough entities in an over max size data entity bucket"""
        # Create two entities for the bucket.
        bucket_datetime = dt.datetime(
            2023,
            12,
            12,
            2,
            30,
            0,
            1000,
            tzinfo=pytz.timezone("America/Los_Angeles"),
        )
        entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_1"),
            content=bytes(10),
            content_size_bytes=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES / 1.5,
        )

        entity2 = DataEntity(
            uri="test_entity_2",
            datetime=bucket_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_1"),
            content=bytes(20),
            content_size_bytes=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES / 1.5,
        )

        entity3 = DataEntity(
            uri="test_entity_2",
            datetime=bucket_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_1"),
            content=bytes(30),
            content_size_bytes=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES / 1.5,
        )

        # Store the entities.
        self.test_storage.store_data_entities([entity1, entity2, entity3])

        # Create the DataEntityBucketId to query by.
        bucket_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket_datetime),
            source=DataSource.X,
            label=DataLabel(value="label_1"),
        )

        # Get the entities by the bucket
        data_entities = self.test_storage.list_data_entities_in_data_entity_bucket(
            bucket_id
        )

        # Confirm we get back exactly two of the entities.
        self.assertEqual(len(data_entities), 2)

    def test_list_entities_in_data_entity_bucket_exactly_max_size(self):
        """Tests getting up to exactly the max size in a over max size data entity bucket"""
        # Create two entities for the bucket.
        bucket_datetime = dt.datetime(
            2023,
            12,
            12,
            2,
            30,
            0,
            1000,
            tzinfo=pytz.timezone("America/Los_Angeles"),
        )
        entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_1"),
            content=bytes(10),
            content_size_bytes=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES,
        )

        entity2 = DataEntity(
            uri="test_entity_2",
            datetime=bucket_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_1"),
            content=bytes(20),
            content_size_bytes=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES,
        )

        # Store the entities.
        self.test_storage.store_data_entities([entity1, entity2])

        # Create the DataEntityBucketId to query by.
        bucket_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket_datetime),
            source=DataSource.X,
            label=DataLabel(value="label_1"),
        )

        # Get the entities by the bucket
        data_entities = self.test_storage.list_data_entities_in_data_entity_bucket(
            bucket_id
        )

        # Confirm we get back exactly one of the entities.
        self.assertEqual(len(data_entities), 1)

    def test_list_data_entity_buckets(self):
        """Tests that we can list the data entity buckets from storage."""
        now = dt.datetime.now()
        # Create an entity for bucket 1.
        bucket1_datetime = now
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=bytes(10),
            content_size_bytes=10,
        )
        # Create two entities for bucket 2.
        bucket2_datetime = now + dt.timedelta(hours=1)
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(20),
            content_size_bytes=20,
        )
        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(30),
            content_size_bytes=30,
        )
        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Get the index.
        data_entity_buckets = self.test_storage.list_data_entity_buckets()

        expected_bucket_1 = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(bucket1_datetime),
                source=DataSource.REDDIT,
                label=DataLabel(value="label_1"),
            ),
            size_bytes=10,
        )

        expected_bucket_2 = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(bucket2_datetime),
                source=DataSource.X,
                label=DataLabel(value="label_2"),
            ),
            size_bytes=50,
        )

        # Confirm we get back the expected summaries in order of size.
        self.assertEqual(data_entity_buckets, [expected_bucket_2, expected_bucket_1])

    def test_list_data_entity_buckets_no_labels(self):
        """Tests that we can list the data entity buckets with no labels from storage."""
        now = dt.datetime.now()
        # Create an entity for bucket 1.
        bucket1_datetime = now
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            content=bytes(10),
            content_size_bytes=10,
        )
        # Create two entities for bucket 2.
        bucket2_datetime = now + dt.timedelta(hours=1)
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            content=bytes(20),
            content_size_bytes=20,
        )
        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            content=bytes(30),
            content_size_bytes=30,
        )
        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Get the index.
        data_entity_buckets = self.test_storage.list_data_entity_buckets()

        expected_bucket_1 = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(bucket1_datetime),
                source=DataSource.REDDIT,
            ),
            size_bytes=10,
        )

        expected_bucket_2 = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(bucket2_datetime),
                source=DataSource.X,
            ),
            size_bytes=50,
        )

        # Confirm we get back the expected summaries in order of size.
        self.assertEqual(data_entity_buckets, [expected_bucket_2, expected_bucket_1])

    def test_list_data_entity_buckets_too_old(self):
        """Tests that we can list the data entity buckets from storage, discarding out of date ones."""
        now = dt.datetime.now()
        # Create an entity for bucket 1.
        bucket1_datetime = now
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=bytes(10),
            content_size_bytes=10,
        )
        # Create two entities for bucket 2.
        bucket2_datetime = now - dt.timedelta(
            days=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS + 1
        )
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(20),
            content_size_bytes=20,
        )
        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(30),
            content_size_bytes=30,
        )
        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Get the index.
        data_entity_buckets = self.test_storage.list_data_entity_buckets()

        expected_bucket_1 = DataEntityBucket(
            id=DataEntityBucketId(
                time_bucket=TimeBucket.from_datetime(bucket1_datetime),
                source=DataSource.REDDIT,
                label=DataLabel(value="label_1"),
            ),
            size_bytes=10,
        )

        # Confirm we get back the expected summaries.
        self.assertEqual(data_entity_buckets, [expected_bucket_1])

    def test_cached_index(self):
        """Tests that the compressed miner index is cached."""
        now = dt.datetime.now()
        # Create an entity for bucket 1.
        bucket1_datetime = now
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=bytes(10),
            content_size_bytes=10,
        )

        # Create two entities for bucket 2.
        bucket2_datetime = now + dt.timedelta(hours=1)
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(20),
            content_size_bytes=20,
        )

        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_2"),
            content=bytes(30),
            content_size_bytes=30,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Check that there is no cache yet.
        self.assertIsNone(self.test_storage.cached_index_4)

        # Get the index twice.
        index = self.test_storage.get_compressed_index()
        cached_index = self.test_storage.get_compressed_index()

        expected_index = CompressedMinerIndex(
            sources={
                DataSource.REDDIT: [
                    CompressedEntityBucket(
                        label="label_1",
                        time_bucket_ids=[TimeBucket.from_datetime(bucket1_datetime).id],
                        sizes_bytes=[10],
                    )
                ],
                DataSource.X: [
                    CompressedEntityBucket(
                        label="label_2",
                        time_bucket_ids=[TimeBucket.from_datetime(bucket2_datetime).id],
                        sizes_bytes=[50],
                    )
                ],
            }
        )

        # Confirm we get back the expected summary.
        self.assertTrue(utils.are_compressed_indexes_equal(index, expected_index))
        # Confirm that the index is cached and that we it still matches on another get.
        self.assertIsNotNone(self.test_storage.cached_index_4)
        self.assertTrue(
            utils.are_compressed_indexes_equal(cached_index, expected_index)
        )

    def test_list_contents_in_data_entity_buckets_empty_bucket(self):
        """Tests getting back no contents from an empty bucket."""
        # Create the DataEntityBucketId to query by.
        empty_bucket_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(dt.datetime.now()),
            source=DataSource.X,
            label=DataLabel(value="bad_label"),
        )

        # Get the contents by the bucket
        buckets_to_contents = self.test_storage.list_contents_in_data_entity_buckets(
            [empty_bucket_id]
        )

        # Confirm we get back an empty map.
        self.assertEqual(len(buckets_to_contents), 0)

    def test_list_contents_in_data_entity_buckets_no_bucket(self):
        """Tests getting back no contents when not requesting a bucket."""
        # Get the contents by the bucket
        buckets_to_contents = self.test_storage.list_contents_in_data_entity_buckets([])

        # Confirm we get back an empty map.
        self.assertEqual(len(buckets_to_contents), 0)

    def test_list_contents_in_data_entity_buckets_too_many_buckets(self):
        """Tests getting back no contents when requesting for too many buckets."""
        bucket_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, tzinfo=dt.timezone.utc)
        time_bucket1 = TimeBucket.from_datetime(bucket_datetime)
        bucket_ids = [
            DataEntityBucketId(
                time_bucket=time_bucket1,
                source=DataSource.X,
                label=DataLabel(value=f"label_{i}"),
            )
            for i in range(constants.BULK_BUCKETS_COUNT_LIMIT + 1)
        ]

        content1 = bytes(10)
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_1"),
            content=content1,
            content_size_bytes=10,
        )

        # Store the entities.
        self.test_storage.store_data_entities([bucket1_entity1])

        # Get the contents by the buckets.
        buckets_to_contents = self.test_storage.list_contents_in_data_entity_buckets(
            bucket_ids
        )

        # Confirm we get back an empty map.
        self.assertEqual(len(buckets_to_contents), 0)

    def test_list_contents_in_data_entity_buckets_one_bucket(self):
        """Tests getting back contents from one bucket."""
        # Create an entity for bucket 1.
        bucket1_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, tzinfo=dt.timezone.utc)
        content1 = bytes(10)
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=content1,
            content_size_bytes=10,
        )

        # Store the entities.
        self.test_storage.store_data_entities([bucket1_entity1])

        # Create the DataEntityBucketId to query by.
        bucket1_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket1_datetime),
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
        )

        # Get the contents by the bucket
        buckets_to_contents = self.test_storage.list_contents_in_data_entity_buckets(
            [bucket1_id]
        )

        # Confirm we get back the expected content.
        self.assertEqual(
            buckets_to_contents[bucket1_id],
            [content1],
        )

    def test_list_contents_in_data_entity_buckets_two_buckets(self):
        """Tests getting back contents from two buckets."""
        # Create an entity for bucket 1.
        bucket1_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, tzinfo=dt.timezone.utc)
        content1 = bytes(10)
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=content1,
            content_size_bytes=10,
        )

        # Create two entities for bucket 2.
        bucket2_datetime = dt.datetime(
            2023,
            12,
            12,
            2,
            30,
            0,
            tzinfo=pytz.timezone("America/Los_Angeles"),
        )
        content2 = bytes(20)
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            label=None,
            content=content2,
            content_size_bytes=20,
        )

        content3 = bytes(30)
        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=None,
            content=content3,
            content_size_bytes=30,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Create the DataEntityBucketId to query by.
        bucket1_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket1_datetime),
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
        )

        bucket2_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket2_datetime),
            source=DataSource.X,
            label=None,
        )

        # Get the entities by the bucket
        buckets_to_entities = self.test_storage.list_contents_in_data_entity_buckets(
            [bucket1_id, bucket2_id]
        )

        # Confirm we get back the expected contents.
        self.assertEqual(
            buckets_to_entities[bucket1_id],
            [content1],
        )
        self.assertEqual(
            buckets_to_entities[bucket2_id],
            [content2, content3],
        )

    def test_list_contents_in_data_entity_buckets_same_time_bucket(
        self,
    ):
        """Tests getting back contents from two entity buckets with the same time bucket."""
        # Create an entity for bucket 1.
        bucket1_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, tzinfo=dt.timezone.utc)
        content1 = bytes(10)
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=content1,
            content_size_bytes=10,
        )

        # Create two entities for bucket 2.
        content2 = bytes(20)
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket1_datetime,
            source=DataSource.X,
            label=None,
            content=content2,
            content_size_bytes=20,
        )

        content3 = bytes(30)
        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket1_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=None,
            content=content3,
            content_size_bytes=30,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Create the DataEntityBucketId to query by.
        bucket1_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket1_datetime),
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
        )

        bucket2_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket1_datetime),
            source=DataSource.X,
            label=None,
        )

        # Get the entities by the bucket
        buckets_to_entities = self.test_storage.list_contents_in_data_entity_buckets(
            [bucket1_id, bucket2_id]
        )

        # Confirm we get back the expected contents.
        self.assertEqual(
            buckets_to_entities[bucket1_id],
            [content1],
        )
        self.assertEqual(
            buckets_to_entities[bucket2_id],
            [content2, content3],
        )

    def test_list_contents_in_data_entity_buckets_same_label(self):
        """Tests getting back contents from two entity buckets with the same label."""
        # Create an entity for bucket 1.
        bucket1_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, tzinfo=dt.timezone.utc)
        content1 = bytes(10)
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=content1,
            content_size_bytes=10,
        )

        # Create two entities for bucket 2.
        bucket2_datetime = dt.datetime(
            2023,
            12,
            12,
            2,
            30,
            0,
            tzinfo=pytz.timezone("America/Los_Angeles"),
        )
        content2 = bytes(20)
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.X,
            label=DataLabel(value="label_1"),
            content=content2,
            content_size_bytes=20,
        )

        content3 = bytes(30)
        bucket2_entity2 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime + dt.timedelta(seconds=1),
            source=DataSource.X,
            label=DataLabel(value="label_1"),
            content=content3,
            content_size_bytes=30,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket2_entity2]
        )

        # Create the DataEntityBucketId to query by.
        bucket1_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket1_datetime),
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
        )

        bucket2_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket2_datetime),
            source=DataSource.X,
            label=DataLabel(value="label_1"),
        )

        # Get the entities by the bucket
        buckets_to_entities = self.test_storage.list_contents_in_data_entity_buckets(
            [bucket1_id, bucket2_id]
        )

        # Confirm we get back the expected contents.
        self.assertEqual(
            buckets_to_entities[bucket1_id],
            [content1],
        )
        self.assertEqual(
            buckets_to_entities[bucket2_id],
            [content2, content3],
        )

    def test_list_contents_in_data_entity_buckets_overlapping(self):
        """Tests getting back contents does not get unspecified buckets with otherwise matching fields."""
        label1 = DataLabel(value="label_1")
        label2 = DataLabel(value="label_2")
        bucket1_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, tzinfo=dt.timezone.utc)
        bucket2_datetime = dt.datetime(2024, 1, 2, 3, 30, 0, tzinfo=dt.timezone.utc)

        # Create an entity for bucket 1.
        content1 = bytes(10)
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=label1,
            content=content1,
            content_size_bytes=10,
        )

        # Create an entity for bucket 2.
        content2 = bytes(20)
        bucket2_entity1 = DataEntity(
            uri="test_entity_2",
            datetime=bucket2_datetime,
            source=DataSource.REDDIT,
            label=label2,
            content=content2,
            content_size_bytes=20,
        )

        # Create an entity for bucket 3 using the label of bucket 1 but the time bucket of bucket 2.
        content3 = bytes(30)
        bucket3_entity1 = DataEntity(
            uri="test_entity_3",
            datetime=bucket2_datetime,
            source=DataSource.REDDIT,
            label=label1,
            content=content3,
            content_size_bytes=30,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket2_entity1, bucket3_entity1]
        )

        # Create the DataEntityBucketId to query by.
        bucket1_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket1_datetime),
            source=DataSource.REDDIT,
            label=label1,
        )

        bucket2_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket2_datetime),
            source=DataSource.REDDIT,
            label=label2,
        )

        # Get the entities by the bucket
        buckets_to_entities = self.test_storage.list_contents_in_data_entity_buckets(
            [bucket1_id, bucket2_id]
        )

        # Confirm we get back the expected contents.
        self.assertEqual(
            buckets_to_entities[bucket1_id],
            [content1],
        )
        self.assertEqual(
            buckets_to_entities[bucket2_id],
            [content2],
        )

    def test_list_contents_in_data_entity_buckets_over_size(self):
        """Tests getting back enough obfuscated data entities from one over-size bucket."""
        # Create an entity for bucket 1.
        bucket1_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, tzinfo=dt.timezone.utc)
        content1 = bytes(10)
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=content1,
            content_size_bytes=constants.BULK_CONTENTS_SIZE_LIMIT_BYTES / 1.5,
        )
        content2 = bytes(10)
        bucket1_entity2 = DataEntity(
            uri="test_entity_2",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=content2,
            content_size_bytes=constants.BULK_CONTENTS_SIZE_LIMIT_BYTES / 1.5,
        )
        content3 = bytes(10)
        bucket1_entity3 = DataEntity(
            uri="test_entity_3",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=content3,
            content_size_bytes=constants.BULK_CONTENTS_SIZE_LIMIT_BYTES / 1.5,
        )

        # Store the entities.
        self.test_storage.store_data_entities(
            [bucket1_entity1, bucket1_entity2, bucket1_entity3]
        )

        # Create the DataEntityBucketId to query by.
        bucket1_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket1_datetime),
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
        )

        # Get the entities by the bucket
        buckets_to_entities = self.test_storage.list_contents_in_data_entity_buckets(
            [bucket1_id]
        )

        # Confirm we get back only the two contents.
        self.assertEqual(
            len(buckets_to_entities[bucket1_id]),
            2,
        )

    def test_list_contents_in_data_entity_buckets_exactly_max_size(self):
        """Tests getting back obfuscated data entities up to exactly the max size."""
        # Create an entity for bucket 1.
        bucket1_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, tzinfo=dt.timezone.utc)
        content1 = bytes(10)
        bucket1_entity1 = DataEntity(
            uri="test_entity_1",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=content1,
            content_size_bytes=constants.BULK_CONTENTS_SIZE_LIMIT_BYTES,
        )
        content2 = bytes(10)
        bucket1_entity2 = DataEntity(
            uri="test_entity_2",
            datetime=bucket1_datetime,
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
            content=content2,
            content_size_bytes=constants.BULK_CONTENTS_SIZE_LIMIT_BYTES,
        )

        # Store the entities.
        self.test_storage.store_data_entities([bucket1_entity1, bucket1_entity2])

        # Create the DataEntityBucketId to query by.
        bucket1_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket1_datetime),
            source=DataSource.REDDIT,
            label=DataLabel(value="label_1"),
        )

        # Get the entities by the bucket
        buckets_to_entities = self.test_storage.list_contents_in_data_entity_buckets(
            [bucket1_id]
        )

        # Confirm we get back only one contents.
        self.assertEqual(
            len(buckets_to_entities[bucket1_id]),
            1,
        )

    @unittest.skip("Skip the max list contents test by default.")
    def test_list_contents_in_data_entity_buckets_max_size(self):
        """Tests getting back a maximum size list of contents."""
        bytes_per_content = (
            constants.BULK_CONTENTS_SIZE_LIMIT_BYTES
            // constants.BULK_CONTENTS_COUNT_LIMIT
        )

        # Use just two buckets for easy of querying.
        bucket1_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, tzinfo=dt.timezone.utc)
        bucket2_datetime = dt.datetime(2024, 1, 2, 3, 30, 0, tzinfo=dt.timezone.utc)
        source1 = DataSource.REDDIT
        source2 = DataSource.X
        label1 = DataLabel(value="label_1")
        label2 = DataLabel(value="label_2")

        creation_start = time.time()
        entities = [
            DataEntity(
                uri=f"test_entity_{i}",
                datetime=bucket1_datetime if i % 2 else bucket2_datetime,
                source=source1 if i % 2 else source2,
                label=label1 if i % 2 else label2,
                content=bytes(bytes_per_content),
                content_size_bytes=bytes_per_content,
            )
            for i in range(constants.BULK_CONTENTS_COUNT_LIMIT)
        ]
        print(
            f"Finished instantiating {constants.BULK_CONTENTS_COUNT_LIMIT} entities in {time.time() - creation_start}"
        )

        # Store the entities.
        store_start = time.time()
        self.test_storage.store_data_entities(entities)
        print(
            f"Finished storing {constants.BULK_CONTENTS_COUNT_LIMIT} entities in {time.time() - store_start}"
        )

        # Create the DataEntityBucketIds to query by.
        bucket1_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket1_datetime),
            source=source1,
            label=label1,
        )

        bucket2_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket2_datetime),
            source=source2,
            label=label2,
        )

        # Get the entities by the bucket
        list_start = time.time()
        buckets_to_entities = self.test_storage.list_contents_in_data_entity_buckets(
            [bucket1_id, bucket2_id]
        )
        print(
            f"Finished listing {constants.BULK_CONTENTS_COUNT_LIMIT} entities in {time.time() - list_start}"
        )

        # Confirm we get the right number of entites
        self.assertEqual(
            len(buckets_to_entities[bucket1_id]) + len(buckets_to_entities[bucket2_id]),
            constants.BULK_CONTENTS_COUNT_LIMIT,
        )


if __name__ == "__main__":
    unittest.main()
