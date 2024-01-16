import contextlib
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
        entity1_copy = entity1.model_copy(
            update={"content": bytes(50), "content_size_bytes": 50}, deep=True
        )
        entity2_copy = entity2.model_copy(
            update={"content": bytes(100), "content_size_bytes": 100}, deep=True
        )

        # Store the entities again.
        self.test_storage.store_data_entities([entity1_copy, entity2_copy])

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
            content_size_bytes=int(1.1 * 1024 * 1024 * 1024),
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


if __name__ == "__main__":
    unittest.main()
