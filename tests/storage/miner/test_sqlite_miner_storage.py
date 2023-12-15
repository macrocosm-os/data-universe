import contextlib
import unittest
import os
from common import constants
from common.data import (
    DataEntityBucket,
    DataEntity,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
)
import datetime as dt
import pytz

from storage.miner.sqlite_miner_storage import SqliteMinerStorage


class TestSqliteMinerStorage(unittest.TestCase):
    def setUp(self):
        # Make a test database for the test to operate against.
        self.test_storage = SqliteMinerStorage("TestDb.sqlite", 500)

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
        self.test_storage.store_data_entities([entity1, entity2])

        # Confirm that only one set of entities were stored.
        with contextlib.closing(self.test_storage._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM DataEntity")
            rows = cursor.fetchone()[0]
            self.assertEqual(rows, 2)

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
            content_size_bytes=1000,
        )

        # Attempt to store the entity.
        with self.assertRaises(ValueError):
            self.test_storage.store_data_entities([large_entity])

    def test_store_over_max_content_size_succeeds(self):
        """Tests that we succeed on clearing space when going over maximum configured content storage."""
        now = dt.datetime.now()
        # Create three entities such that only two fit the maximum allowed content size.
        entity1 = DataEntity(
            uri="test_entity_1",
            datetime=now,
            source=DataSource.REDDIT,
            content=bytes(200),
            content_size_bytes=200,
        )
        entity2 = DataEntity(
            uri="test_entity_2",
            datetime=now + dt.timedelta(hours=1),
            source=DataSource.REDDIT,
            content=bytes(10),
            content_size_bytes=200,
        )
        entity3 = DataEntity(
            uri="test_entity_3",
            datetime=now + dt.timedelta(hours=2),
            source=DataSource.REDDIT,
            content=bytes(200),
            content_size_bytes=200,
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

        # Confirm we get back the expected summaries.
        self.assertEqual(data_entity_buckets, [expected_bucket_1, expected_bucket_2])

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

        # Confirm we get back the expected summaries.
        self.assertEqual(data_entity_buckets, [expected_bucket_1, expected_bucket_2])

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

        # Create the DataEntityBucketId to query by.
        bucket2_id = DataEntityBucketId(
            time_bucket=TimeBucket.from_datetime(bucket2_datetime), source=DataSource.X
        )

        # Get the entities by the bucket
        data_entities = self.test_storage.list_data_entities_in_data_entity_bucket(
            bucket2_id
        )

        # Confirm we get back the expected data entities.
        self.assertEqual(data_entities, [bucket2_entity1, bucket2_entity2])


if __name__ == "__main__":
    unittest.main()
