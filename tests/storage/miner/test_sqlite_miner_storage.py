import unittest
import os
from common.data import DataChunkSummary, DataEntity, DataLabel, DataSource, TimeBucket
import datetime as dt

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
                    content_size_bytes=10)

        # Create an entity with a label.
        entity2 = DataEntity(
                    uri="test_entity_2",
                    datetime=now,
                    source=DataSource.X,
                    content=bytes(20),
                    content_size_bytes=20)

        # Store the entities.
        self.test_storage.store_data_entities([entity1, entity2])

        # Confirm the entities were stored.
        cursor = self.test_storage.connection.cursor()
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
                    content_size_bytes=10)

        # Create an entity with a label.
        entity2 = DataEntity(
                    uri="test_entity_2",
                    datetime=now,
                    source=DataSource.X,
                    label=DataLabel(value="label_2"),
                    content=bytes(20),
                    content_size_bytes=20)

        # Store the entities.
        self.test_storage.store_data_entities([entity1, entity2])
        self.test_storage.store_data_entities([entity1, entity2])

        # Confirm that only one set of entities were stored.
        cursor = self.test_storage.connection.cursor()
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
                    content_size_bytes=1000)

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
                    content_size_bytes=200)
        entity2 = DataEntity(
                    uri="test_entity_2",
                    datetime=now+dt.timedelta(hours=1),
                    source=DataSource.REDDIT,
                    content=bytes(10),
                    content_size_bytes=200)
        entity3 = DataEntity(
                    uri="test_entity_3",
                    datetime=now+dt.timedelta(hours=2),
                    source=DataSource.REDDIT,
                    content=bytes(200),
                    content_size_bytes=200)

        # Store two entities that take up all but 100 bytes of the maximum allowed.
        self.test_storage.store_data_entities([entity1, entity2])
        # Store the third entity that would go over the maximum allowed.
        self.test_storage.store_data_entities([entity3])

        # Confirm the oldest entity was deleted to make room.
        cursor = self.test_storage.connection.cursor()
        cursor.execute("SELECT uri FROM DataEntity")
        uris = []
        for row in cursor:
            uris.append(row["uri"])

        self.assertEqual(uris, ["test_entity_2", "test_entity_3"])


    def test_list_data_chunk_summaries(self):
        """Tests that we can list the data chunk summaries from storage."""
        now = dt.datetime.now()
        # Create an entity for chunk 1.
        chunk1_datetime = now
        chunk1_entity1 = DataEntity(
                            uri="test_entity_1",
                            datetime=chunk1_datetime,
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            content=bytes(10),
                            content_size_bytes=10)

        # Create two entities for chunk 2.
        chunk2_datetime = now + dt.timedelta(hours=1)
        chunk2_entity1 = DataEntity(
                            uri="test_entity_2",
                            datetime=chunk2_datetime,
                            source=DataSource.X,
                            label=DataLabel(value="label_2"),
                            content=bytes(20),
                            content_size_bytes=20)

        chunk2_entity2 = DataEntity(
                            uri="test_entity_3",
                            datetime=chunk2_datetime + dt.timedelta(seconds=1),
                            source=DataSource.X,
                            label=DataLabel(value="label_2"),
                            content=bytes(30),
                            content_size_bytes=30)

        # Store the entities.
        self.test_storage.store_data_entities([chunk1_entity1, chunk2_entity1, chunk2_entity2])

        # Get the index.
        data_chunk_summaries = self.test_storage.list_data_chunk_summaries()

        expected_chunk_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(chunk1_datetime),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10)

        expected_chunk_2 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(chunk2_datetime),
                            source=DataSource.X,
                            label=DataLabel(value="label_2"),
                            size_bytes=50)

        # Confirm we get back the expected summaries.
        self.assertEqual(data_chunk_summaries, [expected_chunk_1, expected_chunk_2])

    def test_list_data_chunk_summaries_no_labels(self):
        """Tests that we can list the data chunk summaries with no labels from storage."""
        now = dt.datetime.now()
        # Create an entity for chunk 1.
        chunk1_datetime = now
        chunk1_entity1 = DataEntity(
                            uri="test_entity_1",
                            datetime=chunk1_datetime,
                            source=DataSource.REDDIT,
                            content=bytes(10),
                            content_size_bytes=10)

        # Create two entities for chunk 2.
        chunk2_datetime = now + dt.timedelta(hours=1)
        chunk2_entity1 = DataEntity(
                            uri="test_entity_2",
                            datetime=chunk2_datetime,
                            source=DataSource.X,
                            content=bytes(20),
                            content_size_bytes=20)

        chunk2_entity2 = DataEntity(
                            uri="test_entity_3",
                            datetime=chunk2_datetime + dt.timedelta(seconds=1),
                            source=DataSource.X,
                            content=bytes(30),
                            content_size_bytes=30)

        # Store the entities.
        self.test_storage.store_data_entities([chunk1_entity1, chunk2_entity1, chunk2_entity2])

        # Get the index.
        data_chunk_summaries = self.test_storage.list_data_chunk_summaries()

        expected_chunk_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(chunk1_datetime),
                            source=DataSource.REDDIT,
                            size_bytes=10)

        expected_chunk_2 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(chunk2_datetime),
                            source=DataSource.X,
                            size_bytes=50)

        # Confirm we get back the expected summaries.
        self.assertEqual(data_chunk_summaries, [expected_chunk_1, expected_chunk_2])

    def test_list_data_chunk_summaries_too_old(self):
        """Tests that we can list the data chunk summaries from storage, discarding out of date ones."""
        now = dt.datetime.now()
        # Create an entity for chunk 1.
        chunk1_datetime = now
        chunk1_entity1 = DataEntity(
                            uri="test_entity_1",
                            datetime=chunk1_datetime,
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            content=bytes(10),
                            content_size_bytes=10)

        # Create two entities for chunk 2.
        chunk2_datetime = now - dt.timedelta(days=8)
        chunk2_entity1 = DataEntity(
                            uri="test_entity_2",
                            datetime=chunk2_datetime,
                            source=DataSource.X,
                            label=DataLabel(value="label_2"),
                            content=bytes(20),
                            content_size_bytes=20)

        chunk2_entity2 = DataEntity(
                            uri="test_entity_3",
                            datetime=chunk2_datetime + dt.timedelta(seconds=1),
                            source=DataSource.X,
                            label=DataLabel(value="label_2"),
                            content=bytes(30),
                            content_size_bytes=30)

        # Store the entities.
        self.test_storage.store_data_entities([chunk1_entity1, chunk2_entity1, chunk2_entity2])

        # Get the index.
        data_chunk_summaries = self.test_storage.list_data_chunk_summaries()

        expected_chunk_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(chunk1_datetime),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10)

        # Confirm we get back the expected summaries.
        self.assertEqual(data_chunk_summaries, [expected_chunk_1])

    def test_list_entities_in_data_chunk(self):
        """Tests that we can get all the enities in a chunk"""
        # Create an entity for chunk 1.
        chunk1_datetime = dt.datetime(2023, 12, 12, 1, 30, 0, 1000)
        chunk1_entity1 = DataEntity(
                            uri="test_entity_1",
                            datetime=chunk1_datetime,
                            source=DataSource.REDDIT,
                            content=bytes(10),
                            content_size_bytes=10)

        # Create two entities for chunk 2.
        chunk2_datetime = dt.datetime(2023, 12, 12, 2, 30, 0, 1000)
        chunk2_entity1 = DataEntity(
                            uri="test_entity_2",
                            datetime=chunk2_datetime,
                            source=DataSource.X,
                            content=bytes(20),
                            content_size_bytes=20)

        chunk2_entity2 = DataEntity(
                            uri="test_entity_3",
                            datetime=chunk2_datetime + dt.timedelta(seconds=1),
                            source=DataSource.X,
                            content=bytes(30),
                            content_size_bytes=30)

        # Store the entities.
        self.test_storage.store_data_entities([chunk1_entity1, chunk2_entity1, chunk2_entity2])

        # Create the DataChunkSummary to query by.
        #TODO Consider breaking out another class that doesn't require size, but pass 0 for now.
        chunk2_summary = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(chunk2_datetime),
                            source=DataSource.X,
                            size_bytes=0)

        # Get the entities by the chunk
        data_entities = self.test_storage.list_data_entities_in_data_chunk(chunk2_summary)

        # Confirm we get back the expected data entities.
        self.assertEqual(data_entities, [chunk2_entity1, chunk2_entity2])


    if __name__ == "__main__":
        unittest.main()
