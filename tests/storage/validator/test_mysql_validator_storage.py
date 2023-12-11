import unittest
import os
from common.data import DataChunkSummary, DataEntity, DataLabel, DataSource, MinerIndex, ScorableDataChunkSummary, TimeBucket
import datetime as dt

from storage.validator.mysql_validator_storage import MysqlValidatorStorage

class TestMysqlValidatorStorage(unittest.TestCase):
    def setUp(self):
        # Make a test database for the test to operate against.
        self.test_storage = MysqlValidatorStorage("localhost", "test-user", "test-pw", "test_db")

    def tearDown(self):
        # Clean up the test database.
        cursor = self.test_storage.connection.cursor()
        cursor.execute("DROP TABLE MinerIndex")
        cursor.execute("DROP TABLE Miner")
        cursor.execute("DROP TABLE Label")
        self.test_storage.connection.commit()

    def test_instantiate_mysql_validator_storage(self):
        # Just ensure the setUp/tearDown methods work.
        self.assertTrue(True)

    def test_upsert_miner(self):
        """Tests that we can store a newly encountered miner."""
        # Upsert a miner.
        now = dt.datetime.utcnow()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        miner_id = self.test_storage._upsert_miner("test_hotkey", now_str)

        # Confirm the miner was stored and that the information matches.
        cursor = self.test_storage.connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM Miner WHERE hotkey = 'test_hotkey'")
        result = cursor.fetchone()

        self.assertEqual(now, result["lastUpdated"])
        self.assertEqual(miner_id, result["minerId"])

    def test_upsert_miner_existing_miner(self):
        """Tests that we can update an already encountered miner."""
        # Upsert a miner.
        now = dt.datetime.utcnow()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        miner_id = self.test_storage._upsert_miner("test_hotkey", now_str)

        # Upsert the same miner at a later time.
        new_now = now + dt.timedelta(seconds=1)
        new_now_str = new_now.strftime("%Y-%m-%d %H:%M:%S.%f")
        new_miner_id = self.test_storage._upsert_miner("test_hotkey", new_now_str)

        # Confirm the miner was stored and that the information matches the second upsert.
        cursor = self.test_storage.connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM Miner WHERE hotkey = 'test_hotkey'")
        result = cursor.fetchone()

        # The lastUpdated should match the newer datetime.
        self.assertEqual(new_now, result["lastUpdated"])
        # The miner_id should be the same.
        self.assertEqual(miner_id, new_miner_id)
        self.assertEqual(new_miner_id, result["minerId"])

    def test_upsert_miner_existing_miner_does_not_increment_id(self):
        """Tests that updating an already encountered miner does not use up an auto increment."""
        # Upsert a miner.
        now = dt.datetime.utcnow()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        miner_id = self.test_storage._upsert_miner("test_hotkey", now_str)

        # Upsert the same miner at a later time.
        new_now = now + dt.timedelta(seconds=1)
        new_now_str = new_now.strftime("%Y-%m-%d %H:%M:%S.%f")
        new_miner_id = self.test_storage._upsert_miner("test_hotkey", new_now_str)

        # Upsert a second miner.
        now_2 = dt.datetime.utcnow()
        now_str_2 = now_2.strftime("%Y-%m-%d %H:%M:%S.%f")
        miner_id_2 = self.test_storage._upsert_miner("test_hotkey_2", now_str_2)

        # Confirm the first miner has the same ID
        self.assertEqual(miner_id, new_miner_id)

        # Confirm the second miner has the next value.
        self.assertEqual(miner_id + 1, miner_id_2)

    def test_get_or_insert_label(self):
        """Tests that we can store a label."""
        # Insert a label.
        label_id = self.test_storage._get_or_insert_label("test_label_value")

        # Confirm the label was stored and that the information matches.
        cursor = self.test_storage.connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM Label WHERE labelValue = 'test_label_value'")
        result = cursor.fetchone()

        self.assertEqual(label_id, result["labelId"])

    def test_get_or_insert_label_existing_label(self):
        """Tests that we can ignore an already existing label."""
        # Insert a label.
        label_id = self.test_storage._get_or_insert_label("test_label_value")
        # Insert the same label.
        new_label_id = self.test_storage._get_or_insert_label("test_label_value")

        # Confirm the label was stored and that the information matches.
        cursor = self.test_storage.connection.cursor(dictionary=True, buffered=True)
        cursor.execute("SELECT * FROM Label WHERE labelValue = 'test_label_value'")
        self.assertEqual(cursor.rowcount, 1)

        result = cursor.fetchone()
        self.assertEqual(label_id, new_label_id)
        self.assertEqual(label_id, result["labelId"])

    def test_get_or_insert_label_existing_label_does_not_increment_id(self):
        """Tests that inserting an already encountered label does not use up an auto increment."""
        # Insert a label.
        label_id = self.test_storage._get_or_insert_label("test_label_value")
        # Insert the same label.
        new_label_id = self.test_storage._get_or_insert_label("test_label_value")
        # Insert a second label.
        label_id_2 = self.test_storage._get_or_insert_label("test_label_value_2")

        # Confirm the first label has the same ID
        self.assertEqual(label_id, new_label_id)

        # Confirm the second label  has the next value.
        self.assertEqual(label_id + 1, label_id_2)

    def test_upsert_miner_index_insrt_index(self):
        """Tests that we can insert a miner index"""
        # Create two chunk summaries for the index.
        now = dt.datetime.utcnow()
        chunk_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10)

        chunk_2 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.X,
                            size_bytes=50)
        
        # Create the index containing the chunk summaries.
        index = MinerIndex(hotkey="hotkey1", chunks=[chunk_1, chunk_2])
        
        # Store the index.
        self.test_storage.upsert_miner_index(index)

        # Confirm we have two rows in the index.
        cursor = self.test_storage.connection.cursor(dictionary=True, buffered=True)
        cursor.execute("SELECT * FROM MinerIndex")
        self.assertEqual(cursor.rowcount, 2)

    def test_upsert_miner_index_update_index(self):
        """Tests that we can update a miner index"""
        # Create three chunk summaries for the index.
        now = dt.datetime.utcnow()
        chunk_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10)

        chunk_2 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.X,
                            size_bytes=50)

        chunk_3 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.X,
                            label=DataLabel(value="label_2"),
                            size_bytes=100)
        
        # Create an index containing the first two chunk summaries.
        index_1 = MinerIndex(hotkey="hotkey1", chunks=[chunk_1, chunk_2])

        # Create an index containing the last two chunk summaries.
        index_2 = MinerIndex(hotkey="hotkey1", chunks=[chunk_2, chunk_3])
        
        # Store the first index.
        self.test_storage.upsert_miner_index(index_1)

        # Store the second index.
        self.test_storage.upsert_miner_index(index_2)

        # Confirm we have only the last two chunks in the index.
        cursor = self.test_storage.connection.cursor(dictionary=True)
        cursor.execute("SELECT source FROM MinerIndex")
        for row in cursor:
            self.assertEqual(DataSource.X, DataSource(row['source']))

    def test_read_miner_index(self):
        """Tests that we can read (and score) a miner index."""
        # Create two chunk summaries for the index.
        now = dt.datetime.utcnow()
        chunk_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10)

        chunk_2 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.X,
                            size_bytes=50)

        expected_chunk_1 = ScorableDataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10,
                            scorable_bytes=10
        )

        expected_chunk_2 = ScorableDataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.X,
                            size_bytes=50,
                            scorable_bytes=50
        )

        # Create the index containing the chunk summaries.
        index = MinerIndex(hotkey="hotkey1", chunks=[chunk_1, chunk_2])
        
        # Store the index.
        self.test_storage.upsert_miner_index(index)

        # Read the index.
        scored_index = self.test_storage.read_miner_index("hotkey1", set(["hotkey1"]))

        # Confirm the scored index matches expectations.
        self.assertEqual(scored_index.hotkey, "hotkey1")
        self.assertEqual(scored_index.scorable_chunks[0], expected_chunk_1)
        self.assertEqual(scored_index.scorable_chunks[1], expected_chunk_2)

    def test_read_miner_index_with_duplicate_chunks(self):
        """Tests that we can read (and score) a miner index when other miners have duplicate chunks."""
        # Create two chunk summaries for the index.
        now = dt.datetime.utcnow()
        chunk_1_miner_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10)

        chunk_1_miner_2 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=40)

        expected_chunk_1 = ScorableDataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10,
                            scorable_bytes=2
        )

        # Create the indexes containing the chunk summaries.
        index_1 = MinerIndex(hotkey="hotkey1", chunks=[chunk_1_miner_1])
        index_2 = MinerIndex(hotkey="hotkey2", chunks=[chunk_1_miner_2])
        
        # Store the indexes.
        self.test_storage.upsert_miner_index(index_1)
        self.test_storage.upsert_miner_index(index_2)

        # Read the index.
        scored_index = self.test_storage.read_miner_index("hotkey1", set(["hotkey1", "hotkey2"]))

        # Confirm the scored index matches expectations.
        self.assertEqual(scored_index.hotkey, "hotkey1")
        self.assertEqual(scored_index.scorable_chunks[0], expected_chunk_1)

    def test_read_miner_index_with_invalid_miners(self):
        """Tests that we can read (and score) a miner index when other invalid miners have duplicate chunks."""
        # Create two chunk summaries for the index.
        now = dt.datetime.utcnow()
        chunk_1_miner_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10)

        chunk_1_miner_2 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=40)

        expected_chunk_1 = ScorableDataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10,
                            scorable_bytes=10
        )

        # Create the indexes containing the chunk summaries.
        index_1 = MinerIndex(hotkey="hotkey1", chunks=[chunk_1_miner_1])
        index_2 = MinerIndex(hotkey="hotkey2", chunks=[chunk_1_miner_2])
        
        # Store the indexes.
        self.test_storage.upsert_miner_index(index_1)
        self.test_storage.upsert_miner_index(index_2)

        # Read the index. Do not pass in hotkey2 as a valid miner.
        scored_index = self.test_storage.read_miner_index("hotkey1", set(["hotkey1"]))

        # Confirm the scored index matches expectations.
        self.assertEqual(scored_index.hotkey, "hotkey1")
        self.assertEqual(scored_index.scorable_chunks[0], expected_chunk_1)

    def test_read_invalid_miner_index(self):
        """Tests that we can read (and score) an invalid miner index when other miners have duplicate chunks."""
        # Create two chunk summaries for the index.
        now = dt.datetime.utcnow()
        chunk_1_miner_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10)

        chunk_1_miner_2 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=40)

        expected_chunk_1 = ScorableDataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10,
                            scorable_bytes=2
        )

        # Create the indexes containing the chunk summaries.
        index_1 = MinerIndex(hotkey="hotkey1", chunks=[chunk_1_miner_1])
        index_2 = MinerIndex(hotkey="hotkey2", chunks=[chunk_1_miner_2])
        
        # Store the indexes.
        self.test_storage.upsert_miner_index(index_1)
        self.test_storage.upsert_miner_index(index_2)

        # Read the index. Do not pass in hotkey1 as a valid miner.
        scored_index = self.test_storage.read_miner_index("hotkey1", set(["hotkey2"]))

        # Confirm the scored index matches expectations.
        self.assertEqual(scored_index.hotkey, "hotkey1")
        self.assertEqual(scored_index.scorable_chunks[0], expected_chunk_1)

    def test_read_non_existing_miner_index(self):
        """Tests that we correctly return none for a non existing miner index."""
        # Read the index.
        scored_index = self.test_storage.read_miner_index("hotkey1", set(["hotkey1"]))

        # Confirm the scored_index is None.
        self.assertEqual(scored_index, None)

    def test_delete_miner_index(self):
        """Tests that we can delete a miner index."""
        # Create two chunk summaries for the indexes.
        now = dt.datetime.utcnow()
        chunk_1 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.REDDIT,
                            label=DataLabel(value="label_1"),
                            size_bytes=10)

        chunk_2 = DataChunkSummary(
                            time_bucket=TimeBucket.from_datetime(now),
                            source=DataSource.X,
                            size_bytes=50)
        
        # Create three indexes containing the chunk summaries.
        index_1 = MinerIndex(hotkey="hotkey1", chunks=[chunk_1, chunk_2])
        index_2 = MinerIndex(hotkey="hotkey2", chunks=[chunk_1, chunk_2])
        index_3 = MinerIndex(hotkey="hotkey3", chunks=[chunk_1, chunk_2])

        # Store the indexes.
        self.test_storage.upsert_miner_index(index_1)
        self.test_storage.upsert_miner_index(index_2)
        self.test_storage.upsert_miner_index(index_3)

        # Delete one index.
        self.test_storage.delete_miner_index("hotkey2")

        # Confirm we have four rows in the index.
        cursor = self.test_storage.connection.cursor(dictionary=True, buffered=True)
        cursor.execute("SELECT * FROM MinerIndex")
        self.assertEqual(cursor.rowcount, 4)

    def test_read_miner_last_updated(self):
        """Tests getting the last time a miner was updated."""
        # Insert a label.
        label_id = self.test_storage._get_or_insert_label("test_label_value")
        # Insert the same label.
        new_label_id = self.test_storage._get_or_insert_label("test_label_value")
        # Insert a second label.
        label_id_2 = self.test_storage._get_or_insert_label("test_label_value_2")

        # Confirm the first label has the same ID
        self.assertEqual(label_id, new_label_id)

        # Confirm the second label  has the next value.
        self.assertEqual(label_id + 1, label_id_2)

    def test_read_miner_last_updated_never_updated(self):
        """Tests getting the last time a miner was updated when it has never been updated."""
        # Insert a miner
        now = dt.datetime.utcnow()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        self.test_storage._upsert_miner("test_hotkey", now_str)

        # Get the last updated
        last_updated = self.test_storage.read_miner_last_updated("test_hotkey2")

        # Confirm the last updated is None.
        self.assertEqual(None, last_updated)

    if __name__ == "__main__":
        unittest.main()