import unittest
import datetime as dt
from storage.validator.mysql_databox_storage import (
    MysqlDataboxStorage,
    DataBoxAgeSize,
    DataBoxLabelSize,
    DataBoxMiner,
)


class TestMysqlDataboxStorage(unittest.TestCase):
    def setUp(self):
        # Make a test database for the test to operate against.
        self.test_storage = MysqlDataboxStorage(
            "localhost", "test-user", "test-pw", "test_db"
        )

    def tearDown(self):
        # Clean up the test database.
        cursor = self.test_storage.connection.cursor()
        cursor.execute("DROP TABLE Miner")
        cursor.execute("DROP TABLE LabelSize")
        cursor.execute("DROP TABLE AgeSize")
        self.test_storage.connection.commit()

    def test_insert_miners(self):
        """Tests that we can store miners."""
        now = dt.datetime.now()

        miners = [
            DataBoxMiner(
                hotkey="miner" + str(i),
                credibility=1,
                bucket_count=i,
                content_size_bytes_reddit=i,
                content_size_bytes_twitter=i,
                last_updated=now,
            )
            for i in range(3)
        ]

        # Insert 1 miner.
        self.test_storage.insert_miners(miners[0:1])

        # Insert 2 new miners.
        self.test_storage.insert_miners(miners[1:])

        # Confirm the miners were stored and that the count matches.
        cursor = self.test_storage.connection.cursor(buffered=True)
        cursor.execute("SELECT * FROM Miner")

        self.assertEqual(2, cursor.rowcount)

    def test_insert_label_sizes(self):
        """Tests that we can store label_sizes."""
        label_sizes = [
            DataBoxLabelSize(
                source=1,
                label_value="label" + str(i),
                content_size_bytes=i,
                adj_content_size_bytes=i,
            )
            for i in range(3)
        ]

        # Insert 1 label sizes.
        self.test_storage.insert_label_sizes(label_sizes[0:1])

        # Insert 2 new label sizes.
        self.test_storage.insert_label_sizes(label_sizes[1:])

        # Confirm the label sizes were stored and that the count matches.
        cursor = self.test_storage.connection.cursor(buffered=True)
        cursor.execute("SELECT * FROM LabelSize")

        self.assertEqual(2, cursor.rowcount)

    def test_insert_age_sizes(self):
        """Tests that we can store age_sizes."""
        age_sizes = [
            DataBoxAgeSize(
                source=1,
                time_bucket_id=i,
                content_size_bytes=i,
                adj_content_size_bytes=i,
            )
            for i in range(3)
        ]

        # Insert 1 age sizes.
        self.test_storage.insert_age_sizes(age_sizes[0:1])

        # Insert 2 new age sizes.
        self.test_storage.insert_age_sizes(age_sizes[1:])

        # Confirm the age sizes were stored and that the count matches.
        cursor = self.test_storage.connection.cursor(buffered=True)
        cursor.execute("SELECT * FROM AgeSize")

        self.assertEqual(2, cursor.rowcount)


if __name__ == "__main__":
    unittest.main()
