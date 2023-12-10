import unittest
import os
from common.data import DataChunkSummary, DataEntity, DataLabel, DataSource, TimeBucket
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
        self.test_storage.connection.commit()

    def test_instantiate_mysql_validator_storage(self):
        # Just ensure the setUp/tearDown methods work.
        self.assertTrue(True)
