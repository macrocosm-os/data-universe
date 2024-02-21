import unittest
import datetime as dt

from scraping import utils


class TestUtils(unittest.TestCase):
    def test_obfuscate_datetime_to_minute(self):
        test_date = dt.datetime(
            year=2024,
            month=1,
            day=2,
            hour=3,
            minute=4,
            second=5,
            microsecond=6,
            tzinfo=dt.timezone.utc,
        )

        obfuscated_date = utils.obfuscate_datetime_to_minute(test_date)

        self.assertEqual(
            obfuscated_date,
            dt.datetime(
                year=2024,
                month=1,
                day=2,
                hour=3,
                minute=4,
                second=0,
                microsecond=0,
                tzinfo=dt.timezone.utc,
            ),
        )
