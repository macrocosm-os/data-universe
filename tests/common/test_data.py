import datetime as dt
from common.data import TimeBucket
import unittest


class TestData(unittest.TestCase):
    def test_time_bucket_to_date_range(self):
        """Tests a Timebucket's date range function"""

        # Create a datetime that should align with the start of a time bucket.
        datetime = dt.datetime.fromtimestamp(36000, tz=dt.timezone.utc)
        time_bucket = TimeBucket.from_datetime(datetime)

        date_range = TimeBucket.to_date_range(time_bucket)

        for i in range(0, 60):
            self.assertTrue(date_range.contains(datetime + dt.timedelta(minutes=i)))

        self.assertFalse(date_range.contains(datetime + dt.timedelta(minutes=60)))

if __name__ == "__main__":
    unittest.main()