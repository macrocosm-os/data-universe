import unittest

from attr import dataclass
from common import constants, utils
from common.data_v2 import ScorableDataEntityBucket
from rewards.data import DataSourceDesirability, DataDesirabilityLookup
from rewards.data_value_calculator import DataValueCalculator
from common.data import (
    DataLabel,
    DataSource,
    TimeBucket,
)
import datetime as dt


class TestDataValueCalculator(unittest.TestCase):
    def setUp(self):
        model = DataDesirabilityLookup(
            distribution={
                DataSource.REDDIT: DataSourceDesirability(
                    weight=0.75,
                    default_scale_factor=0.5,
                    label_scale_factors={
                        # Labels include upper and lower case to ensure matching is case insensitive.
                        DataLabel(value="TestLABEL"): 1.0,
                        DataLabel(value="unscoredLabel"): 0,
                        DataLabel(value="penalizedLABEL"): -1.0,
                    },
                ),
                DataSource.X: DataSourceDesirability(
                    weight=0.25,
                    default_scale_factor=0.8,
                    label_scale_factors={
                        DataLabel(value="#TestLABEL"): 1.0,
                        DataLabel(value="#unscoredLabel"): 0,
                        DataLabel(value="#penalizedLABEL"): -1.0,
                    },
                ),
            },
            max_age_in_hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24,
        )
        self.value_calculator = DataValueCalculator(model=model)

    def test_get_score_for_data_entity_bucket_with_matching_label(self):
        """Generates a bucket with various data sources and labels and ensures the score is correct."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)

        @dataclass(frozen=True)
        class TestCaseInput:
            data_source: DataSource
            data_label: str

        # List of test case input and expected score, where the scores all expect 100 scorable bytes.
        test_cases = [
            (TestCaseInput(DataSource.REDDIT, "testlabel"), 75.0),
            (TestCaseInput(DataSource.REDDIT, "unscoredlabel"), 0),
            (TestCaseInput(DataSource.REDDIT, "PenAlizedLABEL"), -75.0),
            (TestCaseInput(DataSource.REDDIT, "other-label"), 37.5),
            (TestCaseInput(DataSource.REDDIT, None), 37.5),
            (TestCaseInput(DataSource.X, "#testlabel"), 25.0),
            (TestCaseInput(DataSource.X, "#unscoredLabel"), 0),
            (TestCaseInput(DataSource.X, "#penalizedLABEL"), -25.0),
            (TestCaseInput(DataSource.X, "#other-label"), 20.0),
        ]

        # Verify data from the current time_bucket, with various labels are scored correctly.
        time_bucket_id = utils.time_bucket_id_from_datetime(now)
        for tc in test_cases:
            bucket = ScorableDataEntityBucket(
                time_bucket_id=time_bucket_id,
                source=tc[0].data_source,
                label=tc[0].data_label,
                size_bytes=200,
                # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                scorable_bytes=100,
            )
            score = self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            )
            self.assertAlmostEqual(score, tc[1], places=5)

    def test_get_score_for_data_entity_bucket_score_decreases_over_time(self):
        """Generates a bucket containing data of various ages and verifies the score is as expected."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)

        # Verify score at the present time_bucket is scored at 100%.
        time_bucket_id = utils.time_bucket_id_from_datetime(now)
        bucket = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=DataSource.REDDIT,
            label="testlabel",
            size_bytes=200,
            # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
            scorable_bytes=100,
        )
        self.assertAlmostEqual(
            self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            ),
            75.0,
            places=5,
        )

        # Verify the score at the max age is scored at 50%.
        bucket = ScorableDataEntityBucket(
            time_bucket_id=utils.time_bucket_id_from_datetime(
                now
                - dt.timedelta(hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24)
            ),
            source=DataSource.REDDIT,
            label="testlabel",
            size_bytes=200,
            # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
            scorable_bytes=100,
        )
        self.assertAlmostEqual(
            self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            ),
            37.5,
            places=5,
        )

        # Verify the score past the max age is 0.
        bucket = ScorableDataEntityBucket(
            time_bucket_id=utils.time_bucket_id_from_datetime(
                now
                - dt.timedelta(
                    hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24 + 1
                )
            ),
            source=DataSource.REDDIT,
            label="testlabel",
            size_bytes=200,
            # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
            scorable_bytes=100,
        )
        self.assertAlmostEqual(
            self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            ),
            0,
            places=5,
        )

        # Now verify the score decreases between now and max_age.
        previous_score = 75.0
        for hours_back in range(1, constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24):
            bucket = ScorableDataEntityBucket(
                time_bucket_id=utils.time_bucket_id_from_datetime(
                    now - dt.timedelta(hours=hours_back)
                ),
                source=DataSource.REDDIT,
                label="testlabel",
                size_bytes=200,
                # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                scorable_bytes=100,
            )
            score = self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            )
            self.assertLess(score, previous_score)
            previous_score = score


if __name__ == "__main__":
    unittest.main()
