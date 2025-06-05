import unittest
from attr import dataclass
from common import constants, utils
from common.data_v2 import ScorableDataEntityBucket
from rewards.data import DataDesirabilityLookup, Job, JobMatcher, DataSourceDesirability
from rewards.data_value_calculator import DataValueCalculator
from common.data import (
    DataLabel,
    DataSource,
    TimeBucket,
)
import datetime as dt


class TestDataValueCalculator(unittest.TestCase):
    def setUp(self):
        # Convert datetime strings to time buckets for date-constrained job
        dated_label_start = dt.datetime.fromisoformat("2023-12-10T00:00:00+00:00")
        dated_label_end = dt.datetime.fromisoformat("2023-12-15T00:00:00+00:00")
        dated_label_start_timebucket = utils.time_bucket_id_from_datetime(dated_label_start)
        dated_label_end_timebucket = utils.time_bucket_id_from_datetime(dated_label_end)
        
        # Create a job matcher for Reddit with different jobs, all with keyword=None
        reddit_job_matcher = JobMatcher(jobs=[
            Job(
                keyword=None,  # Currently only accepting jobs with keyword=None
                label="testlabel", 
                job_weight=1.0,
                start_timebucket=None,
                end_timebucket=None,
            ),
            Job(
                keyword=None,
                label="unscoredlabel",
                job_weight=0.0,
                start_timebucket=None,
                end_timebucket=None,
            ),
            Job(
                keyword=None,
                label="penalizedlabel",
                job_weight=-1.0,
                start_timebucket=None,
                end_timebucket=None,
            ),
            # Add a job with date constraints
            Job(
                keyword=None,
                label="dated-label",
                job_weight=2.0,
                start_timebucket=dated_label_start_timebucket,  # Converted from start_datetime
                end_timebucket=dated_label_end_timebucket,  # Converted from end_datetime
            ),
        ])

        # Create a job matcher for X with different jobs, all with keyword=None
        x_job_matcher = JobMatcher(jobs=[
            Job(
                keyword=None,
                label="#testlabel",
                job_weight=1.0,
                start_timebucket=None,
                end_timebucket=None,
            ),
            Job(
                keyword=None,
                label="#unscoredlabel",
                job_weight=0.0,
                start_timebucket=None,
                end_timebucket=None,
            ),
            Job(
                keyword=None,
                label="#penalizedlabel",
                job_weight=-1.0,
                start_timebucket=None,
                end_timebucket=None,
            ),
        ])

        model = DataDesirabilityLookup(
            distribution={
                DataSource.REDDIT: DataSourceDesirability(
                    weight=0.75,
                    default_scale_factor=0.5,
                    job_matcher=reddit_job_matcher,
                ),
                DataSource.X: DataSourceDesirability(
                    weight=0.25,
                    default_scale_factor=0.8,
                    job_matcher=x_job_matcher,
                ),
            },
            max_age_in_hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24,
        )
        self.value_calculator = DataValueCalculator(model=model)

    def test_get_score_for_data_entity_bucket_with_matching_job(self):
        """Tests scoring for data entity buckets that match different jobs."""
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
            (TestCaseInput(DataSource.REDDIT, "other-label"), 37.5),  # Default scale factor
            (TestCaseInput(DataSource.REDDIT, None), 37.5),  # Default scale factor
            (TestCaseInput(DataSource.X, "#testlabel"), 25.0),
            (TestCaseInput(DataSource.X, "#unscoredLabel"), 0),
            (TestCaseInput(DataSource.X, "#penalizedLABEL"), -25.0),
            (TestCaseInput(DataSource.X, "#other-label"), 20.0),  # Default scale factor
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

    def test_get_score_for_data_entity_bucket_with_date_constraints(self):
        """Tests scoring for data entity buckets with date-constrained jobs."""
        # Test with a date that falls within the date range for the "dated-label" job
        test_date = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(test_date)
        time_bucket_id = utils.time_bucket_id_from_datetime(test_date)
        
        # This should match the "dated-label" job which has weight 2.0
        bucket = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=DataSource.REDDIT,
            label="dated-label",
            size_bytes=200,
            scorable_bytes=100,
        )
        score = self.value_calculator.get_score_for_data_entity_bucket(
            bucket, current_time_bucket
        )
        # Expected score: data_source_weight(0.75) * job_weight(2.0) * time_scalar(1.0) * scorable_bytes(100) = 150.0
        self.assertAlmostEqual(score, 150.0, places=5)
        
        # Test with a date outside the job's date range
        outside_date = dt.datetime(2023, 12, 16, 12, 30, 0, tzinfo=dt.timezone.utc)
        time_bucket_id = utils.time_bucket_id_from_datetime(outside_date)
        
        bucket = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=DataSource.REDDIT,
            label="dated-label",
            size_bytes=200,
            scorable_bytes=100,
        )
        score = self.value_calculator.get_score_for_data_entity_bucket(
            bucket, current_time_bucket
        )
        # Should fall back to default scale factor: data_source_weight(0.75) * default_scale_factor(0.5) * time_scalar * scorable_bytes(100)
        # The time_scalar will be computed based on the age difference between outside_date and current_time_bucket
        # Since outside_date is in the future relative to test_date, the age should be 0 and time_scalar should be 1.0
        self.assertAlmostEqual(score, 37.5, places=5)

    def test_get_score_for_data_entity_bucket_score_decreases_over_time(self):
        """Tests that scores decrease linearly with age of the data."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)

        # Verify score at the present time_bucket is scored at 100%.
        time_bucket_id = utils.time_bucket_id_from_datetime(now)
        bucket = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=DataSource.REDDIT,
            label="testlabel",
            size_bytes=200,
            scorable_bytes=100,
        )
        self.assertAlmostEqual(
            self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            ),
            75.0,
            places=5,
        )

        # Verify the score at the max age is scored at 50% of what it would be if fresh.
        bucket = ScorableDataEntityBucket(
            time_bucket_id=utils.time_bucket_id_from_datetime(
                now - dt.timedelta(hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24)
            ),
            source=DataSource.REDDIT,
            label="testlabel",
            size_bytes=200,
            scorable_bytes=100,
        )
        self.assertAlmostEqual(
            self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            ),
            37.5,  # 75.0 * 0.5 because of max age
            places=5,
        )

        # Verify the score past the max age is 0.
        bucket = ScorableDataEntityBucket(
            time_bucket_id=utils.time_bucket_id_from_datetime(
                now - dt.timedelta(
                    hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24 + 1
                )
            ),
            source=DataSource.REDDIT,
            label="testlabel",
            size_bytes=200,
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
                scorable_bytes=100,
            )
            score = self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            )
            self.assertLess(score, previous_score)
            previous_score = score

    def test_multiple_matching_jobs(self):
        """Tests scoring when multiple jobs match the same data entity bucket."""
        # Add a test with multiple jobs matching the same data
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)
        
        # Convert datetime strings to time buckets
        multi_match_start = dt.datetime.fromisoformat("2023-12-10T00:00:00+00:00")
        multi_match_end = dt.datetime.fromisoformat("2023-12-15T00:00:00+00:00")
        multi_match_start_timebucket = utils.time_bucket_id_from_datetime(multi_match_start)
        multi_match_end_timebucket = utils.time_bucket_id_from_datetime(multi_match_end)
        
        # Create a custom model with multiple overlapping jobs, all with keyword=None
        reddit_job_matcher = JobMatcher(jobs=[
            Job(
                keyword=None,
                label="multi-match",  
                job_weight=1.0,
                start_timebucket=None,
                end_timebucket=None,
            ),
            Job(
                keyword=None,
                label="multi-match",
                job_weight=2.0,
                start_timebucket=multi_match_start_timebucket,  # Converted from start_datetime
                end_timebucket=multi_match_end_timebucket,  # Converted from end_datetime
            ),
        ])
        
        custom_model = DataDesirabilityLookup(
            distribution={
                DataSource.REDDIT: DataSourceDesirability(
                    weight=0.75,
                    default_scale_factor=0.5,
                    job_matcher=reddit_job_matcher,
                ),
                DataSource.X: DataSourceDesirability(
                    weight=0.25,
                    default_scale_factor=0.8,
                    job_matcher=JobMatcher(),  # Empty job matcher
                ),
            },
            max_age_in_hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24,
        )
        custom_calculator = DataValueCalculator(model=custom_model)
        
        # Create a bucket that should match both jobs
        time_bucket_id = utils.time_bucket_id_from_datetime(now)
        bucket = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=DataSource.REDDIT,
            label="multi-match",
            size_bytes=200,
            scorable_bytes=100,
        )
        
        score = custom_calculator.get_score_for_data_entity_bucket(
            bucket, current_time_bucket
        )
        
        # Expected score is the sum of both job contributions:
        # Job 1: data_source_weight(0.75) * job_weight(1.0) * time_scalar(1.0) * scorable_bytes(100) = 75.0
        # Job 2: data_source_weight(0.75) * job_weight(2.0) * time_scalar(1.0) * scorable_bytes(100) = 150.0
        # Total: 75.0 + 150.0 = 225.0
        self.assertAlmostEqual(score, 225.0, places=5)


if __name__ == "__main__":
    unittest.main()