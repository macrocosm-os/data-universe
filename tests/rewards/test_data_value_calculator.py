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
        # Create a job matcher for Reddit with different jobs, including both "label" and "keyword" types
        reddit_job_matcher = JobMatcher(jobs=[
            Job(
                job_type="label",
                topic="testlabel",
                job_weight=1.0,
                start_datetime=None,
                end_datetime=None,
            ),
            Job(
                job_type="label",
                topic="unscoredlabel",
                job_weight=0.0,
                start_datetime=None,
                end_datetime=None,
            ),
            Job(
                job_type="label",
                topic="penalizedlabel",
                job_weight=-1.0,
                start_datetime=None,
                end_datetime=None,
            ),
            # Add a job with date constraints
            Job(
                job_type="label",
                topic="dated-label",
                job_weight=2.0,
                start_datetime="2023-12-10T00:00:00+00:00",
                end_datetime="2023-12-15T00:00:00+00:00",
            ),
            # Add keyword job types
            Job(
                job_type="keyword",
                topic="test-keyword",
                job_weight=1.5,
                start_datetime=None,
                end_datetime=None,
            ),
            Job(
                job_type="keyword",
                topic="dated-keyword",
                job_weight=2.5,
                start_datetime="2023-12-11T00:00:00+00:00",
                end_datetime="2023-12-14T00:00:00+00:00",
            ),
        ])

        # Create a job matcher for X with different jobs, including both "label" and "keyword" types
        x_job_matcher = JobMatcher(jobs=[
            Job(
                job_type="label",
                topic="#testlabel",
                job_weight=1.0,
                start_datetime=None,
                end_datetime=None,
            ),
            Job(
                job_type="label",
                topic="#unscoredlabel",
                job_weight=0.0,
                start_datetime=None,
                end_datetime=None,
            ),
            Job(
                job_type="label",
                topic="#penalizedlabel",
                job_weight=-1.0,
                start_datetime=None,
                end_datetime=None,
            ),
            # Add keyword job types
            Job(
                job_type="keyword",
                topic="#test-keyword",
                job_weight=1.2,
                start_datetime=None,
                end_datetime=None,
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
        
        # Create a custom model with multiple overlapping jobs
        reddit_job_matcher = JobMatcher(jobs=[
            Job(
                job_type="label",
                topic="multi-match",
                job_weight=1.0,
                start_datetime=None,
                end_datetime=None,
            ),
            Job(
                job_type="label",
                topic="multi-match",
                job_weight=2.0,
                start_datetime="2023-12-10T00:00:00+00:00",
                end_datetime="2023-12-15T00:00:00+00:00",
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

    def test_keyword_job_type(self):
        """Tests scoring for data entity buckets that match keyword job types."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)
        time_bucket_id = utils.time_bucket_id_from_datetime(now)

        # Mock the _time_bucket_to_iso_daterange method to return a controlled daterange
        original_method = self.value_calculator._time_bucket_to_iso_daterange
        
        def mock_time_bucket_to_iso_daterange(time_bucket_id):
            return ("2023-12-12T12:00:00+00:00", "2023-12-12T13:00:00+00:00")
        
        # Apply the mock
        self.value_calculator._time_bucket_to_iso_daterange = mock_time_bucket_to_iso_daterange
        
        try:
            # Test with a keyword job type (no date constraints)
            bucket = ScorableDataEntityBucket(
                time_bucket_id=time_bucket_id,
                source=DataSource.REDDIT,
                label="test-keyword",  # This should match our "keyword" job type
                size_bytes=200,
                scorable_bytes=100,
            )
            
            # Call the method but with job_type explicitly set to "keyword"
            # Mocking the internal call to find_matching_jobs where job_type would be set to "keyword"
            original_find_matching_jobs = self.value_calculator.model.find_matching_jobs
            
            def mock_find_matching_jobs(data_source, job_type, topic, daterange):
                # Override to always use "keyword" as job_type for this test
                if topic == "test-keyword":
                    job_type = "keyword"
                return original_find_matching_jobs(data_source, job_type, topic, daterange)
            
            self.value_calculator.model.find_matching_jobs = mock_find_matching_jobs
            
            score = self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            )
            
            # Expected score: data_source_weight(0.75) * job_weight(1.5) * time_scalar(1.0) * scorable_bytes(100) = 112.5
            self.assertAlmostEqual(score, 112.5, places=5)
            
            # Test with a dated keyword job type
            bucket = ScorableDataEntityBucket(
                time_bucket_id=time_bucket_id,
                source=DataSource.REDDIT,
                label="dated-keyword",
                size_bytes=200,
                scorable_bytes=100,
            )
            
            def mock_find_matching_jobs_dated(data_source, job_type, topic, daterange):
                # Override to always use "keyword" as job_type for this test
                if topic == "dated-keyword":
                    job_type = "keyword"
                return original_find_matching_jobs(data_source, job_type, topic, daterange)
            
            self.value_calculator.model.find_matching_jobs = mock_find_matching_jobs_dated
            
            score = self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            )
            
            # Expected score: data_source_weight(0.75) * job_weight(2.5) * time_scalar(1.0) * scorable_bytes(100) = 187.5
            self.assertAlmostEqual(score, 187.5, places=5)
            
            # Test with X source
            bucket = ScorableDataEntityBucket(
                time_bucket_id=time_bucket_id,
                source=DataSource.X,
                label="#test-keyword",
                size_bytes=200,
                scorable_bytes=100,
            )
            
            def mock_find_matching_jobs_x(data_source, job_type, topic, daterange):
                # Override to always use "keyword" as job_type for this test
                if topic == "#test-keyword":
                    job_type = "keyword"
                return original_find_matching_jobs(data_source, job_type, topic, daterange)
            
            self.value_calculator.model.find_matching_jobs = mock_find_matching_jobs_x
            
            score = self.value_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            )
            
            # Expected score: data_source_weight(0.25) * job_weight(1.2) * time_scalar(1.0) * scorable_bytes(100) = 30.0
            self.assertAlmostEqual(score, 30.0, places=5)
            
        finally:
            # Restore original methods
            self.value_calculator._time_bucket_to_iso_daterange = original_method
            self.value_calculator.model.find_matching_jobs = original_find_matching_jobs

    def test_combined_job_types(self):
        """Tests scoring when a data entity bucket matches both label and keyword jobs."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)
        
        # Create a custom model with both label and keyword jobs for the same topic
        reddit_job_matcher = JobMatcher(jobs=[
            Job(
                job_type="label",
                topic="dual-match",
                job_weight=1.0,
                start_datetime=None,
                end_datetime=None,
            ),
            Job(
                job_type="keyword",
                topic="dual-match",
                job_weight=2.0,
                start_datetime=None,
                end_datetime=None,
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
        
        # Create a bucket that should potentially match both job types
        time_bucket_id = utils.time_bucket_id_from_datetime(now)
        bucket = ScorableDataEntityBucket(
            time_bucket_id=time_bucket_id,
            source=DataSource.REDDIT,
            label="dual-match",
            size_bytes=200,
            scorable_bytes=100,
        )
        
        # Mock the find_matching_jobs method to return both job types
        original_find_matching_jobs = custom_calculator.model.find_matching_jobs
        
        def mock_find_matching_jobs(data_source, job_type, topic, daterange):
            if topic == "dual-match":
                # Return both the label and keyword jobs
                return [
                    {"job_type": "label", "topic": "dual-match", "job_weight": 1.0, "start_datetime": None, "end_datetime": None},
                    {"job_type": "keyword", "topic": "dual-match", "job_weight": 2.0, "start_datetime": None, "end_datetime": None}
                ]
            return []
        
        try:
            custom_calculator.model.find_matching_jobs = mock_find_matching_jobs
            
            score = custom_calculator.get_score_for_data_entity_bucket(
                bucket, current_time_bucket
            )
            
            # Expected score is the sum of both job contributions:
            # Label job: data_source_weight(0.75) * job_weight(1.0) * time_scalar(1.0) * scorable_bytes(100) = 75.0
            # Keyword job: data_source_weight(0.75) * job_weight(2.0) * time_scalar(1.0) * scorable_bytes(100) = 150.0
            # Total: 75.0 + 150.0 = 225.0
            self.assertAlmostEqual(score, 225.0, places=5)
            
        finally:
            # No need to restore original method as we used a custom calculator instance
            pass


if __name__ == "__main__":
    unittest.main()