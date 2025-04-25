import unittest
import datetime as dt
from typing import List

from common.data import DataSource, TimeBucket
from common.data_v2 import ScorableDataEntityBucket
from rewards.data import Job, JobParams, JobLookup
from rewards.data_value_calculator import DataValueCalculator


class TestDataValueCalculator(unittest.TestCase):
    def setUp(self):
        # Create a JobLookup model with test jobs for both platforms with different job types
        jobs = [
            # Reddit label jobs
            Job(
                id="reddit-testlabel",
                weight=1.0,
                params=JobParams(
                    job_type="label",
                    platform="REDDIT",
                    topic="testlabel"
                )
            ),
            Job(
                id="reddit-unscoredlabel",
                weight=0.0,
                params=JobParams(
                    job_type="label",
                    platform="REDDIT",
                    topic="unscoredlabel"
                )
            ),
            # X label jobs
            Job(
                id="x-testlabel",
                weight=1.0,
                params=JobParams(
                    job_type="label",
                    platform="X",
                    topic="#testlabel"
                )
            ),
            Job(
                id="x-unscoredlabel",
                weight=0.0,
                params=JobParams(
                    job_type="label", 
                    platform="X",
                    topic="#unscoredlabel"
                )
            ),
            # keyword jobs that should not be matched during label scoring
            Job(
                id="reddit-keyword-job",
                weight=2.0,
                params=JobParams(
                    job_type="keyword",
                    platform="REDDIT",
                    topic="testlabel"
                )
            ),
            Job(
                id="x-keyword-job",
                weight=2.0,
                params=JobParams(
                    job_type="keyword",
                    platform="X",
                    topic="#testlabel"
                )
            )
        ]
        
        # Set up the job lookup model
        model = JobLookup(
            job_list=jobs,
            platform_weights={
                "reddit": 0.75,
                "x": 0.25
            },
            default_label_weight=0.5,
            default_keyword_weight=0.0,
            max_age_in_hours=30 * 24  # 30 days * 24 hours
        )
        
        self.value_calculator = DataValueCalculator(model=model)

    def test_get_score_for_data_entity_bucket_with_matching_label(self):
        """Generates a bucket with various data sources and labels and ensures the score is correct."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)

        class TestCaseInput:
            def __init__(self, data_source: DataSource, data_label: str):
                self.data_source = data_source
                self.data_label = data_label

        # List of test case input and expected score, where the scores all expect 100 scorable bytes.
        test_cases = [
            (TestCaseInput(DataSource.REDDIT, "testlabel"), 75.0),
            (TestCaseInput(DataSource.REDDIT, "unscoredlabel"), 0),
            (TestCaseInput(DataSource.REDDIT, "other-label"), 37.5),  # uses default_label_weight * platform_weight
            (TestCaseInput(DataSource.REDDIT, None), 37.5),  # uses default_label_weight * platform_weight
            (TestCaseInput(DataSource.X, "#testlabel"), 25.0),
            (TestCaseInput(DataSource.X, "#unscoredlabel"), 0),
            (TestCaseInput(DataSource.X, "#other-label"), 12.5),  # uses default_label_weight * platform_weight
        ]

        # Verify data from the current time_bucket, with various labels are scored correctly.
        time_bucket_id = current_time_bucket.id
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
            self.assertAlmostEqual(score, tc[1], places=5, 
                msg=f"Failed for source={tc[0].data_source}, label={tc[0].data_label}, expected={tc[1]}, got={score}")

    def test_get_score_for_data_entity_bucket_score_decreases_over_time(self):
        """Generates a bucket containing data of various ages and verifies the score is as expected."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)
        max_age_hours = self.value_calculator.model.max_age_in_hours

        # Verify score at the present time_bucket is scored at 100%.
        time_bucket_id = current_time_bucket.id
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
        old_time_bucket = TimeBucket.from_datetime(now - dt.timedelta(hours=max_age_hours))
        bucket = ScorableDataEntityBucket(
            time_bucket_id=old_time_bucket.id,
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
        very_old_time_bucket = TimeBucket.from_datetime(now - dt.timedelta(hours=max_age_hours + 1))
        bucket = ScorableDataEntityBucket(
            time_bucket_id=very_old_time_bucket.id,
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
        for hours_back in range(1, max_age_hours, max_age_hours // 10):  # Test at 10 points
            time_bucket = TimeBucket.from_datetime(now - dt.timedelta(hours=hours_back))
            bucket = ScorableDataEntityBucket(
                time_bucket_id=time_bucket.id,
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
    
    def test_job_type_filtering(self):
        """Test that only jobs with job_type='label' are matched."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)
        
        # Create a bucket with a label that has both keyword and label job types
        bucket = ScorableDataEntityBucket(
            time_bucket_id=current_time_bucket.id,
            source=DataSource.REDDIT,
            label="testlabel",
            size_bytes=200,
            scorable_bytes=100,
        )
        
        # The score should match the label job (1.0) not the keyword job (2.0)
        score = self.value_calculator.get_score_for_data_entity_bucket(
            bucket, current_time_bucket
        )
        
        # Expected: platform_weight (0.75) * label_job_weight (1.0) * scorable_bytes (100) = 75.0
        # If keyword job was matched, it would be 0.75 * 2.0 * 100 = 150.0
        self.assertAlmostEqual(score, 75.0, places=5)
    
    def test_time_bounded_jobs(self):
        """Test jobs with specified time ranges."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0, tzinfo=dt.timezone.utc)
        current_time_bucket = TimeBucket.from_datetime(now)
        
        # Add a time-bounded job to the job list
        time_bounded_job = Job(
            id="time-bounded-job",
            weight=2.0,
            params=JobParams(
                job_type="label",
                platform="REDDIT",
                topic="timelabel",
                post_start_datetime=(now - dt.timedelta(hours=10)).isoformat(),
                post_end_datetime=(now + dt.timedelta(hours=10)).isoformat()
            )
        )
        
        # Update the value calculator with the new job
        self.value_calculator.model.job_list.append(time_bounded_job)
        self.value_calculator._create_lookup_dicts()
        
        # Test data within the time bound
        in_range_bucket = ScorableDataEntityBucket(
            time_bucket_id=current_time_bucket.id,
            source=DataSource.REDDIT,
            label="timelabel",
            size_bytes=200,
            scorable_bytes=100,
        )
        
        # Expected: platform_weight (0.75) * job_weight (2.0) * 0.5 * scorable_bytes (100) = 75.0
        # Time-bounded jobs use a 0.5 multiplier in _calculate_job_score
        score = self.value_calculator.get_score_for_data_entity_bucket(
            in_range_bucket, current_time_bucket
        )
        self.assertAlmostEqual(score, 75.0, places=5)
        
        # Test data before the time bound
        before_range_bucket = ScorableDataEntityBucket(
            time_bucket_id=TimeBucket.from_datetime(now - dt.timedelta(hours=15)).id,
            source=DataSource.REDDIT,
            label="timelabel",
            size_bytes=200,
            scorable_bytes=100,
        )
        
        # Should get default label weight since out of time range
        score = self.value_calculator.get_score_for_data_entity_bucket(
            before_range_bucket, current_time_bucket
        )
        # Expected: platform_weight (0.75) * default_label_weight (0.5) * time_scale_factor * 100
        # For simplicity, just check it's not the job weight (which would be 75.0)
        self.assertNotEqual(score, 75.0)


if __name__ == "__main__":
    unittest.main()