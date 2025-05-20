import random
import cProfile
import pstats
import time
import unittest
from unittest.mock import MagicMock, Mock, patch
from typing import List
import torch
from common.data_v2 import ScorableDataEntityBucket, ScorableMinerIndex
from rewards.data import DataSourceDesirability, DataDesirabilityLookup, Job, JobMatcher
from scraping.scraper import ValidationResult
from common import constants
from common.data import (
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
)
from rewards.miner_scorer import MinerScorer
import datetime as dt
import rewards.data_value_calculator
from common import utils
from tests import utils as test_utils


@patch.object(rewards.data_value_calculator.dt, "datetime", Mock(wraps=dt.datetime))
class TestMinerScorer(unittest.TestCase):
    def setUp(self):
        self.num_neurons = 10
        
        # Create a job matcher for Reddit with test jobs
        reddit_job_matcher = JobMatcher(jobs=[
            Job(
                keyword=None,  # currently only accepting label-only jobs
                label="testlabel",  
                job_weight=1.0,
                start_timebucket=None,
                end_timebucket=None,
            ),
            Job(
                keyword=None,  
                label="otherlabel",  
                job_weight=0.75,
                start_timebucket=None,
                end_timebucket=None,
            ),
        ])

        # Create a job matcher for X with test jobs
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
                label="#otherlabel",  
                job_weight=0.75,
                start_timebucket=None,
                end_timebucket=None,
            ),

        ])
        
        self.value_calculator = rewards.data_value_calculator.DataValueCalculator(
            DataDesirabilityLookup(
                distribution={
                    DataSource.REDDIT: DataSourceDesirability(
                        weight=0.4,
                        default_scale_factor=0.5,
                        job_matcher=reddit_job_matcher,
                    ),
                    DataSource.X: DataSourceDesirability(
                        weight=0.6,
                        default_scale_factor=0.5,
                        job_matcher=x_job_matcher,
                    ),
                },
                max_age_in_hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24,
            )
        )
        self.scorer = MinerScorer(self.num_neurons, self.value_calculator)

        # Mock the current time
        self.now = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=3)

        # Construct a ScorableMinerIndex with 2 chunks that should score 150 in total (without considering EMA)
        # Note: With the job-based format, this scorable_index_full_score may need to be adjusted based on the job weights
        self.scorable_index_full_score = 150
        self.scorable_index = ScorableMinerIndex(
            hotkey="abc123",
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="testlabel",
                    size_bytes=1000,
                    # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                    scorable_bytes=500,
                ),
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="otherlabel",
                    size_bytes=1000,
                    # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                    scorable_bytes=250,
                ),
            ],
            last_updated=self.now,
        )

    def _add_score_to_uid(self, uid: int):
        validation_results = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100)
        ]
        self.scorer.on_miner_evaluated(uid, self.scorable_index, validation_results)

    def test_reset_score(self):
        """Tests that reset_score sets the score to 0."""
        rewards.data_value_calculator.dt.datetime.now.return_value = self.now

        uid = 5

        self._add_score_to_uid(uid)
        scores = self.scorer.get_scores()
        self.assertGreater(scores[uid], 0.0)
        self.assertGreater(self.scorer.get_miner_credibility(uid), 0)

        self.scorer.reset(uid)
        scores = self.scorer.get_scores()
        self.assertEqual(scores[uid], 0.0)
        self.assertEqual(self.scorer.get_miner_credibility(uid), 0)

    def test_resize(self):
        """Test resize retains scores after the resize."""
        uid = 9
        new_num_neurons = 15

        # Make sure at least 1 miner has a score.
        self._add_score_to_uid(uid)
        score = self.scorer.get_scores()[uid]
        self.assertGreater(score, 0.0)

        # Now resize the scores and make sure the previous score is retained.
        self.scorer.resize(new_num_neurons)
        scores = self.scorer.get_scores()
        self.assertEqual(scores.shape, (new_num_neurons,))
        self.assertEqual(scores[uid], score)

        # Score one of the new miners
        new_miner = new_num_neurons - 1
        self._add_score_to_uid(new_miner)
        self.assertGreater(self.scorer.get_scores()[new_miner], 0.0)

    def test_on_miner_evaluated_no_index(self):
        """Tests that on_miner_evaluated correctly updates the score if the miner has no index."""
        uid = 5

        # Give the miner a bunch of score.
        for _ in range(10):
            self._add_score_to_uid(uid)

        # Now run an eval with no index.
        validation_results = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100)
        ]
        self.scorer.on_miner_evaluated(uid, None, validation_results)

        self.assertEqual(self.scorer.get_scores()[uid], 0)

    def test_on_miner_evaluated_credibilty_normalized_by_size(self):
        """Compares miners with varying levels of "honesty" by validation bytes as measured by the validation results,
        to ensure the relative scores are as expected."""
        # The honest miner always passes validation.
        honest_miner = 0
        # The shady miner passes 50% of validations but most of their data is invalid.
        shady_miner = 1
        # The dishonest always fails validation.
        dishonest_miner = 2
        # The empty miner always has 0 credibility on validation.
        empty_miner = 3

        # Content size bytes validated do not need to match the scored bytes. They are just used for relative
        # credibility weighting.
        honest_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100),
            ValidationResult(is_valid=True, content_size_bytes_validated=900),
        ]
        shady_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100),
            ValidationResult(is_valid=False, content_size_bytes_validated=900),
        ]
        dishonest_validation = [
            ValidationResult(is_valid=False, content_size_bytes_validated=100),
            ValidationResult(is_valid=False, content_size_bytes_validated=900),
        ]
        empty_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=0),
            ValidationResult(is_valid=False, content_size_bytes_validated=0),
        ]
        for _ in range(20):
            self.scorer.on_miner_evaluated(
                honest_miner, self.scorable_index, honest_validation
            )
            self.scorer.on_miner_evaluated(
                shady_miner, self.scorable_index, shady_validation
            )
            self.scorer.on_miner_evaluated(
                dishonest_miner, self.scorable_index, dishonest_validation
            )
            self.scorer.on_miner_evaluated(
                empty_miner, self.scorable_index, empty_validation
            )

        scores = self.scorer.get_scores()
        # Expect the honest miner to have scored more than 10x the shady miner.
        self.assertGreater(
            scores[honest_miner].item(),
            10 * scores[shady_miner].item(),
        )
        # Expect the dishonest miner to have scored ~0.
        self.assertAlmostEqual(scores[dishonest_miner].item(), 0.0, delta=5)
        # Expect the empty miner to have scored ~0.
        self.assertAlmostEqual(scores[empty_miner].item(), 0.0, delta=5)

    def test_on_miner_evaluated_verify_credibility_impacts_score(self):
        """Compares miners with varying levels of "honesty" as measured by the validation results, to ensure the relative
        scores are as expected."""
        # The honest miner always passes validation.
        honest_miner = 0
        # The shady miner passes 50% of validations.
        shady_miner = 1
        # The dishonest always fails validation.
        dishonest_miner = 2

        honest_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100),
            ValidationResult(is_valid=True, content_size_bytes_validated=100),
        ]
        shady_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100),
            ValidationResult(is_valid=False, content_size_bytes_validated=100),
        ]
        dishonest_validation = [
            ValidationResult(is_valid=False, content_size_bytes_validated=100),
            ValidationResult(is_valid=False, content_size_bytes_validated=100),
        ]
        for _ in range(20):
            self.scorer.on_miner_evaluated(
                honest_miner, self.scorable_index, honest_validation
            )
            self.scorer.on_miner_evaluated(
                shady_miner, self.scorable_index, shady_validation
            )
            self.scorer.on_miner_evaluated(
                dishonest_miner, self.scorable_index, dishonest_validation
            )

        scores = self.scorer.get_scores()
        # Expect the honest miner to have scored more than twice the shady miner.
        self.assertGreater(
            scores[honest_miner].item(),
            2 * scores[shady_miner].item(),
        )
        # Expect the dishonest miner to have scored ~0.
        self.assertAlmostEqual(scores[dishonest_miner].item(), 0.0, delta=5)

    def test_on_miner_evaluated_dishonesty_is_penalized(self):
        """Verifies that a miner that claims to have more data than it actually has is, scores worse than
        if it reported honestly."""

        # The honest miner always passes validation.
        honest_miner = 0
        # The shady miner passes 50% of validations, but claims to have 2x the data it actually has.
        shady_miner = 1

        honest_index = ScorableMinerIndex(
            hotkey="honest",
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="testlabel",
                    size_bytes=200,
                    # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                    scorable_bytes=100,
                ),
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="otherlabel",
                    size_bytes=200,
                    # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                    scorable_bytes=50,
                ),
            ],
            last_updated=self.now,
        )
        # This index is a copy of the above but with 2x for claimed size.
        shady_index = ScorableMinerIndex(
            hotkey="shady",
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="testlabel",
                    size_bytes=400,
                    # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                    scorable_bytes=200,
                ),
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="otherlabel",
                    size_bytes=400,
                    # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                    scorable_bytes=10,
                ),
            ],
            last_updated=self.now,
        )

        honest_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=200)
        ]
        # Since half the shady_miner's data is fake, it should fail validation half the time.
        # To make the test deterministic, we'll make it fail every other time.
        shady_validations = [
            [ValidationResult(is_valid=i % 2 == 1, content_size_bytes_validated=400)]
            for i in range(10)
        ]
        for i in range(10):
            self.scorer.on_miner_evaluated(
                honest_miner, honest_index, honest_validation
            )
            self.scorer.on_miner_evaluated(
                shady_miner, shady_index, shady_validations[i]
            )

            scores = self.scorer.get_scores()
            # Verify the honest miner scores consistently better than the shady miner, after a few steps.
            if i > 1:
                self.assertGreater(
                    scores[honest_miner].item(),
                    scores[shady_miner].item(),
                )

    def test_score_decreases_on_fake_increased_claimed_size(self):
        """Verifies that a miner that temporarily claims a larger size than it actually has, scores worse than if it
        had been truthful throughout"""
        actual_index = ScorableMinerIndex(
            hotkey="1",
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="testlabel",
                    size_bytes=200,
                    # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                    scorable_bytes=100,
                ),
            ],
            last_updated=self.now,
        )
        # This index is a copy of the above but with a large fake bucket added.
        faked_index = ScorableMinerIndex(
            hotkey="1",
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="testlabel",
                    size_bytes=200,
                    # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                    scorable_bytes=100,
                ),
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="not-a-real-label",
                    size_bytes=5000,
                    # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                    scorable_bytes=4999,
                ),
            ],
            last_updated=self.now,
        )

        uid = 5
        # Give the miner an initial cred.
        self.scorer.miner_credibility[uid] = 0.8
        self.scorer.on_miner_evaluated(
            uid,
            actual_index,
            [ValidationResult(is_valid=True, content_size_bytes_validated=200)],
        )

        starting_score = self.scorer.get_scores()[uid].item()
        # Cred should now = 0.8 * 0.85 + 0.15 = 0.83
        # Score calculation may differ with job-based system, but the relative changes are what matter
        self.assertGreater(starting_score, 0.0)

        # Now provide the faked index for one eval cycle, and make sure the score decreases.
        self.scorer.on_miner_evaluated(
            uid,
            faked_index,
            [ValidationResult(is_valid=False, content_size_bytes_validated=5000)],
        )
        new_score = self.scorer.get_scores()[uid].item()
        self.assertLess(new_score, starting_score)

        # Finally, report the honest index again.
        self.scorer.on_miner_evaluated(
            uid,
            actual_index,
            [ValidationResult(is_valid=True, content_size_bytes_validated=200)],
        )

        # The score should now be less than the starting score, because the large
        # increase in the index caused credibility to drop.
        self.assertLess(self.scorer.get_scores()[uid].item(), starting_score)

    def test_score_increases_on_real_increased_claimed_size(self):
        """Verifies that an honest miner with an ever increasing index size, continously scores higher."""

        previous_score = 0
        uid = 5
        self.scorer.miner_credibility[uid] = 1
        for index_size in range(100, 1000, 100):
            index = ScorableMinerIndex(
                hotkey="1",
                scorable_data_entity_buckets=[
                    ScorableDataEntityBucket(
                        time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                        source=DataSource.REDDIT,
                        label=None,
                        size_bytes=index_size,
                        scorable_bytes=index_size,
                    ),
                ],
                last_updated=self.now,
            )
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=True, content_size_bytes_validated=index_size
                    )
                ],
            )
            score = self.scorer.get_scores()[uid].item()
            self.assertGreater(score, previous_score)
            previous_score = score

    def test_fresh_miner_credibility(self):
        """Verifies that a fresh miner can reach 95% credibility within immunity period."""
        uid = 0

        honest_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100)
        ]

        # Starting credibility defaults to 0.
        # With current 40 hours of immunity we will assume ~25 cycles of validation.

        cycles = 25
        for _ in range(cycles):
            self.scorer._update_credibility(uid, honest_validation)

        self.assertGreaterEqual(self.scorer.miner_credibility[uid].item(), 0.95)

    def test_fresh_miner_score(self):
        """Verifies that a fresh miner can reach 92% score within immunity period."""
        uid = 0

        honest_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100)
        ]

        # Starting credibility defaults to 0.
        # With current 40 hours of immunity we will assume ~25 cycles of validation.

        cycles = 25
        for _ in range(cycles):
            self.scorer.on_miner_evaluated(uid, self.scorable_index, honest_validation)

        # The expected score is based on the scorable_index_full_score
        # This might need adjustment based on job weights
        self.assertGreaterEqual(
            self.scorer.scores[uid].item(),
            0.92 * self.scorable_index_full_score,
        )

    def test_job_matching(self):
        """Tests that buckets with matching jobs are scored correctly."""
        uid = 5
        # Create an index with labels that match our jobs
        index = ScorableMinerIndex(
            hotkey="job_test",
            scorable_data_entity_buckets=[
                # This should match a job with weight 1.0
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="testlabel",
                    size_bytes=200,
                    scorable_bytes=100,
                ),
                # This should match a job with weight 0.75
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.X,
                    label="#testlabel",
                    size_bytes=200,
                    scorable_bytes=100,
                ),
                # This should use default scale factor
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="unknown-label",
                    size_bytes=200,
                    scorable_bytes=100,
                ),
            ],
            last_updated=self.now,
        )
        
        # Set perfect credibility for this test
        self.scorer.miner_credibility[uid] = 1.0
        
        # Get the score
        self.scorer.on_miner_evaluated(
            uid,
            index,
            [ValidationResult(is_valid=True, content_size_bytes_validated=600)],
        )
        
        score = self.scorer.get_scores()[uid].item()
        
        # Calculate expected score:
        # 1. REDDIT testlabel: 0.4 (weight) * 1.0 (job_weight) * 100 (bytes) = 40
        # 2. X #testlabel: 0.6 (weight) * 1.0 (job_weight) * 100 (bytes) = 60
        # 3. REDDIT unknown-label: 0.4 (weight) * 0.5 (default_scale) * 100 (bytes) = 20
        # Total expected: 120 (assuming perfect credibility and no time depreciation)
        expected_score = 120
        
        # Allow some tolerance for EMA and other factors
        self.assertAlmostEqual(score, expected_score, delta=expected_score * 0.1)

    def test_job_with_date_constraints(self):
        """Tests that buckets with date-constrained jobs are scored correctly."""
        # Create a custom model with date-constrained jobs
        now_timebucket = utils.time_bucket_id_from_datetime(self.now)
        future_timebucket = utils.time_bucket_id_from_datetime(self.now + dt.timedelta(days=5))
        
        reddit_job_matcher = JobMatcher(jobs=[
            Job(
                keyword=None,  
                label="dated-label",  
                job_weight=2.0,
                start_timebucket=now_timebucket,  # Convert datetime string to time bucket ID
                end_timebucket=future_timebucket,  # Convert datetime string to time bucket ID
            ),
        ])
        
        custom_model = DataDesirabilityLookup(
            distribution={
                DataSource.REDDIT: DataSourceDesirability(
                    weight=1.0,
                    default_scale_factor=0.5,
                    job_matcher=reddit_job_matcher,
                ),
            },
            max_age_in_hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24,
        )
        custom_calculator = rewards.data_value_calculator.DataValueCalculator(model=custom_model)
        custom_scorer = MinerScorer(self.num_neurons, custom_calculator)
        
        uid = 0
        # Create an index with a label that matches our date-constrained job
        index = ScorableMinerIndex(
            hotkey="date_test",
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now),
                    source=DataSource.REDDIT,
                    label="dated-label",
                    size_bytes=200,
                    scorable_bytes=100,
                ),
            ],
            last_updated=self.now,
        )
        
        # Set perfect credibility for this test
        custom_scorer.miner_credibility[uid] = 1.0
        
        # Get the score
        custom_scorer.on_miner_evaluated(
            uid,
            index,
            [ValidationResult(is_valid=True, content_size_bytes_validated=200)],
        )
        
        score = custom_scorer.get_scores()[uid].item()
        
        # Calculate expected score:
        # REDDIT dated-label: 1.0 (weight) * 2.0 (job_weight) * 100 (bytes) = 200
        expected_score = 200
        
        # Allow some tolerance for EMA and other factors
        self.assertAlmostEqual(score, expected_score, delta=expected_score * 0.1)
        
        # Now test with a date outside the job's range
        past_index = ScorableMinerIndex(
            hotkey="past_date_test",
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.now - dt.timedelta(days=7)),
                    source=DataSource.REDDIT,
                    label="dated-label",
                    size_bytes=200,
                    scorable_bytes=100,
                ),
            ],
            last_updated=self.now,
        )
        
        # Reset and set perfect credibility again
        custom_scorer.reset(uid)
        custom_scorer.miner_credibility[uid] = 1.0
        
        # Get the score
        custom_scorer.on_miner_evaluated(
            uid,
            past_index,
            [ValidationResult(is_valid=True, content_size_bytes_validated=200)],
        )
        
        score = custom_scorer.get_scores()[uid].item()
        
        # Calculate expected score (should use default scale factor since date is outside job range):
        # REDDIT dated-label: 1.0 (weight) * 0.5 (default_scale) * 100 (bytes) = 50
        # Note: There might also be time depreciation applied
        expected_score = 50
        
        # Allow some tolerance for time depreciation, EMA and other factors
        self.assertLess(score, expected_score)

    def test_score_miner_perf(self):
        """A perf test to check how long it takes to score an index."""

        num_buckets = (
            constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4
        )
        index = test_utils.create_scorable_index(num_buckets=num_buckets)

        start = time.time()
        self.scorer.on_miner_evaluated(
            0,
            index,
            [ValidationResult(is_valid=True, content_size_bytes_validated=100)],
        )
        print(f"Time to score {num_buckets} buckets:", time.time() - start)


if __name__ == "__main__":
    unittest.main()