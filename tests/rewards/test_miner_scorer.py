import random
import unittest
from unittest.mock import MagicMock, Mock, patch
from typing import List
import torch
from common.data_v2 import ScorableDataEntityBucket, ScorableMinerIndex
from rewards.data import DataSourceDesirability, DataDesirabilityLookup
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


@patch.object(rewards.data_value_calculator.dt, "datetime", Mock(wraps=dt.datetime))
class TestMinerScorer(unittest.TestCase):
    def setUp(self):
        self.num_neurons = 10
        self.value_calculator = rewards.data_value_calculator.DataValueCalculator(
            DataDesirabilityLookup(
                distribution={
                    DataSource.REDDIT: DataSourceDesirability(
                        weight=1,
                        default_scale_factor=1,
                    ),
                },
                max_age_in_hours=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24,
            )
        )
        self.scorer = MinerScorer(self.num_neurons, self.value_calculator, alpha=0.2)

        # Mock the current time
        self.now = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=3)

        # Construct a ScorableMinerIndex with 2 chunks that should score 150 in total (without considering EMA)
        self.scorable_index = ScorableMinerIndex(
            hotkey="abc123",
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

    def test_get_credible_miners(self):
        """Test get_credible_miners returns the correct set of miners."""

        # Miners should start with a credibility score below the threshold to be credible.
        self.assertEqual([], self.scorer.get_credible_miners())

        # Create 2 miners: 1 honest, 1 shady
        # The honest miner always passes validation.
        honest_miner = 0
        # The shady miner passes 50% of validations.
        shady_miner = 1

        honest_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100),
            ValidationResult(is_valid=True, content_size_bytes_validated=100),
        ]
        shady_validation = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100),
            ValidationResult(is_valid=False, content_size_bytes_validated=100),
        ]

        # Perform a bunch of validations for them.
        for _ in range(20):
            self.scorer.on_miner_evaluated(
                honest_miner, self.scorable_index, honest_validation
            )
            self.scorer.on_miner_evaluated(
                shady_miner, self.scorable_index, shady_validation
            )

        # Now verify the honest miner is credible
        self.assertEqual([honest_miner], self.scorer.get_credible_miners())

    def test_on_miner_evaluated_no_index(self):
        """Tests that on_miner_evaluated correctly updates the score if the miner has no index."""
        uid = 5

        # Give the miner a bunch of score.
        for _ in range(10):
            self._add_score_to_uid(uid)

        score = self.scorer.get_scores()[uid]

        # Now run an eval with no index.
        validation_results = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100)
        ]
        self.scorer.on_miner_evaluated(uid, None, validation_results)

        new_score = self.scorer.get_scores()[uid]
        self.assertGreater(new_score, 0)
        self.assertLess(new_score, score)

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

    def test_update_score_respects_growth_limit_threshhold(self):
        """Verifies that a score can only increase up to the threshold in one cycle."""
        uid = 0
        self.scorer.scores[uid] = 0

        # Update by an amount big enough to account for alpha.
        self.scorer._update_score(uid, constants.SCORE_GROWTH_LIMIT_THRESHOLD * 1000)

        self.assertEqual(
            self.scorer.scores[uid], constants.SCORE_GROWTH_LIMIT_THRESHOLD
        )

    def test_update_score_respects_percent_limit(self):
        """Verifies that a score can only increase by a percent amount in one cycle."""
        uid = 0
        # Start with an amount big enough to account for alpha and beat out the flat increase.
        self.scorer.scores[uid] = constants.SCORE_GROWTH_LIMIT_THRESHOLD

        self.scorer._update_score(uid, constants.SCORE_GROWTH_LIMIT_THRESHOLD * 1000)

        self.assertEqual(
            self.scorer.scores[uid],
            constants.SCORE_GROWTH_LIMIT_THRESHOLD
            * constants.SCORE_GROWTH_LIMIT_PERCENT,
        )


if __name__ == "__main__":
    unittest.main()
