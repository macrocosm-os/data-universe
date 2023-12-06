import unittest
from unittest import mock
from unittest.mock import Mock, patch

from attr import dataclass
from rewards.data import DataSourceReward, RewardDistributionModel
from rewards.reward_distribution import RewardDistribution
from common.data import DataLabel, DataSource, Hour, ScorableDataChunkSummary
import datetime as dt
import rewards.reward_distribution


@patch.object(rewards.reward_distribution, "datetime", Mock(wraps=dt.datetime))
class TestRewardDistribution(unittest.TestCase):
    def setUp(self):
        model = RewardDistributionModel(
            distribution={
                DataSource.REDDIT: DataSourceReward(
                    weight=0.75,
                    default_scale_factor=0.5,
                    label_scale_factors={
                        # Labels include upper and lower case to ensure matching is case insensitive.
                        DataLabel(value="TestLABEL"): 1.0,
                        DataLabel(value="unscoredLabel"): 0,
                        DataLabel(value="penalizedLABEL"): -1.0,
                    },
                ),
                DataSource.X: DataSourceReward(
                    weight=0.25,
                    default_scale_factor=0.8,
                    label_scale_factors={
                        DataLabel(value="#TestLABEL"): 1.0,
                        DataLabel(value="#unscoredLabel"): 0,
                        DataLabel(value="#penalizedLABEL"): -1.0,
                    },
                ),
            },
            max_age_in_hours=7 * 24,
        )
        self.reward_distribution = RewardDistribution(model=model)

    def test_get_score_for_chunk_with_matching_label(self):
        """Generates a chunk with various data sources and labels and ensures the score is correct."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0)
        rewards.reward_distribution.datetime.utcnow.return_value = now

        @dataclass(frozen=True)
        class TestCaseInput:
            data_source: DataSource
            data_label: DataLabel

        # List of test case input and expected score, where the scores all expect 100 scorable bytes.
        test_cases = [
            (TestCaseInput(DataSource.REDDIT, DataLabel(value="testlabel")), 75.0),
            (TestCaseInput(DataSource.REDDIT, DataLabel(value="unscoredlabel")), 0),
            (
                TestCaseInput(DataSource.REDDIT, DataLabel(value="PenAlizedLABEL")),
                -75.0,
            ),
            (TestCaseInput(DataSource.REDDIT, DataLabel(value="other-label")), 37.5),
            (TestCaseInput(DataSource.X, DataLabel(value="#testlabel")), 25.0),
            (TestCaseInput(DataSource.X, DataLabel(value="#unscoredLabel")), 0),
            (TestCaseInput(DataSource.X, DataLabel(value="#penalizedLABEL")), -25.0),
            (TestCaseInput(DataSource.X, DataLabel(value="#other-label")), 20.0),
        ]

        # Verify data from the current hour, with various labels are scored correctly.
        hour = Hour.from_datetime(now)
        for tc in test_cases:
            chunk = ScorableDataChunkSummary(
                hour=hour,
                source=tc[0].data_source,
                label=tc[0].data_label,
                size_bytes=200,
                # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                scorable_bytes=100,
            )
            score = self.reward_distribution.get_score_for_chunk(chunk)
            self.assertAlmostEqual(score, tc[1], places=5)

    def test_get_score_for_chunk_score_decreases_over_time(self):
        """Generates a chunk containing data of various ages and verifies the score is as expected."""
        now = dt.datetime(2023, 12, 12, 12, 30, 0)
        rewards.reward_distribution.datetime.utcnow.return_value = now

        # Verify score at the present hour is scored at 100%.
        hour = Hour.from_datetime(now)
        chunk = ScorableDataChunkSummary(
            hour=hour,
            source=DataSource.REDDIT,
            label=DataLabel(value="testlabel"),
            size_bytes=200,
            # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
            scorable_bytes=100,
        )
        self.assertAlmostEqual(
            self.reward_distribution.get_score_for_chunk(chunk), 75.0, places=5
        )

        # Verify the score at the max age is scored at 50%.
        chunk = ScorableDataChunkSummary(
            hour=Hour.from_datetime(now - dt.timedelta(hours=7 * 24)),
            source=DataSource.REDDIT,
            label=DataLabel(value="testlabel"),
            size_bytes=200,
            # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
            scorable_bytes=100,
        )
        self.assertAlmostEqual(
            self.reward_distribution.get_score_for_chunk(chunk), 37.5, places=5
        )

        # Verify the score past the max age is 0.
        chunk = ScorableDataChunkSummary(
            hour=Hour.from_datetime(now - dt.timedelta(hours=7 * 24 + 1)),
            source=DataSource.REDDIT,
            label=DataLabel(value="testlabel"),
            size_bytes=200,
            # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
            scorable_bytes=100,
        )
        self.assertAlmostEqual(
            self.reward_distribution.get_score_for_chunk(chunk), 0, places=5
        )

        # Now verify the score decreases between now and max_age.
        previous_score = 75.0
        for hours_back in range(1, 7 * 24):
            chunk = ScorableDataChunkSummary(
                hour=Hour.from_datetime(now - dt.timedelta(hours=hours_back)),
                source=DataSource.REDDIT,
                label=DataLabel(value="testlabel"),
                size_bytes=200,
                # scorable_bytes is different from size_bytes to ensure the score is based on scorable_bytes.
                scorable_bytes=100,
            )
            score = self.reward_distribution.get_score_for_chunk(chunk)
            self.assertLess(score, previous_score)
            previous_score = score


if __name__ == "__main__":
    unittest.main()
