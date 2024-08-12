import unittest
import random
import matplotlib.pyplot as plt
import datetime as dt
from rewards.data_value_calculator import DataValueCalculator
from scraping.scraper import ValidationResult
from common.data_v2 import ScorableMinerIndex, ScorableDataEntityBucket
from rewards.miner_scorer import MinerScorer
from common.data import DataSource
from common import utils
from unittest.mock import Mock, patch
import rewards.data_value_calculator

@patch.object(rewards.data_value_calculator.dt, "datetime", Mock(wraps=dt.datetime))
class TestMinerScorer(unittest.TestCase):
    def setUp(self):
        num_neurons = 10  # Number of miners
        value_calculator = DataValueCalculator()
        self.scorer = MinerScorer(num_neurons, value_calculator)
        self.start_date = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=30)
        self.now = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=3)

    def simulate_evaluation(self, day_offset):
        """Simulate an evaluation for a miner on a specific day offset."""
        uid = 0  # Testing with a single miner
        index = ScorableMinerIndex(
            hotkey="abc123",
            scorable_data_entity_buckets=[
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.start_date + dt.timedelta(days=day_offset)),
                    source=DataSource.REDDIT,
                    label="testlabel",
                    size_bytes=1000,
                    scorable_bytes=500,
                ),
                ScorableDataEntityBucket(
                    time_bucket_id=utils.time_bucket_id_from_datetime(self.start_date + dt.timedelta(days=day_offset)),
                    source=DataSource.REDDIT,
                    label="otherlabel",
                    size_bytes=1000,
                    scorable_bytes=250,
                ),
            ],
            last_updated=self.start_date + dt.timedelta(days=day_offset),
        )
        validation_results = [
            ValidationResult(is_valid=True, content_size_bytes_validated=100)
        ]
        validation_date = self.start_date + dt.timedelta(days=day_offset)
        invalid_hf = True                            #never good uploads
        #invalid_hf = random.choice([True, False])     #sometimes good uploads (50%)
        #invalid_hf = False                            #always good uploads

        self.scorer.on_miner_evaluated(
            uid, index, validation_results, invalid_hf, validation_date
        )

    def test_miner_scores_over_time(self):
        """Test miner scores and credibility over a 30-day period and plot results."""
        days = list(range(31))  # From day 0 to day 30
        scores = []
        credibilities = []

        for day in days:
            self.simulate_evaluation(day)
            scores.append(self.scorer.get_scores()[0].item())
            credibilities.append(self.scorer.get_credibilities()[0].item())

        # Plot the results
        fig, ax1 = plt.subplots()

        color = 'tab:red'
        ax1.set_xlabel('Day')
        ax1.set_xlim(-1, 31)
        ax1.set_ylabel('Score', color=color)
        ax1.plot(days, scores, color=color, label='Score')
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.set_ylim(-10, 210)

        ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
        color = 'tab:blue'
        ax2.set_ylabel('Credibility', color=color)  # we already handled the x-label with ax1
        ax2.plot(days, credibilities, color=color, linestyle='--', label='Credibility')
        ax2.tick_params(axis='y', labelcolor=color)
        ax2.set_ylim(-0.05, 1.05)

        fig.tight_layout(pad=20)  # otherwise the right y-label is slightly clipped
        plt.title('Miner Score and Credibility Over Time - Pow, Never Uploads', pad=20)
        plt.show()

if __name__ == '__main__':
    unittest.main()