import unittest

from attr import dataclass
from common import constants, utils
from common.data import DataLabel, DataSource
from rewards.data import DynamicSourceDesirability, DynamicDesirabilityLookup
from tests.rewards import test_lookup


class TestDynamicDesirabilityLookup(unittest.TestCase):
    def setUp(self):
        self.lookup = test_lookup.LOOKUP

    def test_reddit_labels_start_with_r(self):
        reddit_distribution = self.lookup.distribution[DataSource.REDDIT]
        for label, _ in reddit_distribution.label_config.items():
            self.assertTrue(label.value.startswith("r/"))

    def test_twitter_labels_start_with_hashtag(self):
        twitter_distribution = self.lookup.distribution[DataSource.X]
        for label, _ in twitter_distribution.label_config.items():
            self.assertTrue(label.value.startswith("#"))

if __name__ == "__main__":
    unittest.main()