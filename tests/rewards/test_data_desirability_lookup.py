import unittest

from attr import dataclass
from common import constants, utils
from common.data import DataLabel, DataSource
from rewards.data import DataSourceDesirability, DataDesirabilityLookup
from rewards import data_desirability_lookup


class TestDataDesirabilityLookup(unittest.TestCase):
    def setUp(self):
        self.lookup = data_desirability_lookup.LOOKUP

    def test_reddit_labels_start_with_r(self):
        reddit_distribution = self.lookup.distribution[DataSource.REDDIT]
        for label, _ in reddit_distribution.label_scale_factors.items():
            self.assertTrue(label.value.startswith("r/"))

    def test_twitter_labels_start_with_hashtag(self):
        twitter_distribution = self.lookup.distribution[DataSource.X]
        for label, _ in twitter_distribution.label_scale_factors.items():
            self.assertTrue(label.value.startswith("#"))
