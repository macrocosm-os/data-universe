import os
import unittest
from unittest.mock import patch
from common.data import DataLabel, DataSource
from scraping.config.config_reader import ConfigReader
from scraping.coordinator import CoordinatorConfig, DataSourceScrapingConfig, LabelScrapeConfig

class TestConfigReader(unittest.TestCase):

    def test_load_config_valid(self):
        """Tests a valid config is loaded correctly."""
        expected_config = CoordinatorConfig(
            scraping_configs=[
                DataSourceScrapingConfig(
                    source=DataSource.X,
                    cadence_secs=300,
                    labels_to_scrape=[
                        LabelScrapeConfig(
                            label_choices=[DataLabel(value="#bittensor"), DataLabel(value="#TAO")],
                            max_age_in_minutes=1440,
                            max_items=100
                        ),
                        LabelScrapeConfig(
                            max_age_in_minutes=10080,
                            max_items=500,
                        )
                    ]
                ),
                DataSourceScrapingConfig(
                    source=DataSource.REDDIT,
                    cadence_secs=900,
                    labels_to_scrape=[
                        LabelScrapeConfig(
                            label_choices=[DataLabel(value="r/bittensor_"), DataLabel(value="r/bitcoin")],
                            max_age_in_minutes=10080,
                            max_items=50
                        ),
                    ]
                ),
            ]
        )
        
        this_dir = os.path.abspath(os.path.dirname(__file__))
        filepath = os.path.join(this_dir, "valid_config.json")
        loaded_config = ConfigReader.load_config(filepath)
        
        print(f"loaded config: {loaded_config}")
        
        self.assertEqual(loaded_config, expected_config)
    
    def test_load_config_invalid(self):
        """Tests that loading an invalid config raises an exception."""
        this_dir = os.path.abspath(os.path.dirname(__file__))
        filepath = os.path.join(this_dir, "invalid_config.json")
        
        with self.assertRaises(Exception):
            ConfigReader.load_config(filepath)

if __name__ == '__main__':
    unittest.main()
