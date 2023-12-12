import os
import unittest
from unittest.mock import patch
from common.data import DataLabel, DataSource
from scraping.config.config_reader import ConfigReader
from scraping.coordinator import (
    CoordinatorConfig,
    ScraperConfig,
    LabelScrapingConfig,
)
from scraping.scraper import ScraperId


class TestConfigReader(unittest.TestCase):
    def test_load_config_valid(self):
        """Tests a valid config is loaded correctly."""
        expected_config = CoordinatorConfig(
            scraper_configs={
                ScraperId.X_FLASH: ScraperConfig(
                    scraper_id=ScraperId.X_FLASH,
                    cadence_seconds=300,
                    labels_to_scrape=[
                        LabelScrapingConfig(
                            label_choices=[
                                DataLabel(value="#bittensor"),
                                DataLabel(value="#TAO"),
                            ],
                            max_age_hint_minutes=1440,
                            max_data_entities=100,
                        ),
                        LabelScrapingConfig(
                            max_age_hint_minutes=10080,
                            max_data_entities=500,
                        ),
                    ],
                ),
                ScraperId.REDDIT_LITE: ScraperConfig(
                    scraper_id=ScraperId.REDDIT_LITE,
                    cadence_seconds=900,
                    labels_to_scrape=[
                        LabelScrapingConfig(
                            label_choices=[
                                DataLabel(value="r/bittensor_"),
                                DataLabel(value="r/bitcoin"),
                            ],
                            max_age_hint_minutes=10080,
                            max_data_entities=50,
                        ),
                    ],
                )
            }
        )

        this_dir = os.path.abspath(os.path.dirname(__file__))
        filepath = os.path.join(this_dir, "valid_config.json")
        loaded_config = ConfigReader.load_config(filepath)

        self.assertEqual(loaded_config, expected_config)

    def test_load_config_invalid(self):
        """Tests that loading an invalid config raises an exception."""
        this_dir = os.path.abspath(os.path.dirname(__file__))
        filepath = os.path.join(this_dir, "invalid_config.json")

        with self.assertRaises(Exception) as e:
            ConfigReader.load_config(filepath)
        self.assertIn("scraper_id\n  value is not a valid enumeration member", str(e.exception))


if __name__ == "__main__":
    unittest.main()
