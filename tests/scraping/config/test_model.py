import unittest

from scraping.config.model import (
    ScraperConfig,
    LabelScrapingConfig,
    ScrapingConfig,
)
from scraping.scraper import ScraperId


class TestScrapingConfig(unittest.TestCase):
    def test_serialization_deserialization(self):
        """Verifies a round-trip serialization/deserialization of the ScrapingConfig"""

        config = ScrapingConfig(
            scraper_configs=[
                ScraperConfig(
                    scraper_id=ScraperId.X_MICROWORLDS,
                    cadence_seconds=300,
                    labels_to_scrape=[
                        LabelScrapingConfig(
                            label_choices=["#bittensor", "#TAO"],
                            max_age_hint_minutes=1440,
                            max_data_entities=100,
                        ),
                        LabelScrapingConfig(
                            max_age_hint_minutes=10080,
                            max_data_entities=500,
                        ),
                    ],
                ),
                ScraperConfig(
                    scraper_id=ScraperId.REDDIT_LITE,
                    cadence_seconds=900,
                    labels_to_scrape=[
                        LabelScrapingConfig(
                            label_choices=["r/bittensor_"],
                            max_data_entities=50,
                        ),
                    ],
                ),
            ]
        )

        # Serialize the object to JSON
        json_data = config.json()
        print(json_data)

        # Deserialize the JSON back to an object
        deserialized_config = ScrapingConfig.parse_raw(json_data)

        # Verify the deserialized object is equal to the starting object
        self.assertEqual(config, deserialized_config)


if __name__ == "__main__":
    unittest.main()
