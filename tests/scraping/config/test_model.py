import unittest

from scraping.config.model import (
    DataSourceScrapingConfig,
    LabelScrapeConfig,
    ScrapingConfig,
)


class TestScrapingConfig(unittest.TestCase):
    def test_serialization_deserialization(self):
        """Verifies a round-trip serialization/deserialization of the ScrapingConfig"""

        config = ScrapingConfig(
            scraping_configs=[
                DataSourceScrapingConfig(
                    source="X",
                    cadence_secs=300,
                    labels_to_scrape=[
                        LabelScrapeConfig(
                            label_choices=["#bittensor", "#TAO"],
                            max_age_in_minutes=1440,
                            max_items=100,
                        ),
                        LabelScrapeConfig(
                            max_age_in_minutes=10080,
                            max_items=500,
                        ),
                    ],
                ),
                DataSourceScrapingConfig(
                    source="reddit",
                    cadence_secs=900,
                    labels_to_scrape=[
                        LabelScrapeConfig(
                            label_choices=["r/bittensor_"],
                            max_items=50,
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
