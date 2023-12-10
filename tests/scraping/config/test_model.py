import unittest

from scraping.config.model import ScrapingConfig

class TestScrapingConfig(unittest.TestCase):

    def test_serialization_deserialization(self):
        """Verifies a round-trip serialization/deserialization of the ScrapingConfig"""
        
        config = ScrapingConfig(
            data_source_configs=[
                {
                    "source": "X",
                    "cadence_secs": 60,
                    "labels_to_scrape": [
                        {
                            "label_choices": ["#bittensor", "#tao"],
                            "max_age_in_minutes": 60,
                            "max_items": 100
                        },
                        {
                            "label_choices": ["#decentralizedfinance", "#btc"],
                        }
                    ]
                },
                {
                    "source": "REDDIT",
                    "cadence_secs": 120,
                    "labels_to_scrape": [
                        {
                            "label_choices": ["r/bittensor_", "r/bitcoin"],
                            "max_age_in_minutes": 180,
                            "max_items": 200
                        }
                    ]
                }
            ]
        )

        # Serialize the object to JSON
        json_data = config.json()
        print(json_data)

        # Deserialize the JSON back to an object
        deserialized_config = ScrapingConfig.parse_raw(json_data)

        # Verify the deserialized object is equal to the starting object
        self.assertEqual(config, deserialized_config)


if __name__ == '__main__':
    unittest.main()

