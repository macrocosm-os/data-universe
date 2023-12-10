import datetime as dt
import unittest
from scraping.coordinator import CoordinatorConfig, DataSource, DataSourceScrapingConfig, ScraperCoordinator

class TestScraperCoordinator(unittest.TestCase):
    def test_tracker_get_sources_ready_to_scrape(self):
        # Create a CoordinatorConfig with two DataSourceScrapingConfig objects
        config = CoordinatorConfig(
            scraping_configs={
                DataSource.REDDIT: DataSourceScrapingConfig(
                    source=DataSource.REDDIT,
                    cadence_secs=60,
                    labels_to_scrape=[],
                ),
                DataSource.X: DataSourceScrapingConfig(
                    source=DataSource.X,
                    cadence_secs=120,
                    labels_to_scrape=[],
                ),
            }
        )

        # Create a Tracker with the config
        tracker = ScraperCoordinator.Tracker(config)

        # Get the sources ready to scrape
        now = dt.datetime.now()
        sources = tracker.get_sources_ready_to_scrape(now)

        # Verify that both data sources are returned
        expected_sources = [DataSource.REDDIT, DataSource.X]
        self.assertEqual(sources, expected_sources)
        
        # Advance the clock by 45 seconds, and make sure no sources are returned.
        now += dt.timedelta(seconds=45)
        self.assertEqual([], tracker.get_sources_ready_to_scrape(now))
        
        # Advance the clock by 15 seconds, and make sure only the first source is returned.
        now += dt.timedelta(seconds=15)
        self.assertEqual([DataSource.REDDIT], tracker.get_sources_ready_to_scrape(now))
        

if __name__ == "__main__":
    unittest.main()