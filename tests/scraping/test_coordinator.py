from collections import defaultdict
import datetime as dt
from typing import Any, Callable, Iterable
import unittest
from unittest.mock import Mock
from common import constants

from common.data import DataEntity, DataLabel, TimeBucket
from scraping.coordinator import (
    CoordinatorConfig,
    DataSource,
    ScraperConfig,
    LabelScrapingConfig,
    ScraperCoordinator,
    _choose_scrape_configs,
)
from scraping.provider import ScraperProvider
from scraping.scraper import ScrapeConfig, Scraper, ScraperId
from storage.miner.miner_storage import MinerStorage
import tests.utils as test_utils


class TestScraperCoordinator(unittest.TestCase):
    def test_tracker_get_sources_ready_to_scrape(self):
        """Tests the Coordinator's Tracker returns the sources that are ready to scrape."""
        # Create a CoordinatorConfig with two DataSourceScrapingConfig objects
        config = CoordinatorConfig(
            scraper_configs={
                ScraperId.REDDIT_LITE: ScraperConfig(
                    cadence_seconds=60,
                    labels_to_scrape=[],
                ),
                ScraperId.X_MICROWORLDS: ScraperConfig(
                    cadence_seconds=120,
                    labels_to_scrape=[],
                ),
            }
        )

        # Get the sources ready to scrape
        now = dt.datetime.now()

        # Create a Tracker with the config
        tracker = ScraperCoordinator.Tracker(config, now)

        # Verify that the data sources aren't ready to scrape yet because the
        # tracker should wait until the cadence has passed.
        self.assertEqual([], tracker.get_scraper_ids_ready_to_scrape(now))

        # Advance the clock by 60 seconds, and make sure only the first source is returned.
        now += dt.timedelta(seconds=60)
        self.assertEqual(
            [ScraperId.REDDIT_LITE], tracker.get_scraper_ids_ready_to_scrape(now)
        )

        tracker.on_scrape_scheduled(ScraperId.REDDIT_LITE, now)

        # Advance the clock by 15 seconds, and make sure nothing is returned.
        now += dt.timedelta(seconds=15)
        self.assertEqual([], tracker.get_scraper_ids_ready_to_scrape(now))

        # Advance the clock by 45 seconds, and make sure both sources are returned.
        now += dt.timedelta(seconds=45)
        self.assertEqual(
            [ScraperId.REDDIT_LITE, ScraperId.X_MICROWORLDS],
            tracker.get_scraper_ids_ready_to_scrape(now),
        )

    def test_choose_scrape_configs(self):
        """Verifies the Coordinator logic for choosing scrape configs."""

        config = CoordinatorConfig(
            scraper_configs={
                ScraperId.REDDIT_LITE: ScraperConfig(
                    cadence_seconds=60,
                    labels_to_scrape=[
                        LabelScrapingConfig(
                            label_choices=[
                                DataLabel(value="label1"),
                                DataLabel(value="label2"),
                            ],
                            max_data_entities=10,
                            max_age_hint_minutes=60 * 24 * 5,
                        ),
                        LabelScrapingConfig(
                            max_data_entities=20,
                            max_age_hint_minutes=30,
                        ),
                    ],
                ),
            }
        )

        # Choose the time, such that the labelless LabelScrapingConfig above should always land
        # in the TimeBucket associated with "now".
        now = dt.datetime(2023, 12, 12, 12, 45, 0)
        latest_time_bucket = TimeBucket.from_datetime(now)
        oldest_expected_time_bucket = TimeBucket.from_datetime(
            now - dt.timedelta(days=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
        )
        label_counts = defaultdict(int)
        time_counts = defaultdict(int)
        runs = 20000
        for _ in range(runs):
            scrape_configs = _choose_scrape_configs(ScraperId.REDDIT_LITE, config, now)

            self.assertEqual(2, len(scrape_configs))

            # Find the config that defines a label. There should be exactly 1.
            labeled_config: ScrapeConfig = test_utils.get_only_element_matching_filter(
                scrape_configs, lambda x: x.labels is not None
            )

            # Make sure only 1 label was picked.
            self.assertEqual(1, len(labeled_config.labels))
            label = labeled_config.labels[0]
            self.assertTrue(
                label in [DataLabel(value="label1"), DataLabel(value="label2")]
            )
            self.assertEqual(10, labeled_config.entity_limit)

            # Verify the time range is within the expected bounds.
            self.assertGreaterEqual(
                labeled_config.date_range.start,
                TimeBucket.to_date_range(oldest_expected_time_bucket).start,
            )
            self.assertLessEqual(
                labeled_config.date_range.end,
                TimeBucket.to_date_range(latest_time_bucket).end,
            )

            label_counts[label] += 1
            time_counts[labeled_config.date_range.end] += 1

            # Verify there's a scraping config without a label, and that the config is as expected.
            labelless_config = test_utils.get_only_element_matching_filter(
                scrape_configs, lambda x: x.labels is None
            )
            self.assertEqual(20, labelless_config.entity_limit)
            self.assertEqual(
                TimeBucket.to_date_range(latest_time_bucket),
                labelless_config.date_range,
            )

        # Check each label was chosen roughly half the time
        self.assertAlmostEqual(
            0.5, label_counts[DataLabel(value="label1")] / runs, delta=0.05
        )
        self.assertAlmostEqual(
            0.5, label_counts[DataLabel(value="label2")] / runs, delta=0.05
        )

        # Check that the time buckets are skewed toward newer buckets.
        # Group the time buckets into the day they belong to. Then iterate the days in order and make sure each
        # newer day was chosen more than the previous.
        day_bucket_counts = defaultdict(int)
        for datetime, count in time_counts.items():
            day_bucket_counts[datetime.date()] += count

        previous_count = 0
        for date in sorted(day_bucket_counts.keys()):
            count = day_bucket_counts[date]
            self.assertGreater(count, previous_count)
            previous_count = count

    def test_scraping_coordinator_runs(self):
        """Tests the ScrapingCoordinator successfully performs a scrape and stores it into storage."""
        # Create some DataEntities to return from the Mock Scraper.
        expected_entities = [
            DataEntity(
                uri="http://example.com",
                datetime=dt.datetime.now(),
                source=DataSource.REDDIT,
                content=b"content",
                content_size_bytes=7,
            )
        ]

        # Create a Mock Scraper
        mock_scraper = Mock(spec=Scraper)
        mock_scraper.scrape.return_value = expected_entities

        mock_storage = Mock(spec=MinerStorage)

        # Create a ScraperProvider that uses the Mock Scraper
        provider = ScraperProvider(
            factories={ScraperId.REDDIT_LITE: lambda: mock_scraper}
        )

        config = CoordinatorConfig(
            scraper_configs={
                ScraperId.REDDIT_LITE: ScraperConfig(
                    # Use a small cadence because the Coordinator will wait this amount of time
                    # before performing the first scrape.
                    cadence_seconds=1,
                    labels_to_scrape=[
                        LabelScrapingConfig(
                            label_choices=[DataLabel(value="label1")],
                            max_data_entities=10,
                            max_age_hint_minutes=60
                            * 24
                            * constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS,
                        ),
                    ],
                ),
            }
        )

        # Create the ScraperCoordinator with the ScraperProvider
        coordinator = ScraperCoordinator(
            scraper_provider=provider, miner_storage=mock_storage, config=config
        )

        # Start the ScraperCoordinator in the background.
        coordinator.run_in_background_thread()

        # Wait until the Mock Scraper's scrape() method has been called.
        test_utils.wait_for_condition(lambda: mock_scraper.scrape.called, timeout=30)

        # Get the ScrapeConfig passed to the mock and verify it's as expected
        args, _ = mock_scraper.scrape.call_args
        scrape_config = args[0]
        self.assertEqual(10, scrape_config.entity_limit)
        self.assertEqual([DataLabel(value="label1")], scrape_config.labels)

        # Now verify the entities were passed to the storage layer.
        test_utils.wait_for_condition(lambda: mock_storage.store_data_entities.called)
        args, _ = mock_storage.store_data_entities.call_args
        self.assertEqual(expected_entities, args[0])

        coordinator.stop()


if __name__ == "__main__":
    unittest.main()
