import asyncio
import functools
import random
import threading
import traceback
import bittensor as bt
import datetime as dt
from typing import Dict, List, Optional
import numpy
from pydantic import Field, PositiveInt, ConfigDict
from common.date_range import DateRange
from common.data import DataLabel, DataSource, StrictBaseModel, TimeBucket
from scraping.provider import ScraperProvider
from scraping.scraper import ScrapeConfig, ScraperId
from storage.miner.miner_storage import MinerStorage


class LabelScrapingConfig(StrictBaseModel):
    """Describes what labels to scrape."""

    label_choices: Optional[List[DataLabel]] = Field(
        description="""The collection of labels to choose from when performing a scrape.
        On a given scrape, 1 label will be chosen at random from this list.

        If the list is None, the scraper will scrape "all".
        """
    )

    max_age_hint_minutes: int = Field(
        description="""The maximum age of data that this scrape should fetch. A random TimeBucket (currently hour block),
        will be chosen within the time frame (now - max_age_hint_minutes, now), using a probality distribution aligned
        with how validators score data freshness.

        Note: not all data sources provide date filters, so this property should be thought of as a hint to the scraper, not a rule.
        """,
    )

    max_data_entities: Optional[PositiveInt] = Field(
        default=None,
        description="The maximum number of items to fetch in a single scrape for this label. If None, the scraper will fetch as many items possible.",
    )


class ScraperConfig(StrictBaseModel):
    """Describes what to scrape for a Scraper."""

    cadence_seconds: PositiveInt = Field(
        description="Configures how often to scrape with this scraper, measured in seconds."
    )

    labels_to_scrape: List[LabelScrapingConfig] = Field(
        description="""Describes the type of data to scrape with this scraper.

        The scraper will perform one scrape per entry in this list every 'cadence_seconds'.
        """
    )


class CoordinatorConfig(StrictBaseModel):
    """Informs the Coordinator how to schedule scrapes."""

    scraper_configs: Dict[ScraperId, ScraperConfig] = Field(
        description="The configs for each scraper."
    )


def _choose_scrape_configs(
        scraper_id: ScraperId, config: CoordinatorConfig, now: dt.datetime
) -> List[ScrapeConfig]:
    """For the given scraper, returns a list of scrapes (defined by ScrapeConfig) to be run."""
    assert (
            scraper_id in config.scraper_configs
    ), f"Scraper Id {scraper_id} not in config"

    # Ensure now has timezone information
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)

    scraper_config = config.scraper_configs[scraper_id]
    results = []

    for label_config in scraper_config.labels_to_scrape:
        # First, choose a label
        labels_to_scrape = None
        if label_config.label_choices:
            labels_to_scrape = [random.choice(label_config.label_choices)]

        # Get max age from config or use default
        max_age_minutes = label_config.max_age_hint_minutes

        # For YouTube transcript scraper, use a wider date range
        if scraper_id == ScraperId.YOUTUBE_TRANSCRIPT:
            # Calculate the start time using max_age_minutes
            start_time = now - dt.timedelta(minutes=max_age_minutes)

            # Ensure start_time has timezone information
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=dt.timezone.utc)

            date_range = DateRange(start=start_time, end=now)

            bt.logging.info(f"Created special date range for YouTube: {date_range.start} to {date_range.end}")
            bt.logging.info(f"Date range span: {(date_range.end - date_range.start).total_seconds() / 3600} hours")

            results.append(
                ScrapeConfig(
                    entity_limit=label_config.max_data_entities,
                    date_range=date_range,
                    labels=labels_to_scrape,
                )
            )
        else:
            # For other scrapers, use the normal time bucket approach
            current_bucket = TimeBucket.from_datetime(now)
            oldest_bucket = TimeBucket.from_datetime(
                now - dt.timedelta(minutes=max_age_minutes)
            )

            chosen_bucket = current_bucket
            # If we have more than 1 bucket to choose from, choose a bucket in the range
            if oldest_bucket.id < current_bucket.id:
                # Use a triangular distribution for bucket selection
                chosen_id = int(numpy.random.default_rng().triangular(
                    left=oldest_bucket.id, mode=current_bucket.id, right=current_bucket.id
                ))

                chosen_bucket = TimeBucket(id=chosen_id)

            date_range = TimeBucket.to_date_range(chosen_bucket)

            # Ensure date_range has timezone info
            if date_range.start.tzinfo is None:
                date_range = DateRange(
                    start=date_range.start.replace(tzinfo=dt.timezone.utc),
                    end=date_range.end.replace(tzinfo=dt.timezone.utc)
                )

            results.append(
                ScrapeConfig(
                    entity_limit=label_config.max_data_entities,
                    date_range=date_range,
                    labels=labels_to_scrape,
                )
            )

    return results

class ScraperCoordinator:
    """Coordinates all the scrapers necessary based on the specified target ScrapingDistribution."""

    class Tracker:
        """Tracks scrape runs for the coordinator."""

        def __init__(self, config: CoordinatorConfig, now: dt.datetime):
            self.cadence_by_scraper_id = {
                scraper_id: dt.timedelta(seconds=cfg.cadence_seconds)
                for scraper_id, cfg in config.scraper_configs.items()
            }

            # Initialize the last scrape time as now, to protect against frequent scraping during Miner crash loops.
            self.last_scrape_time_per_scraper_id: Dict[ScraperId, dt.datetime] = {
                scraper_id: now for scraper_id in config.scraper_configs.keys()
            }

        def get_scraper_ids_ready_to_scrape(self, now: dt.datetime) -> List[ScraperId]:
            """Returns a list of ScraperIds which are due to run."""
            results = []
            for scraper_id, cadence in self.cadence_by_scraper_id.items():
                last_scrape_time = self.last_scrape_time_per_scraper_id.get(
                    scraper_id, None
                )
                if last_scrape_time is None or now - last_scrape_time >= cadence:
                    results.append(scraper_id)
            return results

        def on_scrape_scheduled(self, scraper_id: ScraperId, now: dt.datetime):
            """Notifies the tracker that a scrape has been scheduled."""
            self.last_scrape_time_per_scraper_id[scraper_id] = now

    def __init__(
        self,
        scraper_provider: ScraperProvider,
        miner_storage: MinerStorage,
        config: CoordinatorConfig,
    ):
        self.provider = scraper_provider
        self.storage = miner_storage
        self.config = config

        self.tracker = ScraperCoordinator.Tracker(self.config, dt.datetime.utcnow())
        self.max_workers = 5
        self.is_running = False
        self.queue = asyncio.Queue()

    def run_in_background_thread(self):
        """
        Runs the Coordinator on a background thread. The coordinator will run until the process dies.
        """
        assert not self.is_running, "ScrapingCoordinator already running"

        bt.logging.info("Starting ScrapingCoordinator in a background thread.")

        self.is_running = True
        self.thread = threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        """Blocking call to run the Coordinator, indefinitely."""
        asyncio.run(self._start())

    def stop(self):
        bt.logging.info("Stopping the ScrapingCoordinator.")
        self.is_running = False

    async def _start(self):
        workers = []
        for i in range(self.max_workers):
            worker = asyncio.create_task(
                self._worker(
                    f"worker-{i}",
                )
            )
            workers.append(worker)

        while self.is_running:
            now = dt.datetime.utcnow()
            scraper_ids_to_scrape_now = self.tracker.get_scraper_ids_ready_to_scrape(
                now
            )
            if not scraper_ids_to_scrape_now:
                bt.logging.trace("Nothing ready to scrape yet. Trying again in 15s.")
                # Nothing is due a scrape. Wait a few seconds and try again
                await asyncio.sleep(15)
                continue

            for scraper_id in scraper_ids_to_scrape_now:
                scraper = self.provider.get(scraper_id)

                scrape_configs = _choose_scrape_configs(scraper_id, self.config, now)

                for config in scrape_configs:
                    # Use .partial here to make sure the functions arguments are copied/stored
                    # now rather than being lazily evaluated (if a lambda was used).
                    # https://pylint.readthedocs.io/en/latest/user_guide/messages/warning/cell-var-from-loop.html#cell-var-from-loop-w0640
                    bt.logging.trace(f"Adding scrape task for {scraper_id}: {config}.")
                    self.queue.put_nowait(functools.partial(scraper.scrape, config))

                self.tracker.on_scrape_scheduled(scraper_id, now)

        bt.logging.info("Coordinator shutting down. Waiting for workers to finish.")
        await asyncio.gather(*workers)
        bt.logging.info("Coordinator stopped.")

    async def _worker(self, name):
        """A worker thread"""
        while self.is_running:
            try:
                # Wait for a scraping task to be added to the queue.
                scrape_fn = await self.queue.get()

                # Perform the scrape
                data_entities = await scrape_fn()

                self.storage.store_data_entities(data_entities)
                self.queue.task_done()
            except Exception as e:
                bt.logging.error("Worker " + name + ": " + traceback.format_exc())
