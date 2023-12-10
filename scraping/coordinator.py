import asyncio
import functools
import random
import threading
import traceback
import bittensor as bt
import datetime as dt
from typing import Dict, List, Optional
import numpy
from pydantic import BaseModel, Field, PositiveInt

from common.data import DataLabel, DataSource, TimeBucket
from scraping.provider import ScraperProvider
from scraping.scraper import ScrapeConfig
from storage.miner.miner_storage import MinerStorage


class LabelScrapeConfig(BaseModel):
    """Describes what labels to scrape."""

    label_choices: Optional[List[DataLabel]] = Field(
        description="""The collection of labels to choose from when performing a scrape.
        On a given scrape, 1 label will be chosen at random from this list.
        
        If the list is None, the scraper will scrape "all".
        """
    )

    max_age_in_minutes: int = Field(
        description="""The maximum age of data that this scrape should fetch. A random TimeBucket (currently hour block),
        will be chosen within the time frame (now - max_age_in_minutes, now), using a probality distribution aligned
        with how validators score data freshness.
        
        Note: not all data sources provide date filters, so this property should be thought of as a hint to the scraper, not a rule.
        """,
    )

    max_items: Optional[PositiveInt] = Field(
        default=None,
        description="The maximum number of items to fetch in a single scrape for this label. If None, the scraper will fetch as many items possible.",
    )


class DataSourceScrapingConfig(BaseModel):
    """Describes what to scrape for a DataSource."""

    source: DataSource

    cadence_secs: PositiveInt = Field(
        description="Configures how often to scrape from this data source, measured in seconds."
    )

    labels_to_scrape: List[LabelScrapeConfig] = Field(
        description="""Describes the type of data to scrape from this source.
        
        The scraper will perform one scrape per entry in this list every 'cadence_secs'.
        """
    )


class CoordinatorConfig(BaseModel):
    """Informs the Coordinator how to schedule scrapes."""

    scraping_configs: Dict[DataSource, DataSourceScrapingConfig] = Field(
        description="The scraping config for each data source to be scraped."
    )


def _choose_scrape_configs(
    source: DataSource, config: CoordinatorConfig, now: dt.datetime
) -> List[ScrapeConfig]:
    """For the given source, returns a list of scrapes (defined by ScrapeConfig) to be run."""
    assert source in config.scraping_configs, f"Source {source} not in config"

    source_config = config.scraping_configs[source]
    results = []
    for label_config in source_config.labels_to_scrape:
        # First, choose a label
        labels_to_scrape = None
        if label_config.label_choices:
            labels_to_scrape = [random.choice(label_config.label_choices)]

        # Now, choose a time bucket to scrape.
        current_bucket = TimeBucket.from_datetime(now)
        oldest_bucket = TimeBucket.from_datetime(
            now - dt.timedelta(minutes=label_config.max_age_in_minutes)
        )

        # Use a triangular distribution to choose a bucket in this range. We choose a triangular distribution because
        # this roughly aligns with the linear depreciation scoring that the validators use for data freshness.
        chosen_id = numpy.random.Generator.triangular(
            left=oldest_bucket.id, mode=current_bucket.id, right=current_bucket.id
        )
        chosen_bucket = TimeBucket(id=chosen_id)

        results.append(
            ScrapeConfig(
                entity_limit=label_config.max_items,
                date_range=chosen_bucket.get_date_range(),
                labels=labels_to_scrape,
            )
        )

    return results


class ScraperCoordinator:
    """Coordinates all the scrapers necessary based on the specified target ScrapingDistribution."""

    class Tracker:
        """Tracks scrape runs for the coordinator."""

        def __init__(self, config: CoordinatorConfig):
            self.cadence_by_source = {
                cfg.source: dt.timedelta(seconds=cfg.cadence_secs)
                for cfg in config.scraping_configs
            }
            self.last_scrape_time_per_source: Dict[DataSource, dt.datetime] = {}

        def get_sources_ready_to_scrape(self, now: dt.datetime) -> List[DataSource]:
            """Returns a list of DataSources which are due to run."""
            results = []
            for source, cadence in self.cadence_by_source.items():
                last_scrape_time = self.last_scrape_time_per_source.get(source, None)
                if last_scrape_time is None or now - last_scrape_time > cadence:
                    results.append(source)
            return results

        def on_scrape_scheduled(self, source: DataSource, now: dt.datetime):
            """Notifies the tracker that a scrape has been scheduled."""
            self.last_scrape_time_per_source[source] = now

    def __init__(
        self,
        scraper_provider: ScraperProvider,
        miner_storage: MinerStorage,
        config: CoordinatorConfig,
    ):
        self.provider = scraper_provider
        self.storage = miner_storage
        self.config = config

        self.tracker = ScraperCoordinator.Tracker(self.config)
        self.max_workers = 5
        self.is_running = False
        self.queue = asyncio.Queue()

    def run_in_background_thread(self):
        """
        Runs the Coordinator on a background thread. The coordinator will run until the process dies.
        """
        assert not self.is_running, "ScrapingCoordinator already running"
        threading.Thread(target=self.run, daemon=True).start()
        self.is_running = True

    def run(self):
        """Blocking call to run the Coordinator, indefinitely."""
        asyncio.run(self._start())

    def stop(self):
        self.is_running = False
        bt.logging.info("Stopping the ScrapingCoordinator")

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
            sources_to_scrape_now = self.tracker.get_sources_ready_to_scrape(now)
            if not sources_to_scrape_now:
                # Nothing is due a scrape. Wait a few seconds and try again
                await asyncio.sleep(15)
                continue

            for source in sources_to_scrape_now:
                scraper = self.provider.get(source)

                scrape_configs = _choose_scrape_configs(source, self.config, now)

                for config in scrape_configs:
                    # Use .partial here to make sure the functions arguments are copied/stored
                    # now rather than being lazily evaluated (if a lambda was used).
                    # https://pylint.readthedocs.io/en/latest/user_guide/messages/warning/cell-var-from-loop.html#cell-var-from-loop-w0640
                    bt.logging.trace(f"Adding scrape task for {source}: {config}")
                    self.queue.put(functools.partial(scraper.scrape, config))

                self.tracker.on_scrape_scheduled(source, now)

        bt.logging.info("Coordinator shutting down. Waiting for workers to finish")
        await asyncio.gather(*workers)
        bt.logging.info("Coordinator stopped")

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
