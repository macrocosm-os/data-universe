

import asyncio
import traceback
import bittensor as bt
from typing import Dict, List, Optional
from pydantic import BaseModel, Field, PositiveInt

from common.data import DataLabel, DataSource
from scraping.provider import ScraperProvider
from scraping.scraper import ScrapingDistribution
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
        description="The maximum number of items to fetch in a single scrape for this label. If None, the scraper will fetch as many items possible."
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
    scraping_configs: List[DataSourceScrapingConfig] = Field(
        description="The scraping config for each data source to be scraped."
    )
    
class ScraperCoordinator:
    """Coordinates all the scrapers necessary based on the specified target ScrapingDistribution."""

    def __init__(
        self,
        scraper_provider: ScraperProvider,
        scraping_distribution: ScrapingDistribution,
        miner_storage: MinerStorage,
        max_scrapes_per_minute: Dict[DataSource, PositiveInt],
    ):
        self.provider = scraper_provider
        self.distribution = scraping_distribution
        self.storage = miner_storage
        self.scrape_limits = max_scrapes_per_minute
        self.max_workers = 5
        self.queue = asyncio.Queue()

    def start(self):
        asyncio.run(self._start())

    async def _start(self):
        tasks = []
        for i in range(self.max_workers):
            task = asyncio.create_task(
                self.worker(
                    f"worker-{i}",
                )
            )
            tasks.append(task)

        while True:
            # TODO implement as below
            # for each source
            # up to rate limit, sample distribution
            # add to queue a function to scrape
            # wait a minute

            # 1) timer based add to queue based on the
            # loop per async method
            # while true
            # make ScrapeConfig randomly from scraping_distribution
            # await scrape()
            # store in storage
            pass

    async def worker(self, name):
        while True:
            try:
                function = await self.queue.get()
                data_entities = await function()
                self.storage.store_data_entities(data_entities)
                self.queue.task_done()
            except Exception as e:
                bt.logging.error("Worker " + name + ": " + traceback.format_exc())