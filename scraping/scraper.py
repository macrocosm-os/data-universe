import abc
import bittensor as bt
import asyncio
import traceback
from typing import Dict, List

from pydantic import BaseModel, PositiveInt

from common.data import DataEntity, DataLabel, DataSource, DateRange
from storage.miner import MinerStorage


class ValidationResult(BaseModel):
    """Data class to contain the result of a scraping validation."""

    class Config:
        frozen = True

    # For now, let's just indicate a pass/fail, but in future we may want to extend this to
    # include more information about the validation.
    is_valid: bool


class ScrapeConfig(BaseModel):
    """Data class to contain the configuration to be used for scraping."""

    class Config:
        frozen = True

    # Number of entities (based on source) to get per scrape attempt.
    entity_limit: int

    # Date range within which the scraper should scrape.
    date_range: DateRange

    # Labels for the scrape to scrape from.
    labels: List[DataLabel]


class LabelScrapingFrequency(BaseModel):
    """Data class to contain the frequency distribution for a set of labels."""

    class Config:
        frozen = True

    # The collection of labels that share this total frequency.
    labels: List[DataLabel]
    # The frequency for which this set of labels should be scraped.
    frequency: float


class SourceScrapingFrequency(BaseModel):
    """Data class to contain the frequency distribution for a source across labels."""

    class Config:
        frozen = True

    # The source being scraped.
    source: DataSource
    # The frequency for which this source should be scraped.
    frequency: float

    # TODO A validator to ensure that that sum of all label frequencies is 1.
    label_frequencies: List[LabelScrapingFrequency]


class ScapingDistribution(BaseModel):
    """A relative distribution across sources and labels."""

    class Config:
        frozen = True

    # TODO A validator to ensure that the sum of all source frequencies is 1.
    distribution: List[SourceScrapingFrequency]


class Scraper(abc.ABC):
    """An abstract base class for scrapers across all data sources.

    A scraper should be able to scrape batches of data and verify the correctness of a DataEntity by URI.
    """

    @abc.abstractmethod
    async def validate(self, entity: DataEntity) -> ValidationResult:
        """_summary_

        Args:
            uri (str): _description_

        Returns:
            ValidationResult: _description_
        """
        ...

    @abc.abstractmethod
    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        raise NotImplemented


class ScraperProvider:
    """A scraper provider will provide the correct scraper based on the source to be scraped."""

    def get(self, data_source: DataSource) -> Scraper:
        # Return the appropriate Scraper for the specified DataSource.
        pass


class ScraperCoordinator:
    """Coordinates all the scrapers necessary based on the specified target ScrapingDistribution."""

    def __init__(self, scraper_provider: ScraperProvider, scraping_distribution: ScapingDistribution,
                 miner_storage: MinerStorage, max_scrapes_per_minute: Dict[DataSource, PositiveInt]):
        self.provider = scraper_provider
        self.distribution = scraping_distribution
        self.storage = miner_storage
        self.scrape_limits = max_scrapes_per_minute
        self.max_workers = 5
        self.queue = asyncio.Queue

    def start(self):
        asyncio.run(self._start())

    async def _start(self):
        tasks = []
        for i in range(self.max_workers):
            task = asyncio.create_task(self.worker(f'worker-{i}',))
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
