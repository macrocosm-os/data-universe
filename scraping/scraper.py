import abc
import bittensor as bt
import asyncio
import traceback
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, PositiveInt

from common.data import DataEntity, DataLabel, DataSource, DateRange
from storage.miner.miner_storage import MinerStorage


class ValidationResult(BaseModel):
    """Data class to contain the result of a scraping validation."""

    class Config:
        frozen = True

    # For now, let's just indicate a pass/fail, but in future we may want to extend this to
    # include more information about the validation.
    is_valid: bool

    reason: str = Field(
        description="An optional reason for the validation result.",
        default="",
    )


class ScrapeConfig(BaseModel):
    """Data class to contain the configuration to be used for scraping."""

    class Config:
        frozen = True

    # Number of entities (based on source) to get per scrape attempt.
    entity_limit: int

    # Date range within which the scraper should scrape.
    date_range: DateRange

    # Optional Labels for the scrape to scrape from.
    # .
    labels: Optional[List[DataLabel]] = Field(
        default=None,
        description="Optional labels to filter the scrape by. If none are provided, the data source will issue a scrape for 'all' data, without any label filters applied",
    )


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


class ScrapingDistribution(BaseModel):
    """A relative distribution across sources and labels."""

    class Config:
        frozen = True

    # TODO A validator to ensure that the sum of all source frequencies is 1.
    distribution: List[SourceScrapingFrequency]


class Scraper(abc.ABC):
    """An abstract base class for scrapers across all data sources.

    A scraper should be able to scrape batches of data and verify the correctness of a DataEntity by URI.
    """

    class ValidationError(Exception):
        """An exception raised when a validation fails."""

        def __init__(self, message: str):
            self.message = message
            super().__init__(self.message)

    @abc.abstractmethod
    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a list of DataEntities by URI.

        The validation only needs to verify if the data content is correct. It doesn't need to verify that the size of the data matches because that validation is performed elsewhere.

        Raises:
            ValidationError: If the validation was unable to complete.
        """
        pass

    @abc.abstractmethod
    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of data based on the specified ScrapeConfig."""
        pass


class ScraperProvider:
    """A scraper provider will provide the correct scraper based on the source to be scraped."""

    def get(self, data_source: DataSource) -> Scraper:
        # Return the appropriate Scraper for the specified DataSource.
        pass


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
