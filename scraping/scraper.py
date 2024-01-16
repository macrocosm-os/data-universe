import abc
from enum import Enum
import bittensor as bt
import asyncio
import traceback
from typing import Dict, List, Optional

from pydantic import ConfigDict, BaseModel, Field, PositiveInt

from common.data import DataEntity, DataLabel, DataSource, StrictBaseModel
from common.date_range import DateRange
from storage.miner.miner_storage import MinerStorage


class ScraperId(str, Enum):
    """The id for each of the scrapers."""

    REDDIT_LITE = "Reddit.lite"
    X_FLASH = "X.flash"
    REDDIT_CUSTOM = "Reddit.custom"


class ValidationResult(StrictBaseModel):
    """Data class to contain the result of a scraping validation."""
    model_config = ConfigDict(frozen=True)

    # For now, let's just indicate a pass/fail, but in future we may want to extend this to
    # include more information about the validation.
    is_valid: bool

    content_size_bytes_validated: int = Field(
        description="The content size in bytes validated as part of this check", ge=0
    )

    reason: str = Field(
        description="An optional reason for the validation result.",
        default="",
    )


class ScrapeConfig(StrictBaseModel):
    """Data class to contain the configuration to be used for scraping."""
    model_config = ConfigDict(frozen=True)

    # Number of entities (based on source) to get per scrape attempt.
    # TODO: Update the scrapers to respect this as an Optional.
    entity_limit: Optional[PositiveInt]

    # Date range within which the scraper should scrape.
    date_range: DateRange

    # Optional Labels for the scrape to scrape from.
    labels: Optional[List[DataLabel]] = Field(
        default=None,
        description="Optional labels to filter the scrape by. If none are provided, the data source will issue a scrape for 'all' data, without any label filters applied",
    )


class LabelScrapingFrequency(StrictBaseModel):
    """Data class to contain the frequency distribution for a set of labels."""
    model_config = ConfigDict(frozen=True)

    # The collection of labels that share this total frequency.
    labels: List[DataLabel]
    # The frequency for which this set of labels should be scraped.
    frequency: float


class SourceScrapingFrequency(StrictBaseModel):
    """Data class to contain the frequency distribution for a source across labels."""
    model_config = ConfigDict(frozen=True)

    # The source being scraped.
    source: DataSource
    # The frequency for which this source should be scraped.
    frequency: float

    # TODO A validator to ensure that that sum of all label frequencies is 1.
    label_frequencies: List[LabelScrapingFrequency]


class ScrapingDistribution(StrictBaseModel):
    """A relative distribution across sources and labels."""
    model_config = ConfigDict(frozen=True)

    # TODO A validator to ensure that the sum of all source frequencies is 1.
    distribution: List[SourceScrapingFrequency]


class Scraper(abc.ABC):
    """An abstract base class for scrapers across all data sources.

    A scraper should be able to scrape batches of data and verify the correctness of a DataEntity by URI.

    Scrapers must be thread-safe.
    """

    @abc.abstractmethod
    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a list of DataEntities by URI.

        The validation only needs to verify if the data content is correct. It doesn't need to verify that the size of the data matches because that validation is performed elsewhere.
        """
        pass

    @abc.abstractmethod
    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of data based on the specified ScrapeConfig."""
        pass
