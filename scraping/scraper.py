import abc
from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel, Field, PositiveInt, ConfigDict

from common.data import DataEntity, DataLabel, DataSource, StrictBaseModel
from common.date_range import DateRange
from storage.miner.miner_storage import MinerStorage


class ScraperId(str, Enum):
    """The id for each of the scrapers."""

    REDDIT_LITE = "Reddit.lite"
    X_FLASH = "X.flash"
    REDDIT_CUSTOM = "Reddit.custom"
    X_MICROWORLDS = "X.microworlds"
    X_APIDOJO = "X.apidojo"
    X_QUACKER = "X.quacker"
    YOUTUBE_TRANSCRIPT = "YouTube.transcript"


class ValidationResult(StrictBaseModel):
    """Data class to contain the result of a scraping validation."""

    model_config = ConfigDict(frozen=True)

    is_valid: bool
    content_size_bytes_validated: int = Field(
        description="The content size in bytes validated as part of this check", ge=0
    )
    reason: str = Field(
        description="An optional reason for the validation result.",
        default="",
    )


class HFValidationResult(StrictBaseModel):
    """Data class to contain the result of a validation for a miner's Hugging Face dataset. """

    class Config:
        frozen = True

    is_valid: bool

    validation_percentage: float = Field(
        description="The percentage of successfully validated HF rows. "
    )

    reason: str = Field(
        description="An optional reason for the validation result. ",
        default=""
    )


class ScrapeConfig(StrictBaseModel):
    """Data class to contain the configuration to be used for scraping."""

    model_config = ConfigDict(frozen=True)

    entity_limit: Optional[PositiveInt]
    date_range: DateRange
    labels: Optional[List[DataLabel]] = Field(
        default=None,
        description="Optional labels to filter the scrape by. If none are provided, the data source will issue a scrape for 'all' data, without any label filters applied",
    )


class LabelScrapingFrequency(StrictBaseModel):
    """Data class to contain the frequency distribution for a set of labels."""

    model_config = ConfigDict(frozen=True)

    labels: List[DataLabel]
    frequency: float


class SourceScrapingFrequency(StrictBaseModel):
    """Data class to contain the frequency distribution for a source across labels."""

    model_config = ConfigDict(frozen=True)

    source: DataSource
    frequency: float
    label_frequencies: List[LabelScrapingFrequency]


class ScrapingDistribution(StrictBaseModel):
    """A relative distribution across sources and labels."""

    model_config = ConfigDict(frozen=True)

    distribution: List[SourceScrapingFrequency]


class Scraper(abc.ABC):
    """An abstract base class for scrapers across all data sources."""

    @abc.abstractmethod
    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a list of DataEntities by URI."""
        pass

    @abc.abstractmethod
    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of data based on the specified ScrapeConfig."""
        pass

    @abc.abstractmethod
    async def validate_hf(self, entities) -> bool:
        """Validate the correctness of a list of HF retrieved data"""
        pass