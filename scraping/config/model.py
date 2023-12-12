"""This file contains the pydantic classes for the scraping config JSON file.

We use JSON for the configuring the scraping distribution config to make it easier
for miner's to customize their miners, while still being able to take advantage of 
auto-updates, in future.

The classes here are ~identical to their sibling classes in scraping/scraper.py, except
they contain natively serializable/deseriazable fields. All code should use the classes
in scraping/scraper.py. These classes are only intended to be used for deserializing
the scraping_config JSON file.
"""


from typing import List, Optional, Sequence

from pydantic import BaseModel, Field, PositiveInt, ValidationError
from common.data import DataLabel, DataSource
from scraping import coordinator
from scraping.scraper import ScraperId

class LabelScrapingConfig(BaseModel):
    """Describes what labels to scrape."""


    label_choices: Optional[List[str]] = Field(
        description="""The collection of labels to choose from when performing a scrape.
        On a given scrape, 1 label will be chosen at random from this list.
        
        An empty list is treated as a non-existant label. In that case, no filter is applied when scraping data from this source.
        """
    )
    
    max_age_hint_minutes: int = Field(
        description="""The maximum age of data that this scrape should fetch. A random TimeBucket (currently hour block),
        will be chosen within the time frame (now - max_age_hint_minutes, now), using a probality distribution aligned
        with how validators score data freshness.
        
        Note: not all data sources provide date filters, so this property should be thought of as a hint to the scraper, not a rule.
        """,
        default = 60 * 24 * 7,  # 7 days.
    )
    
    max_data_entities: Optional[PositiveInt] = Field(
        default=None,
        description="The maximum number of items to fetch in a single scrape for this label. If None, the scraper will fetch as many items possible."
    )
    
    def to_coordinator_label_scrape_config(self) -> coordinator.LabelScrapingConfig:
        """Returns the internal LabelScrapingConfig representation

        Raises:
            ValidationError: if the conversion fails.
        """
        labels = [DataLabel(value=val) for val in self.label_choices] if self.label_choices else None
        return coordinator.LabelScrapingConfig(label_choices=labels, max_age_hint_minutes=self.max_age_hint_minutes, max_data_entities=self.max_data_entities)


class ScraperConfig(BaseModel):
    """Configures a specific scraper."""
    
    scraper_id: ScraperId = Field(description="The scraper being configured.")
    
    cadence_seconds: PositiveInt = Field(
        description=
        """Configures how often to scrape from this data source, measured in seconds."""
    )
    
    labels_to_scrape: List[LabelScrapingConfig] = Field(
        description="""Describes the type of data to scrape from this source.
        
        The scraper will perform one scrape per entry in this list every 'cadence_seconds'.
        """
    )
    
    def to_coordinator_data_source_scraping_config(self) -> coordinator.ScraperConfig:
        """Returns the internal DataSourceScrapingConfig representation

        Raises:
            ValueError: if the conversion fails.
            ValidationError: if the conversion fails.
        """
        return coordinator.ScraperConfig(
            scraper_id=self.scraper_id,
            cadence_seconds=self.cadence_seconds,
            labels_to_scrape=[label.to_coordinator_label_scrape_config() for label in self.labels_to_scrape]
        )

class ScrapingConfig(BaseModel):
    
    data_source_scraping_configs: List[ScraperConfig] = Field(
        description="The list of data sources (and their scraping config) this miner should scrape from. Only data sources in this list will be scraped."
    )
    
    def to_coordinator_config(self) -> coordinator.CoordinatorConfig:
        """Returns the CoordinatorConfig.
        
        Raises:
            ValidationError: if the conversion fails
        """
        configs = [config.to_coordinator_data_source_scraping_config() for config in self.data_source_scraping_configs]
        return coordinator.CoordinatorConfig(scraper_configs={cfg.scraper_id: cfg for cfg in configs})
    