"""This file contains the pydantic classes for the scraping config JSON file.

We use JSON for the configuring the scraping distribution config to make it easier
for miner's to customize their miners, while still being able to take advantage of 
auto-updates, in future.

The classes here are ~identical to their sibling classes in scraping/scraper.py, except
they contain natively serializable/deseriazable fields. All code should use the classes
in scraping/scraper.py. These classes are only intended to be used for deserializing
the scraping_config JSON file.
"""


from typing import List, Optional

from pydantic import BaseModel, Field, PositiveInt


class LabelScrapeConfig(BaseModel):
    """Describes what labels to scrape."""


    label_choices: List[str] = Field(
        description="""The collection of labels to choose from when performing a scrape.
        On a given scrape, 1 label will be chosen at random from this list.
        """
    )
    
    max_age_in_minutes: int = Field(
        description="""The maximum age of data that this scrape should fetch. A random TimeBucket (currently hour block),
        will be chosen within the time frame (now - max_age_in_minutes, now), using a probality distribution aligned
        with how validators score data freshness.
        
        Note: not all data sources provide date filters, so this property should be thought of as a hint to the scraper, not a rule.
        """
        default = 60 * 24 * 7,  # 7 days.
    )
    
    max_items: Optional[PositiveInt] = Field(
        default=None,
        description="The maximum number of items to fetch in a single scrape for this label. If None, the scraper will fetch as many items possible."
    )


class DataSourceScrapingConfig(BaseModel):
    
    source: str = Field(
        min_length=1,
        description="The data source being configured. Acceptable values are 'X', 'REDDIT'.")
    
    cadence_secs: PositiveInt = Field(
        description=
        """Configures how often to scrape from this data source, measured in seconds."""
    )
    
    labels_to_scrape: List[LabelScrapeConfig] = Field(
        description="""Describes the type of data to scrape from this source.
        
        The scraper will perform one scrape per entry in this list every 'cadence_secs'.
        """
    )
    

class ScrapingConfig(BaseModel):
    
    distribution: List[DataSourceScrapingConfig] = Field(
        description="The list of data sources (and their scraping config) this miner should scrape from. Only data sources in this list will be scraped."
    )
    