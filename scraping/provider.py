from typing import Callable, Dict
from common.data import DataSource
from scraping.reddit.reddit_lite_scraper import RedditLiteScraper
from scraping.scraper import Scraper
from scraping.x.twitter_flash_scraper import TwitterFlashScraper

DEFAULT_FACTORIES = {
    DataSource.REDDIT: RedditLiteScraper,
    DataSource.X: TwitterFlashScraper,
}

class ScraperProvider:
    """A scraper provider will provide the correct scraper based on the source to be scraped."""

    def __init__(self, factories: Dict[DataSource, Callable[[], Scraper]] = DEFAULT_FACTORIES):
        self.factories = factories

    def get(self, data_source: DataSource) -> Scraper:
        """Returns a scraper for the given data source."""
        
        assert data_source in self.factories, f"Data source {data_source} not supported."
        
        return self.factories[data_source]()
        
