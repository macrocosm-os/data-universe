from common.data import DataSource
from scraping.scraper import Scraper


class ScraperProvider:
    """A scraper provider will provide the correct scraper based on the source to be scraped."""

    def __init__(self):
        pass

    def get(self, data_source: DataSource) -> Scraper:
        # Return the appropriate Scraper for the specified DataSource.
        pass