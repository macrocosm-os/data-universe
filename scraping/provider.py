from typing import Callable, Dict
from common.data import DataSource
from scraping.reddit.reddit_lite_scraper import RedditLiteScraper
from scraping.reddit.reddit_custom_scraper import RedditCustomScraper
from scraping.scraper import Scraper, ScraperId
from scraping.x.twitter_flash_scraper import TwitterFlashScraper

DEFAULT_FACTORIES = {
    ScraperId.REDDIT_LITE: RedditLiteScraper,
    ScraperId.X_FLASH: TwitterFlashScraper,
    ScraperId.REDDIT_CUSTOM: RedditCustomScraper,
}


class ScraperProvider:
    """A scraper provider will provide the correct scraper based on the source to be scraped."""

    def __init__(
        self, factories: Dict[ScraperId, Callable[[], Scraper]] = DEFAULT_FACTORIES
    ):
        self.factories = factories
        self.scrapers = dict()

    def get(self, scraper_id: ScraperId) -> Scraper:
        """Returns a scraper for the given scraper id."""

        assert scraper_id in self.factories, f"Scraper id {scraper_id} not supported."

        # Instantiate the scraper if it has not been instantiated yet.
        if scraper_id not in self.scrapers:
            self.scrapers[scraper_id] = self.factories[scraper_id]()

        return self.scrapers[scraper_id]
