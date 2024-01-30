import threading
from typing import Callable, Dict
from common.data import DataSource
from scraping.reddit.reddit_lite_scraper import RedditLiteScraper
from scraping.reddit.reddit_custom_scraper import RedditCustomScraper
from scraping.scraper import Scraper, ScraperId
from scraping.x.twitter_flash_scraper import TwitterFlashScraper
from scraping.x.twitter_custom_scraper import TwitterCustomScraper


DEFAULT_FACTORIES = {
    ScraperId.REDDIT_LITE: RedditLiteScraper,
    ScraperId.X_FLASH: TwitterFlashScraper,
    ScraperId.REDDIT_CUSTOM: RedditCustomScraper,
    ScraperId.X_CUSTOM: TwitterCustomScraper,
}

concurrent_lock = threading.RLock()
concurrent_count = 0


def get_and_increment_count():
    global concurrent_count
    with concurrent_lock:
        concurrent_count += 1
        return concurrent_count


def decrement_count():
    global concurrent_count
    with concurrent_lock:
        concurrent_count -= 1


class ScraperProvider:
    """A scraper provider will provide the correct scraper based on the source to be scraped."""

    def __init__(
        self, factories: Dict[DataSource, Callable[[], Scraper]] = DEFAULT_FACTORIES
    ):
        self.factories = factories

    def get(self, scraper_id: ScraperId) -> Scraper:
        """Returns a scraper for the given scraper id."""

        assert scraper_id in self.factories, f"Scraper id {scraper_id} not supported."

        return self.factories[scraper_id]()
