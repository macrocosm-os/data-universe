import threading
from typing import Callable, Dict
from common.data import DataSource
from scraping.reddit.reddit_lite_scraper import RedditLiteScraper
from scraping.reddit.reddit_custom_scraper import RedditCustomScraper
from scraping.reddit.reddit_json_scraper import RedditJsonScraper
from scraping.scraper import Scraper, ScraperId
from scraping.x.microworlds_scraper import MicroworldsTwitterScraper
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.x.quacker_url_scraper import QuackerUrlScraper
from scraping.youtube.youtube_custom_scraper import YouTubeTranscriptScraper
from scraping.youtube.invideoiq_transcript_scraper import YouTubeChannelTranscriptScraper
from scraping.youtube.crawlmaster_transcript_scraper import YouTubeChannelTranscriptScraper as CrawlmasterScraper
from scraping.youtube.starvibe_transcript_scraper import YouTubeChannelTranscriptScraper as StarvibeScraper
from scraping.youtube.youtube_multi_actor_scraper import YouTubeMultiActorScraper


DEFAULT_FACTORIES = {
    ScraperId.REDDIT_LITE: RedditLiteScraper,
    # For backwards compatibility with old configs, remap x.flash to x.apidojo.
    ScraperId.X_FLASH: MicroworldsTwitterScraper,
    ScraperId.REDDIT_CUSTOM: RedditCustomScraper,
    ScraperId.REDDIT_JSON: RedditJsonScraper,
    ScraperId.X_MICROWORLDS: MicroworldsTwitterScraper,
    ScraperId.X_APIDOJO: ApiDojoTwitterScraper,
    ScraperId.X_QUACKER: QuackerUrlScraper,
    ScraperId.YOUTUBE_CUSTOM_TRANSCRIPT: YouTubeTranscriptScraper,
    ScraperId.YOUTUBE_APIFY_TRANSCRIPT: YouTubeChannelTranscriptScraper,
    ScraperId.YOUTUBE_CRAWLMASTER_TRANSCRIPT: CrawlmasterScraper,
    ScraperId.YOUTUBE_STARVIBE_TRANSCRIPT: StarvibeScraper,
    ScraperId.YOUTUBE_MULTI_ACTOR: YouTubeMultiActorScraper
}


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
