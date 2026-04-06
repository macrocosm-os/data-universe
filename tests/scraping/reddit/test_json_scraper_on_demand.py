import asyncio
import unittest
from unittest import mock

from scraping.reddit.reddit_json_scraper import RedditJsonScraper


class TestRedditJsonScraperOnDemand(unittest.TestCase):
    def test_subreddit_keyword_is_promoted_from_keywords(self):
        subreddit, keywords = RedditJsonScraper._split_ondemand_subreddit_and_keywords(
            None,
            ["r/CryptoCurrency"],
        )

        self.assertEqual("CryptoCurrency", subreddit)
        self.assertEqual([], keywords)

    def test_keyword_only_search_uses_global_search(self):
        scraper = RedditJsonScraper()

        async def run():
            with mock.patch("aiohttp.ClientSession") as session_cls:
                session = mock.AsyncMock()
                session.__aenter__.return_value = session
                session.__aexit__.return_value = False
                session_cls.return_value = session

                scraper._fetch_posts = mock.AsyncMock(return_value=[])

                data = await scraper.on_demand_scrape(
                    subreddit=None,
                    keywords=["hustle culture"],
                    keyword_mode="any",
                    limit=25,
                )

                self.assertEqual([], data)
                called_url = scraper._fetch_posts.await_args.args[1]
                self.assertIn("/search.json?", called_url)
                self.assertNotIn("/r/", called_url)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
