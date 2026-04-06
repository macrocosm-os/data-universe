import asyncio
import unittest
from unittest import mock

from common.data import DataSource
from common.protocol import OnDemandRequest
from neurons.miner import Miner


class TestMinerOnDemandReddit(unittest.TestCase):
    def test_keyword_only_reddit_job_keeps_keywords(self):
        miner = Miner.__new__(Miner)

        async def run():
            synapse = OnDemandRequest(
                source=DataSource.REDDIT,
                usernames=[],
                keywords=["hustle culture"],
                keyword_mode="any",
                start_date="2026-04-05T00:00:00+00:00",
                end_date="2026-04-06T00:00:00+00:00",
                limit=50,
                data=[],
            )
            with mock.patch("neurons.miner.RedditJsonScraper") as scraper_cls:
                scraper = scraper_cls.return_value
                scraper.on_demand_scrape = mock.AsyncMock(return_value=[])
                await miner.loop_poll_on_demand_active_jobs(synapse)
                scraper.on_demand_scrape.assert_awaited_once_with(
                    usernames=[],
                    subreddit=None,
                    keywords=["hustle culture"],
                    keyword_mode="any",
                    start_datetime=mock.ANY,
                    end_datetime=mock.ANY,
                    limit=50,
                )

        asyncio.run(run())

    def test_subreddit_like_keyword_is_promoted_to_subreddit(self):
        miner = Miner.__new__(Miner)

        async def run():
            synapse = OnDemandRequest(
                source=DataSource.REDDIT,
                usernames=[],
                keywords=["r/CryptoCurrency"],
                keyword_mode="any",
                start_date="2026-04-05T00:00:00+00:00",
                end_date="2026-04-06T00:00:00+00:00",
                limit=50,
                data=[],
            )
            with mock.patch("neurons.miner.RedditJsonScraper") as scraper_cls:
                scraper = scraper_cls.return_value
                scraper.on_demand_scrape = mock.AsyncMock(return_value=[])
                await miner.loop_poll_on_demand_active_jobs(synapse)
                scraper.on_demand_scrape.assert_awaited_once_with(
                    usernames=[],
                    subreddit="r/cryptocurrency",
                    keywords=None,
                    keyword_mode="any",
                    start_datetime=mock.ANY,
                    end_datetime=mock.ANY,
                    limit=50,
                )

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
