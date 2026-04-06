import asyncio
import unittest
from unittest import mock

from vali_utils.on_demand.on_demand_validation import (
    OnDemandValidator,
    ValidationContext,
)


class TestOnDemandValidation(unittest.TestCase):
    def test_reddit_data_exists_probe_handles_keyword_only_query(self):
        evaluator = mock.Mock()
        evaluator.PREFERRED_SCRAPERS = {}
        validator = OnDemandValidator(evaluator)

        scraper = mock.Mock()
        scraper.on_demand_scrape = mock.AsyncMock(return_value=[object()])
        validator._get_scraper = mock.Mock(return_value=scraper)

        async def run():
            exists = await validator.check_data_exists(
                ValidationContext(
                    source="reddit",
                    usernames=[],
                    keywords=["hustle culture"],
                    keyword_mode="any",
                    start_date="2026-04-05T00:00:00+00:00",
                    end_date="2026-04-06T00:00:00+00:00",
                    limit=10,
                )
            )
            self.assertTrue(exists)
            scraper.on_demand_scrape.assert_awaited_once_with(
                usernames=[],
                subreddit=None,
                keywords=["hustle culture"],
                keyword_mode="any",
                start_datetime=mock.ANY,
                end_datetime=mock.ANY,
                limit=1,
            )

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
