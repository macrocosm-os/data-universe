import unittest
import asyncio
import bittensor as bt
import datetime as dt
import random
from common.data import DataLabel, DataSource, DataEntity
from common.protocol import OnDemandRequest
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper


class TestOnDemandProtocol(unittest.TestCase):
    def test_on_demand_flow(self):
        """Test the complete on-demand data flow"""

        async def run_test():
            # Create OnDemand request
            test_request = OnDemandRequest(
                source=DataSource.X,
                keywords=["#TAO"],
                start_date=(dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).isoformat(),
                end_date=dt.datetime.now(dt.timezone.utc).isoformat(),
                limit=5
            )

            # Set up scraper
            scraper = ApiDojoTwitterScraper()

            # Create scrape config from request
            scrape_config = ScrapeConfig(
                entity_limit=test_request.limit,
                date_range=DateRange(
                    start=dt.datetime.fromisoformat(test_request.start_date),
                    end=dt.datetime.fromisoformat(test_request.end_date)
                ),
                labels=[DataLabel(value=k) for k in test_request.keywords]
            )

            # Get data using scraper
            data = await scraper.scrape(scrape_config)

            # Verify data was retrieved
            self.assertTrue(len(data) > 0, "No data returned from scraper")

            # Select 1 random samples for validation
            if data:
                samples = random.sample(data, min(1, len(data)))
                validation_results = await scraper.validate(samples)
                print(data)
                # Check if any validation passed
                self.assertTrue(
                    any(result.is_valid for result in validation_results),
                    "All validation failed for sample data"
                )

        # Run the async test
        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()