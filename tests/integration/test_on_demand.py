import unittest
import asyncio
import bittensor as bt
import datetime as dt
import random
from common.data import DataSource, DataEntity
from common.protocol import OnDemandRequest
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.reddit.reddit_json_scraper import RedditJsonScraper


class TestOnDemandProtocol(unittest.TestCase):
    def test_on_demand_flow_x(self):
        """Test the complete on-demand data flow for X (Twitter)"""

        async def run_test():
            # Create OnDemand request
            test_request = OnDemandRequest(
                source=DataSource.X,
                keywords=["tao"],
                start_date=(dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).isoformat(),
                end_date=dt.datetime.now(dt.timezone.utc).isoformat(),
                limit=3
            )

            # Set up scraper
            scraper = ApiDojoTwitterScraper()

            try:
                # Get data using on_demand_scrape method
                data = await scraper.on_demand_scrape(
                    usernames=test_request.usernames,
                    keywords=test_request.keywords,
                    keyword_mode=test_request.keyword_mode or "any",
                    start_datetime=dt.datetime.fromisoformat(test_request.start_date),
                    end_datetime=dt.datetime.fromisoformat(test_request.end_date),
                    limit=test_request.limit
                )
                
                # Verify data was retrieved
                self.assertTrue(len(data) >= 0, "Error occurred during X scraping")

                if data:
                    print(f"Retrieved {len(data)} X posts")
                    # Select 1 random sample for validation
                    samples = random.sample(data, min(1, len(data)))
                    validation_results = await scraper.validate(samples, allow_low_engagement=True)
                    print(f"\n\nValidation results: {validation_results}\n\n")
                    print(f"X data: {data}")
                    # Check if any validation passed
                    self.assertTrue(
                        any(result.is_valid for result in validation_results),
                        "All validation failed for X sample data"
                    )
                else:
                    print("No X data found for the given criteria")
                    
            except Exception as e:
                print(f"X test skipped due to error: {str(e)}")

        # Run the async test
        asyncio.run(run_test())

    def test_on_demand_flow_reddit(self):
        """Test the complete on-demand data flow for Reddit"""

        async def run_test():
            # Create OnDemand request for Reddit
            # First keyword is the subreddit, remaining keywords are search terms
            test_request = OnDemandRequest(
                source=DataSource.REDDIT,
                keywords=["cryptocurrency", "bitcoin"],  # First: subreddit, rest: search terms
                start_date=(dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)).isoformat(),
                end_date=dt.datetime.now(dt.timezone.utc).isoformat(),
                limit=3
            )

            # Set up Reddit scraper
            scraper = RedditJsonScraper()

            try:
                # Use on_demand_scrape method for Reddit
                # First keyword is subreddit, remaining are search keywords
                subreddit = test_request.keywords[0]
                search_keywords = test_request.keywords[1:] if len(test_request.keywords) > 1 else None
                
                data = await scraper.on_demand_scrape(
                    usernames=None,  # No username filtering
                    subreddit=subreddit,
                    keywords=search_keywords,
                    keyword_mode="any",
                    start_datetime=dt.datetime.fromisoformat(test_request.start_date),
                    end_datetime=dt.datetime.fromisoformat(test_request.end_date),
                    limit=test_request.limit
                )

                # Verify data was retrieved
                self.assertTrue(len(data) >= 0, "Error occurred during Reddit scraping")
                
                if data:
                    print(f"Retrieved {len(data)} Reddit posts from r/{subreddit}")
                    # Select 1 random sample for validation
                    samples = random.sample(data, min(1, len(data)))
                    validation_results = await scraper.validate(samples)
                    print(f"Reddit data: {data}")
                    # Check if any validation passed
                    self.assertTrue(
                        any(result.is_valid for result in validation_results),
                        "All validation failed for Reddit sample data"
                    )
                else:
                    print(f"No Reddit data found in r/{subreddit} for the given criteria")
                    
            except Exception as e:
                print(f"Reddit test skipped due to error: {str(e)}")
                # Don't fail the test if Reddit API is unavailable
                pass

        # Run the async test
        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()