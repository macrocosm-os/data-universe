import asyncio
import os
import datetime as dt
import bittensor as bt
from typing import List
from datetime import datetime, timezone
from dotenv import load_dotenv

from apify_client import ApifyClientAsync

from common.data import DataEntity, DataLabel, DataSource
from scraping.scraper import ScrapeConfig, Scraper, ScraperId, ValidationResult
from scraping.reddit.model import RedditContent
from scraping.reddit.utils import (
    is_valid_reddit_url,
    validate_reddit_content,
    validate_media_content,
    validate_nsfw_content,
    validate_score_content,
    validate_comment_count,
    normalize_label
)


load_dotenv()


class RedditMCScraper(Scraper):
    """Scraper that uses the Apify macrocosmos/reddit-scraper actor."""

    ACTOR_ID = "macrocosmos/reddit-scraper"

    def __init__(self, apify_api_token: str = None):
        """Initialize the Apify Reddit scraper.

        Args:
            apify_api_token: Apify API token. If not provided, will read from APIFY_API_TOKEN env var.
        """
        token = apify_api_token or os.getenv("APIFY_API_TOKEN")
        self.client = ApifyClientAsync(token=token)

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrape Reddit using the Apify actor."""

        labels = scrape_config.labels or []

        # Extract string values from DataLabel objects and normalize (strip "r/" prefix)
        subreddit_names = [normalize_label(label) for label in labels]

        # Prepare actor input
        actor_input = {
            "subreddits": subreddit_names,
            "limit": 25,
            "sort": "new"
        }

        # Run the actor with increased timeout
        run = await self.client.actor(self.ACTOR_ID).call(
            run_input=actor_input,
            timeout_secs=300  # 5 minutes timeout
        )

        # Fetch results from the dataset
        dataset_client = self.client.dataset(run["defaultDatasetId"])
        items = []

        async for item in dataset_client.iterate_items():
            items.append(item)

        # Convert to DataEntity
        entities = []
        for item in items:
            try:
                # Fix field names from Apify actor output to match RedditContent model
                if 'isNsfw' in item:
                    item['is_nsfw'] = item.pop('isNsfw')

                # Convert Apify output to RedditContent
                content = RedditContent(**item, scrapedAt=dt.datetime.now(dt.timezone.utc))
                entity = RedditContent.to_data_entity(content)
                entities.append(entity)
            except Exception as e:
                bt.logging.error(f"Error converting item to DataEntity: {e}")
                continue

        return entities

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate a list of DataEntity objects by scraping their URLs."""
        if not entities:
            return []

        results: List[ValidationResult] = []

        for entity in entities:
            # Basic URI sanity check
            if not is_valid_reddit_url(entity.uri):
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Invalid URI.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # Decode RedditContent
            try:
                ent_content = RedditContent.from_data_entity(entity)
            except Exception:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Failed to decode data entity.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # Validate by fetching from Apify actor
            actor_input = {
                "url": ent_content.url
            }

            try:
                # Run the actor with single URL and increased timeout
                run = await self.client.actor(self.ACTOR_ID).call(
                    run_input=actor_input,
                    timeout_secs=300  # 5 minutes timeout
                )

                # Check if we got results
                dataset_client = self.client.dataset(run["defaultDatasetId"])
                items = []

                async for item in dataset_client.iterate_items():
                    items.append(item)
                    break  # Only need first item

                if len(items) > 0:
                    # Fix field names from Apify actor output
                    item = items[0]
                    bt.logging.trace(f"Apify actor returned for URL {ent_content.url}: {item}")

                    if 'isNsfw' in item:
                        item['is_nsfw'] = item.pop('isNsfw')

                    live_content = RedditContent(**item, scrapedAt=dt.datetime.now(dt.timezone.utc))

                    # Validate content matches using the same validation flow as reddit_custom_scraper
                    # 1) Field-by-field validation (same as custom scraper line 129-132)
                    validation_result = validate_reddit_content(
                        actual_content=live_content,
                        entity_to_validate=entity,
                    )

                    # 2) Media validation (same as custom scraper line 135-138)
                    if validation_result.is_valid:
                        media_validation_result = validate_media_content(ent_content, live_content, entity)
                        if not media_validation_result.is_valid:
                            validation_result = media_validation_result

                    # 3) NSFW validation (same as custom scraper line 141-144)
                    if validation_result.is_valid:
                        nsfw_validation_result = validate_nsfw_content(ent_content, live_content, entity)
                        if not nsfw_validation_result.is_valid:
                            validation_result = nsfw_validation_result

                    # 4) Score validation (CRITICAL: was missing! - from utils.py line 223-225)
                    if validation_result.is_valid:
                        score_validation_result = validate_score_content(ent_content, live_content, entity)
                        if not score_validation_result.is_valid:
                            validation_result = score_validation_result

                    # 5) Comment count validation (CRITICAL: was missing! - from utils.py line 228-230)
                    if validation_result.is_valid:
                        comment_validation_result = validate_comment_count(ent_content, live_content, entity)
                        if not comment_validation_result.is_valid:
                            validation_result = comment_validation_result

                    # Append final validation result
                    results.append(validation_result)
                else:
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="URL not found or inaccessible.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )

            except Exception as e:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason=f"Validation error: {str(e)}",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )

        return results

    @staticmethod
    def get_scraper_id() -> ScraperId:
        return ScraperId.REDDIT_MC


# Test cases
async def test_scrape():
    """Test scraping subreddits."""
    import datetime as dt
    from datetime import timedelta

    bt.logging.info("=" * 60)
    bt.logging.info("TESTING REDDIT MC SCRAPER - SCRAPE")
    bt.logging.info("=" * 60)

    scraper = RedditMCScraper()

    # Test scraping
    bt.logging.info("\n1. Scraping subreddits: bittensor_, python")
    bt.logging.info("-" * 60)

    from common.date_range import DateRange

    scrape_config = ScrapeConfig(
        entity_limit=25,
        labels=[DataLabel(value="bittensor_"), DataLabel(value="python")],
        date_range=DateRange(start=dt.datetime.now(dt.timezone.utc) - timedelta(days=7), end=dt.datetime.now(dt.timezone.utc))
    )

    entities = await scraper.scrape(scrape_config)
    bt.logging.info(f"   Scraped {len(entities)} entities")

    # Log first 3 entities
    bt.logging.info("\n2. Logging first 3 DataEntity details:")
    bt.logging.info("-" * 60)
    for i, entity in enumerate(entities[:3], 1):
        bt.logging.info(f"\n   Entity #{i}:")
        bt.logging.info(f"   URI: {entity.uri}")
        bt.logging.info(f"   Datetime: {entity.datetime}")
        bt.logging.info(f"   Source: {entity.source}")
        bt.logging.info(f"   Label: {entity.label}")
        bt.logging.info(f"   Content Size: {entity.content_size_bytes} bytes")

        # Decode and log the content
        try:
            content = RedditContent.from_data_entity(entity)
            bt.logging.info(f"   Content ID: {content.id}")
            bt.logging.info(f"   Username: {content.username}")
            bt.logging.info(f"   Community: {content.community}")
            bt.logging.info(f"   Data Type: {content.data_type}")
            bt.logging.info(f"   Title: {content.title[:80] + '...' if content.title and len(content.title) > 80 else content.title}")
            bt.logging.info(f"   Score: {content.score}")
            bt.logging.info(f"   Media: {content.media}")
        except Exception as e:
            bt.logging.error(f"   Failed to decode content: {e}")

    bt.logging.info("\n" + "=" * 60)
    bt.logging.info("SCRAPE TEST COMPLETED")
    bt.logging.info("=" * 60)


async def test_validate():
    """Test validating entities."""
    import datetime as dt

    bt.logging.info("\n" + "=" * 60)
    bt.logging.info("TESTING REDDIT MC SCRAPER - VALIDATE")
    bt.logging.info("=" * 60)

    scraper = RedditMCScraper()

    # Create test entity
    test_entity = DataEntity(
        uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/",
        datetime=dt.datetime(2023, 12, 5, 15, 59, 13, tzinfo=dt.timezone.utc),
        source=DataSource.REDDIT,
        label=DataLabel(value="r/bittensor_"),
        content=b'{"id": "t3_18bf67l", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Hey all!!", "createdAt": "2023-12-05T15:59:13+00:00", "dataType": "post", "title": "How do you add TAO to MetaMask?", "parentId": null}',
        content_size_bytes=300,
    )

    bt.logging.info("\n1. Validating test entity:")
    bt.logging.info("-" * 60)
    bt.logging.info(f"   URI: {test_entity.uri}")

    results = await scraper.validate([test_entity])

    bt.logging.info(f"\n   Validation Result:")
    bt.logging.info(f"   Valid: {results[0].is_valid}")
    bt.logging.info(f"   Reason: {results[0].reason}")
    bt.logging.info(f"   Content Size Validated: {results[0].content_size_bytes_validated} bytes")

    bt.logging.info("\n" + "=" * 60)
    bt.logging.info("VALIDATION TEST COMPLETED")
    bt.logging.info("=" * 60)


async def test_scrape_and_validate():
    """Test scraping a URL and then validating it."""
    import datetime as dt

    bt.logging.info("\n" + "=" * 60)
    bt.logging.info("TESTING REDDIT MC SCRAPER - SCRAPE & VALIDATE")
    bt.logging.info("=" * 60)

    scraper = RedditMCScraper()

    # Test URL - recent Reddit post
    test_url = "https://www.reddit.com/r/bittensor_/comments/1or4vcv/james_altucher_on_bittensor/"

    bt.logging.info(f"\n1. Scraping URL: {test_url}")
    bt.logging.info("-" * 60)

    # Scrape the URL using Apify actor
    actor_input = {"url": test_url}

    try:
        run = await scraper.client.actor(scraper.ACTOR_ID).call(
            run_input=actor_input,
            timeout_secs=300
        )

        # Fetch results
        dataset_client = scraper.client.dataset(run["defaultDatasetId"])
        items = []

        async for item in dataset_client.iterate_items():
            items.append(item)
            break  # Only need first item

        if len(items) > 0:
            item = items[0]
            bt.logging.info(f"   Successfully scraped! Got item with ID: {item.get('id')}")

            # Fix field names
            if 'isNsfw' in item:
                item['is_nsfw'] = item.pop('isNsfw')

            # Convert to RedditContent and DataEntity
            content = RedditContent(**item, scrapedAt=dt.datetime.now(dt.timezone.utc))
            entity = RedditContent.to_data_entity(content)

            bt.logging.info(f"   Created DataEntity:")
            bt.logging.info(f"     URI: {entity.uri}")
            bt.logging.info(f"     Source: {entity.source}")
            bt.logging.info(f"     Size: {entity.content_size_bytes} bytes")
            bt.logging.info(f"     Score: {content.score}")
            bt.logging.info(f"     Comments: {content.num_comments}")
            bt.logging.info(f"     Media: {content.media}")
            bt.logging.info(f"     Content: {entity.content}")

            # Now validate the entity we just scraped
            bt.logging.info(f"\n2. Validating the scraped entity")
            bt.logging.info("-" * 60)

            validation_results = await scraper.validate([entity])

            if validation_results:
                result = validation_results[0]
                bt.logging.info(f"\n   Validation Result:")
                bt.logging.info(f"     Valid: {result.is_valid}")
                bt.logging.info(f"     Reason: {result.reason}")
                bt.logging.info(f"     Content Size Validated: {result.content_size_bytes_validated} bytes")

                if result.is_valid:
                    bt.logging.success("   ✅ Validation PASSED - Entity is valid!")
                else:
                    bt.logging.warning(f"   ❌ Validation FAILED - {result.reason}")
            else:
                bt.logging.error("   No validation results returned")
        else:
            bt.logging.error("   Failed to scrape URL - no items returned")

    except Exception as e:
        bt.logging.error(f"   Error during scrape and validate test: {str(e)}")
        import traceback
        bt.logging.error(traceback.format_exc())

    bt.logging.info("\n" + "=" * 60)
    bt.logging.info("SCRAPE & VALIDATE TEST COMPLETED")
    bt.logging.info("=" * 60)


if __name__ == "__main__":
    bt.logging.set_trace()
    # asyncio.run(test_scrape())
    # asyncio.run(test_validate())
    asyncio.run(test_scrape_and_validate())
