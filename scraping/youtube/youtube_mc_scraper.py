import asyncio
import os
import datetime as dt
import bittensor as bt
from typing import List, Optional
from dotenv import load_dotenv

from apify_client import ApifyClientAsync

from common.data import DataEntity, DataLabel, DataSource
from scraping.scraper import ScrapeConfig, Scraper, ScraperId, ValidationResult
from scraping.youtube.model import YouTubeContent
from scraping.youtube import utils as youtube_utils


load_dotenv()


class YouTubeMCScraper(Scraper):
    """Scraper that uses the Apify macrocosmos/youtube-scraper actor."""

    ACTOR_ID = "macrocosmos/youtube-scraper"

    # Timeout for Apify actor runs
    APIFY_TIMEOUT_SECS = 300

    def __init__(self, apify_api_token: str = None):
        """Initialize the Apify YouTube scraper.

        Args:
            apify_api_token: Apify API token. If not provided, will read from APIFY_API_TOKEN env var.
        """
        token = apify_api_token or os.getenv("APIFY_API_TOKEN")
        self.client = ApifyClientAsync(token=token)

    async def scrape(
        self,
        youtube_url: Optional[str] = None,
        channel_url: Optional[str] = None,
        language: str = "en",
        max_videos: int = 10,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[DataEntity]:
        """
        Scrape YouTube using the Apify actor.
        
        Args:
            youtube_url: Single video URL to scrape
            channel_url: Channel URL to scrape multiple videos from
            language: Language for transcripts (default: en)
            max_videos: Maximum videos to scrape from channel (default: 10)
            start_date: Only scrape videos after this date (YYYY-MM-DD)
            end_date: Only scrape videos before this date (YYYY-MM-DD)
        """
        if youtube_url and channel_url:
            bt.logging.error("Provide only one of youtube_url or channel_url.")
            return []
        if not youtube_url and not channel_url:
            bt.logging.error("Must provide either youtube_url or channel_url.")
            return []

        # Prepare actor input
        actor_input = {
            "language": language  # Pass language for transcript selection
        }

        if youtube_url:
            actor_input["urls"] = [youtube_url]
        elif channel_url:
            actor_input["channel_url"] = channel_url
            actor_input["max_videos"] = max_videos
            if start_date:
                actor_input["start_date"] = start_date
            if end_date:
                actor_input["end_date"] = end_date

        bt.logging.info(f"Running YouTube MC scraper with input: {actor_input}")

        try:
            # Run the actor
            run = await self.client.actor(self.ACTOR_ID).call(
                run_input=actor_input,
                timeout_secs=self.APIFY_TIMEOUT_SECS
            )

            # Fetch results from the dataset
            dataset_client = self.client.dataset(run["defaultDatasetId"])
            items = []

            async for item in dataset_client.iterate_items():
                items.append(item)

            bt.logging.info(f"YouTube MC scraper returned {len(items)} items")

            # Convert to DataEntity
            entities = []
            for item in items:
                try:
                    entity = self._convert_to_data_entity(item)
                    if entity:
                        entities.append(entity)
                except Exception as e:
                    bt.logging.error(f"Error converting item to DataEntity: {e}")
                    continue

            return entities

        except Exception as e:
            bt.logging.error(f"Error running YouTube MC scraper: {e}")
            return []

    def _convert_to_data_entity(self, item: dict) -> Optional[DataEntity]:
        """Convert Apify actor output to DataEntity."""
        try:
            # Parse upload date
            upload_date_str = item.get('upload_date', '')
            if upload_date_str:
                # Handle ISO format
                if 'T' in upload_date_str:
                    upload_date = dt.datetime.fromisoformat(upload_date_str.replace('Z', '+00:00'))
                else:
                    # Handle YYYY-MM-DD format
                    upload_date = dt.datetime.strptime(upload_date_str, '%Y-%m-%d')
                    upload_date = upload_date.replace(tzinfo=dt.timezone.utc)
            else:
                upload_date = dt.datetime.now(dt.timezone.utc)

            # Create YouTubeContent
            content = YouTubeContent(
                video_id=item.get('video_id', ''),
                title=item.get('title', ''),
                channel_name=item.get('channel_name', ''),
                upload_date=upload_date,
                transcript=item.get('transcript', []),
                url=item.get('url', f"https://www.youtube.com/watch?v={item.get('video_id', '')}"),
                duration_seconds=int(item.get('duration_seconds', 0)),
                language=item.get('language') or 'en',  # Actor returns actual language or None
                description=item.get('description'),
                thumbnails=item.get('thumbnails', youtube_utils.generate_thumbnails(item.get('video_id', ''))),
                view_count=item.get('view_count', 0),
                like_count=item.get('like_count'),
                subscriber_count=item.get('subscriber_count')
            )

            # Convert to DataEntity
            entity = YouTubeContent.to_data_entity(content)
            return entity

        except Exception as e:
            bt.logging.error(f"Error creating YouTubeContent: {e}")
            return None

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate YouTube transcript entities using unified validation function."""
        if not entities:
            return []

        results: List[ValidationResult] = []

        for entity in entities:
            try:
                content_to_validate = YouTubeContent.from_data_entity(entity)
                original_language = content_to_validate.language

                bt.logging.info(
                    f"Validating video {content_to_validate.video_id} in original language: {original_language}")

                # Scrape fresh data from actor (single call)
                actual_entities = await self.scrape(
                    youtube_url=f"https://www.youtube.com/watch?v={content_to_validate.video_id}",
                    language=original_language
                )

                if not actual_entities:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Video not available for validation in original language",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                actual_entity = actual_entities[0]

                # Validate view count (minimum engagement check) from scraped entity
                actual_content = YouTubeContent.from_data_entity(actual_entity)
                view_count = actual_content.view_count or 0
                if int(view_count) < 100:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason=f"Video has low engagement ({view_count} views, minimum 100 required)",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Use unified validation function (same as starvibe/crawlmaster)
                validation_result = youtube_utils.validate_youtube_data_entities(
                    entity_to_validate=entity,
                    actual_entity=actual_entity
                )
                results.append(validation_result)

            except Exception as e:
                bt.logging.error(f"Validation error: {e}")
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
        return ScraperId.YOUTUBE_MC


# Test cases
async def test_scrape_video():
    """Test scraping a single video."""
    bt.logging.info("=" * 60)
    bt.logging.info("TESTING YOUTUBE MC SCRAPER - SINGLE VIDEO")
    bt.logging.info("=" * 60)

    scraper = YouTubeMCScraper()

    test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    bt.logging.info(f"\nScraping video: {test_url}")

    entities = await scraper.scrape(youtube_url=test_url)
    bt.logging.info(f"Scraped {len(entities)} entities")

    for entity in entities:
        content = YouTubeContent.from_data_entity(entity)
        bt.logging.info(f"  Title: {content.title}")
        bt.logging.info(f"  Channel: {content.channel_name}")
        bt.logging.info(f"  Duration: {content.duration_seconds}s")
        bt.logging.info(f"  Transcript segments: {len(content.transcript)}")
        bt.logging.info(f"  Label: {entity.label.value}")

    return entities


async def test_scrape_channel():
    """Test scraping a channel."""
    bt.logging.info("=" * 60)
    bt.logging.info("TESTING YOUTUBE MC SCRAPER - CHANNEL")
    bt.logging.info("=" * 60)

    scraper = YouTubeMCScraper()

    channel_url = "https://www.youtube.com/@Fireship"
    bt.logging.info(f"\nScraping channel: {channel_url}")

    entities = await scraper.scrape(
        channel_url=channel_url,
        max_videos=3,
        start_date="2024-01-01"
    )
    bt.logging.info(f"Scraped {len(entities)} entities")

    for entity in entities:
        content = YouTubeContent.from_data_entity(entity)
        bt.logging.info(f"  Title: {content.title[:50]}...")
        bt.logging.info(f"  Upload: {content.upload_date}")

    return entities


async def test_validate():
    """Test validation."""
    bt.logging.info("=" * 60)
    bt.logging.info("TESTING YOUTUBE MC SCRAPER - VALIDATE")
    bt.logging.info("=" * 60)

    scraper = YouTubeMCScraper()

    # First scrape a video
    entities = await test_scrape_video()

    if entities:
        bt.logging.info("\nValidating scraped entity...")
        results = await scraper.validate(entities[:1])

        for result in results:
            bt.logging.info(f"  Valid: {result.is_valid}")
            bt.logging.info(f"  Reason: {result.reason}")

    return results


if __name__ == "__main__":
    bt.logging.set_trace()
    # asyncio.run(test_scrape_video())
    # asyncio.run(test_scrape_channel())
    asyncio.run(test_validate())
