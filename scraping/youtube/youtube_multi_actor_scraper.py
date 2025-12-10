import asyncio
import datetime as dt
import hashlib
from typing import List, Optional

import bittensor as bt
from dotenv import load_dotenv

from common.data import DataEntity
from scraping.scraper import Scraper, ValidationResult
from scraping.youtube import utils as youtube_utils

# Import individual scrapers that already return DataEntity objects
from scraping.youtube.starvibe_transcript_scraper import YouTubeChannelTranscriptScraper as StarvibeScraper
from scraping.youtube.invideoiq_transcript_scraper import YouTubeChannelTranscriptScraper as InvideoiqScraper
from scraping.youtube.youtube_mc_scraper import YouTubeMCScraper

load_dotenv()


class YouTubeMultiActorScraper(Scraper):
    """
    Multi-actor YouTube scraper that uses individual scrapers with fallback.
    Each individual scraper returns properly formatted DataEntity objects.
    """

    def __init__(self):
        """Initialize the Multi-Actor YouTube Transcript Scraper."""

        # Initialize individual scrapers
        # Primary scrapers: macrocosmos (95%), starvibe (5%)
        self.primary_scrapers = [
            ("macrocosmos", YouTubeMCScraper()),
            ("starvibe", StarvibeScraper())
        ]

        # Fallback scraper (always last)
        self.fallback_scraper = ("invideoiq", InvideoiqScraper())

        # All scrapers for logging
        all_scraper_names = [name for name, _ in self.primary_scrapers] + [self.fallback_scraper[0]]

        bt.logging.info("YouTube Multi-Actor Scraper initialized with scrapers: " +
                       ", ".join(all_scraper_names))

    def _get_ordered_scrapers(self, video_id: str) -> List[tuple]:
        """Get scrapers in order: macrocosmos 95%, starvibe 5%, invideoiq fallback."""
        video_hash = int(hashlib.md5(video_id.encode()).hexdigest(), 16)

        # Distribute: macrocosmos (95%), starvibe (5%)
        hash_mod = video_hash % 100
        if hash_mod < 95:
            primary_index = 0  # macrocosmos
        else:
            primary_index = 1  # starvibe

        selected_primary = self.primary_scrapers[primary_index]

        # Get other primary scraper
        other_primaries = [s for i, s in enumerate(self.primary_scrapers) if i != primary_index]

        bt.logging.debug(f"Selected primary scraper {selected_primary[0]} for video {video_id}")

        # Order: selected primary -> other primary -> invideoiq fallback
        return [selected_primary] + other_primaries + [self.fallback_scraper]

    def _extract_video_id_from_url(self, url: str) -> Optional[str]:
        """Extract video ID from YouTube URL."""
        if not url:
            return None

        import re
        patterns = [
            r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',
            r'(?:embed|v|vi|youtu\.be\/)([0-9A-Za-z_-]{11}).*',
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    async def scrape(
        self,
        youtube_url: Optional[str] = None,
        channel_url: Optional[str] = None,
        language: str = "en",
        max_videos: int = 3,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[DataEntity]:
        """
        Scrape transcript from a single YouTube video URL or multiple videos from a channel URL.
        Uses scraper rotation with fallback for reliability.
        """
        if youtube_url and channel_url:
            bt.logging.error("Provide only one of youtube_url or channel_url.")
            return []
        if not youtube_url and not channel_url:
            bt.logging.error("Must provide either youtube_url or channel_url.")
            return []

        if youtube_url:
            return await self._scrape_single_video(youtube_url, language)
        else:
            return await self._scrape_channel(channel_url, max_videos, start_date, end_date, language)

    async def _scrape_single_video(self, youtube_url: str, language: str) -> List[DataEntity]:
        """Internal method to scrape a single video using scraper fallback."""
        video_id = self._extract_video_id_from_url(youtube_url)
        if not video_id:
            bt.logging.error(f"Invalid YouTube URL: {youtube_url}")
            return []

        # Get ordered scrapers: primary (random between crawlmaster/starvibe) -> other primary -> invideoiq
        ordered_scrapers = self._get_ordered_scrapers(video_id)

        for scraper_name, scraper_instance in ordered_scrapers:
            bt.logging.debug(f"Trying scraper {scraper_name} for video {video_id}")

            try:
                entities = await scraper_instance.scrape(
                    youtube_url=youtube_url,
                    language=language
                )

                if entities:
                    if scraper_name != ordered_scrapers[0][0]:  # Not primary scraper
                        bt.logging.info(f"Fallback successful: {scraper_name} worked for video {video_id}")

                    bt.logging.success(f"Successfully scraped video {video_id} using {scraper_name} scraper")
                    return entities
                else:
                    bt.logging.warning(f"Scraper {scraper_name} returned no entities for video {video_id}")

            except Exception as e:
                bt.logging.warning(f"Scraper {scraper_name} failed for video {video_id}: {str(e)}")
                continue

        bt.logging.error(f"All scrapers failed for video {video_id}")
        return []

    async def _scrape_channel(
        self,
        channel_url: str,
        max_videos: int,
        start_date: Optional[str],
        end_date: Optional[str],
        language: str
    ) -> List[DataEntity]:
        """Internal method to scrape videos from a channel using scraper fallback."""

        # Try scrapers in order: primary scrapers first, then invideoiq as fallback
        # For channel scraping, we'll try all in the same order (using a dummy video_id for consistent ordering)
        ordered_scrapers = self._get_ordered_scrapers("channel_scraping")

        for scraper_name, scraper_instance in ordered_scrapers:
            bt.logging.debug(f"Trying scraper {scraper_name} for channel {channel_url}")

            try:
                entities = await scraper_instance.scrape(
                    channel_url=channel_url,
                    max_videos=max_videos,
                    start_date=start_date,
                    end_date=end_date
                )

                if entities:
                    bt.logging.success(f"Successfully scraped channel {channel_url} using {scraper_name} scraper")
                    return entities
                else:
                    bt.logging.warning(f"Scraper {scraper_name} returned no entities for channel {channel_url}")

            except Exception as e:
                bt.logging.warning(f"Scraper {scraper_name} failed for channel {channel_url}: {str(e)}")
                continue

        bt.logging.error(f"All scrapers failed for channel {channel_url}")
        return []

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate entities using unified YouTube validation."""
        if not entities:
            return []

        results = []
        for entity in entities:
            try:
                # Use the same validation logic as individual scrapers
                # Re-scrape the video to get fresh data for comparison
                from scraping.youtube.model import YouTubeContent
                content_to_validate = YouTubeContent.from_data_entity(entity)

                bt.logging.info(f"Validating video {content_to_validate.video_id} in language: {content_to_validate.language}")

                # Try to get fresh data using our scraping fallback
                fresh_entities = await self._scrape_single_video(
                    f"https://www.youtube.com/watch?v={content_to_validate.video_id}",
                    content_to_validate.language
                )

                if not fresh_entities:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Could not retrieve video for validation from any scraper",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                actual_entity = fresh_entities[0]

                # Use unified validation function
                validation_result = youtube_utils.validate_youtube_data_entities(
                    entity_to_validate=entity,
                    actual_entity=actual_entity
                )
                results.append(validation_result)

            except Exception as e:
                bt.logging.error(f"Validation error for entity: {str(e)}")
                results.append(ValidationResult(
                    is_valid=False,
                    reason=f"Validation failed due to error: {str(e)}",
                    content_size_bytes_validated=entity.content_size_bytes
                ))

        return results


async def test_single_video_scrape():
    """Test scraping a single video."""
    scraper = YouTubeMultiActorScraper()

    test_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    bt.logging.info(f"🎯 Testing single video scrape: {test_url}")

    try:
        entities = await scraper.scrape(youtube_url=test_url, language="en")

        if entities:
            entity = entities[0]
            from scraping.youtube.model import YouTubeContent
            content = YouTubeContent.from_data_entity(entity)

            bt.logging.success(f"✅ SUCCESS: Scraped video {content.video_id}")
            bt.logging.info(f"   Title: {content.title}")
            bt.logging.info(f"   Channel: {content.channel_name}")
            bt.logging.info(f"   Upload Date: {content.upload_date}")
            bt.logging.info(f"   Transcript segments: {len(content.transcript)}")
            bt.logging.info(f"   Content size: {entity.content_size_bytes} bytes")

            return entities
        else:
            bt.logging.error("❌ FAILED: No entities returned")
            return []

    except Exception as e:
        bt.logging.error(f"❌ ERROR: {str(e)}")
        return []


async def test_validation():
    """Test validation functionality."""
    bt.logging.info("🎯 Testing validation...")

    # First scrape a video
    entities = await test_single_video_scrape()
    if not entities:
        bt.logging.error("❌ Cannot test validation without scraped entities")
        return

    scraper = YouTubeMultiActorScraper()
    entity = entities[0]

    try:
        bt.logging.info("🔍 Starting validation...")
        validation_results = await scraper.validate([entity])

        if validation_results:
            result = validation_results[0]

            if result.is_valid:
                bt.logging.success(f"✅ VALIDATION PASSED: {result.reason}")
            else:
                bt.logging.error(f"❌ VALIDATION FAILED: {result.reason}")

            bt.logging.info(f"   Bytes validated: {result.content_size_bytes_validated}")
        else:
            bt.logging.error("❌ No validation results returned")

    except Exception as e:
        bt.logging.error(f"❌ VALIDATION ERROR: {str(e)}")


async def test_scraper_fallback():
    """Test the scraper fallback mechanism."""
    bt.logging.info("🎯 Testing scraper fallback mechanism...")

    scraper = YouTubeMultiActorScraper()
    test_videos = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",  # Rick Roll
        "https://www.youtube.com/watch?v=fJ9rUzIMcZQ",  # Bohemian Rhapsody
    ]

    for video_url in test_videos:
        video_id = scraper._extract_video_id_from_url(video_url)
        bt.logging.info(f"📹 Testing video: {video_id}")

        ordered_scrapers = scraper._get_ordered_scrapers(video_id)
        bt.logging.info(f"   Scraper order: {[name for name, _ in ordered_scrapers]}")

        entities = await scraper.scrape(youtube_url=video_url, language="en")
        if entities:
            bt.logging.success(f"✅ Successfully scraped video {video_id}")
        else:
            bt.logging.error(f"❌ Failed to scrape video {video_id}")


async def main():
    """Main test function."""
    bt.logging.set_trace(True)
    print("\nYouTube Multi-Actor Scraper Tests")
    print("=" * 40)
    print("1. Test single video scraping")
    print("2. Test validation")
    print("3. Test scraper fallback mechanism")
    print("4. Test 20 videos with different languages (comprehensive)")
    print("5. Test full pipeline")
    print("6. Exit")

    choice = input("\nEnter your choice (1-6): ")

    if choice == "1":
        await test_single_video_scrape()
    elif choice == "2":
        await test_validation()
    elif choice == "3":
        await test_scraper_fallback()
    elif choice == "5":
        bt.logging.info("Running full pipeline test...")
        await test_single_video_scrape()
        await test_validation()
        await test_scraper_fallback()
    elif choice == "6":
        print("Exiting.")
        return
    else:
        print("Invalid choice.")
        await main()


if __name__ == "__main__":
    bt.logging.info("Starting YouTube Multi-Actor Scraper tests...")
    asyncio.run(main())