import asyncio
import datetime as dt
import hashlib
import re
from typing import List, Dict, Any, Optional

import bittensor as bt
import httpx
from dotenv import load_dotenv

from common.data import DataEntity, DataLabel, DataSource
from scraping.apify import ActorRunner, RunConfig, ActorRunError
from scraping.scraper import Scraper, ValidationResult
from scraping.youtube import utils as youtube_utils
from scraping.youtube.model import YouTubeContent


load_dotenv()


class YouTubeMultiActorScraper(Scraper):
    """
    Multi-actor YouTube scraper that rotates between different Apify actors
    for load distribution and fault tolerance.
    """

    # Actor configurations with their specific input formats and response handling
    ACTOR_CONFIGS = [
        {
            "id": "crawlmaster/youtube-transcript-fetcher",
            "name": "crawlmaster",
            "input_format": lambda youtube_url, language: {"url": youtube_url, "lang": language},
            "upload_date_field": "upload_date",
            "has_youtube_api": False,
            "priority": 1  # Higher priority = preferred choice
        },
        {
            "id": "starvibe/youtube-video-transcript",
            "name": "starvibe",
            "input_format": lambda youtube_url, language: {"youtube_url": youtube_url, "language": language},
            "upload_date_field": "published_at",
            "has_youtube_api": False,
            "priority": 2
        },
        {
            "id": "invideoiq/video-transcript-scraper",
            "name": "invideoiq",
            "input_format": lambda youtube_url, language: {"video_url": youtube_url, "language": language},
            "upload_date_field": "published_at",
            "has_youtube_api": True,
            "priority": 3  # Fallback option
        }
    ]

    # Maximum number of validation attempts
    MAX_VALIDATION_ATTEMPTS = 2

    # Timeout for Apify actor runs
    APIFY_TIMEOUT_SECS = 180

    # Default language for transcripts (ISO 639-1 format)
    DEFAULT_LANGUAGE = "en"

    def __init__(self, runner: ActorRunner = None):
        """Initialize the Multi-Actor YouTube Transcript Scraper."""
        self.runner = runner or ActorRunner()

        # HTTP client for YouTube API calls (for invideoiq fallback)
        self._http_timeout = httpx.Timeout(10.0)

        bt.logging.info("YouTube Multi-Actor Transcript Scraper initialized with actors: " +
                       ", ".join([config["name"] for config in self.ACTOR_CONFIGS]))

    def _select_primary_actor(self, video_id: str) -> Dict[str, Any]:
        """Select primary actor for a video based on consistent hashing."""
        # Use consistent hashing to ensure same video always uses same actor (when available)
        video_hash = int(hashlib.md5(video_id.encode()).hexdigest(), 16)
        selected_index = video_hash % len(self.ACTOR_CONFIGS)
        selected_actor = self.ACTOR_CONFIGS[selected_index]

        bt.logging.debug(f"Selected actor {selected_actor['name']} for video {video_id}")
        return selected_actor

    def _get_ordered_actors(self, primary_actor: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get actors in order: primary first, then others by priority."""
        other_actors = [config for config in self.ACTOR_CONFIGS if config["id"] != primary_actor["id"]]
        # Sort others by priority (lower number = higher priority)
        other_actors_sorted = sorted(other_actors, key=lambda x: x["priority"])
        return [primary_actor] + other_actors_sorted

    async def _try_actor_with_fallback(self, video_id: str, language: str) -> Optional[Dict[str, Any]]:
        """Try to get video data with automatic fallback between actors."""
        primary_actor = self._select_primary_actor(video_id)
        ordered_actors = self._get_ordered_actors(primary_actor)

        for actor_config in ordered_actors:
            bt.logging.debug(f"Trying actor {actor_config['name']} for video {video_id}")
            result = await self._try_single_actor(video_id, language, actor_config)

            if result:
                if actor_config != primary_actor:
                    bt.logging.info(f"Fallback successful: {actor_config['name']} worked for video {video_id}")
                return result
            else:
                bt.logging.warning(f"Actor {actor_config['name']} failed for video {video_id}")

        bt.logging.error(f"All actors failed for video {video_id}")
        return None

    async def _try_single_actor(self, video_id: str, language: str, actor_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Try to get video data from a single actor."""
        try:
            youtube_url = f"https://www.youtube.com/watch?v={video_id}"

            run_config = RunConfig(
                actor_id=actor_config["id"],
                debug_info=f"Scrape {video_id} with {language} using {actor_config['name']}",
                timeout_secs=self.APIFY_TIMEOUT_SECS
            )

            # Use actor-specific input format
            run_input = actor_config["input_format"](youtube_url, language)

            bt.logging.debug(f"Running actor {actor_config['name']} with input: {run_input}")

            result = await self.runner.run(run_config, run_input)

            if result and isinstance(result, list) and len(result) > 0:
                response = result[0]
                normalized_response = self._normalize_actor_response(response, actor_config)
                return normalized_response

            return None

        except ActorRunError as e:
            bt.logging.error(f"Actor {actor_config['name']} run error for video {video_id}: {str(e)}")
            return None
        except Exception as e:
            bt.logging.error(f"Unexpected error with actor {actor_config['name']} for video {video_id}: {str(e)}")
            return None

    def _normalize_actor_response(self, response: Dict[str, Any], actor_config: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize actor response to a common format."""
        normalized = response.copy()

        # Normalize upload date field name
        upload_date_field = actor_config["upload_date_field"]
        if upload_date_field in response and upload_date_field != "upload_date":
            normalized["upload_date"] = response[upload_date_field]

        # Add metadata about which actor was used
        normalized["_actor_used"] = actor_config["name"]
        normalized["_actor_id"] = actor_config["id"]

        return normalized

    def _extract_video_id_from_url(self, url: str) -> Optional[str]:
        """Extract video ID from YouTube URL."""
        if not url:
            return None

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
        Uses actor rotation for load distribution.
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
            return await self._scrape_channel(channel_url, max_videos, start_date, end_date)

    async def _scrape_single_video(self, youtube_url: str, language: str) -> List[DataEntity]:
        """Internal method to scrape a single video."""
        video_id = self._extract_video_id_from_url(youtube_url)
        if not video_id:
            bt.logging.error(f"Invalid YouTube URL: {youtube_url}")
            return []

        try:
            # Use multi-actor approach
            meta_data = await self._try_actor_with_fallback(video_id, language)
            if not meta_data:
                bt.logging.error(f"No data returned for video {video_id} from any actor")
                return []

            # Get upload date from meta
            published_at = meta_data.get('upload_date')
            if not published_at:
                bt.logging.error(f"No upload_date for video {video_id}")
                return []

            # Handle different date formats from different actors
            if isinstance(published_at, str):
                if 'Z' in published_at:
                    upload_date = dt.datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                else:
                    upload_date = dt.datetime.fromisoformat(published_at)
            else:
                upload_date = published_at

            results = []
            content = YouTubeContent(
                video_id=meta_data.get('video_id'),
                title=meta_data.get('title'),
                channel_name=meta_data.get('channel_name', meta_data.get('channel', '')),
                upload_date=upload_date,
                transcript=meta_data.get('transcript', []),
                url=f"https://www.youtube.com/watch?v={video_id}",
                duration_seconds=meta_data.get('duration_seconds', 0),
                language=language
            )

            # Use the proper to_data_entity method which handles obfuscation correctly
            data_entity = YouTubeContent.to_data_entity(content)

            results.append(data_entity)
            bt.logging.success(f"Successfully scraped video {video_id} using actor {meta_data.get('_actor_used', 'unknown')}")

        except Exception as e:
            bt.logging.error(f"Error scraping video {video_id}: {str(e)}")
            return []

        return results

    async def _scrape_channel(self, channel_url: str, max_videos: int, start_date: Optional[str], end_date: Optional[str]) -> List[DataEntity]:
        """Internal method to scrape channel videos."""
        # This method would need to be implemented based on how channel scraping works
        # For now, return empty list as this is primarily focused on validation
        bt.logging.warning("Channel scraping not yet implemented in multi-actor scraper")
        return []

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate YouTube transcript entities using actor rotation."""
        if not entities:
            return []

        results = []

        for entity in entities:
            try:
                content_to_validate = YouTubeContent.from_data_entity(entity)
                original_language = content_to_validate.language

                bt.logging.info(
                    f"Validating video {content_to_validate.video_id} in original language: {original_language}")

                # Convert actor payload to DataEntity for comparison
                actual_entities = await self.scrape(
                    youtube_url=f"https://www.youtube.com/watch?v={content_to_validate.video_id}",
                    language=original_language
                )

                if not actual_entities:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Video not available for validation in original language from any actor",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                actual_entity = actual_entities[0]

                # Use unified validation function
                validation_result = youtube_utils.validate_youtube_data_entities(
                    entity_to_validate=entity,
                    actual_entity=actual_entity
                )
                results.append(validation_result)

            except Exception as e:
                bt.logging.error(f"Validation error: {str(e)}")
                results.append(ValidationResult(
                    is_valid=False,
                    reason=f"Validation failed due to error: {str(e)}",
                    content_size_bytes_validated=entity.content_size_bytes
                ))

        return results


# Test functions
async def test_single_video_scrape():
    """Test single video scraping functionality with multiple actors."""
    bt.logging.info("=" * 60)
    bt.logging.info("STARTING MULTI-ACTOR SINGLE VIDEO SCRAPE TEST")
    bt.logging.info("=" * 60)

    scraper = YouTubeMultiActorScraper()

    # Test with a known video ID
    test_video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"  # Rick Roll - widely available
    language = "en"

    bt.logging.info(f"Testing with video: {test_video_url}")
    bt.logging.info(f"Language: {language}")

    try:
        entities = await scraper._scrape_single_video(test_video_url, language)

        if entities:
            bt.logging.success(f"✅ Successfully scraped {len(entities)} entity(ies)")
            for i, entity in enumerate(entities, 1):
                content = YouTubeContent.from_data_entity(entity)
                bt.logging.info(f"Entity {i}:")
                bt.logging.info(f"  Video ID: {content.video_id}")
                bt.logging.info(f"  Title: {content.title[:50]}...")
                bt.logging.info(f"  Channel: {content.channel_name}")
                bt.logging.info(f"  Language: {content.language}")
                bt.logging.info(f"  Upload Date: {content.upload_date}")
                bt.logging.info(f"  Transcript Length: {len(content.transcript)} segments")
                bt.logging.info(f"  Content Size: {entity.content_size_bytes} bytes")
        else:
            bt.logging.error("❌ No entities scraped")

        return entities

    except Exception as e:
        bt.logging.error(f"❌ Scrape test failed: {str(e)}")
        return []


async def test_actor_fallback():
    """Test actor fallback functionality."""
    bt.logging.info("=" * 60)
    bt.logging.info("STARTING ACTOR FALLBACK TEST")
    bt.logging.info("=" * 60)

    scraper = YouTubeMultiActorScraper()

    # Test with different video IDs to see actor selection
    test_videos = [
        "dQw4w9WgXcQ",  # Rick Roll
        "9bZkp7q19f0",  # Gangnam Style
        "kJQP7kiw5Fk"   # Despacito
    ]

    for video_id in test_videos:
        bt.logging.info(f"\nTesting actor fallback for video: {video_id}")

        try:
            # Test primary actor selection
            primary_actor = scraper._select_primary_actor(video_id)
            bt.logging.info(f"Primary actor selected: {primary_actor['name']}")

            # Test actor fallback
            meta_data = await scraper._try_actor_with_fallback(video_id, "en")
            if meta_data:
                actor_used = meta_data.get('_actor_used', 'unknown')
                bt.logging.success(f"✅ Success with actor: {actor_used}")
                bt.logging.info(f"  Title: {meta_data.get('title', 'N/A')[:40]}...")
                bt.logging.info(f"  View Count: {meta_data.get('view_count', 'N/A')}")
            else:
                bt.logging.error(f"❌ All actors failed for video {video_id}")

        except Exception as e:
            bt.logging.error(f"❌ Fallback test failed for {video_id}: {str(e)}")


async def test_multi_actor_validation():
    """Test validation functionality with multiple actors."""
    bt.logging.info("=" * 60)
    bt.logging.info("STARTING MULTI-ACTOR VALIDATION TEST")
    bt.logging.info("=" * 60)

    # Create scraper first
    scraper = YouTubeMultiActorScraper()

    # Scrape a simple test video directly instead of calling another test
    test_video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    language = "en"

    bt.logging.info(f"Scraping test video: {test_video_url}")

    try:
        entities = await scraper._scrape_single_video(test_video_url, language)

        if not entities:
            bt.logging.error("❌ No entities scraped - cannot proceed with validation test")
            return

        bt.logging.info(f"✅ Successfully scraped {len(entities)} entity(ies)")

        for i, entity in enumerate(entities, 1):
            content = YouTubeContent.from_data_entity(entity)
            bt.logging.info(f"Entity {i}: Video {content.video_id} ({content.title[:50]}...) in language {content.language}")

        bt.logging.info("=" * 40)
        bt.logging.info("STARTING VALIDATION PROCESS")
        bt.logging.info("=" * 40)

        results = []
        for i, entity in enumerate(entities, 1):
            content = YouTubeContent.from_data_entity(entity)
            bt.logging.info(f"\n🔍 Validating entity {i}: {content.video_id}")

            # Get actor data with verbose logging
            bt.logging.info(f"Getting metadata for video {content.video_id} in language {content.language}")
            meta_data = await scraper._try_actor_with_fallback(content.video_id, content.language)

            if not meta_data:
                bt.logging.error(f"❌ No metadata available for {content.video_id}")
                result = ValidationResult(
                    is_valid=False,
                    reason="Video metadata not available from any actor",
                    content_size_bytes_validated=entity.content_size_bytes
                )
                results.append(result)
                continue

            actor_used = meta_data.get('_actor_used', 'unknown')
            bt.logging.info(f"✅ Got metadata from actor: {actor_used}")

            # Use unified validation function
            bt.logging.info("Running unified validation...")
            from scraping.youtube import utils as youtube_utils

            # Create DataEntity from actor metadata for comparison
            actual_entities = await scraper.scrape(
                youtube_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                language="en"
            )
            actual_entity = actual_entities[0]

            result = youtube_utils.validate_youtube_data_entities(
                entity_to_validate=entity,
                actual_entity=actual_entity
            )

            results.append(result)

            if result.is_valid:
                bt.logging.success(f"✅ Validation PASSED for {content.video_id}")
            else:
                bt.logging.error(f"❌ Validation FAILED for {content.video_id}")

            bt.logging.info(f"   Reason: {result.reason}")
            bt.logging.info(f"   Bytes validated: {result.content_size_bytes_validated}")

        bt.logging.info("=" * 40)
        bt.logging.info("FINAL VALIDATION RESULTS:")
        bt.logging.info("=" * 40)

        valid_count = sum(1 for r in results if r.is_valid)
        invalid_count = len(results) - valid_count

        for i, result in enumerate(results, 1):
            entity = entities[i - 1]
            content = YouTubeContent.from_data_entity(entity)

            status = "✅ VALID" if result.is_valid else "❌ INVALID"
            bt.logging.info(f"Entity {i}: {status}")
            bt.logging.info(f"   Video: {content.video_id} ({content.title[:40]}...)")
            bt.logging.info(f"   Reason: {result.reason}")
            bt.logging.info("-" * 30)

        bt.logging.info("VALIDATION SUMMARY:")
        bt.logging.success(f"✅ Valid: {valid_count}/{len(results)} ({valid_count / len(results) * 100:.1f}%)")
        if invalid_count > 0:
            bt.logging.warning(f"❌ Invalid: {invalid_count}/{len(results)} ({invalid_count / len(results) * 100:.1f}%)")

        bt.logging.info("MULTI-ACTOR VALIDATION TEST COMPLETED")
        return results

    except Exception as e:
        bt.logging.error(f"❌ Validation test failed: {str(e)}")
        import traceback
        bt.logging.error(f"Traceback: {traceback.format_exc()}")
        return []


async def test_language_fallback():
    """Test language handling and fallback across actors."""
    bt.logging.info("=" * 60)
    bt.logging.info("STARTING LANGUAGE FALLBACK TEST")
    bt.logging.info("=" * 60)

    scraper = YouTubeMultiActorScraper()

    # Test different languages
    test_cases = [
        ("dQw4w9WgXcQ", "en"),  # English
        ("x5dArZ6MaKk", "th"),  # Thai
        ("LfCYDBOdaN4", "th"),  # Thai (smaller video)
        ("9bZkp7q19f0", "ko"),  # Korean (Gangnam Style)
    ]

    for video_id, language in test_cases:
        bt.logging.info(f"\nTesting language '{language}' for video: {video_id}")

        try:
            meta_data = await scraper._try_actor_with_fallback(video_id, language)
            if meta_data:
                actor_used = meta_data.get('_actor_used', 'unknown')
                bt.logging.success(f"✅ Success with actor: {actor_used}")
                bt.logging.info(f"  Language requested: {language}")
                bt.logging.info(f"  Title: {meta_data.get('title', 'N/A')[:40]}...")
                bt.logging.info(f"  Transcript segments: {len(meta_data.get('transcript', []))}")
            else:
                bt.logging.error(f"❌ No actor could provide {language} transcript for {video_id}")

        except Exception as e:
            bt.logging.error(f"❌ Language test failed for {video_id} ({language}): {str(e)}")


async def test_unified_validation_function():
    """Test the unified validation function with multiple entities like other scrapers."""
    bt.logging.info("=" * 60)
    bt.logging.info("STARTING COMPREHENSIVE UNIFIED VALIDATION TEST")
    bt.logging.info("=" * 60)

    scraper = YouTubeMultiActorScraper()

    # Test with multiple videos like other scrapers do
    test_videos = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",  # Rick Roll
        "https://www.youtube.com/watch?v=9bZkp7q19f0",  # Gangnam Style
        "https://www.youtube.com/watch?v=kffacxfA7G4",  # Baby Shark
        "https://www.youtube.com/watch?v=fJ9rUzIMcZQ",  # Bohemian Rhapsody
        "https://www.youtube.com/watch?v=60ItHLz5WEA",  # Alan Walker - Faded
    ]

    results = []
    valid_count = 0

    bt.logging.info(f"Testing unified validation on {len(test_videos)} videos...")
    bt.logging.info("=" * 60)

    for i, video_url in enumerate(test_videos, 1):
        bt.logging.info(f"\n🎯 Test {i}/{len(test_videos)}: {video_url}")

        try:
            # Create entity from scraping
            entities = await scraper.scrape(youtube_url=video_url, language="en")

            if not entities:
                bt.logging.warning(f"❌ Could not scrape {video_url} - skipping")
                continue

            entity = entities[0]
            content = YouTubeContent.from_data_entity(entity)
            bt.logging.info(f"   Entity: {content.video_id} - {content.title[:50]}...")

            # Get fresh data to compare against
            actual_entities = await scraper.scrape(youtube_url=video_url, language="en")

            if not actual_entities:
                bt.logging.warning(f"❌ Could not get fresh data for {video_url}")
                continue

            actual_entity = actual_entities[0]

            # Test unified validation function directly
            from scraping.youtube import utils as youtube_utils
            validation_result = youtube_utils.validate_youtube_data_entities(
                entity_to_validate=entity,
                actual_entity=actual_entity
            )

            results.append(validation_result)

            if validation_result.is_valid:
                valid_count += 1
                bt.logging.success(f"   ✅ VALID - {validation_result.reason}")
            else:
                bt.logging.error(f"   ❌ INVALID - {validation_result.reason}")

            bt.logging.info(f"   Bytes validated: {validation_result.content_size_bytes_validated}")

        except Exception as video_error:
            bt.logging.error(f"   ❌ Error testing {video_url}: {str(video_error)}")
            continue

    # Summary
    bt.logging.info("\n" + "=" * 60)
    bt.logging.info("UNIFIED VALIDATION TEST SUMMARY:")
    bt.logging.info("=" * 60)
    total_tests = len(results)
    invalid_count = total_tests - valid_count

    bt.logging.success(f"✅ Valid: {valid_count}/{total_tests} ({valid_count/total_tests*100:.1f}%)" if total_tests > 0 else "No tests completed")
    if invalid_count > 0:
        bt.logging.warning(f"❌ Invalid: {invalid_count}/{total_tests} ({invalid_count/total_tests*100:.1f}%)")

    # Detailed results
    for i, result in enumerate(results, 1):
        status = "✅ VALID" if result.is_valid else "❌ INVALID"
        bt.logging.info(f"Test {i}: {status} - {result.reason}")

    return results


async def main():
    """Main test function."""
    print("\nYouTube Multi-Actor Transcript Scraper Test Suite")
    print("=" * 55)
    print("1. Test single video scraping")
    print("2. Test actor fallback")
    print("3. Test multi-actor validation")
    print("4. Test language fallback")
    print("5. Test unified validation function")
    print("6. Run all tests")
    print("7. Exit")

    choice = input("\nEnter your choice (1-7): ")

    if choice == "1":
        await test_single_video_scrape()
    elif choice == "2":
        await test_actor_fallback()
    elif choice == "3":
        await test_multi_actor_validation()
    elif choice == "4":
        await test_language_fallback()
    elif choice == "5":
        await test_unified_validation_function()
    elif choice == "6":
        bt.logging.info("Running all tests...")
        await test_single_video_scrape()
        await test_actor_fallback()
        await test_multi_actor_validation()
        await test_language_fallback()
        await test_unified_validation_function()
        bt.logging.info("All tests completed!")
    elif choice == "7":
        print("Exiting...")
    else:
        print("Invalid choice. Please try again.")


if __name__ == "__main__":
    bt.logging.set_trace(True)
    bt.logging.info("Starting YouTube Multi-Actor Scraper tests...")
    import asyncio
    asyncio.run(main())
