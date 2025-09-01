import asyncio
import traceback
import re
import bittensor as bt
from typing import List, Dict, Any, Optional
import datetime as dt
import httpx

from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.youtube.model import YouTubeContent
from scraping.youtube import utils as youtube_utils
from scraping.apify import ActorRunner, RunConfig, ActorRunError


class YouTubeChannelTranscriptScraper(Scraper):
    """
    Final YouTube scraper using:
    1. invideoiq/video-transcript-scraper for transcripts and language support
    2. Minimal YouTube API for upload dates and video discovery
    3. Channel-based labeling (no language in labels)
    4. Respects miner's original language choices during validation
    """

    # Apify actor ID for the video transcript scraper
    ACTOR_ID = "invideoiq/video-transcript-scraper"

    # Maximum number of validation attempts
    MAX_VALIDATION_ATTEMPTS = 2

    # Timeout for Apify actor runs
    APIFY_TIMEOUT_SECS = 180

    # Default language for transcripts (ISO 639-1 format)
    DEFAULT_LANGUAGE = "en"

    def __init__(self, runner: ActorRunner = None):
        """Initialize the YouTube Channel Transcript Scraper."""
        self.runner = runner or ActorRunner()

        # Minimal YouTube API setup for upload dates and video discovery
        import os
        self.youtube_api_key = os.getenv("YOUTUBE_API_KEY")
        if not self.youtube_api_key:
            bt.logging.warning(
                "YOUTUBE_API_KEY not found - upload date validation and channel discovery will be limited")

        # HTTP client for YouTube API calls
        self._http_timeout = httpx.Timeout(10.0)

        bt.logging.info("YouTube Channel Transcript Scraper initialized")

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """
        Scrapes YouTube transcripts with channel-based approach.
        Language is determined automatically by the actor for each video.
        """
        bt.logging.info(f"Starting YouTube channel scrape with config: {scrape_config}")

        # Parse channel identifiers from labels
        channel_identifiers = self._parse_channel_configs_from_labels(scrape_config.labels)

        if not channel_identifiers:
            bt.logging.warning("No channel identifiers found in labels")
            return []

        return await self._scrape_channels_hybrid(channel_identifiers, scrape_config)

    def _parse_channel_configs_from_labels(self, labels: List[DataLabel]) -> List[str]:
        """
        Parse channel configurations from labels.

        Supported format: #ytc_c_{channel_identifier}
        """
        channel_identifiers = []

        if not labels:
            bt.logging.warning("No labels provided")
            return channel_identifiers

        bt.logging.info(f"Parsing {len(labels)} labels: {[label.value for label in labels]}")

        for label in labels:
            bt.logging.debug(f"Processing label: {label.value}")
            channel_identifier = YouTubeContent.parse_channel_label(label.value)
            if channel_identifier:
                channel_identifiers.append(channel_identifier)
                bt.logging.info(
                    f"Successfully parsed channel identifier: '{channel_identifier}' from label: '{label.value}'")
            else:
                bt.logging.warning(f"Failed to parse channel identifier from label: '{label.value}'")

        bt.logging.info(f"Found {len(channel_identifiers)} valid channel identifiers: {channel_identifiers}")
        return channel_identifiers

    async def _scrape_channels_hybrid(self, channel_identifiers: List[str], scrape_config: ScrapeConfig) -> List[
        DataEntity]:
        """
        Scrape videos from channels using hybrid approach.
        Lets the actor choose the best available language for each video.
        """
        results = []
        max_entities = scrape_config.entity_limit or 10

        for channel_identifier in channel_identifiers:
            try:
                bt.logging.info(f"Processing channel: {channel_identifier}")

                # Get recent videos from the channel
                video_ids = await self._get_channel_videos(channel_identifier, scrape_config.date_range, max_entities)

                if not video_ids:
                    bt.logging.warning(f"No videos found for channel {channel_identifier}")
                    continue

                bt.logging.info(f"Found {len(video_ids)} videos for channel {channel_identifier}")

                # Process each video
                for i, video_id in enumerate(video_ids, 1):
                    try:
                        bt.logging.info(f"Processing video {i}/{len(video_ids)}: {video_id}")

                        # Try to get transcript in any available language (let actor decide)
                        transcript_data = await self._get_transcript_from_actor_any_language(video_id)
                        if not transcript_data:
                            bt.logging.warning(f"No transcript available for video {video_id}")
                            continue

                        # Get view count from Apify actor response
                        view_count = transcript_data.get('view_count', 0)

                        # Filter low engagement videos (100+ views required)
                        if view_count < 100:
                            bt.logging.info(
                                f"Video {video_id} has only {view_count} views, skipping (minimum 100 required)")
                            continue

                        # Get upload date from API
                        upload_date = await self._get_upload_date_from_api(video_id)
                        if not upload_date:
                            bt.logging.warning(f"No upload date for video {video_id}, skipping to prevent timestamp validation bypass")
                            continue

                        # Check date range
                        if not self._is_within_date_range(upload_date, scrape_config.date_range):
                            bt.logging.trace(f"Video {video_id} outside date range, skipping")
                            continue

                        # Use the language that the actor actually returned
                        actual_language = transcript_data.get('selected_language', self.DEFAULT_LANGUAGE)
                        bt.logging.info(f"Video {video_id} transcript obtained in language: {actual_language}")

                        # Create content
                        content = self._create_youtube_content(
                            video_id=video_id,
                            language=actual_language,
                            upload_date=upload_date,
                            transcript_data=transcript_data,
                            channel_identifier=channel_identifier
                        )

                        # Convert to DataEntity
                        entity = YouTubeContent.to_data_entity(content)
                        results.append(entity)

                        bt.logging.success(
                            f"Successfully scraped video {video_id} from channel {channel_identifier} in language {actual_language}")

                        if len(results) >= max_entities:
                            bt.logging.info(f"Reached maximum entities limit ({max_entities})")
                            break

                    except Exception as e:
                        bt.logging.error(f"Error scraping video {video_id}: {str(e)}")
                        bt.logging.debug(traceback.format_exc())
                        continue

                if len(results) >= max_entities:
                    break

            except Exception as e:
                bt.logging.error(f"Error scraping channel {channel_identifier}: {str(e)}")
                bt.logging.debug(traceback.format_exc())
                continue

        bt.logging.success(f"Scraped {len(results)} YouTube videos from channels")
        return results

    async def _get_channel_videos(self, channel_identifier: str, date_range: DateRange, max_videos: int = 10) -> List[
        str]:
        """Get video IDs from a channel using YouTube API."""
        if not self.youtube_api_key:
            bt.logging.warning("No YouTube API key - cannot discover channel videos")
            return []

        try:
            bt.logging.info(f"Discovering videos for channel: {channel_identifier}")

            # First, resolve the channel identifier to actual channel ID
            channel_id = await self._resolve_channel_id(channel_identifier)
            if not channel_id:
                bt.logging.warning(f"Could not resolve channel ID for: {channel_identifier}")
                return []

            bt.logging.info(f"Resolved channel '{channel_identifier}' to ID: {channel_id}")

            # Get the uploads playlist ID
            url = "https://www.googleapis.com/youtube/v3/channels"
            params = {
                "id": channel_id,
                "part": "contentDetails",
                "key": self.youtube_api_key
            }

            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                print('DATA from YT google response')
                print(data)
                if not data.get("items"):
                    bt.logging.warning(f"No channel data found for ID: {channel_id}")
                    return []

                uploads_playlist_id = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
                bt.logging.info(f"Found uploads playlist: {uploads_playlist_id}")

                # Get videos from uploads playlist
                return await self._get_playlist_videos(uploads_playlist_id, max_videos)

        except Exception as e:
            bt.logging.error(f"Error getting videos for channel {channel_identifier}: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return []

    async def _resolve_channel_id(self, channel_identifier: str) -> Optional[str]:
        """Resolve channel identifier to actual channel ID."""
        try:
            # If it looks like a channel ID already (starts with UC), return it
            if channel_identifier.startswith('UC') and len(channel_identifier) == 24:
                bt.logging.info(f"Channel identifier '{channel_identifier}' appears to be a channel ID")
                return channel_identifier

            # Try to search for the channel by name/handle
            bt.logging.info(f"Searching for channel: {channel_identifier}")
            url = "https://www.googleapis.com/youtube/v3/search"
            params = {
                "q": channel_identifier,
                "type": "channel",
                "part": "snippet",
                "maxResults": 1,
                "key": self.youtube_api_key
            }

            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if data.get("items"):
                    channel_id = data["items"][0]["snippet"]["channelId"]
                    bt.logging.success(f"Found channel ID: {channel_id}")
                    return channel_id
                else:
                    bt.logging.warning(f"No search results for channel: {channel_identifier}")

        except Exception as e:
            bt.logging.error(f"Error resolving channel ID for {channel_identifier}: {str(e)}")

        return None

    async def _get_playlist_videos(self, playlist_id: str, max_videos: int) -> List[str]:
        """Get video IDs from a playlist."""
        video_ids = []

        try:
            bt.logging.info(f"Getting videos from playlist: {playlist_id}")
            url = "https://www.googleapis.com/youtube/v3/playlistItems"
            params = {
                "playlistId": playlist_id,
                "part": "contentDetails",
                "maxResults": min(max_videos, 50),
                "key": self.youtube_api_key
            }

            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                for item in data.get("items", []):
                    video_id = item["contentDetails"]["videoId"]
                    video_ids.append(video_id)

                bt.logging.success(f"Retrieved {len(video_ids)} video IDs from playlist")

        except Exception as e:
            bt.logging.error(f"Error getting playlist videos: {str(e)}")

        return video_ids

    async def _get_transcript_from_actor_any_language(self, video_id: str) -> Optional[Dict[str, Any]]:
        """Get transcript data using the Apify actor - let it choose the best available language."""
        try:
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            run_input = {
                "video_url": video_url,
                "best_effort": True,
                "get_yt_original_metadata": True
            }

            run_config = RunConfig(
                actor_id=self.ACTOR_ID,
                debug_info=f"Transcript for {video_id} (any language)",
                max_data_entities=1,
                timeout_secs=self.APIFY_TIMEOUT_SECS
            )

            bt.logging.debug(f"Getting transcript for video {video_id} (any available language)")

            dataset = await self.runner.run(run_config, run_input)

            if not dataset or len(dataset) == 0:
                bt.logging.warning(f"No transcript data returned for video {video_id}")
                return None

            result = dataset[0]
            selected_language = result.get('selected_language', 'unknown')
            available_languages = result.get('available_languages', [])

            bt.logging.info(f"Got transcript for {video_id} in language: {selected_language}")
            if available_languages:
                bt.logging.debug(f"Available languages: {', '.join(available_languages)}")

            return result

        except Exception as e:
            bt.logging.error(f"Error getting transcript for video {video_id}: {str(e)}")
            return None

    async def _get_transcript_from_actor(self, video_id: str, language: str) -> Optional[Dict[str, Any]]:
        """Get transcript data using the Apify actor with specific language."""
        try:
            video_url = f"https://www.youtube.com/watch?v={video_id}"

            run_input = {
                "video_url": video_url,
                "language": language,
                "best_effort": True,
                "get_yt_original_metadata": True
            }

            run_config = RunConfig(
                actor_id=self.ACTOR_ID,
                debug_info=f"Transcript for {video_id} in {language}",
                max_data_entities=1,
                timeout_secs=self.APIFY_TIMEOUT_SECS
            )

            dataset = await self.runner.run(run_config, run_input)

            if not dataset or len(dataset) == 0:
                return None

            return dataset[0]

        except Exception as e:
            bt.logging.error(f"Error getting transcript for video {video_id} in {language}: {str(e)}")
            return None

    async def _get_upload_date_from_api(self, video_id: str) -> Optional[dt.datetime]:
        """Get upload date from YouTube API."""
        if not self.youtube_api_key:
            return None

        try:
            url = "https://www.googleapis.com/youtube/v3/videos"
            params = {
                "id": video_id,
                "part": "snippet",
                "fields": "items(snippet(publishedAt))",
                "key": self.youtube_api_key
            }

            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if data.get("items") and len(data["items"]) > 0:
                    item = data["items"][0]
                    published_at = item["snippet"]["publishedAt"]
                    upload_date = dt.datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                    return upload_date

        except Exception as e:
            bt.logging.warning(f"Failed to get upload date for {video_id}: {str(e)}")

        return None

    def _create_youtube_content(self, video_id: str, language: str, upload_date: dt.datetime,
                                transcript_data: Dict[str, Any], channel_identifier: str) -> YouTubeContent:
        """Create YouTubeContent object from scraped data."""

        # Extract channel info
        channel_name = transcript_data.get('channel', '')

        # Use the provided channel_identifier as channel_id (normalized)

        return YouTubeContent(
            video_id=video_id,
            title=transcript_data.get('title', ''),
            channel_name=channel_name,
            upload_date=upload_date,
            transcript=transcript_data.get('transcript', []),
            url=f"https://www.youtube.com/watch?v={video_id}",
            duration_seconds=int(transcript_data.get('duration', '0')),
            language=language
        )

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate YouTube transcript entities - respects the original language used by miner."""
        if not entities:
            return []

        results = []

        for entity in entities:
            try:
                content_to_validate = YouTubeContent.from_data_entity(entity)
                # Use the language that was originally stored by the miner
                original_language = content_to_validate.language

                bt.logging.info(
                    f"Validating video {content_to_validate.video_id} in original language: {original_language}")

                # Validate upload date against YouTube API to prevent timeBucketId bypass
                real_upload_date = await self._get_upload_date_from_api(content_to_validate.video_id)
                
                if not real_upload_date:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Cannot verify upload date from YouTube API - potential timestamp manipulation",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue
                
                # Use proper timestamp validation with obfuscation (like X and Reddit)
                timestamp_validation = youtube_utils.validate_youtube_timestamp(
                    content_to_validate, real_upload_date, entity
                )
                if not timestamp_validation.is_valid:
                    results.append(timestamp_validation)
                    continue
                
                # Get current data for validation using the SAME language the miner used
                transcript_data = await self._get_transcript_from_actor(content_to_validate.video_id, original_language)
                if not transcript_data:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Video transcript not available in original language",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Validate view count during validation
                view_count = transcript_data.get('view_count', 0)
                if view_count < 100:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason=f"Video has low engagement ({view_count} views, minimum 100 required)",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Validate content match first
                content_validation_result = self._validate_content_match(transcript_data, content_to_validate, entity)
                if not content_validation_result.is_valid:
                    results.append(content_validation_result)
                    continue
                
                # Create actual YouTube content for DataEntity validation
                actual_youtube_content = self._create_youtube_content(
                    content_to_validate.video_id, 
                    original_language, 
                    real_upload_date,
                    transcript_data, 
                    transcript_data.get('channel', '')
                )
                
                # Validate DataEntity fields (including channel label) like X and Reddit do
                entity_validation_result = youtube_utils.validate_youtube_data_entity_fields(actual_youtube_content, entity)
                results.append(entity_validation_result)

            except Exception as e:
                bt.logging.error(f"Validation error: {str(e)}")
                results.append(ValidationResult(
                    is_valid=False,
                    reason=f"Validation error: {str(e)}",
                    content_size_bytes_validated=entity.content_size_bytes
                ))

        return results

    def _validate_content_match(self, actual_data: Dict[str, Any], stored_content: YouTubeContent,
                                entity: DataEntity) -> ValidationResult:
        """Validate that stored content matches actual data."""

        # Validate video ID (should be exact match)
        actual_video_id = actual_data.get('video_id', '')
        if actual_video_id and actual_video_id != stored_content.video_id:
            return ValidationResult(
                is_valid=False,
                reason="Video ID mismatch",
                content_size_bytes_validated=entity.content_size_bytes
            )

        # Validate title (allow minor differences)
        if not self._texts_are_similar(actual_data.get('title', ''), stored_content.title, threshold=0.8):
            return ValidationResult(
                is_valid=False,
                reason="Title mismatch",
                content_size_bytes_validated=entity.content_size_bytes
            )

        # Validate transcript content
        actual_transcript = actual_data.get('transcript', [])
        if not self._transcripts_are_similar(actual_transcript, stored_content.transcript):
            return ValidationResult(
                is_valid=False,
                reason="Transcript content mismatch",
                content_size_bytes_validated=entity.content_size_bytes
            )

        # Validate language match
        actual_language = actual_data.get('selected_language', '')
        expected_language = stored_content.language
        # if expected_language and actual_language != expected_language:
        #     return ValidationResult(
        #         is_valid=False,
        #         reason=f"Language mismatch: expected {expected_language}, got {actual_language}",
        #         content_size_bytes_validated=entity.content_size_bytes
        #     )

        return ValidationResult(
            is_valid=True,
            reason="Content validated successfully",
            content_size_bytes_validated=entity.content_size_bytes
        )

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

    def _extract_transcript_text(self, transcript: List[Dict]) -> str:
        """Extract full text from transcript."""
        if not transcript:
            return ""
        return " ".join([item.get('text', '') for item in transcript])

    def _texts_are_similar(self, text1: str, text2: str, threshold: float = 0.8) -> bool:
        """Check if two texts are similar enough."""
        if not text1 or not text2:
            return text1 == text2

        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())

        if not words1 or not words2:
            return False

        overlap = len(words1.intersection(words2))
        similarity = overlap / max(len(words1), len(words2))

        return similarity >= threshold

    def _transcripts_are_similar(self, transcript1: List[Dict], transcript2: List[Dict],
                                 threshold: float = 0.7) -> bool:
        """Check if two transcripts are similar enough."""
        text1 = self._extract_transcript_text(transcript1)
        text2 = self._extract_transcript_text(transcript2)
        return self._texts_are_similar(text1, text2, threshold)

    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two texts."""
        if not text1 or not text2:
            return 0.0

        words1 = set(re.sub(r'[^\w\s]', '', text1.lower()).split())
        words2 = set(re.sub(r'[^\w\s]', '', text2.lower()).split())

        if not words1 or not words2:
            return 0.0

        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        return intersection / union if union > 0 else 0.0

    def _is_within_date_range(self, date: dt.datetime, date_range: DateRange) -> bool:
        """Check if a date is within the specified date range."""
        return date_range.contains(date)


# Test functions
async def test_channel_scrape():
    """Test channel-based scraping functionality."""
    bt.logging.info("=" * 60)
    bt.logging.info("STARTING CHANNEL SCRAPE TEST")
    bt.logging.info("=" * 60)

    scraper = YouTubeChannelTranscriptScraper()

    test_labels = [
        DataLabel(value="#ytc_c_fireship"),
        DataLabel(value="#ytc_c_ted"),
    ]

    bt.logging.info(f"Testing with labels: {[label.value for label in test_labels]}")

    scrape_config = ScrapeConfig(
        entity_limit=3,
        date_range=DateRange(
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime.now(dt.timezone.utc)
        ),
        labels=test_labels
    )

    bt.logging.info(f"Scrape config - Entity limit: {scrape_config.entity_limit}")
    bt.logging.info(f"Date range: {scrape_config.date_range.start} to {scrape_config.date_range.end}")

    bt.logging.info("Starting scrape process...")
    entities = await scraper.scrape(scrape_config)

    bt.logging.success(f"Scrape completed! Found {len(entities)} entities from channels")

    if entities:
        bt.logging.info("=" * 40)
        bt.logging.info("SCRAPED ENTITIES DETAILS:")
        bt.logging.info("=" * 40)

        for i, entity in enumerate(entities, 1):
            content = YouTubeContent.from_data_entity(entity)
            bt.logging.info(f"Entity {i}:")
            bt.logging.info(f"  Title: {content.title}")
            bt.logging.info(f"  Video ID: {content.video_id}")
            bt.logging.info(f"  Label: {entity.label.value}")
            bt.logging.info(f"  Language: {content.language}")
            bt.logging.info(f"  Upload Date: {content.upload_date}")
            bt.logging.info(f"  Duration: {content.duration_seconds}s")
            bt.logging.info(f"  Transcript segments: {len(content.transcript)}")
            bt.logging.info(f"  Content size: {entity.content_size_bytes} bytes")
            bt.logging.info(f"  URL: {content.url}")
            if content.transcript:
                first_text = content.transcript[0].get('text', '')[:100]
                bt.logging.info(f"  First transcript: {first_text}...")
            bt.logging.info("-" * 40)
    else:
        bt.logging.warning("No entities were scraped!")

    bt.logging.info("CHANNEL SCRAPE TEST COMPLETED")
    return entities


async def test_validation():
    """Test validation functionality."""
    bt.logging.info("=" * 60)
    bt.logging.info("STARTING VALIDATION TEST")
    bt.logging.info("=" * 60)

    bt.logging.info("First, scraping some entities to validate...")
    entities = await test_channel_scrape()

    if not entities:
        bt.logging.error("❌ No entities scraped - cannot proceed with validation test")
        return

    bt.logging.info(f"Got {len(entities)} entities from scrape, proceeding with validation...")

    scraper = YouTubeChannelTranscriptScraper()

    bt.logging.info("Starting validation process...")
    bt.logging.info(f"Validating {len(entities)} entities...")

    for i, entity in enumerate(entities, 1):
        content = YouTubeContent.from_data_entity(entity)
        bt.logging.info(
            f"Entity {i}: Video {content.video_id} ({content.title[:30]}...) in language {content.language}")

    results = await scraper.validate(entities)

    bt.logging.info(f"Validation completed! Got {len(results)} results")

    bt.logging.info("=" * 40)
    bt.logging.info("VALIDATION RESULTS:")
    bt.logging.info("=" * 40)

    valid_count = 0
    invalid_count = 0

    for i, result in enumerate(results, 1):
        entity = entities[i - 1]
        content = YouTubeContent.from_data_entity(entity)

        if result.is_valid:
            valid_count += 1
            bt.logging.success(f"✅ Entity {i}: VALID")
        else:
            invalid_count += 1
            bt.logging.error(f"❌ Entity {i}: INVALID")

        bt.logging.info(f"   Video: {content.video_id} ({content.title[:40]}...)")
        bt.logging.info(f"   Language: {content.language}")
        bt.logging.info(f"   Reason: {result.reason}")
        bt.logging.info(f"   Bytes validated: {result.content_size_bytes_validated}")
        bt.logging.info("-" * 30)

    bt.logging.info("VALIDATION SUMMARY:")
    bt.logging.success(f"✅ Valid: {valid_count}/{len(results)} ({valid_count / len(results) * 100:.1f}%)")
    bt.logging.warning(f"❌ Invalid: {invalid_count}/{len(results)} ({invalid_count / len(results) * 100:.1f}%)")

    bt.logging.info("VALIDATION TEST COMPLETED")
    return results


async def main():
    """Main test function."""
    print("\nFinal YouTube Channel-Based Transcript Scraper")
    print("=" * 50)
    print("1. Test channel scraping")
    print("2. Test validation")
    print("3. Test full pipeline")
    print("4. Exit")

    choice = input("\nEnter your choice (1-4): ")

    if choice == "1":
        await test_channel_scrape()
    elif choice == "2":
        await test_validation()
    elif choice == "3":
        bt.logging.info("Running full pipeline test...")
        await test_channel_scrape()
        await test_validation()
    elif choice == "4":
        print("Exiting.")
        return
    else:
        print("Invalid choice.")
        await main()


if __name__ == "__main__":
    bt.logging.set_trace(True)
    bt.logging.info("Starting Final YouTube Channel Transcript Scraper tests...")
    asyncio.run(main())
