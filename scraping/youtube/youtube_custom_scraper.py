import asyncio
import hashlib
import traceback
import random
import re
import bittensor as bt
from typing import List, Dict, Any, Optional, Tuple
import datetime as dt
import os
import json
from xml.etree.ElementTree import ParseError
from youtube_transcript_api.proxies import WebshareProxyConfig, GenericProxyConfig

from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
import httpx
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult, HFValidationResult
from scraping.youtube.model import YouTubeContent
import isodate
from dotenv import load_dotenv
import logging

# Try to import googleapiclient, but don't fail if it's not available
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLEAPI_AVAILABLE = True
except ImportError:
    bt.logging.warning("googleapiclient not available, will use httpx fallback")
    GOOGLEAPI_AVAILABLE = False
    # Create dummy classes to prevent errors
    class HttpError(Exception):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.resp = type('MockResp', (), {'status': 500})()

load_dotenv(override=True)

bt.logging.set_trace(True)
logging.getLogger("httpx").setLevel(logging.WARNING)  # Keep this so httpx does not log api key

_KEY_RE = re.compile(r"(?:key|apiKey)=[^&\s]+", re.I)


class YouTubeTranscriptScraper(Scraper):
    """
    Enhanced YouTube scraper that combines:
    1. Official YouTube Data API for reliable metadata (with googleapiclient fallback to httpx)
    2. youtube-transcript-api for transcript extraction with flexible proxy support

    This provides better quality metadata and more reliable transcript extraction.
    """

    # Maximum number of transcripts to fetch in a single request
    MAX_TRANSCRIPTS_PER_REQUEST = 5

    # Maximum number of validation attempts
    MAX_VALIDATION_ATTEMPTS = 2

    # Default chunk size for transcript compression (in characters)
    DEFAULT_CHUNK_SIZE = 3000

    def __init__(self):
        """Initialize the Enhanced YouTube Transcript Scraper."""
        # Get API key from environment variables
        self.api_key = os.getenv("YOUTUBE_API_KEY")
        if not self.api_key:
            bt.logging.warning("YOUTUBE_API_KEY not found in environment variables")

        # Track rate limits to avoid API throttling
        self.last_request_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=10)
        self.request_interval = dt.timedelta(seconds=1)  # Minimum time between requests

        # Initialize YouTube API client (googleapiclient)
        self.youtube = None
        self.use_googleapi = False

        if GOOGLEAPI_AVAILABLE and self.api_key:
            try:
                self.youtube = build("youtube", "v3", developerKey=self.api_key)
                self.use_googleapi = True
                bt.logging.info("YouTube API client (googleapiclient) initialized successfully")
            except Exception as e:
                bt.logging.error(f"Failed to initialize YouTube API client (googleapiclient): {str(e)}")
                bt.logging.info("Will fall back to httpx for API calls")

        # Initialize httpx fallback settings
        self._yt_base = "https://www.googleapis.com/youtube/v3"
        self._http_timeout = httpx.Timeout(10.0)

        if not self.use_googleapi:
            bt.logging.info("Using httpx for YouTube API calls")

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """
        Scrapes YouTube transcripts according to the scrape config.

        Args:
            scrape_config: Configuration specifying what to scrape.

        Returns:
            List of DataEntity objects containing the transcripts.
        """
        bt.logging.info(f"Starting YouTube transcript scrape with config: {scrape_config}")

        # Extract channel or video IDs from the labels
        video_ids = []
        channel_ids = []

        if scrape_config.labels:
            for label in scrape_config.labels:
                # Handle YouTube video IDs - BOTH old and new formats
                if (label.value.startswith('#youtube_v_') or
                        label.value.startswith('#ytc_v_')):
                    if label.value.startswith('#youtube_v_'):
                        video_id = label.value.replace('#youtube_v_', '')
                    else:
                        video_id = label.value.replace('#ytc_v_', '')
                    video_ids.append(video_id)

                # Handle YouTube channel IDs - BOTH old and new formats
                elif (label.value.startswith('#youtube_c_') or
                      label.value.startswith('#ytc_c_')):
                    if label.value.startswith('#youtube_c_'):
                        channel_id = label.value.replace('#youtube_c_', '')
                    else:
                        channel_id = label.value.replace('#ytc_c_', '')
                    channel_ids.append(channel_id)

        # Limit the number of videos to scrape
        max_entities = scrape_config.entity_limit or 10

        # If we have specific video IDs, prioritize those
        if video_ids:
            return await self._scrape_video_ids(video_ids[:max_entities], scrape_config.date_range)

        # Otherwise, try to get videos from the specified channels
        elif channel_ids:
            return await self._scrape_channels(
                channel_ids,
                max_entities,
                scrape_config.date_range
            )
        else:
            # If no specific videos or channels are specified, return empty list
            bt.logging.warning("No video or channel IDs specified in scrape config")
            return []

    async def _scrape_video_ids(self, video_ids: List[str], date_range: DateRange) -> List[DataEntity]:
        """
        Scrape transcripts for specific video IDs.

        Args:
            video_ids: List of YouTube video IDs.
            date_range: Date range for filtering.

        Returns:
            List of DataEntity objects.
        """
        results = []

        for video_id in video_ids:
            # Respect rate limiting
            await self._wait_for_rate_limit()

            try:
                # Get video metadata from the YouTube API
                video_metadata = await self._get_video_metadata_from_api(video_id)

                if not video_metadata:
                    bt.logging.warning(f"No metadata found for video ID: {video_id}")
                    continue

                # Parse the upload date
                upload_date = dt.datetime.fromisoformat(
                    video_metadata.get('publishedAt', '').replace('Z', '+00:00')
                )

                # Skip if the video is outside the date range
                if not self._is_within_date_range(upload_date, date_range):
                    bt.logging.trace(f"Video {video_id} outside date range, skipping")
                    continue

                # Try to get the transcript
                try:
                    transcript_data = await self._get_transcript(video_id)

                    if not transcript_data:
                        bt.logging.warning(f"No transcript found for video ID: {video_id}")
                        continue

                    # Create the content object
                    content = YouTubeContent(
                        video_id=video_id,
                        title=video_metadata.get('title', ''),
                        channel_id=video_metadata.get('channelId', ''),
                        channel_name=video_metadata.get('channelTitle', ''),
                        upload_date=upload_date,
                        transcript=transcript_data,
                        url=f"https://www.youtube.com/watch?v={video_id}",
                        language=transcript_data[0].get('language', 'en') if transcript_data else 'en',
                        duration_seconds=video_metadata.get('duration_seconds', 0)
                    )

                    # Compress the transcript to reduce storage size
                    content = self._compress_transcript(content)

                    # Convert to DataEntity
                    entity = YouTubeContent.to_data_entity(content)
                    results.append(entity)

                except (TranscriptsDisabled, NoTranscriptFound) as e:
                    bt.logging.warning(f"No transcript available for video {video_id}: {str(e)}")
                    continue

            except Exception as e:
                bt.logging.error(f"Error scraping transcript for video {video_id}: {str(e)}")
                bt.logging.error(traceback.format_exc())
                continue

        bt.logging.info(f"Scraped {len(results)} YouTube transcripts from specific video IDs")
        return results

    async def _scrape_channels(self, channel_ids: List[str], max_entities: int, date_range: DateRange) -> List[DataEntity]:
        """
        Scrape transcripts from specified YouTube channels.

        Args:
            channel_ids: List of YouTube channel IDs.
            max_entities: Maximum number of videos to scrape.
            date_range: Date range for filtering.

        Returns:
            List of DataEntity objects.
        """
        results = []
        total_videos = 0

        for channel_id in channel_ids:
            if total_videos >= max_entities:
                break

            try:
                # Get videos from the channel
                channel_videos = await self._get_channel_videos(channel_id, date_range, max_entities - total_videos)

                if not channel_videos:
                    bt.logging.warning(f"No videos found for channel ID: {channel_id}")
                    continue

                # Process each video to create DataEntity objects
                for video_info in channel_videos:
                    try:
                        video_id = video_info["id"]

                        # Get the transcript
                        transcript_data = await self._get_transcript(video_id)
                        if not transcript_data:
                            bt.logging.warning(f"Failed to get transcript for video {video_id}")
                            continue

                        # Parse upload date
                        upload_date = dt.datetime.fromisoformat(
                            video_info.get("publishedAt", "").replace('Z', '+00:00')
                        )

                        # Create YouTubeContent object
                        content = YouTubeContent(
                            video_id=video_id,
                            title=video_info.get("title", ""),
                            channel_id=video_info.get("channelId", ""),
                            channel_name=video_info.get("channelTitle", ""),
                            upload_date=upload_date,
                            transcript=transcript_data,
                            url=f"https://www.youtube.com/watch?v={video_id}",
                            language=transcript_data[0].get("language", "en") if transcript_data else "en",
                            duration_seconds=0  # You could add this if you get video details
                        )

                        # Compress the transcript (optional)
                        content = self._compress_transcript(content)

                        # Convert to DataEntity
                        entity = YouTubeContent.to_data_entity(content)
                        results.append(entity)
                        total_videos += 1

                        bt.logging.info(f"Created DataEntity for video {video_id}")

                        if total_videos >= max_entities:
                            break

                    except Exception as e:
                        bt.logging.error(f"Error processing video {video_info.get('id')}: {str(e)}")
                        bt.logging.error(traceback.format_exc())
                        continue

            except Exception as e:
                bt.logging.error(f"Error scraping channel {channel_id}: {str(e)}")
                continue

        bt.logging.info(f"Created {len(results)} DataEntity objects from channel scrape")
        return results

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """
        Validate the correctness of a list of YouTube transcript DataEntities.

        Args:
            entities: List of DataEntity objects to validate.

        Returns:
            List of ValidationResult objects.
        """
        if not entities:
            return []

        results = []

        for entity in entities:
            # Extract the YouTube content from the entity
            try:
                content_to_validate = YouTubeContent.from_data_entity(entity)
            except Exception as e:
                bt.logging.error(f"Failed to decode YouTubeContent from entity: {str(e)}")
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Failed to decode entity",
                        content_size_bytes_validated=entity.content_size_bytes
                    )
                )
                continue

            # Validate the content by checking metadata and transcript
            attempt = 0
            while attempt < self.MAX_VALIDATION_ATTEMPTS:
                try:
                    # Respect rate limiting
                    await self._wait_for_rate_limit()

                    # Verify metadata from the API
                    video_metadata = await self._get_video_metadata_from_api(content_to_validate.video_id)

                    if not video_metadata:
                        results.append(
                            ValidationResult(
                                is_valid=False,
                                reason="Video not found or API error",
                                content_size_bytes_validated=entity.content_size_bytes
                            )
                        )
                        break

                    # Verify basic metadata (title, channel)
                    metadata_valid = self._verify_metadata(video_metadata, content_to_validate)

                    if not metadata_valid:
                        results.append(
                            ValidationResult(
                                is_valid=False,
                                reason="Metadata does not match",
                                content_size_bytes_validated=entity.content_size_bytes
                            )
                        )
                        break

                    # Verify transcript
                    try:
                        transcript_data = await self._get_transcript(content_to_validate.video_id)
                        transcript_valid = self._verify_transcript(transcript_data, content_to_validate.transcript)

                        if transcript_valid:
                            results.append(
                                ValidationResult(
                                    is_valid=True,
                                    reason="Video validated successfully",
                                    content_size_bytes_validated=entity.content_size_bytes
                                )
                            )
                        else:
                            results.append(
                                ValidationResult(
                                    is_valid=False,
                                    reason="Transcript content does not match",
                                    content_size_bytes_validated=entity.content_size_bytes
                                )
                            )
                        break

                    except (TranscriptsDisabled, NoTranscriptFound):
                        # If content has empty transcript, this is valid
                        if not content_to_validate.transcript:
                            results.append(
                                ValidationResult(
                                    is_valid=True,
                                    reason="Correctly identified video without transcript",
                                    content_size_bytes_validated=entity.content_size_bytes
                                )
                            )
                        else:
                            results.append(
                                ValidationResult(
                                    is_valid=False,
                                    reason="Transcript no longer available",
                                    content_size_bytes_validated=entity.content_size_bytes
                                )
                            )
                        break

                except Exception as e:
                    # Only retry on temporary errors
                    if "429" in str(e) or "timeout" in str(e).lower():
                        attempt += 1
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        results.append(
                            ValidationResult(
                                is_valid=False,
                                reason=f"Validation error: {str(e)}",
                                content_size_bytes_validated=entity.content_size_bytes
                            )
                        )
                        break

            # If we exhausted all attempts
            if attempt == self.MAX_VALIDATION_ATTEMPTS:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Validation failed after multiple attempts",
                        content_size_bytes_validated=entity.content_size_bytes
                    )
                )

        return results

    async def validate_hf(self, entities) -> HFValidationResult:
        """
        Validate the correctness of a list of HF-stored YouTube transcript entries.

        Args:
            entities: List of dictionary records from HuggingFace dataset.

        Returns:
            HFValidationResult with validation status.
        """
        if not entities:
            return HFValidationResult(
                is_valid=True,
                validation_percentage=100,
                reason="No entities to validate"
            )

        validation_results = []

        for entity in entities:
            # Extract the video ID from the URL
            video_id = self._extract_video_id(entity.get('url', ''))
            if not video_id:
                validation_results.append(False)
                continue

            try:
                # Respect rate limiting
                await self._wait_for_rate_limit()

                # Verify metadata
                video_metadata = await self._get_video_metadata_from_api(video_id)
                if not video_metadata:
                    validation_results.append(False)
                    continue

                # Check if the text field contains valid content
                if entity.get('text'):
                    # Verify transcript
                    try:
                        transcript_data = await self._get_transcript(video_id)
                        if transcript_data:
                            # Extract full text from transcript
                            actual_text = " ".join([item.get('text', '') for item in transcript_data])

                            # Compare with stored text (simplified comparison)
                            similarity = self._calculate_text_similarity(actual_text, entity.get('text', ''))
                            validation_results.append(similarity >= 0.7)  # 70% similarity threshold
                        else:
                            validation_results.append(False)
                    except (TranscriptsDisabled, NoTranscriptFound):
                        # If entity indicates no transcript, this is valid
                        if entity.get('status') == 'no_transcript':
                            validation_results.append(True)
                        else:
                            validation_results.append(False)
                else:
                    validation_results.append(False)

            except Exception as e:
                bt.logging.error(f"Error validating video {video_id}: {str(e)}")
                validation_results.append(False)

        # Calculate the validation percentage
        valid_count = sum(1 for result in validation_results if result)
        validation_percentage = (valid_count / len(validation_results)) * 100 if validation_results else 0

        return HFValidationResult(
            is_valid=validation_percentage >= 60,  # 60% threshold for valid dataset
            validation_percentage=validation_percentage,
            reason=f"Validation Percentage = {validation_percentage}"
        )

    def _compress_transcript(self, content: YouTubeContent) -> YouTubeContent:
        """
        Compress the transcript to reduce storage size.

        Args:
            content: YouTubeContent object containing transcript.

        Returns:
            YouTubeContent with compressed transcript.
        """
        if not content.transcript:
            return content

        # Convert transcript to a more compact format
        full_text = " ".join([item.get('text', '') for item in content.transcript])

        # Remove redundant whitespace
        full_text = re.sub(r'\s+', ' ', full_text).strip()

        # Create chunks to maintain some time information
        chunks = []
        current_pos = 0
        chunk_size = self.DEFAULT_CHUNK_SIZE

        while current_pos < len(full_text):
            # Find a good break point (end of sentence) within the chunk size
            end_pos = min(current_pos + chunk_size, len(full_text))

            # If we're not at the end, try to find a sentence break
            if end_pos < len(full_text):
                # Look for sentence endings (.!?) followed by space or end of text
                sentence_break = max(
                    full_text.rfind('. ', current_pos, end_pos),
                    full_text.rfind('! ', current_pos, end_pos),
                    full_text.rfind('? ', current_pos, end_pos)
                )

                if sentence_break > current_pos:
                    end_pos = sentence_break + 2  # Include the punctuation and space

            # Add the chunk
            chunks.append(full_text[current_pos:end_pos].strip())
            current_pos = end_pos

        # Create a compressed transcript with fewer entries
        compressed_transcript = []
        total_duration = content.duration_seconds or sum(item.get('duration', 0) for item in content.transcript)

        if chunks and total_duration > 0:
            chunk_duration = total_duration / len(chunks)

            for i, chunk in enumerate(chunks):
                compressed_transcript.append({
                    'text': chunk,
                    'start': i * chunk_duration,
                    'duration': chunk_duration
                })

        # Return the content with compressed transcript
        return YouTubeContent(
            video_id=content.video_id,
            title=content.title,
            channel_id=content.channel_id,
            channel_name=content.channel_name,
            upload_date=content.upload_date,
            transcript=compressed_transcript,
            url=content.url,
            language=content.language,
            duration_seconds=content.duration_seconds
        )

    def _verify_metadata(self, api_metadata: Dict[str, Any], content_to_validate: YouTubeContent) -> bool:
        """
        Verify if the metadata from the API matches the stored metadata.

        Args:
            api_metadata: The metadata from the YouTube API.
            content_to_validate: The YouTubeContent to validate.

        Returns:
            True if the metadata matches sufficiently, False otherwise.
        """
        # Check title (allow for minor differences)
        if not self._texts_are_similar(api_metadata.get('title', ''), content_to_validate.title, threshold=0.8):
            return False

        # Check channel ID (must match exactly)
        if api_metadata.get('channelId', '') != content_to_validate.channel_id:
            return False

        # Check channel name (allow for minor differences)
        if not self._texts_are_similar(api_metadata.get('channelTitle', ''), content_to_validate.channel_name,
                                       threshold=0.8):
            return False

        return True

    def _verify_transcript(self, api_transcript: List[Dict[str, Any]], stored_transcript: List[Dict[str, Any]]) -> bool:
        """
        Verify if the transcript from the API matches the stored transcript.

        Args:
            api_transcript: The transcript from the YouTube API.
            stored_transcript: The transcript to validate.

        Returns:
            True if the transcripts match sufficiently, False otherwise.
        """
        # Extract the full text from the transcripts
        api_full_text = " ".join([item.get('text', '') for item in api_transcript])
        stored_full_text = " ".join([item.get('text', '') for item in stored_transcript])

        # Clean up whitespace
        api_full_text = re.sub(r'\s+', ' ', api_full_text).strip()
        stored_full_text = re.sub(r'\s+', ' ', stored_full_text).strip()

        # Compare the texts (allow for compression differences)
        return self._texts_are_similar(api_full_text, stored_full_text, threshold=0.7)

    def _texts_are_similar(self, text1: str, text2: str, threshold: float = 0.8) -> bool:
        """
        Check if two texts are similar enough using a simplified approach.

        Args:
            text1: First text.
            text2: Second text.
            threshold: Similarity threshold (0-1).

        Returns:
            True if the texts are similar enough, False otherwise.
        """
        if not text1 or not text2:
            return False

        # Simple approach: check if enough words from one text appear in the other
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())

        if not words1 or not words2:
            return False

        # Calculate overlap ratio
        overlap = len(words1.intersection(words2))
        similarity = overlap / max(len(words1), len(words2))

        return similarity >= threshold

    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate similarity between two texts using a word-based approach.

        Args:
            text1: First text.
            text2: Second text.

        Returns:
            Similarity score between 0 and 1.
        """
        if not text1 or not text2:
            return 0.0

        # Normalize and split into words
        words1 = set(re.sub(r'[^\w\s]', '', text1.lower()).split())
        words2 = set(re.sub(r'[^\w\s]', '', text2.lower()).split())

        if not words1 or not words2:
            return 0.0

        # Calculate Jaccard similarity
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        return intersection / union if union > 0 else 0.0

    def _is_within_date_range(self, date: dt.datetime, date_range: DateRange) -> bool:
        """
        Check if a date is within the specified date range.

        Args:
            date: The date to check.
            date_range: The date range.

        Returns:
            True if the date is within the range, False otherwise.
        """
        return date_range.contains(date)

    async def _wait_for_rate_limit(self):
        """Wait if necessary to respect rate limiting."""
        now = dt.datetime.now(dt.timezone.utc)
        time_since_last_request = now - self.last_request_time

        if time_since_last_request < self.request_interval:
            wait_time = (self.request_interval - time_since_last_request).total_seconds()
            await asyncio.sleep(wait_time)

        self.last_request_time = dt.datetime.now(dt.timezone.utc)

    async def _get_video_metadata_from_api(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a YouTube video using the official API.
        First tries googleapiclient, then falls back to httpx.

        Args:
            video_id: The YouTube video ID.

        Returns:
            Dictionary containing video metadata or None if not found.
        """
        if not self.api_key:
            bt.logging.warning("YouTube API key missing")
            return self._get_fallback_video_metadata(video_id)

        # Try googleapiclient first
        if self.use_googleapi and self.youtube:
            try:
                response = self.youtube.videos().list(
                    id=video_id,
                    part="snippet,contentDetails,statistics"
                ).execute()

                if not response.get("items"):
                    bt.logging.warning(f"Video {video_id} not found or private")
                    return None

                item = response["items"][0]
                snippet = item.get("snippet", {})
                content_details = item.get("contentDetails", {})

                # Convert ISO 8601 duration to seconds
                duration_str = content_details.get("duration", "PT0S")
                duration_seconds = int(isodate.parse_duration(duration_str).total_seconds())

                return {
                    "title": snippet.get("title", ""),
                    "channelId": snippet.get("channelId", ""),
                    "channelTitle": snippet.get("channelTitle", ""),
                    "publishedAt": snippet.get("publishedAt", ""),
                    "description": snippet.get("description", ""),
                    "duration_seconds": duration_seconds
                }

            except HttpError as e:
                bt.logging.error(
                    "YouTube API error (%s) for video %s: %s",
                    e.resp.status, video_id, self._sanitize_http_error(e)
                )
                # Fall through to httpx fallback
            except Exception as e:
                bt.logging.error(f"Unexpected error with googleapiclient: {str(e)}")
                # Fall through to httpx fallback

        # Fallback to httpx
        resp = await self._yt_get(
            "videos",
            {"id": video_id, "part": "snippet,contentDetails,statistics"},
        )
        if not resp or not resp.get("items"):
            bt.logging.warning(f"Video {video_id} not found or private")
            return None

        item = resp["items"][0]
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})

        duration_str = content_details.get("duration", "PT0S")
        duration_seconds = int(isodate.parse_duration(duration_str).total_seconds())

        return {
            "title": snippet.get("title", ""),
            "channelId": snippet.get("channelId", ""),
            "channelTitle": snippet.get("channelTitle", ""),
            "publishedAt": snippet.get("publishedAt", ""),
            "description": snippet.get("description", ""),
            "duration_seconds": duration_seconds,
        }

    async def _yt_get(self, path: str, params: dict) -> dict | None:
        """
        One REST call to YouTube Data API v3 using httpx. Returns parsed JSON or None.
        """
        if not self.api_key:
            bt.logging.warning("YOUTUBE_API_KEY missing - cannot call YouTube Data API")
            return None

        await self._wait_for_rate_limit()

        qs = params.copy()
        qs["key"] = self.api_key
        url = f"{self._yt_base}/{path}"

        try:
            async with httpx.AsyncClient(timeout=self._http_timeout) as client:
                r: httpx.Response = await client.get(url, params=qs)
                r.raise_for_status()

                bt.logging.debug(f"HTTP {r.status_code} {self._sanitize(r)}")
                return r.json()

        except httpx.HTTPStatusError as e:
            bt.logging.error(f"YouTube API HTTP {e.response.status_code}: {self._sanitize(e)}")
        except Exception as e:
            bt.logging.error(f"YouTube API request failed: {self._sanitize(e)}")
        return None

    def _get_fallback_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Generate fallback metadata when the API is unavailable.
        This is used only when the API key is invalid or quota is exceeded.

        Args:
            video_id: The YouTube video ID.

        Returns:
            Dictionary with basic metadata.
        """
        bt.logging.warning(f"Using fallback metadata generation for video {video_id}")

        # Create a deterministic but random-looking date based on the video ID
        hash_value = int(hashlib.md5(video_id.encode()).hexdigest(), 16)
        days_ago = hash_value % 365  # 0-364 days ago
        upload_date = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_ago)

        return {
            "title": f"Video {video_id}",
            "channelId": f"UC{video_id[:10]}",
            "channelTitle": f"Channel {video_id[:5]}",
            "publishedAt": upload_date.isoformat(),
            "description": f"Description for video {video_id}",
            "duration_seconds": hash_value % 600  # 0-599 seconds
        }

    def _get_proxy_config(self):
        """
        Get proxy configuration based on environment variables.
        Supports WebShare, generic HTTP proxies, or no proxy.

        Returns:
            Proxy configuration object or None
        """
        try:
            # Check for generic HTTP proxy first (YTT_PROXY_*)
            host = os.getenv("YTT_PROXY_HOST")
            port = os.getenv("YTT_PROXY_PORT")

            if host and port:
                user = os.getenv("YTT_PROXY_USERNAME", "")
                pwd = os.getenv("YTT_PROXY_PASSWORD", "")
                cred = f"{user}:{pwd}@" if user and pwd else ""
                http_url = f"http://{cred}{host}:{port}"
                https_url = f"https://{cred}{host}:{port}"
                return GenericProxyConfig(http_url=http_url, https_url=https_url)

            # Check for WebShare proxy (WEB_SHARE_PROXY_*)
            elif os.getenv("WEB_SHARE_PROXY_USERNAME") and os.getenv("WEB_SHARE_PROXY_PASSWORD"):
                return WebshareProxyConfig(
                    proxy_username=os.getenv("WEB_SHARE_PROXY_USERNAME"),
                    proxy_password=os.getenv("WEB_SHARE_PROXY_PASSWORD"),
                )

            # No proxy configured
            return None

        except Exception as e:
            bt.logging.error(f"Error in proxy configuration: {str(e)}")
            return None

    async def _get_transcript(
        self,
        video_id: str,
        max_retries: int = 3
    ):
        """
        Fetch a raw transcript list for a single YouTube video,
        with up to `max_retries` attempts if there are transient errors.
        Wait 5 seconds between each retry attempt.

        If a transcript is disabled or not found, we short-circuit immediately.
        If there's an XML parse error (like 'no element found'), we log a simpler
        warning without printing the full traceback.
        """
        proxy_config = self._get_proxy_config()

        for attempt in range(max_retries):
            try:
                ytt_api = YouTubeTranscriptApi(proxy_config=proxy_config)
                return ytt_api.fetch(video_id).to_raw_data()

            except (TranscriptsDisabled, NoTranscriptFound) as e:
                # Known "no transcript" errors → no retries needed
                bt.logging.warning(
                    f"Transcript fetch failed (no transcript) for {video_id}: {e!s}"
                )
                return None

            except ParseError as e:
                # Typically "no element found: line 1, column 0" means
                # an empty or invalid response from YouTube.
                bt.logging.warning(
                    f"[Attempt {attempt + 1}/{max_retries}] Transcript parse error "
                    f"for video {video_id}: {e}"
                )
                # Optionally: no big traceback here, just a simpler log message.
                if attempt < max_retries - 1:
                    bt.logging.info("Retrying in 5s..")
                    await asyncio.sleep(5)
                else:
                    bt.logging.warning(
                        f"Giving up after {max_retries} attempts for {video_id}."
                    )
                    return None

            except Exception as e:
                # General fallback for other errors (network, SSL, etc.)
                bt.logging.warning(
                    f"[Attempt {attempt + 1}/{max_retries}] Transcript fetch failed "
                    f"for {video_id}: {e!s}"
                )

                bt.logging.warning(traceback.format_exc())

                if attempt < max_retries - 1:
                    bt.logging.info("Retrying in 5s...")
                    await asyncio.sleep(5)
                else:
                    bt.logging.warning(
                        f"Giving up after {max_retries} attempts for {video_id}."
                    )
                    return None

    def _extract_video_id(self, url: str) -> Optional[str]:
        """
        Extract YouTube video ID from a URL.

        Args:
            url: YouTube video URL

        Returns:
            Video ID if found, None otherwise
        """
        # YouTube URL patterns
        patterns = [
            r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',  # Standard URLs
            r'(?:embed|v|vi|youtu\.be\/)([0-9A-Za-z_-]{11}).*',  # Embed/short URLs
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)

        return None

    async def _get_channel_videos(
        self,
        channel_id: str,
        date_range: DateRange,
        max_results: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get up to `max_results` videos with transcripts from a channel, limited to
        `date_range`. Uses googleapiclient first, then falls back to httpx.
        """
        # Try googleapiclient first
        if self.use_googleapi and self.youtube:
            try:
                return await self._get_channel_videos_googleapi(channel_id, date_range, max_results)
            except Exception as e:
                bt.logging.error(f"Error with googleapiclient channel fetch: {str(e)}")
                bt.logging.info("Falling back to httpx for channel videos")

        # Fallback to httpx
        return await self._get_channel_videos_httpx(channel_id, date_range, max_results)

    async def _get_channel_videos_googleapi(
        self,
        channel_id: str,
        date_range: DateRange,
        max_results: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get channel videos using googleapiclient.
        """
        # Ensure date_range has timezone info
        start_date = date_range.start
        end_date = date_range.end

        # Add timezone if missing
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=dt.timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=dt.timezone.utc)

        # Get channel uploads playlist
        channels_response = self.youtube.channels().list(
            id=channel_id,
            part="contentDetails,snippet"
        ).execute()

        if not channels_response.get("items"):
            bt.logging.warning(f"Channel {channel_id} not found")
            return []

        channel_info = channels_response["items"][0]
        channel_name = channel_info["snippet"]["title"]
        uploads_playlist_id = channel_info["contentDetails"]["relatedPlaylists"]["uploads"]

        bt.logging.info(f"Found channel: {channel_name}")

        # Get videos from the uploads playlist
        videos = []
        page_token = None
        api_calls = 0
        max_api_calls = 5  # Limit API calls to prevent quota issues

        while len(videos) < max_results and api_calls < max_api_calls:
            api_calls += 1

            try:
                playlist_response = self.youtube.playlistItems().list(
                    playlistId=uploads_playlist_id,
                    part="snippet,contentDetails",
                    maxResults=50,  # Maximum allowed by API
                    pageToken=page_token
                ).execute()

                if not playlist_response.get("items"):
                    bt.logging.info("No more items in playlist")
                    break

                # Process each video
                for item in playlist_response.get("items", []):
                    video_id = item["contentDetails"]["videoId"]

                    # Parse published date properly with timezone handling
                    try:
                        published_at_str = item["snippet"]["publishedAt"]

                        # Ensure the string has timezone info
                        if published_at_str.endswith('Z'):
                            published_at_str = published_at_str.replace('Z', '+00:00')

                        # Parse with timezone
                        published_at = dt.datetime.fromisoformat(published_at_str)

                        # Ensure timezone is UTC
                        if published_at.tzinfo is None:
                            published_at = published_at.replace(tzinfo=dt.timezone.utc)

                    except Exception as e:
                        bt.logging.error(f"Error parsing date for video {video_id}: {str(e)}")
                        continue

                    # Check if the video is within the date range using timezone-aware comparison
                    in_range = start_date <= published_at <= end_date

                    bt.logging.debug(
                        f"Video {video_id} published at {published_at.isoformat()}, "
                        f"within range {start_date.isoformat()} to {end_date.isoformat()}: {in_range}"
                    )

                    if not in_range:
                        bt.logging.debug(f"Video {video_id} outside date range, skipping")
                        continue

                    # Check for transcript availability
                    try:
                        transcript = await self._get_transcript(video_id)
                        if transcript:
                            videos.append({
                                "id": video_id,
                                "title": item["snippet"]["title"],
                                "publishedAt": published_at.isoformat(),
                                "channelId": channel_id,
                                "channelTitle": channel_name
                            })

                            if len(videos) >= max_results:
                                bt.logging.info(f"Reached maximum videos limit ({max_results})")
                                break
                    except (TranscriptsDisabled, NoTranscriptFound):
                        bt.logging.debug(f"No transcript for video {video_id}, skipping")
                        continue
                    except Exception as e:
                        bt.logging.error(f"Error checking transcript for video {video_id}: {str(e)}")
                        continue

                # Get next page token
                page_token = playlist_response.get("nextPageToken")
                if not page_token:
                    break

            except Exception as e:
                bt.logging.error(f"Error processing playlist page: {str(e)}")
                bt.logging.error(traceback.format_exc())
                break

        bt.logging.info(f"Found {len(videos)} videos with transcripts within the date range")
        return videos

    async def _get_channel_videos_httpx(
        self,
        channel_id: str,
        date_range: DateRange,
        max_results: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get channel videos using httpx fallback.
        """
        chan_resp = await self._yt_get(
            "channels",
            {"id": channel_id, "part": "contentDetails,snippet"},
        )
        if not chan_resp or not chan_resp.get("items"):
            bt.logging.warning(f"Channel {channel_id} not found")
            return []

        channel_info = chan_resp["items"][0]
        channel_name = channel_info["snippet"]["title"]
        uploads_playlist_id = channel_info["contentDetails"]["relatedPlaylists"]["uploads"]

        bt.logging.info(f"Found channel: {channel_name}")

        collected: list[dict] = []
        page_token: str | None = None
        api_calls = 0
        MAX_API_CALLS = 5

        while len(collected) < max_results and api_calls < MAX_API_CALLS:
            api_calls += 1
            pl_resp = await self._yt_get(
                "playlistItems",
                {
                    "playlistId": uploads_playlist_id,
                    "part": "snippet,contentDetails",
                    "maxResults": 50,
                    "pageToken": page_token or "",
                },
            )
            if not pl_resp or not pl_resp.get("items"):
                break

            for item in pl_resp["items"]:
                video_id = item["contentDetails"]["videoId"]

                # Parse publish date
                pub_str = item["snippet"]["publishedAt"].replace("Z", "+00:00")
                try:
                    published = dt.datetime.fromisoformat(pub_str)
                except Exception as e:
                    bt.logging.error(f"Bad date for {video_id}: {e}")
                    continue

                if not date_range.contains(published):
                    continue

                # Proxy-aware transcript check
                transcript = await self._get_transcript(video_id)
                if not transcript:
                    continue

                collected.append(
                    {
                        "id": video_id,
                        "title": item["snippet"]["title"],
                        "publishedAt": published.isoformat(),
                        "channelId": channel_id,
                        "channelTitle": channel_name,
                    }
                )

                if len(collected) >= max_results:
                    break

            page_token = pl_resp.get("nextPageToken")
            if not page_token:
                break

        bt.logging.info(f"Collected {len(collected)} videos from channel {channel_id}")
        return collected

    def _sanitize(self, obj: object) -> str:
        """
        Return *obj* as str with any YouTube API key stripped from query strings.
        Accepts str, httpx.Request/Response, exceptions, etc.
        """
        if isinstance(obj, httpx.Request):
            target = str(obj.url)
        elif isinstance(obj, httpx.Response):
            target = str(obj.request.url)
        else:
            target = str(obj)

        return _KEY_RE.sub("key=REDACTED", target)

    def _sanitize_http_error(self, e: HttpError | Exception) -> str:
        """
        Return a safe string of any exception, with YouTube API keys removed.
        """
        return _KEY_RE.sub("key=REDACTED", str(e))


# Test utility methods

async def test_scrape_video():
    """Test function for scraping a specific video."""
    # Create an instance of the scraper
    scraper = YouTubeTranscriptScraper()

    # Test video ID (Rick Astley - Never Gonna Give You Up)
    video_id = "dQw4w9WgXcQ"

    # Create a date range that includes all videos
    date_range = DateRange(
        start=dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
        end=dt.datetime.now(dt.timezone.utc)
    )

    # Scrape the video
    entities = await scraper._scrape_video_ids([video_id], date_range)

    # Print the results
    bt.logging.info(f"Scraped {len(entities)} entities")
    for entity in entities:
        content = YouTubeContent.from_data_entity(entity)
        bt.logging.info(f"Video: {content.title}")
        bt.logging.info(f"Channel: {content.channel_name}")
        bt.logging.info(f"Transcript length: {len(content.transcript)}")
        if content.transcript:
            bt.logging.info(f"First transcript chunk: {content.transcript[0]['text'][:100]}...")

    return entities


async def test_scrape_channel():
    """Test function for scraping a channel."""
    # Create an instance of the scraper
    scraper = YouTubeTranscriptScraper()

    # Test channel ID (TED channel)
    channel_id = "UCAuUUnT6oDeKwE6v1NGQxug"

    # Create a date range that includes recent videos
    date_range = DateRange(
        start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=365),
        end=dt.datetime.now(dt.timezone.utc)
    )

    # Scrape the channel
    entities = await scraper._scrape_channels([channel_id], 3, date_range)

    # Print the results
    bt.logging.info(f"Scraped {len(entities)} entities from channel")
    for entity in entities:
        content = YouTubeContent.from_data_entity(entity)
        bt.logging.info(f"Video: {content.title}")
        bt.logging.info(f"Channel: {content.channel_name}")
        bt.logging.info(f"Transcript length: {len(content.transcript)}")

    return entities


async def test_validate_entity(entity: DataEntity):
    """Test function for validating a specific entity."""
    # Create an instance of the scraper
    scraper = YouTubeTranscriptScraper()

    # Validate the entity
    results = await scraper.validate([entity])

    # Print the results
    bt.logging.info(f"Validation results: {results}")

    return results


async def test_full_pipeline():
    """Test the full scraping and validation pipeline."""
    # 1. Scrape a specific video
    bt.logging.info("STEP 1: Scraping a specific video")
    entities = await test_scrape_video()

    if not entities:
        bt.logging.error("No entities scraped, can't continue with validation")
        return

    # 2. Validate the first entity
    bt.logging.info("\nSTEP 2: Validating the scraped entity")
    await test_validate_entity(entities[0])

    # 3. Test channel scraping
    bt.logging.info("\nSTEP 3: Scraping a channel")
    await test_scrape_channel()

    bt.logging.info("\nAll tests completed!")


async def test_scrape_specific_channel():
    """Test function for scraping a specific channel."""
    # Create an instance of the scraper
    scraper = YouTubeTranscriptScraper()

    # Get channel ID from user input
    channel_id = input("Enter YouTube channel ID: ")

    # Create a date range for the past year
    date_range = DateRange(
        start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=365),
        end=dt.datetime.now(dt.timezone.utc)
    )

    # Get number of videos to scrape
    try:
        max_videos = int(input("Enter maximum number of videos to scrape (1-10): "))
        max_videos = max(1, min(10, max_videos))  # Clamp between 1 and 10
    except ValueError:
        max_videos = 3  # Default value

    bt.logging.info(f"Scraping up to {max_videos} videos from channel {channel_id}")

    # Scrape the channel
    entities = await scraper._scrape_channels([channel_id], max_videos, date_range)

    # Print the results
    bt.logging.info(f"Scraped {len(entities)} entities from channel")
    for entity in entities:
        content = YouTubeContent.from_data_entity(entity)
        bt.logging.info(f"Video: {content.title}")
        bt.logging.info(f"Channel: {content.channel_name}")
        bt.logging.info(f"Transcript length: {len(content.transcript)}")
        if content.transcript:
            bt.logging.info(f"First transcript chunk: {content.transcript[0]['text'][:100]}...")

    return entities


async def test_scrape_specific_video():
    """Test function for scraping a specific video."""
    # Create an instance of the scraper
    scraper = YouTubeTranscriptScraper()

    # Get video ID from user input
    video_id = input("Enter YouTube video ID: ")

    # Create a date range that includes all videos
    date_range = DateRange(
        start=dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
        end=dt.datetime.now(dt.timezone.utc)
    )

    bt.logging.info(f"Scraping video {video_id}")

    # Scrape the video
    entities = await scraper._scrape_video_ids([video_id], date_range)

    # Print the results
    bt.logging.info(f"Scraped {len(entities)} entities")
    for entity in entities:
        content = YouTubeContent.from_data_entity(entity)
        bt.logging.info(f"Video: {content.title}")
        bt.logging.info(f"Channel: {content.channel_name}")
        bt.logging.info(f"Transcript length: {len(content.transcript)}")
        if content.transcript:
            bt.logging.info(f"Sample transcript: {content.transcript[0]['text'][:100]}...")
            bt.logging.info(f"Total transcript chunks: {len(content.transcript)}")

    return entities


# Menu-based test runner
async def main():
    print("\nEnhanced YouTube Transcript Scraper - Test Menu")
    print("=" * 50)
    print("1. Run full pipeline test (default video and channel)")
    print("2. Scrape a specific video (provide video ID)")
    print("3. Scrape videos from a specific channel (provide channel ID)")
    print("4. Exit")

    choice = input("\nEnter your choice (1-4): ")

    if choice == "1":
        await test_full_pipeline()
    elif choice == "2":
        await test_scrape_specific_video()
    elif choice == "3":
        await test_scrape_specific_channel()
    elif choice == "4":
        print("Exiting test program.")
        return
    else:
        print("Invalid choice. Please try again.")
        await main()


# Entry point for running the tests
if __name__ == "__main__":
    bt.logging.info("Starting YouTube scraper tests...")
    asyncio.run(main())