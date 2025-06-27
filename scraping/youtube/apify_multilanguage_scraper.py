import asyncio
import traceback
import re
import bittensor as bt
from typing import List, Dict, Any, Optional
import datetime as dt
import os

from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult, HFValidationResult
from scraping.youtube.model import YouTubeContent
from scraping.apify import ActorRunner, RunConfig, ActorRunError
from dotenv import load_dotenv

load_dotenv(override=True)

bt.logging.set_trace(True)


class YouTubeApifyMultilingualScraper(Scraper):
    """
    YouTube scraper using the new Apify actor (I4braoOCJE7dh3lUS).
    This actor provides complete video metadata and transcript extraction with translation capabilities.
    Updated to get only manual transcripts in the miner-specified language.
    """

    # New Apify actor ID for multilingual YouTube transcript scraper
    YOUTUBE_ENHANCED_ACTOR_ID = "I4braoOCJE7dh3lUS"

    # Maximum number of validation attempts
    MAX_VALIDATION_ATTEMPTS = 2

    # Default chunk size for transcript compression (in characters)
    DEFAULT_CHUNK_SIZE = 3000

    # Timeout for Apify actor runs
    APIFY_TIMEOUT_SECS = 180

    def __init__(self, runner: ActorRunner = None, preferred_language: str = "en"):
        """Initialize the Multilingual YouTube Transcript Scraper.

        Args:
            runner: ActorRunner instance
            preferred_language: Language code for manual transcripts (e.g., 'en', 'es', 'fr', 'ru')
        """
        # Initialize Apify runner
        self.runner = runner or ActorRunner()

        # Store the preferred language for manual transcripts
        self.preferred_language = preferred_language

        # Track rate limits to avoid API throttling
        self.last_request_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=2)
        self.request_interval = dt.timedelta(seconds=2)  # Minimum time between requests

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
        Scrape transcripts for specific video IDs using the multilingual Apify actor.

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
                # Get transcript and metadata using multilingual Apify actor
                video_data = await self._get_video_data_apify(video_id)

                if not video_data:
                    bt.logging.warning(f"No data found for video ID: {video_id}")
                    continue

                # Parse the upload date from Apify data
                upload_date_str = video_data.get('upload_date', '')
                if upload_date_str:
                    try:
                        # Handle the format from Apify: "2025-06-22 22:14:27+00:00"
                        upload_date = dt.datetime.fromisoformat(upload_date_str)
                        if upload_date.tzinfo is None:
                            upload_date = upload_date.replace(tzinfo=dt.timezone.utc)
                    except ValueError:
                        # Fallback to current time if parsing fails
                        upload_date = dt.datetime.now(dt.timezone.utc)
                        bt.logging.warning(f"Could not parse upload date '{upload_date_str}' for video {video_id}")
                else:
                    upload_date = dt.datetime.now(dt.timezone.utc)

                # Skip if the video is outside the date range
                if not self._is_within_date_range(upload_date, date_range):
                    bt.logging.trace(f"Video {video_id} outside date range, skipping")
                    continue

                # Extract transcript data
                transcript_data = video_data.get('transcript', [])
                if not transcript_data:
                    bt.logging.warning(f"No transcript found for video ID: {video_id}")
                    continue

                # Get the actual language code from video_data
                language = video_data.get('language', self.preferred_language)

                # Create the content object with language field
                content = YouTubeContent(
                    video_id=video_data.get('video_id', video_id),
                    title=video_data.get('title', ''),
                    channel_id=video_data.get('channel_id', ''),
                    channel_name=video_data.get('channel_name', ''),
                    upload_date=upload_date,
                    transcript=transcript_data,
                    url=video_data.get('url', f"https://www.youtube.com/watch?v={video_id}"),
                    duration_seconds=video_data.get('duration_seconds', 0),
                    language=language
                )

                # Compress the transcript to reduce storage size
                content = self._compress_transcript(content)

                # Convert to DataEntity
                entity = YouTubeContent.to_data_entity(content)
                results.append(entity)

            except Exception as e:
                bt.logging.error(f"Error scraping transcript for video {video_id}: {str(e)}")
                bt.logging.error(traceback.format_exc())
                continue

        bt.logging.info(f"Scraped {len(results)} YouTube transcripts from specific video IDs")
        return results

    async def _scrape_channels(self, channel_ids: List[str], max_entities: int, date_range: DateRange) -> List[
        DataEntity]:
        """
        Scrape transcripts from specified YouTube channels.
        Note: This would require getting channel video lists from somewhere else,
        as the current Apify actor only handles individual video URLs.

        Args:
            channel_ids: List of YouTube channel IDs.
            max_entities: Maximum number of videos to scrape.
            date_range: Date range for filtering.

        Returns:
            List of DataEntity objects.
        """
        bt.logging.warning(
            "Channel scraping not implemented with current Apify actor - it only handles individual video URLs")
        return []

    async def _get_video_data_apify(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Get complete video data (metadata + manual transcript only) for a YouTube video
        using the multilingual Apify actor.

        Args:
            video_id: YouTube video ID

        Returns:
            Dictionary containing video data and manual transcript or None if unavailable
        """
        try:
            # Construct the full YouTube URL - this is what the Apify actor expects
            video_url = self._construct_youtube_url(video_id)

            # Prepare the run input - ONLY request original language transcripts (manual)
            run_input = {
                "url": video_url,
                "preferredOrigLang": [self.preferred_language],  # Only get manual transcripts in this language
                "preferredLangs": []  # Don't get any translated transcripts
            }

            run_config = RunConfig(
                actor_id=self.YOUTUBE_ENHANCED_ACTOR_ID,
                debug_info=f"Manual transcript for {video_id} in {self.preferred_language} ({video_url})",
                max_data_entities=1,  # We only expect one video result
                timeout_secs=self.APIFY_TIMEOUT_SECS
            )

            bt.logging.info(f"Getting manual transcript for {video_id} in language {self.preferred_language}")

            # Run the actor
            dataset = await self.runner.run(run_config, run_input)

            if not dataset:
                bt.logging.warning(f"No data returned for video {video_id} (URL: {video_url})")
                return None

            bt.logging.debug(f"Multilingual Apify returned {len(dataset)} items for video {video_id}")

            # Process the video data - should be a single item
            for item in dataset:
                if isinstance(item, dict):
                    # Extract manual transcript data ONLY from original_transcripts
                    transcript_segments = []
                    language = self.preferred_language  # Use the language we requested

                    # ONLY get the manual transcript from original_transcripts
                    # original_transcripts is a dict like {'ru': [transcript_segments], 'en': [transcript_segments]}
                    if 'original_transcripts' in item and item['original_transcripts']:
                        original_transcripts = item['original_transcripts']
                        if isinstance(original_transcripts, dict):
                            # Try to get the preferred language first
                            if self.preferred_language in original_transcripts:
                                language = self.preferred_language
                                transcript_data = original_transcripts[self.preferred_language]
                                bt.logging.info(
                                    f"Found manual transcript in preferred language: {self.preferred_language}")
                            else:
                                # If preferred language not available, log available languages and skip
                                available_langs = list(original_transcripts.keys())
                                bt.logging.warning(
                                    f"Preferred language {self.preferred_language} not available for video {video_id}. "
                                    f"Available languages: {available_langs}. Skipping video."
                                )
                                return None
                        else:
                            bt.logging.warning(f"original_transcripts is not a dict for video {video_id}")
                            return None
                    else:
                        bt.logging.warning(f"No manual transcripts found for video {video_id}")
                        return None

                    # Process transcript segments if available
                    for segment in transcript_data:
                        if isinstance(segment, dict) and 'text' in segment and segment['text']:
                            try:
                                start_time = float(segment.get('start', 0))
                                end_time = float(segment.get('end', start_time))
                                duration = float(segment.get('duration', end_time - start_time))

                                processed_segment = {
                                    'start': start_time,
                                    'end': end_time,
                                    'duration': duration,
                                    'text': str(segment['text']).strip()
                                }
                                transcript_segments.append(processed_segment)
                            except (ValueError, TypeError) as e:
                                bt.logging.warning(
                                    f"Error parsing transcript segment for video {video_id}: {e}, segment: {segment}")
                                continue

                    if not transcript_segments:
                        bt.logging.warning(f"No valid transcript segments found for video {video_id}")
                        return None

                    # Return structured data with all metadata from Apify
                    result = {
                        'video_id': item.get('video_id', video_id),
                        'title': item.get('title', ''),
                        'channel_id': item.get('channel_id', ''),
                        'channel_name': item.get('channel_name', ''),
                        'upload_date': item.get('upload_date', ''),
                        'url': item.get('url', f"https://www.youtube.com/watch?v={video_id}"),
                        'duration_seconds': item.get('duration_seconds', 0),
                        'language': language,
                        'transcript': transcript_segments
                    }

                    bt.logging.info(
                        f"Retrieved manual transcript for {video_id}: {len(transcript_segments)} segments, language: {language}")
                    return result

            bt.logging.warning(f"No valid video data found in any items for video {video_id}")
            return None

        except ActorRunError as e:
            bt.logging.error(f"Apify actor failed for video {video_id} (URL: {video_url}): {str(e)}")
            return None
        except Exception as e:
            bt.logging.error(f"Error getting video data for {video_id} (URL: {video_url}): {str(e)}")
            bt.logging.error(traceback.format_exc())
            return None

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

            # Validate the content by checking against fresh Apify data
            attempt = 0
            while attempt < self.MAX_VALIDATION_ATTEMPTS:
                try:
                    # Respect rate limiting
                    await self._wait_for_rate_limit()

                    # Get fresh data from Apify for comparison
                    fresh_data = await self._get_video_data_apify(content_to_validate.video_id)

                    if not fresh_data:
                        results.append(
                            ValidationResult(
                                is_valid=False,
                                reason="Video not found or Apify error",
                                content_size_bytes_validated=entity.content_size_bytes
                            )
                        )
                        break

                    # Verify basic metadata including language
                    metadata_valid = self._verify_metadata(fresh_data, content_to_validate)

                    if not metadata_valid:
                        results.append(
                            ValidationResult(
                                is_valid=False,
                                reason="Metadata does not match",
                                content_size_bytes_validated=entity.content_size_bytes
                            )
                        )
                        break

                    # Verify transcript if available
                    if fresh_data.get('transcript'):
                        transcript_valid = self._verify_transcript(fresh_data['transcript'],
                                                                   content_to_validate.transcript)

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
                    else:
                        # If content has empty transcript and we can't get one, this might be valid
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

                # Get fresh data from Apify
                fresh_data = await self._get_video_data_apify(video_id)
                if not fresh_data:
                    validation_results.append(False)
                    continue

                # Check if the text field contains valid content
                if entity.get('text'):
                    # Verify transcript
                    if fresh_data.get('transcript'):
                        # Extract full text from transcript
                        actual_text = " ".join([item.get('text', '') for item in fresh_data['transcript']])

                        # Compare with stored text (simplified comparison)
                        similarity = self._calculate_text_similarity(actual_text, entity.get('text', ''))
                        validation_results.append(similarity >= 0.7)  # 70% similarity threshold
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
        total_duration = content.duration_seconds or sum(
            item.get('end', 0) - item.get('start', 0) for item in content.transcript)

        if chunks and total_duration > 0:
            chunk_duration = total_duration / len(chunks)

            for i, chunk in enumerate(chunks):
                start_time = i * chunk_duration
                compressed_transcript.append({
                    'start': start_time,
                    'end': start_time + chunk_duration,
                    'duration': chunk_duration,
                    'text': chunk
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
            duration_seconds=content.duration_seconds,
            language=content.language
        )

    def _verify_metadata(self, apify_data: Dict[str, Any], content_to_validate: YouTubeContent) -> bool:
        """
        Verify if the metadata from Apify matches the stored metadata.

        Args:
            apify_data: The metadata from the Apify actor.
            content_to_validate: The YouTubeContent to validate.

        Returns:
            True if the metadata matches sufficiently, False otherwise.
        """
        # Check title (allow for minor differences)
        if not self._texts_are_similar(apify_data.get('title', ''), content_to_validate.title, threshold=0.8):
            bt.logging.info(f"Title mismatch: '{apify_data.get('title', '')}' vs '{content_to_validate.title}'")
            return False

        # Check channel ID (must match exactly)
        if apify_data.get('channel_id', '') != content_to_validate.channel_id:
            bt.logging.info(
                f"Channel ID mismatch: '{apify_data.get('channel_id', '')}' vs '{content_to_validate.channel_id}'")
            return False

        # Check channel name (allow for minor differences)
        if not self._texts_are_similar(apify_data.get('channel_name', ''), content_to_validate.channel_name,
                                       threshold=0.8):
            bt.logging.info(
                f"Channel name mismatch: '{apify_data.get('channel_name', '')}' vs '{content_to_validate.channel_name}'")
            return False

        # Check language (must match exactly for language codes)
        if apify_data.get('language', self.preferred_language) != content_to_validate.language:
            bt.logging.info(
                f"Language mismatch: '{apify_data.get('language', self.preferred_language)}' vs '{content_to_validate.language}'")
            return False

        return True

    def _verify_transcript(self, apify_transcript: List[Dict[str, Any]],
                           stored_transcript: List[Dict[str, Any]]) -> bool:
        """
        Verify if the transcript from Apify matches the stored transcript.

        Args:
            apify_transcript: The transcript from the Apify actor.
            stored_transcript: The transcript to validate.

        Returns:
            True if the transcripts match sufficiently, False otherwise.
        """
        if not apify_transcript and not stored_transcript:
            return True

        if not apify_transcript or not stored_transcript:
            return False

        # Extract the full text from the transcripts
        apify_full_text = " ".join([item.get('text', '') for item in apify_transcript])
        stored_full_text = " ".join([item.get('text', '') for item in stored_transcript])

        # Clean up whitespace
        apify_full_text = re.sub(r'\s+', ' ', apify_full_text).strip()
        stored_full_text = re.sub(r'\s+', ' ', stored_full_text).strip()

        # Compare the texts (allow for compression differences)
        return self._texts_are_similar(apify_full_text, stored_full_text, threshold=0.7)

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

    def _construct_youtube_url(self, video_id: str) -> str:
        """
        Construct a proper YouTube URL from a video ID.

        Args:
            video_id: YouTube video ID

        Returns:
            Full YouTube URL
        """
        # If it's already a URL, return as-is
        if video_id.startswith('http'):
            return video_id

        # Clean the video ID (remove any extra characters)
        clean_id = video_id.strip()

        # Construct the standard YouTube URL
        return f"https://www.youtube.com/watch?v={clean_id}"

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


# Test utility methods

async def test_scrape_video():
    """Test function for scraping a specific video."""
    # Create an instance of the scraper with preferred language
    scraper = YouTubeApifyMultilingualScraper(preferred_language="en")

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
        bt.logging.info(f"Language: {content.language}")
        bt.logging.info(f"Transcript length: {len(content.transcript)}")
        if content.transcript:
            bt.logging.info(f"First transcript chunk: {content.transcript[0]['text'][:100]}...")

    return entities


async def test_validate_entity(entity: DataEntity):
    """Test function for validating a specific entity."""
    # Create an instance of the scraper
    scraper = YouTubeApifyMultilingualScraper(preferred_language="en")

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

    bt.logging.info("\nAll tests completed!")


async def test_multilingual_scraping():
    """Test scraping videos in different languages."""
    test_videos = {
        "dQw4w9WgXcQ": "en",  # English video
        "kJQP7kiw5Fk": "es",  # Spanish video (example)
        "rYEDA3JcQqw": "fr",  # French video (example)
    }

    date_range = DateRange(
        start=dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
        end=dt.datetime.now(dt.timezone.utc)
    )

    for video_id, language in test_videos.items():
        bt.logging.info(f"\nTesting video {video_id} with language {language}")

        # Create scraper with specific language preference
        scraper = YouTubeApifyMultilingualScraper(preferred_language=language)

        try:
            entities = await scraper._scrape_video_ids([video_id], date_range)

            if entities:
                content = YouTubeContent.from_data_entity(entities[0])
                bt.logging.info(f"✅ Successfully scraped {video_id}")
                bt.logging.info(f"   Title: {content.title}")
                bt.logging.info(f"   Language: {content.language}")
                bt.logging.info(f"   Transcript segments: {len(content.transcript)}")
                if content.transcript:
                    bt.logging.info(f"   Sample text: {content.transcript[0]['text'][:100]}...")
            else:
                bt.logging.warning(f"❌ No transcript found for {video_id} in language {language}")

        except Exception as e:
            bt.logging.error(f"❌ Error scraping {video_id}: {str(e)}")


# Menu-based test runner
async def main():
    print("\nMultilingual YouTube Apify Transcript Scraper - Test Menu")
    print("=" * 70)
    print("1. Run full pipeline test (default video)")
    print("2. Scrape a specific video (provide video ID)")
    print("3. Test multilingual scraping")
    print("4. Test with custom language")
    print("5. Exit")

    choice = input("\nEnter your choice (1-5): ")

    if choice == "1":
        await test_full_pipeline()
    elif choice == "2":
        video_id = input("Enter YouTube video ID: ")
        language = input("Enter language code (e.g., 'en', 'es', 'fr', 'ru') [default: en]: ").strip() or "en"

        scraper = YouTubeApifyMultilingualScraper(preferred_language=language)
        date_range = DateRange(
            start=dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime.now(dt.timezone.utc)
        )
        entities = await scraper._scrape_video_ids([video_id], date_range)
        bt.logging.info(f"Scraped {len(entities)} entities")
        if entities:
            content = YouTubeContent.from_data_entity(entities[0])
            print(f"\nVideo Title: {content.title}")
            print(f"Channel: {content.channel_name}")
            print(f"Language: {content.language}")
            print(f"Duration: {content.duration_seconds} seconds")
            print(f"Transcript segments: {len(content.transcript)}")
            if content.transcript:
                print(f"Sample text: {content.transcript[0]['text'][:200]}...")
        else:
            print(f"No manual transcript found for video {video_id} in language {language}")
    elif choice == "3":
        await test_multilingual_scraping()
    elif choice == "4":
        language = input("Enter language code (e.g., 'en', 'es', 'fr', 'ru'): ").strip()
        if not language:
            print("Invalid language code")
            return

        video_id = input("Enter YouTube video ID: ").strip()
        if not video_id:
            print("Invalid video ID")
            return

        scraper = YouTubeApifyMultilingualScraper(preferred_language=language)
        date_range = DateRange(
            start=dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime.now(dt.timezone.utc)
        )

        try:
            entities = await scraper._scrape_video_ids([video_id], date_range)
            if entities:
                content = YouTubeContent.from_data_entity(entities[0])
                print(f"\n✅ Success! Found manual transcript in {language}")
                print(f"Video Title: {content.title}")
                print(f"Channel: {content.channel_name}")
                print(f"Language: {content.language}")
                print(f"Transcript segments: {len(content.transcript)}")
                if content.transcript:
                    print(f"Sample text: {content.transcript[0]['text'][:200]}...")
            else:
                print(f"\n❌ No manual transcript found for video {video_id} in language {language}")
                print("This could mean:")
                print("- The video doesn't have manual captions in this language")
                print("- The video only has auto-generated captions")
                print("- The video doesn't exist or is private")
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
    elif choice == "5":
        print("Exiting test program.")
        return
    else:
        print("Invalid choice. Please try again.")
        await main()


# Entry point for running the tests
if __name__ == "__main__":
    bt.logging.info("Starting YouTube Apify multilingual scraper tests...")
    asyncio.run(main())