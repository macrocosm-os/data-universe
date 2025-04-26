import asyncio
import hashlib
import traceback
import random
import bittensor as bt
from typing import List, Dict, Any, Optional
import datetime as dt
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult, HFValidationResult
from scraping.youtube.model import YouTubeContent
from scraping.youtube import utils
import json
import re


class YouTubeTranscriptScraper(Scraper):
    """
    Scrapes transcripts from YouTube videos using the YouTube Transcript API.
    Also fetches basic metadata about the videos to provide context.
    """

    # Maximum number of transcripts to fetch in a single request
    MAX_TRANSCRIPTS_PER_REQUEST = 5

    # Maximum number of validation attempts
    MAX_VALIDATION_ATTEMPTS = 2

    # Default chunk size for transcript compression (in characters)
    DEFAULT_CHUNK_SIZE = 3000

    def __init__(self):
        """Initialize the YouTube Transcript Scraper."""
        # Track rate limits to avoid API throttling
        self.last_request_time = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=10)
        self.request_interval = dt.timedelta(seconds=1)  # Minimum time between requests

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
        channel_labels = []

        if scrape_config.labels:
            for label in scrape_config.labels:
                # Handle YouTube video IDs
                if label.value.startswith('#youtube_v_'):
                    video_id = label.value.replace('#youtube_v_', '')
                    video_ids.append(video_id)

                # Handle YouTube channel IDs or names
                elif label.value.startswith('#youtube_c_'):
                    channel_label = label.value
                    channel_labels.append(channel_label)

        # Limit the number of videos to scrape
        max_entities = scrape_config.entity_limit or self.MAX_TRANSCRIPTS_PER_REQUEST

        # If we have specific video IDs, prioritize those
        if video_ids:
            return await self._scrape_video_ids(video_ids[:max_entities], scrape_config.date_range)

        # Otherwise, try to get videos from the specified channels
        elif channel_labels:
            # In a real implementation, you would fetch recent videos from these channels
            # For this example, we'll just use some placeholder logic
            return await self._scrape_channels(
                channel_labels,
                max_entities,
                scrape_config.date_range
            )

        # If no specific videos or channels, get trending videos
        else:
            return await self._scrape_trending(max_entities, scrape_config.date_range)

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
                # Get the transcript from the YouTube API
                transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

                # Prefer English transcript if available, otherwise use the first available
                try:
                    transcript = transcript_list.find_transcript(['en'])
                except NoTranscriptFound:
                    # Fall back to the first available transcript
                    transcript = transcript_list.find_generated_transcript()

                # Fetch the transcript data
                transcript_data = transcript.fetch()

                # Get video metadata (title, channel, etc.) - in a real implementation,
                # you would use YouTube Data API to get this information
                video_metadata = await self._get_video_metadata(video_id)

                # Skip if the video is outside the date range
                video_date = dt.datetime.fromisoformat(video_metadata.get('upload_date', ''))
                if not self._is_within_date_range(video_date, date_range):
                    continue

                # Create the content object
                content = YouTubeContent(
                    video_id=video_id,
                    title=video_metadata.get('title', ''),
                    channel_id=video_metadata.get('channel_id', ''),
                    channel_name=video_metadata.get('channel_name', ''),
                    upload_date=video_date,
                    transcript=transcript_data,
                    url=f"https://www.youtube.com/watch?v={video_id}",
                    language=transcript.language,
                    duration_seconds=video_metadata.get('duration_seconds', 0),
                    compressed=True
                )

                # Compress the transcript to reduce storage size
                content = self._compress_transcript(content)

                # Convert to DataEntity
                results.append(YouTubeContent.to_data_entity(content))

            except (TranscriptsDisabled, NoTranscriptFound) as e:
                bt.logging.warning(f"No transcript available for video {video_id}: {str(e)}")
                continue
            except Exception as e:
                bt.logging.error(f"Error scraping transcript for video {video_id}: {str(e)}")
                continue

        bt.logging.info(f"Scraped {len(results)} YouTube transcripts")
        return results

    async def _scrape_channels(self, channel_labels: List[str], max_entities: int, date_range: DateRange) -> List[
        DataEntity]:
        """
        Scrape transcripts from specified YouTube channels.

        Args:
            channel_labels: List of channel labels.
            max_entities: Maximum number of videos to scrape.
            date_range: Date range for filtering.

        Returns:
            List of DataEntity objects.
        """
        # In a real implementation, you would use the YouTube Data API to:
        # 1. Get channel IDs from labels
        # 2. Get recent videos from those channels
        # 3. Filter by date range
        # 4. Get transcripts for those videos

        # For the example, we'll use a placeholder implementation
        # that simulates fetching videos from channels
        results = []
        channel_videos = await self._get_channel_videos(channel_labels, date_range)

        # Limit to max_entities
        video_ids = channel_videos[:max_entities]

        if video_ids:
            results = await self._scrape_video_ids(video_ids, date_range)

        return results

    async def _scrape_trending(self, max_entities: int, date_range: DateRange) -> List[DataEntity]:
        """
        Scrape transcripts from trending YouTube videos.

        Args:
            max_entities: Maximum number of videos to scrape.
            date_range: Date range for filtering.

        Returns:
            List of DataEntity objects.
        """
        # In a real implementation, you would use the YouTube Data API to:
        # 1. Get trending videos
        # 2. Filter by date range
        # 3. Get transcripts for those videos

        # For the example, we'll use a placeholder implementation
        # that simulates fetching trending videos
        video_ids = await self._get_trending_videos(date_range)

        # Limit to max_entities
        video_ids = video_ids[:max_entities]

        return await self._scrape_video_ids(video_ids, date_range)

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

            # Validate the content by checking if the transcript is still available
            attempt = 0
            while attempt < self.MAX_VALIDATION_ATTEMPTS:
                try:
                    # Respect rate limiting
                    await self._wait_for_rate_limit()

                    # Get the transcript from the YouTube API
                    transcript_list = YouTubeTranscriptApi.list_transcripts(content_to_validate.video_id)

                    # Try to get the same language transcript
                    try:
                        transcript = transcript_list.find_transcript([content_to_validate.language])
                    except NoTranscriptFound:
                        # Fall back to any available transcript
                        transcript = transcript_list.find_generated_transcript()

                    # Fetch the transcript data
                    transcript_data = transcript.fetch()

                    # If we got here, the transcript exists
                    # Now we need to verify it matches what we have stored
                    if self._verify_transcript(transcript_data, content_to_validate):
                        results.append(
                            ValidationResult(
                                is_valid=True,
                                reason="Transcript validated successfully",
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
                    # If no transcript found, check if this is a valid error
                    if self._is_valid_no_transcript_error(content_to_validate):
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
            video_id = utils.extract_video_id(entity.get('url', ''))
            if not video_id:
                validation_results.append(False)
                continue

            try:
                # Respect rate limiting
                await self._wait_for_rate_limit()

                # Get the transcript from the YouTube API
                transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

                # Try to get any transcript
                transcript = transcript_list.find_generated_transcript()

                # Fetch the transcript data
                transcript_data = transcript.fetch()

                # Check if the text field contains valid transcript content
                if entity.get('text'):
                    # Simple verification by checking if key phrases from the transcript
                    # appear in the stored text
                    text_matches = self._verify_hf_transcript_text(entity.get('text'), transcript_data)
                    validation_results.append(text_matches)
                else:
                    validation_results.append(False)

            except (TranscriptsDisabled, NoTranscriptFound):
                # If the entity correctly indicates no transcript, mark as valid
                if entity.get('status') == 'no_transcript':
                    validation_results.append(True)
                else:
                    validation_results.append(False)
            except Exception:
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
        full_text = " ".join([item['text'] for item in content.transcript])

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

        if chunks:
            chunk_duration = total_duration / len(chunks)

            for i, chunk in enumerate(chunks):
                compressed_transcript.append({
                    'text': chunk,
                    'start': i * chunk_duration,
                    'duration': chunk_duration
                })

        # Update the content with compressed transcript
        return YouTubeContent(
            video_id=content.video_id,
            title=content.title,
            channel_id=content.channel_id,
            channel_name=content.channel_name,
            upload_date=content.upload_date,
            transcript=compressed_transcript,
            url=content.url,
            language=content.language,
            duration_seconds=content.duration_seconds,
            compressed=True
        )

    def _verify_transcript(self, actual_transcript: List[Dict[str, Any]], content_to_validate: YouTubeContent) -> bool:
        """
        Verify if the actual transcript matches the stored transcript.

        Args:
            actual_transcript: The transcript from the YouTube API.
            content_to_validate: The YouTubeContent to validate.

        Returns:
            True if the transcripts match sufficiently, False otherwise.
        """
        # If the stored transcript is compressed, we need a different comparison approach
        if content_to_validate.compressed:
            return self._verify_compressed_transcript(actual_transcript, content_to_validate.transcript)

        # For uncompressed transcripts, compare directly
        # Allow for small differences in timing and formatting
        if len(actual_transcript) != len(content_to_validate.transcript):
            return False

        # Check a sample of the transcript (beginning, middle, end)
        indices_to_check = [
            0,
            len(actual_transcript) // 2,
            len(actual_transcript) - 1
        ]

        for idx in indices_to_check:
            actual_text = actual_transcript[idx]['text'].strip()
            stored_text = content_to_validate.transcript[idx]['text'].strip()

            # Compare text content (allowing for minor differences)
            if not self._texts_are_similar(actual_text, stored_text):
                return False

        return True

    def _verify_compressed_transcript(self, actual_transcript: List[Dict[str, Any]],
                                      compressed_transcript: List[Dict[str, Any]]) -> bool:
        """
        Verify if a compressed transcript matches the actual transcript.

        Args:
            actual_transcript: The full transcript from the YouTube API.
            compressed_transcript: The compressed transcript to validate.

        Returns:
            True if the transcripts match sufficiently, False otherwise.
        """
        # Extract the full text from the actual transcript
        actual_full_text = " ".join([item['text'] for item in actual_transcript]).strip()
        actual_full_text = re.sub(r'\s+', ' ', actual_full_text)

        # Extract the full text from the compressed transcript
        compressed_full_text = " ".join([item['text'] for item in compressed_transcript]).strip()
        compressed_full_text = re.sub(r'\s+', ' ', compressed_full_text)

        # Check if the compressed text is a subset of the actual text
        # or if they're similar enough
        return self._texts_are_similar(actual_full_text, compressed_full_text, threshold=0.7)

    def _verify_hf_transcript_text(self, stored_text: str, actual_transcript: List[Dict[str, Any]]) -> bool:
        """
        Verify if the text stored in HuggingFace matches the actual transcript.

        Args:
            stored_text: The text stored in the HuggingFace dataset.
            actual_transcript: The transcript from the YouTube API.

        Returns:
            True if the text matches the transcript sufficiently, False otherwise.
        """
        # Extract the full text from the actual transcript
        actual_full_text = " ".join([item['text'] for item in actual_transcript]).strip()
        actual_full_text = re.sub(r'\s+', ' ', actual_full_text)

        # Clean up the stored text
        stored_text = re.sub(r'\s+', ' ', stored_text).strip()

        # Check if the texts are similar enough
        return self._texts_are_similar(actual_full_text, stored_text, threshold=0.7)

    def _texts_are_similar(self, text1: str, text2: str, threshold: float = 0.8) -> bool:
        """
        Check if two texts are similar enough.

        Args:
            text1: First text.
            text2: Second text.
            threshold: Similarity threshold (0-1).

        Returns:
            True if the texts are similar enough, False otherwise.
        """
        # Simple approach: check if enough words from one text appear in the other
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())

        # Calculate overlap ratio
        if not words1 or not words2:
            return False

        overlap = len(words1.intersection(words2))
        similarity = overlap / max(len(words1), len(words2))

        return similarity >= threshold

    def _is_valid_no_transcript_error(self, content: YouTubeContent) -> bool:
        """
        Check if a NoTranscriptFound error is valid for this content.

        Args:
            content: The YouTubeContent.

        Returns:
            True if the error is valid, False otherwise.
        """
        # If the content indicates it has no transcript, the error is valid
        return content.transcript is None or len(content.transcript) == 0

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

    async def _get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get metadata for a YouTube video.

        Args:
            video_id: The YouTube video ID.

        Returns:
            Dictionary containing video metadata.
        """
        # In a real implementation, you would use the YouTube Data API
        # For this example, we'll simulate a response

        # Create a deterministic but random-looking date based on the video ID
        hash_value = int(hashlib.md5(video_id.encode()).hexdigest(), 16)
        days_ago = hash_value % 30  # 0-29 days ago
        upload_date = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_ago)

        return {
            'title': f"Video {video_id}",
            'channel_id': f"UC{video_id[:10]}",
            'channel_name': f"Channel {video_id[:5]}",
            'upload_date': upload_date.isoformat(),
            'duration_seconds': hash_value % 1800  # 0-1799 seconds (up to 30 minutes)
        }

    async def _get_channel_videos(self, channel_labels: List[str], date_range: DateRange) -> List[str]:
        """
        Get video IDs from specified channels.

        Args:
            channel_labels: List of channel labels.
            date_range: Date range for filtering.

        Returns:
            List of video IDs.
        """
        # In a real implementation, you would use the YouTube Data API
        # For this example, we'll simulate a response with some hardcoded video IDs
        # that are known to have transcripts

        # Some example videos with transcripts
        example_videos = [
            "kJQP7kiw5Fk",  # "Despacito" by Luis Fonsi
            "JGwWNGJdvx8",  # "Shape of You" by Ed Sheeran
            "RgKAFK5djSk",  # "See You Again" by Wiz Khalifa
            "9bZkp7q19f0",  # "Gangnam Style" by PSY
            "OPf0YbXqDm0",  # "Uptown Funk" by Mark Ronson ft. Bruno Mars
            "hT_nvWreIhg",  # "Counting Stars" by OneRepublic
            "YQHsXMglC9A",  # "Hello" by Adele
            "CevxZvSJLk8",  # "Roar" by Katy Perry
            "JRfuAukYTKg",  # "Sorry" by Justin Bieber
            "nfWlot6h_JM"  # "Shake It Off" by Taylor Swift
        ]

        # Shuffle the list deterministically based on the first channel label
        if channel_labels:
            seed = int(hashlib.md5(channel_labels[0].encode()).hexdigest(), 16)
            random.seed(seed)
            random.shuffle(example_videos)

        return example_videos

    async def _get_trending_videos(self, date_range: DateRange) -> List[str]:
        """
        Get trending video IDs.

        Args:
            date_range: Date range for filtering.

        Returns:
            List of video IDs.
        """
        # In a real implementation, you would use the YouTube Data API
        # For this example, we'll return the same example videos
        return await self._get_channel_videos(["#trending"], date_range)



