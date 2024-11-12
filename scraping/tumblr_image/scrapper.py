import time
from common import constants, utils
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
import bittensor as bt
from common.data import DataEntity, DataLabel, DataSource
from typing import List, Optional
import aiohttp
from scraping.tumblr_image.model import TumblrContent
import traceback
import datetime as dt
import asyncio
import pytumblr
import json
import requests
import re
from dotenv import load_dotenv
import os
load_dotenv()


class TumblrCustomScraper(Scraper):
    """
    Scrapes Tumblr data using a personal tumblr account.
    """

    def __init__(self):
        self.client = pytumblr.TumblrRestClient(
            os.getenv('TUMBLR_API_KEY'),
            os.getenv('TUMBLR_SECRET_KEY'),
            os.getenv('TUMBLR_USER_KEY'),
            os.getenv('TUMBLR_USER_SECRET_KEY')
        )
        self.last_request = 0
        self.min_interval = 0.5

    def _wait_rate_limit(self):
        """Basic rate limiting"""
        now = time.time()
        wait_time = max(0, self.min_interval - (now - self.last_request))
        if wait_time > 0:
            time.sleep(wait_time)
        self.last_request = now

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate Tumblr entities"""
        if not entities:
            return []

        results = []
        for entity in entities:
            try:
                content = TumblrContent.from_data_entity(entity)

                # Get blog name and post ID
                blog_name, post_id = self._extract_blog_name_and_post_id(content.post_url)
                if not blog_name or not post_id:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Invalid Tumblr URL format",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Get original post
                self._wait_rate_limit()
                post = self.client.posts(blog_name, id=post_id)

                if not post or 'posts' not in post or not post['posts']:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Post not found",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Parse and validate post
                parsed_content = self._best_effort_parse_post(post['posts'][0])
                if not parsed_content:
                    results.append(ValidationResult(
                        is_valid=False,
                        reason="Failed to parse post",
                        content_size_bytes_validated=entity.content_size_bytes
                    ))
                    continue

                # Compare content
                is_valid = True
                reasons = []

                if content.creator != parsed_content.creator:
                    is_valid = False
                    reasons.append("Creator mismatch")

                if content.post_url != parsed_content.post_url:
                    is_valid = False
                    reasons.append("URL mismatch")

                if content.image_bytes != parsed_content.image_bytes:
                    is_valid = False
                    reasons.append("Image content mismatch")

                if content.tags != parsed_content.tags:
                    is_valid = False
                    reasons.append("Tags mismatch")

                results.append(ValidationResult(
                    is_valid=is_valid,
                    reason=" | ".join(reasons) if reasons else "Valid post",
                    content_size_bytes_validated=entity.content_size_bytes
                ))

            except Exception as e:
                bt.logging.error(f"Validation error for {entity.uri}: {str(e)}")
                results.append(ValidationResult(
                    is_valid=False,
                    reason=f"Validation error: {str(e)}",
                    content_size_bytes_validated=entity.content_size_bytes
                ))

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrape Tumblr posts"""
        bt.logging.info(f"Tumblr scraper performing scrape with config: {scrape_config}")

        if not scrape_config.labels:
            return []

        tag = scrape_config.labels[0].value
        entities = []

        try:
            self._wait_rate_limit()
            # Simple tag search with limit
            posts = self.client.tagged(
                tag=tag,
                limit=scrape_config.entity_limit,
                filter='raw'
            )

            if posts:
                for post in posts:
                    try:
                        print(post.get('type'))
                        if post.get('type') != 'photo':
                            continue

                        content = self._best_effort_parse_post(post)
                        if content:
                            entities.append(TumblrContent.to_data_entity(content))
                    except Exception as e:
                        bt.logging.error(f"Error processing post: {str(e)}")
                        continue

        except Exception as e:
            bt.logging.error(f"Failed to scrape tag {tag}: {str(e)}")
            return []

        bt.logging.info(f"Scraped {len(entities)} images from tag {tag}")
        return entities

    def _best_effort_parse_post(self, post: dict) -> Optional[TumblrContent]:
        """Parse Tumblr post, similar to Reddit's _best_effort_parse methods"""
        try:
            # Get image URL - first try photos
            image_url = None
            if post.get('photos'):
                photo = post['photos'][0]
                if photo.get('original_size'):
                    image_url = photo['original_size']['url']

            print(image_url)
            # Fallback to body parsing
            if not image_url and post.get('body'):
                match = re.search(r'src="(https://64\.media\.tumblr\.com/[^"]+)"', post['body'])
                if match:
                    image_url = match.group(1)

            if not image_url:
                return None

            # Download image
            self._wait_rate_limit()
            response = requests.get(image_url)
            if response.status_code != 200:
                return None

            return TumblrContent(
                timestamp=dt.datetime.fromtimestamp(post['timestamp'], dt.timezone.utc),
                image_bytes=response.content,
                tags=post.get('tags', []),
                description=post.get('summary', ''),
                post_url=post.get('post_url', ''),
                creator=post.get('blog_name', '')
            )

        except Exception as e:
            bt.logging.error(f"Error parsing post: {str(e)}")
            return None

    def _extract_blog_name_and_post_id(self, url: str) -> tuple[Optional[str], Optional[str]]:
        """Extract blog name and post ID"""
        match = re.match(r'https?://([^.]+)\.tumblr\.com/post/(\d+)', url)
        if match:
            return match.group(1), match.group(2)
        return None, None

    async def validate_hf(self, entities) -> bool:
        """Placeholder for HF validation"""
        return True


async def test_scrape():
    """Test scraper"""
    scraper = TumblrCustomScraper()

    config = ScrapeConfig(
        entity_limit=200,
        date_range=DateRange(
            start=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=30),
            end=dt.datetime.now(tz=dt.timezone.utc),
        ),
        labels=[DataLabel(value="Bittensor")]
    )

    entities = await scraper.scrape(config)
    print(f"Scraped {len(entities)} Tumblr images")

    if entities:
        print("\nTesting validation:")
        results = await scraper.validate(entities)
        print(f"Validation results: {results}")

    return entities


if __name__ == "__main__":
    asyncio.run(test_scrape())
