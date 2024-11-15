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
import base64

load_dotenv()


class TumblrCustomScraper(Scraper):
    """
    Scrapes Tumblr data using a personal tumblr account.
    Focuses on image content with pagination support.
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

                if content.image_format != parsed_content.image_format:
                    is_valid = False
                    reasons.append("Image format mismatch")

                if abs((content.timestamp - parsed_content.timestamp).total_seconds()) > 60:
                    is_valid = False
                    reasons.append("Timestamp mismatch")

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
        """Scrape Tumblr posts with pagination"""
        bt.logging.info(f"Tumblr scraper performing scrape with config: {scrape_config}")

        if not scrape_config.labels:
            return []

        tag = scrape_config.labels[0].value
        entities = []
        timestamp = int(dt.datetime.now().timestamp())

        posts_processed = 0
        try:
            self._wait_rate_limit()

            # Get posts - single fetch
            posts = self.client.tagged(
                tag=tag,
                limit=20,
                before=timestamp,
                filter='raw'
            )

            if not posts:
                bt.logging.info(f"No posts available for tag: {tag}")
                return []

            bt.logging.info(f"Got {len(posts)} posts for tag: {tag}")

            # Process posts
            for post in posts:
                try:
                    content = self._best_effort_parse_post(post)
                    if content:
                        entities.append(TumblrContent.to_data_entity(content))
                        posts_processed += 1

                except Exception as e:
                    bt.logging.error(f"Error processing post: {str(e)}")
                    continue

        except Exception as e:
            bt.logging.error(f"Failed to scrape tag {tag}: {str(e)}")

        bt.logging.info(f"Scraped {len(entities)} images from {posts_processed} processed posts")
        return entities

    def _best_effort_parse_post(self, post: dict) -> Optional[TumblrContent]:
        """Parse Tumblr post for main image only"""
        try:
            image_url = None

            # Get description/caption
            description = post.get('summary', '')
            if not description and post.get('caption'):
                description = post.get('caption')
            if not description and post.get('body'):
                # Clean HTML tags for readability
                description = re.sub(r'<[^>]+>', '', post.get('body'))

            # Get main photo
            if post.get('photos'):
                photo = post['photos'][0]
                if photo.get('original_size', {}).get('url'):
                    image_url = photo['original_size']['url']
                elif photo.get('alt_sizes'):
                    alt_sizes = sorted(
                        photo['alt_sizes'],
                        key=lambda x: int(x.get('width', 0)),
                        reverse=True
                    )
                    if alt_sizes:
                        image_url = alt_sizes[0]['url']

            if not image_url:
                return None

            # Clean URL and get format
            image_url = image_url.split('?')[0]
            image_format = image_url.split('.')[-1].lower()

            # Download image
            self._wait_rate_limit()
            response = requests.get(image_url)
            if response.status_code != 200:
                return None

            # Verify format with PIL
            try:
                from PIL import Image
                import io
                img = Image.open(io.BytesIO(response.content))
                actual_format = img.format.lower()
                img_size = img.size
            except:
                actual_format = image_format
                img_size = (0, 0)

            bt.logging.debug(f"""
                Found image:
                URL: {image_url}
                Format: {actual_format}
                Size: {img_size}
                Description: {description[:200]}...
                Tags: {post.get('tags', [])}
                Notes: {post.get('note_count', 0)}
                Blog: {post.get('blog_name')}
            """)

            return TumblrContent(
                timestamp=dt.datetime.fromtimestamp(post['timestamp'], dt.timezone.utc),
                image_bytes=response.content,
                image_format=actual_format,
                tags=post.get('tags', []),
                description=description,
                post_url=post.get('post_url', ''),
                creator=post.get('blog_name', '')
            )

        except Exception as e:
            bt.logging.error(f"Error parsing post: {str(e)}")
            return None

    def _extract_blog_name_and_post_id(self, url: str) -> tuple[Optional[str], Optional[str]]:
        """Extract blog name and post ID from Tumblr URL"""
        try:
            patterns = [
                r'https?://([^.]+)\.tumblr\.com/post/(\d+)',  # standard tumblr
                r'https?://(?:www\.)?tumblr\.com/blog/view/([^/]+)/(\d+)',  # tumblr blog view
                r'https?://(?:www\.)?([^/]+)/post/(\d+)',  # custom domain
            ]

            for pattern in patterns:
                match = re.match(pattern, url)
                if match:
                    return match.group(1), match.group(2)

            bt.logging.debug(f"Could not parse Tumblr URL: {url}")
            return None, None

        except Exception as e:
            bt.logging.error(f"Error parsing Tumblr URL {url}: {str(e)}")
            return None, None

    async def validate_hf(self, entities) -> bool:
        """Placeholder for HuggingFace validation"""
        return True


async def test_scrape():
    """Test scraper functionality"""
    scraper = TumblrCustomScraper()

    config = ScrapeConfig(
        entity_limit=50,
        date_range=DateRange(
            start=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=30),
            end=dt.datetime.now(tz=dt.timezone.utc),
        ),
        labels=[DataLabel(value="space")]
    )

    entities = await scraper.scrape(config)
    print(f"\nScraped {len(entities)} Tumblr images")

    if entities:
        print("\nTesting validation:")
        results = await scraper.validate(entities)
        print(f"Validation results: {results}")

        # Print detailed info for each post
        for i, entity in enumerate(entities):
            content = TumblrContent.from_data_entity(entity)
            print(f"\n=== Post {i+1} ===")
            print(f"URL: {content.post_url}")
            print(f"Creator: {content.creator}")
            print(f"Format: {content.image_format}")
            print(f"Image size: {len(content.image_bytes):,} bytes")
            print(f"Tags: {content.tags}")
            print("Description:")
            print(f"{content.description[:500]}...")
            print("-" * 80)

    return entities

if __name__ == "__main__":
    asyncio.run(test_scrape())
