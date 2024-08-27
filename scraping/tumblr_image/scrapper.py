import time
from common import constants, utils
from common.date_range import DateRange
from scraping.reddit import model
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
import bittensor as bt
from common.data import DataEntity, DataLabel, DataSource
from typing import List
import asyncpraw
from scraping.tumblr_image.utils import download_image_url_to_bytes, convert_timestamp_to_utc
# from scraping.reddit.utils import ( TODO DEVELOP AN FUNCTION TO VALIDATE THE TUMBLER IMAGES
#     is_valid_reddit_url,
#     validate_reddit_content,
#     get_time_input,
#     get_custom_sort_input,
#     normalize_label,
#     normalize_permalink,
# )
from scraping.tumblr_image.model import TumblrContent, TumblrDataType
import traceback
import datetime as dt
import asyncio
import random
import os
import pytumblr
import json
import re
from dotenv import load_dotenv

load_dotenv()


class TumblrCustomScrapper(Scraper):
    """
    Scrapes Tumblr data using a personal tumblr account.
    """

    TUMBLR_API = os.getenv('TUMBLR_API_KEY')
    TUMBLR_SECRET = os.getenv('TUMBLR_SECRET_KEY')
    TUMBLR_USER_KEY = os.getenv('TUMBLR_USER_KEY')
    TUMBLR_USER_SECRET_KEY = os.getenv('TUMBLR_USER_SECRET_KEY')

    def _extract_blog_name_and_post_id(self, url):
        # Extract blog name and post ID from URL
        match = re.match(r'https?://([^.]+)\.tumblr\.com/post/(\d+)', url)
        if match:
            return match.group(1), match.group(2)
        return None, None

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a DataEntity by URL."""
        if not entities:
            return []

        results = []

        for entity in entities:
            try:
                # Parse the content of the entity
                content = TumblrContent.from_data_entity(entity)


                # Extract blog name and post ID from URL
                blog_name, post_id = self._extract_blog_name_and_post_id(content.post_url)

                if not blog_name or not post_id:
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="Invalid Tumblr URL format.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )
                    continue

                # Use the Tumblr API to fetch the post
                tumblr_client = pytumblr.TumblrRestClient(
                    self.TUMBLR_API,
                    self.TUMBLR_SECRET,
                    self.TUMBLR_USER_KEY,
                    self.TUMBLR_USER_SECRET_KEY
                )

                post = tumblr_client.posts(blog_name, id=post_id)

                if not post or 'posts' not in post or len(post['posts']) == 0:
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="Post not found on Tumblr.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )
                    continue

                # Use _best_effort_parse_tumblr_image to parse the fetched post
                parsed_contents = self._best_effort_parse_tumblr_image(post['posts'])

                if not parsed_contents:
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="Failed to parse Tumblr post content.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )
                    continue

                tumblr_post = parsed_contents[0]

                # Validate the post details
                is_valid = True
                reason = ""

                if content.creator != tumblr_post.creator:
                    is_valid = False
                    reason += "Creator mismatch. "

                if content.post_url != tumblr_post.post_url:
                    is_valid = False
                    reason += "Post URL mismatch. "

                if content.description != tumblr_post.description:
                    is_valid = False
                    reason += "Description mismatch. "

                if set(content.tags) != set(tumblr_post.tags):
                    is_valid = False
                    reason += "Tags mismatch. "

                # if content.timestamp != tumblr_post.post_url:
                #     is_valid = False
                #     reason += "Timestamp mismatch. "


                # Validate image contents
                if content.image_bytes != tumblr_post.image_bytes:
                    is_valid = False
                    reason += "Image bytes doesn't match"

                results.append(
                    ValidationResult(
                        is_valid=is_valid,
                        reason=reason.strip() if not is_valid else "Valid Tumblr post.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )

            except Exception as e:
                bt.logging.error(f"Failed to validate entity ({entity.uri}): {traceback.format_exc()}.")
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason=f"Validation failed due to an error: {str(e)}",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )

        return results

    async def scrape(self, scrape_config) -> List[DataEntity]:
        """Scrapes a batch of reddit posts/comments according to the scrape config."""
        bt.logging.info(
            f" Tumblr custom scraper performing scrape with config: {scrape_config}."
        )

        tumblr_tag_name = scrape_config.labels[0]

        bt.logging.success(
            f"Running custom Tumblr scraper with search: {tumblr_tag_name}."
        )

        # Randomize between fetching submissions and comments to reduce api calls.

        # # Get the search terms for the reddit query.
        search_limit = scrape_config.entity_limit
        # search_sort = get_custom_sort_input(scrape_config.date_range.end) # TODO
        # search_time = get_time_input(scrape_config.date_range.end) # TODO

        # In either case we parse the response into a list of RedditContents.
        contents = None
        #try: todo uncomment try expect
        tumblr_client = pytumblr.TumblrRestClient(
            self.TUMBLR_API,
            self.TUMBLR_SECRET,
            self.TUMBLR_USER_KEY,
            self.TUMBLR_USER_SECRET_KEY
        )
        # TODO ADD before parameter in tagged tumblr method!
        tumblr_image_dataset: List = tumblr_client.tagged(tag=tumblr_tag_name.value, limit=search_limit, filter='image')
        # print(tumblr_image_dataset)
        contents = self._best_effort_parse_tumblr_image(tumblr_image_dataset)
        data_entities = [TumblrContent.to_data_entity(content) for content in contents]

        # except Exception as exp:
        #     bt.logging.error(
        #         f"Failed to scrape tumbr. {exp}"
        #     )
        #     # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
        #     return []

        return data_entities


    def _best_effort_parse_tumblr_image(self, tumblr_image_dataset: List[dict]):
        results = []
        for tumblr_image in tumblr_image_dataset:
            # Extract image URL
            image_url = None
            if 'body' in tumblr_image:
                import re
                match = re.search(r'src="(https://64\.media\.tumblr\.com/[^"]+)"', tumblr_image['body'])
                if match:
                    image_url = match.group(1)


            if image_url:
                print(image_url)
            else:
                print('no url:(')
                continue

            image_bytes, image_content_size = download_image_url_to_bytes(image_url)
            # Check if image isn't empty
            if not image_url:
                continue

            # Create TumblrContent object
            content = TumblrContent(
                timestamp=convert_timestamp_to_utc(tumblr_image['timestamp']),
                image_bytes=image_bytes,
                tags=tumblr_image.get('tags', []),
                description=tumblr_image.get('summary', ''),
                post_url=tumblr_image.get('post_url', ''),
                creator=tumblr_image.get('blog_name', '')
            )
            results.append(content)

        return results



async def test_scrape():
    scraper = TumblrCustomScrapper()

    entities = await scraper.scrape(ScrapeConfig(
        entity_limit=4,
        date_range=DateRange(
            start=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=2),
            end=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=2),
        ),
        labels=[DataLabel(value="space")],
    ))

    print(f"Scraped Tumblr. Got entities: {len(entities)}")
    return entities

async def test_validate(entities):
    scraper = TumblrCustomScrapper()

    print("Testing validation with original entities:")
    results = await scraper.validate(entities=entities)
    print(f"Validation results: {results}")


async def main():
    entities = await test_scrape()
    await test_validate(entities)

if __name__ == "__main__":
    asyncio.run(main())