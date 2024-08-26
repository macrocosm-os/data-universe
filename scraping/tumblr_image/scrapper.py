import time
from common import constants, utils
from common.date_range import DateRange
from scraping.reddit import model
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
import bittensor as bt
from common.data import DataEntity, DataLabel, DataSource
from typing import List
import asyncpraw
from scraping.tumblr_image.utils import download_image_url_to_bytes
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
                content = json.loads(entity.content)

                # Extract blog name and post ID from URL
                blog_name, post_id = self._extract_blog_name_and_post_id(content['post_url'])

                if not blog_name or not post_id:
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="Invalid Tumblr URL format.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )
                    continue

                # Use the Tumblr API to fetch the post TODO TRANSFER IT FROM THE SCRAPPER?
                post = self.tumblr_client.posts(blog_name, id=post_id)

                if not post or 'posts' not in post or len(post['posts']) == 0:
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="Post not found on Tumblr.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )
                    continue

                tumblr_post = post['posts'][0]

                # Validate the post details
                is_valid = True
                reason = ""

                if content['creator'] != tumblr_post['blog_name']:
                    is_valid = False
                    reason += "Creator mismatch. "

                if content['post_url'] != tumblr_post['post_url']:
                    is_valid = False
                    reason += "Post URL mismatch. "

                if content['description'] != tumblr_post.get('summary', ''):
                    is_valid = False
                    reason += "Description mismatch. "

                if set(content['tags']) != set(tumblr_post.get('tags', [])):
                    is_valid = False
                    reason += "Tags mismatch. "

                post_timestamp = datetime.fromtimestamp(tumblr_post['timestamp'], tz=timezone.utc)
                if content['timestamp'].replace(tzinfo=timezone.utc) != post_timestamp:
                    is_valid = False
                    reason += "Timestamp mismatch. "

                # Validate image URL (if applicable)
                if 'photos' in tumblr_post and tumblr_post['photos']:
                    tumblr_image_url = tumblr_post['photos'][0]['original_size']['url']
                    # Note: We don't have image_url in our content, so we skip this check
                    # You might want to add a way to compare image_bytes if needed

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
        bt.logging.trace(
            f" Tumblr custom scraper peforming scrape with config: {scrape_config}."
        )

        # assert ( # TODO do we need something like this?
        #     not scrape_config.labels or len(scrape_config.labels) <= 1
        # ), "Can only scrape 1 subreddit at a time."

        # Strip the r/ from the config or use 'all' if no label is provided.
        # subreddit_name = (
        #     normalize_label(scrape_config.labels[0]) if scrape_config.labels else "all"
        # )
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
        try:
            tumblr_client = pytumblr.TumblrRestClient(
                self.TUMBLR_API,
                self.TUMBLR_SECRET,
                self.TUMBLR_USER_KEY,
                self.TUMBLR_USER_SECRET_KEY
            )
            tumblr_image_dataset: List = tumblr_client.tagged(tag='sear', limit=10, filter='image')
            contents = self._best_effort_parse_tumblr_image(tumblr_image_dataset)
            data_entities = [TumblrContent.to_data_entity(content) for content in contents]

        except Exception:
            bt.logging.error(
                f"Failed to scrape tumbr."
            )
            # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
            return []

        # TODO CONVERT IT INTO DATA ENTITIES
        # data_entities = []
        # for content in parsed_contents:
        #     data_entities.append(RedditContent.to_data_entity(content=content))

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

            image_bytes, image_content_size = download_image_url_to_bytes(image_url)
            # Check if image isn't empty
            if not image_url:
                continue

            # get date
            # created_date_gmt = dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc) if timestamp else None

            # Create TumblrContent object
            content = TumblrContent(
                timestamp=dt.datetime.fromtimestamp(tumblr_image['timestamp'], tz=dt.timezone.utc),
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

    entities = await scraper.scrape()

    print(f"Scraped r/bittensor_. Got entities: {len(entities)}")



async def test_validate():
    scraper = TumblrCustomScrapper()

    # This test covers a top level comment, a submission, and a nested comment with both the correct parent id and the submission id in order.
    # Previous versions of the custom scraper incorrectly got the submission id as the parent id for nested comments.
    true_entities = [
        DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3vd3n/",
            datetime=dt.datetime(2023, 12, 5, 16, 29, 27, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t1_kc3vd3n", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3vd3n/", "username": "one-bad-dude", "communityName": "r/bittensor_", "body": "Its not an EVM chain or ERC-20 token. Its a subnet/substrate of Polkadot ecosystem. So you need the polkadot.js wallet.", "createdAt": "2023-12-05T16:29:27+00:00", "dataType": "comment", "title": null, "parentId": "t3_18bf67l"}',
            content_size_bytes=476,
        ),
        DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/",
            datetime=dt.datetime(2023, 12, 5, 15, 59, 13, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t3_18bf67l", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Hey all!!\\n\\nHow do we add TAO to MetaMask? Online gives me these network configurations and still doesn\\u2019t work? \\n\\nHow are you all storing TAO? I wanna purchase on MEXC, but holding off until I can store it!  \\ud83d\\ude11 \\n\\nThanks in advance!!!\\n\\n=====\\n\\nhere is a manual way.\\nNetwork Name\\nTao Network\\n\\nRPC URL\\nhttp://rpc.testnet.tao.network\\n\\nChain ID\\n558\\n\\nCurrency Symbol\\nTAO", "createdAt": "2023-12-05T15:59:13+00:00", "dataType": "post", "title": "How do you add TAO to MetaMask?", "parentId": null}',
            content_size_bytes=775,
        ),
        DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            datetime=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:16+00:00", "dataType": "comment", "parentId": "t1_kc3vd3n"}',
            content_size_bytes=392,
        ),
        DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            datetime=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:16+00:00", "dataType": "comment", "parentId": "t3_18bf67l"}',
            content_size_bytes=392,
        ),
    ]
    results = await scraper.validate(entities=true_entities)
    print(f"Expecting Pass. Validation results: {results}")

    # Now modify the entities to make them invalid and check validation fails.
    good_entity = true_entities[1]
    good_comment_entity = true_entities[2]
    bad_entities = [
        # Change url.
        good_entity.copy(
            update={
                "uri": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask-abc123/"
            }
        ),
        # Change title.
        good_entity.copy(
            update={
                "content": b'{"id": "t3_18bf67l", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Hey all!!\\n\\nHow do we add TAO to MetaMask? Online gives me these network configurations and still doesn\\u2019t work? \\n\\nHow are you all storing TAO? I wanna purchase on MEXC, but holding off until I can store it!  \\ud83d\\ude11 \\n\\nThanks in advance!!!\\n\\n=====\\n\\nhere is a manual way.\\nNetwork Name\\nTao Network\\n\\nRPC URL\\nhttp://rpc.testnet.tao.network\\n\\nChain ID\\n558\\n\\nCurrency Symbol\\nTAO", "createdAt": "2023-12-05T15:59:13+00:00", "dataType": "post", "title": "How do you add TAO to MetaMask??!!?", "parent_id": null}',
            }
        ),
        # Change created_at.
        good_entity.copy(
            update={"datetime": good_entity.datetime + dt.timedelta(seconds=1)}
        ),
        # Change label.
        good_entity.copy(update={"label": DataLabel(value="bittensor_")}),
        # Change comment parent id.
        good_comment_entity.copy(
            update={
                "content": b'{"id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:16+00:00", "dataType": "comment", "parentId": "extra-long-parent-id"}'
            }
        ),
        # Change submission parent id.
        good_entity.copy(
            update={
                "content": b'{"id": "t3_18bf67l", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Hey all!!\\n\\nHow do we add TAO to MetaMask? Online gives me these network configurations and still doesn\\u2019t work? \\n\\nHow are you all storing TAO? I wanna purchase on MEXC, but holding off until I can store it!  \\ud83d\\ude11 \\n\\nThanks in advance!!!\\n\\n=====\\n\\nhere is a manual way.\\nNetwork Name\\nTao Network\\n\\nRPC URL\\nhttp://rpc.testnet.tao.network\\n\\nChain ID\\n558\\n\\nCurrency Symbol\\nTAO", "createdAt": "2023-12-05T15:59:13+00:00", "dataType": "post", "title": "How do you add TAO to MetaMask?", "parentId": "extra-long-parent-id"}'
            }
        ),
    ]

    for entity in bad_entities:
        results = await scraper.validate(entities=[entity])
        print(f"Expecting a failed validation. Result={results}")


async def test_u_deleted():
    """Verifies that the RedditCustomScraper can handle deleted users."""
    comment = DataEntity(
        uri="https://www.reddit.com/r/AskReddit/comments/ablzuq/people_who_havent_pooped_in_2019_yet_why_are_you/ed1j7is/",
        datetime=dt.datetime(2019, 1, 1, 22, 59, 9, tzinfo=dt.timezone.utc),
        source=1,
        label=DataLabel(value="r/askreddit"),
        content=b'{"id": "t1_ed1j7is", "url": "https://www.reddit.com/r/AskReddit/comments/ablzuq/people_who_havent_pooped_in_2019_yet_why_are_you/ed1j7is/", "username": "[deleted]", "communityName": "r/AskReddit", "body": "Aw man what a terrible way to spend NYE! I hope you feel better soon bud!", "createdAt": "2019-01-01T22:59:09+00:00", "dataType": "comment", "title": null, "parentId": "t1_ed1dqvy"}',
        content_size_bytes=387,
    )

    scraper = RedditCustomScraper()
    result = await scraper.validate(entities=[comment])
    print(f"Expecting a passed validation: {result}")


if __name__ == "__main__":
    asyncio.run(test_scrape())
    #asyncio.run(test_validate())
    #asyncio.run(test_u_deleted())
