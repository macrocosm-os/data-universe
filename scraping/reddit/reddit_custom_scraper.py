import time
from common import constants, utils
from common.date_range import DateRange
from scraping.reddit import model
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult, HFValidationResult
import bittensor as bt
from common.data import DataEntity, DataLabel, DataSource
from typing import List
import asyncpraw
from scraping.reddit.utils import (
    is_valid_reddit_url,
    validate_reddit_content,
    get_time_input,
    get_custom_sort_input,
    normalize_label,
    normalize_permalink,
)
from scraping.reddit.model import RedditContent, RedditDataType
import traceback
import datetime as dt
import asyncio
import random
import os

from dotenv import load_dotenv

load_dotenv()


class RedditCustomScraper(Scraper):
    """
    Scrapes Reddit data using a personal reddit account.
    """

    USER_AGENT = f"User-Agent: python: {os.getenv('REDDIT_USERNAME')}"

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a DataEntity by URI."""
        if not entities:
            return []

        results = []

        # For verification, it's easiest to perform each query separately.
        for entity in entities:
            # First check the URI is a valid Reddit URL.
            if not is_valid_reddit_url(entity.uri):
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Invalid URI.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # Parse out the RedditContent object that we're validating
            reddit_content_to_verify = None
            try:
                reddit_content_to_verify = RedditContent.from_data_entity(entity)
            except Exception:
                bt.logging.error(
                    f"Failed to decode RedditContent from data entity bytes: {traceback.format_exc()}."
                )
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Failed to decode data entity.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # Retrieve the Reddit Post/Comment from PRAW.
            content = None

            try:
                async with asyncpraw.Reddit(
                    client_id=os.getenv("REDDIT_CLIENT_ID"),
                    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
                    username=os.getenv("REDDIT_USERNAME"),
                    password=os.getenv("REDDIT_PASSWORD"),
                    user_agent=RedditCustomScraper.USER_AGENT,
                ) as reddit:
                    if reddit_content_to_verify.data_type == RedditDataType.POST:
                        submission = await reddit.submission(
                            url=reddit_content_to_verify.url
                        )
                        # Parse the response.
                        content = self._best_effort_parse_submission(submission)
                    else:
                        comment = await reddit.comment(url=reddit_content_to_verify.url)
                        # Parse the response.
                        content = self._best_effort_parse_comment(comment)
            except Exception as e:
                bt.logging.error(
                    f"Failed to validate entity ({entity.uri})[{entity.content}]: {traceback.format_exc()}."
                )
                # This is an unfortunate situation. We have no way to distinguish a genuine failure from
                # one caused by malicious input. In my own testing I was able to make the request timeout by
                # using a bad URI. As such, we have to penalize the miner here. If we didn't they could
                # pass malicious input for chunks they don't have.
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Failed to retrieve submission. This can happen if the URI is invalid, or Reddit is having an issue.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            if not content:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Reddit post/comment not found or is invalid.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # We found the Reddit content. Validate it.
            results.append(
                validate_reddit_content(
                    actual_content=content,
                    entity_to_validate=entity,
                )
            )

        return results

    async def validate_hf(self, entities) -> HFValidationResult:
        """Validate the correctness of HFEntities by URL, focusing on username, date (hour), and text."""
        if not entities:
            return True

        validation_results = []

        for entity in entities:
            if not is_valid_reddit_url(entity.get('url')):
                validation_results.append(False)
                continue

            content = None
            try:
                async with asyncpraw.Reddit(
                        client_id=os.getenv("REDDIT_CLIENT_ID"),
                        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
                        username=os.getenv("REDDIT_USERNAME"),
                        password=os.getenv("REDDIT_PASSWORD"),
                        user_agent=RedditCustomScraper.USER_AGENT,
                ) as reddit:
                    if entity.get('dataType') == RedditDataType.COMMENT:
                        comment = await reddit.comment(url=entity.get('url'))
                        content = self._best_effort_parse_comment(comment)
                    else:
                        submission = await reddit.submission(url=entity.get('url'))
                        content = self._best_effort_parse_submission(submission)
            except Exception as e:
                bt.logging.error(
                    f"Failed to validate entity ({entity.get('url')}): {traceback.format_exc()}."
                )
                validation_results.append(False)
                continue

            if not content:
                validation_results.append(False)
                continue

            validation_result = self._validate_hf_reddit_content(content, entity)
            validation_results.append(validation_result)

        valid_percentage = sum(validation_results) / len(validation_results) * 100

        # Check if at least 60% of the data is valid
        is_valid = valid_percentage >= 60
        return HFValidationResult(is_valid=is_valid, validation_percentage=valid_percentage, reason=f"Validation Percentage = {valid_percentage}")

    def _validate_hf_reddit_content(self, actual_content: RedditContent, entity_to_validate: dict) -> bool:
        """Validate the Reddit content against the entity to validate, focusing on username, date (hour), and text."""

        # Compare date (year, month, day)
        entity_datetime = dt.datetime.fromisoformat(entity_to_validate.get('datetime'))
        actual_datetime = actual_content.created_at

        if isinstance(actual_datetime, str):
            actual_datetime = dt.datetime.fromisoformat(actual_datetime)

        if (entity_datetime.year != actual_datetime.year or
                entity_datetime.month != actual_datetime.month or
                entity_datetime.day != actual_datetime.day):
            bt.logging.info(f'HF validation failed for {entity_to_validate} due to date mismatch')
            return False

        # Compare text content
        if actual_content.data_type == RedditDataType.POST:
            # For posts, combine title and body
            actual_text = f"{actual_content.title}\n\n{actual_content.body}".strip()
        else:
            actual_text = actual_content.body.strip()

        if actual_text != entity_to_validate.get('text', '').strip():
            bt.logging.info(f'HF validation failed for {entity_to_validate} due to text mismatch')
            return False

        return True

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of reddit posts/comments according to the scrape config."""
        bt.logging.trace(
            f"Reddit custom scraper peforming scrape with config: {scrape_config}."
        )

        assert (
            not scrape_config.labels or len(scrape_config.labels) <= 1
        ), "Can only scrape 1 subreddit at a time."

        # Strip the r/ from the config or use 'all' if no label is provided.
        subreddit_name = (
            normalize_label(scrape_config.labels[0]) if scrape_config.labels else "all"
        )

        bt.logging.trace(
             f"Running custom Reddit scraper with search: {subreddit_name}."
        )

        # Randomize between fetching submissions and comments to reduce api calls.
        fetch_submissions = bool(random.getrandbits(1))

        # Get the search terms for the reddit query.
        search_limit = scrape_config.entity_limit
        search_sort = get_custom_sort_input(scrape_config.date_range.end)
        search_time = get_time_input(scrape_config.date_range.end)

        # In either case we parse the response into a list of RedditContents.
        contents = None
        try:
            async with asyncpraw.Reddit(
                client_id=os.getenv("REDDIT_CLIENT_ID"),
                client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
                username=os.getenv("REDDIT_USERNAME"),
                password=os.getenv("REDDIT_PASSWORD"),
                user_agent=RedditCustomScraper.USER_AGENT,
            ) as reddit:
                subreddit = await reddit.subreddit(subreddit_name)

                if fetch_submissions:
                    submissions = None
                    match search_sort:
                        case "new":
                            submissions = subreddit.new(limit=search_limit)
                        case "top":
                            submissions = subreddit.top(
                                limit=search_limit, time_filter=search_time
                            )
                        case "hot":
                            submissions = subreddit.hot(limit=search_limit)

                    contents = [
                        self._best_effort_parse_submission(submission)
                        async for submission in submissions
                    ]
                else:
                    comments = subreddit.comments(limit=search_limit)

                    contents = [
                        self._best_effort_parse_comment(comment)
                        async for comment in comments
                    ]
        except Exception:
            bt.logging.error(
                f"Failed to scrape reddit using subreddit {subreddit_name}, limit {search_limit}, time {search_time}, sort {search_sort}: {traceback.format_exc()}."
            )
            # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
            return []

        # Return the parsed results, ignoring data that can't be parsed.
        parsed_contents = [content for content in contents if content != None]

        bt.logging.success(
            f"Completed scrape for subreddit {subreddit_name}. Scraped {len(parsed_contents)} items."
        )

        data_entities = []
        for content in parsed_contents:
            data_entities.append(RedditContent.to_data_entity(content=content))

        return data_entities

    def _best_effort_parse_submission(
        self, submission: asyncpraw.models.Submission
    ) -> RedditContent:
        """Performs a best effort parsing of a Reddit submission into a RedditContent

        Any errors are logged and ignored."""
        content = None

        try:
            user = submission.author.name if submission.author else model.DELETED_USER
            content = RedditContent(
                id=submission.name,
                url="https://www.reddit.com"
                + normalize_permalink(submission.permalink),
                username=user,
                communityName=submission.subreddit_name_prefixed,
                body=submission.selftext,
                createdAt=dt.datetime.utcfromtimestamp(submission.created_utc).replace(
                    tzinfo=dt.timezone.utc
                ),
                dataType=RedditDataType.POST,
                # Post only fields
                title=submission.title,
                # Comment only fields
                parentId=None,
            )
        except Exception:
            bt.logging.trace(
                f"Failed to decode RedditContent from Reddit Submission: {traceback.format_exc()}."
            )

        return content

    def _best_effort_parse_comment(
        self, comment: asyncpraw.models.Comment
    ) -> RedditContent:
        """Performs a best effort parsing of a Reddit comment into a RedditContent

        Any errors are logged and ignored."""
        content = None

        try:
            user = comment.author.name if comment.author else model.DELETED_USER
            content = RedditContent(
                id=comment.name,
                url="https://www.reddit.com" + normalize_permalink(comment.permalink),
                username=user,
                communityName=comment.subreddit_name_prefixed,
                body=comment.body,
                createdAt=dt.datetime.utcfromtimestamp(comment.created_utc).replace(
                    tzinfo=dt.timezone.utc
                ),
                dataType=RedditDataType.COMMENT,
                # Post only fields
                title=None,
                # Comment only fields
                parentId=comment.parent_id,
            )
        except Exception:
            bt.logging.trace(
                f"Failed to decode RedditContent from Reddit Submission: {traceback.format_exc()}."
            )

        return content


async def test_scrape():
    scraper = RedditCustomScraper()

    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=3,
            date_range=DateRange(
                start=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=2),
                end=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=2),
            ),
            labels=[DataLabel(value="r/bittensor_")],
        )
    )

    # Scrape some older content without a label.
    start = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=2)
    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=3,
            date_range=DateRange(
                start=start,
                end=start + dt.timedelta(hours=1),
            ),
        )
    )


async def test_validate():
    scraper = RedditCustomScraper()

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
    asyncio.run(test_validate())
    asyncio.run(test_u_deleted())
