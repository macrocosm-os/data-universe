import asyncio
import random
import traceback
import bittensor as bt
from typing import Any, Dict, List
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunError, ActorRunner, RunConfig
from scraping.reddit.model import RedditContent, RedditDataType
from scraping.reddit.utils import (
    is_valid_reddit_url,
    validate_reddit_content,
    get_time_input,
    get_sort_input,
    normalize_label,
)
import datetime as dt


class RedditLiteScraper(Scraper):
    """
    Scrapes Reddit data using the Reddit Scraper Lite actor.
    """

    ACTOR_ID = "oAuCIx3ItNrs2okjQ"

    # The Reddit Actor seems a lot slower. Bump the timeout to 2 mins.
    SCRAPE_TIMEOUT_SECS = 120

    BASE_RUN_INPUT = {
        "debugMode": False,
        "includeNSFW": False,
        "proxy": {"useApifyProxy": True},
        "scrollTimeout": 40,
        "searchCommunities": False,
        "searchUsers": False,
        "skipComments": False,
        "searchComments": True,
        "searchPosts": True,
    }

    def __init__(self, runner: ActorRunner = ActorRunner()):
        self.runner = runner

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

            run_input = self._get_validation_run_input(reddit_content_to_verify)
            run_config = RunConfig(
                actor_id=RedditLiteScraper.ACTOR_ID,
                debug_info=f"Validate {entity.uri}",
                max_data_entities=1,
            )

            # Retrieve the Reddit Post/Comment from Apify.
            dataset: List[dict] = None
            try:
                dataset: List[dict] = await self.runner.run(run_config, run_input)
            except ActorRunError as e:
                bt.logging.error(
                    f"Failed to validate entity: {traceback.format_exc()}."
                )
                # This is an unfortunate situation. We have no way to distinguish a genuine failure from
                # one caused by malicious input. In my own testing I was able to make the Actor timeout by
                # using a bad URI. As such, we have to penalize the miner here. If we didn't they could
                # pass malicious input for chunks they don't have.
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Failed to run Actor. This can happen if the URI is invalid, or APIfy is having an issue.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # Parse the response
            items = self._best_effort_parse_dataset(dataset)
            if len(items) < 1:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Reddit post/comment not found or is invalid.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # We found the Reddit content. Validate it.
            actual_content = items[0]

            results.append(
                validate_reddit_content(
                    actual_content=actual_content,
                    entity_to_validate=entity,
                )
            )

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of reddit posts/comments according to the scrape config."""

        assert (
            not scrape_config.labels or len(scrape_config.labels) <= 1
        ), "Can only scrape 1 subreddit at a time."

        # The scraper defaults to 10 max items, so make sure we always override it.
        max_items = scrape_config.entity_limit or 100
        run_input = {
            **RedditLiteScraper.BASE_RUN_INPUT,
            "time": get_time_input(scrape_config.date_range.end),
            "sort": get_sort_input(scrape_config.date_range.end),
            "maxItems": max_items,
            "maxPostCount": max_items,
            "maxComments": max_items,
        }

        if scrape_config.labels:
            run_input["searches"] = [
                f"subreddit:{normalize_label(scrape_config.labels[0])}"
            ]
        else:
            # No label provided. Search all
            # I'm sure there's some magic combination of inputs to the Actor that'll
            # make it do the sane thing and just search Reddit's home page, while respecting
            # the 'time' field, but I couldn't get that to work.
            # Instead, let's just search for a random and super common word.
            # I'm sorry for making this English only.
            # TODO: Figure out something a little more sophisticated.
            word = random.choice(["the", "he", "she", "they", "it", "and"])
            run_input["searches"] = [word]

        bt.logging.trace(
            f"Running Reddit scraper with search: {run_input['searches']}."
        )

        # Construct the input to the runner.
        run_config = RunConfig(
            actor_id=RedditLiteScraper.ACTOR_ID,
            debug_info=f"Scrape {run_input['searches']}",
            max_data_entities=scrape_config.entity_limit,
            timeout_secs=RedditLiteScraper.SCRAPE_TIMEOUT_SECS,
        )

        # Run the Actor and retrieve the scraped data.
        dataset: List[dict] = None
        try:
            dataset: List[dict] = await self.runner.run(run_config, run_input)
        except ActorRunError:
            bt.logging.error(
                f"Failed to scrape reddit using query {run_input['searches']}: {traceback.format_exc()}."
            )
            # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
            return []

        # Return the parsed results, ignoring data that can't be parsed.
        contents = self._best_effort_parse_dataset(dataset)
        bt.logging.success(
            f"Completed scrape for {run_input['searches']}. Scraped {len(contents)} items."
        )

        data_entities = []
        for content in contents:
            data_entities.append(RedditContent.to_data_entity(content=content))

        return data_entities

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> List[RedditContent]:
        """Performs a best effort parsing of Apify dataset into List[RedditContent]

        Any errors are logged and ignored."""
        results: List[RedditContent] = []
        for data in dataset:
            try:
                results.append(RedditContent(**data))
            except Exception:
                bt.logging.warning(
                    f"Failed to decode RedditContent from Apify response: {traceback.format_exc()}."
                )
        return results

    def _get_validation_run_input(self, content: RedditContent) -> Dict[str, Any]:
        run_input = {
            **RedditLiteScraper.BASE_RUN_INPUT,
            "startUrls": [{"url": content.url}],
        }

        # Add run_inputs based on the type of content we're validating
        if content.data_type == RedditDataType.POST:
            run_input["searchComments"] = False
            run_input["skipComments"] = True
            run_input["maxPostCount"] = 1
            run_input["maxComments"] = 0
        elif content.data_type == RedditDataType.COMMENT:
            run_input["searchPosts"] = False
            run_input["maxComments"] = 1
            run_input["maxPostCount"] = 0
        else:
            assert (
                False
            ), "Someone forgot to update this code after adding a RedditDataType..."

        return run_input


async def test_scrape():
    scraper = RedditLiteScraper()

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

    print(f"Scraped r/bittensor_. Got entities: {entities}")

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

    print(f"Scraped without a label. Got entities: {entities}")


async def test_validate():
    scraper = RedditLiteScraper()

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
        good_entity.copy(
            update={
                "uri": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask-abc123/"
            }
        ),
        good_entity.copy(
            update={
                "content": b'{"id": "t3_18bf67l", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Hey all!!\\n\\nHow do we add TAO to MetaMask? Online gives me these network configurations and still doesn\\u2019t work? \\n\\nHow are you all storing TAO? I wanna purchase on MEXC, but holding off until I can store it!  \\ud83d\\ude11 \\n\\nThanks in advance!!!\\n\\n=====\\n\\nhere is a manual way.\\nNetwork Name\\nTao Network\\n\\nRPC URL\\nhttp://rpc.testnet.tao.network\\n\\nChain ID\\n558\\n\\nCurrency Symbol\\nTAO", "createdAt": "2023-12-05T15:59:13+00:00", "dataType": "post", "title": "How do you add TAO to MetaMask??!!?", "parent_id": null}',
            }
        ),
        good_entity.copy(
            update={"datetime": good_entity.datetime + dt.timedelta(seconds=1)}
        ),
        good_entity.copy(update={"label": DataLabel(value="bittensor_")}),
        good_comment_entity.copy(
            update={
                "content": b'{"id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:16+00:00", "dataType": "comment", "parentId": "extra-long-parent-id"}'
            }
        ),
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
    """Verifies that the RedditLiteScraper can handle deleted users."""
    comment = DataEntity(
        uri="https://www.reddit.com/r/AskReddit/comments/ablzuq/people_who_havent_pooped_in_2019_yet_why_are_you/ed1j7is/",
        datetime=dt.datetime(2019, 1, 1, 22, 59, 9, tzinfo=dt.timezone.utc),
        source=1,
        label=DataLabel(value="r/askreddit"),
        content=b'{"id": "t1_ed1j7is", "url": "https://www.reddit.com/r/AskReddit/comments/ablzuq/people_who_havent_pooped_in_2019_yet_why_are_you/ed1j7is/", "username": "[deleted]", "communityName": "r/AskReddit", "body": "Aw man what a terrible way to spend NYE! I hope you feel better soon bud!", "createdAt": "2019-01-01T22:59:09+00:00", "dataType": "comment", "title": null, "parentId": "t1_ed1dqvy"}',
        content_size_bytes=387,
    )

    scraper = RedditLiteScraper()
    result = await scraper.validate(entities=[comment])
    print(f"Expecting a passed validation: {result}")


if __name__ == "__main__":
    asyncio.run(test_scrape())
    asyncio.run(test_validate())
    asyncio.run(test_u_deleted())
