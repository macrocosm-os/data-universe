import asyncio
import traceback
import bittensor as bt
from typing import List
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunError, ActorRunner, RunConfig
from scraping.x.model import XContent
from scraping.x.utils import is_valid_twitter_url
import datetime as dt


class TwitterFlashScraper(Scraper):
    """
    Scrapes tweets using the Tweet Flash Actor.
    """

    ACTOR_ID = "wHMoznVs94gOcxcZl"

    SCRAPE_TIMEOUT_SECS = 120

    BASE_RUN_INPUT = {
        "filter:blue_verified": False,
        "filter:has_engagement": False,
        "filter:images": False,
        "filter:media": False,
        "filter:nativeretweets": False,
        "filter:quote": False,
        "filter:replies": False,
        "filter:retweets": False,
        "filter:safe": False,
        "filter:twimg": False,
        "filter:verified": False,
        "filter:videos": False,
        "only_tweets": False,
        "use_experimental_scraper": False,
        "language": "any",
        "user_info": "only user info",
        "max_attempts": 5,
    }

    def __init__(self, runner: ActorRunner = ActorRunner()):
        self.runner = runner

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a DataEntity by URI."""
        if not entities:
            return []

        # Treat the entities as guilty until proven innocent.
        results = []

        # The Apify Actor does not support searching for multiple tweet_urls at once. So we must perform each run separately.
        for entity in entities:
            # First check the URI is a valid Twitter URL.
            if not is_valid_twitter_url(entity.uri):
                results.append(
                    ValidationResult(is_valid=False, reason="Invalid URI."),
                    content_size_bytes_validated=entity.content_size_bytes,
                )
                continue

            run_input = {
                **TwitterFlashScraper.BASE_RUN_INPUT,
                "tweet_urls": [entity.uri],
            }
            run_config = RunConfig(
                actor_id=TwitterFlashScraper.ACTOR_ID,
                debug_info=f"Validate {entity.uri}",
                max_data_entities=1,
            )

            # Retrieve the tweet from Apify.
            dataset: List[dict] = None
            try:
                dataset: List[dict] = await self.runner.run(run_config, run_input)
            except ActorRunError as e:
                bt.logging.error(
                    f"Failed to validate entities: {traceback.format_exc()}."
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
            tweets = self._best_effort_parse_dataset(dataset)
            if len(tweets) < 1:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Tweet not found or is invalid.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # We found the tweet. Validate it.
            actual_tweet = tweets[0]
            results.append(TwitterFlashScraper._validate_tweet(actual_tweet, entity))

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""

        # Construct the query string.
        date_format = "%Y-%m-%d_%H:%M:%S_UTC"
        query = f"since:{scrape_config.date_range.start.astimezone(tz=dt.timezone.utc).strftime(date_format)} until:{scrape_config.date_range.end.astimezone(tz=dt.timezone.utc).strftime(date_format)}"
        if scrape_config.labels:
            label_query = " OR ".join([label.value for label in scrape_config.labels])
            query += f" ({label_query})"
        else:
            # HACK: The search query doesn't work if only a time range is provided.
            # If no label is specified, just search for "e", the most common letter in the English alphabet.
            # I attempted using "#" instead, but that still returned empty results ¯\_(ツ)_/¯
            query += " e"

        # Construct the input to the runner.
        max_items = scrape_config.entity_limit or 150
        run_input = {
            **TwitterFlashScraper.BASE_RUN_INPUT,
            "queries": [query],
            "max_tweets": max_items,
        }
        run_config = RunConfig(
            actor_id=TwitterFlashScraper.ACTOR_ID,
            debug_info=f"Scrape {query}",
            max_data_entities=scrape_config.entity_limit,
            timeout_secs=TwitterFlashScraper.SCRAPE_TIMEOUT_SECS,
        )

        bt.logging.trace(f"Performing Twitter scrape for query: {query}.")

        # Run the Actor and retrieve the scraped data.
        dataset: List[dict] = None
        try:
            dataset: List[dict] = await self.runner.run(run_config, run_input)
        except ActorRunError:
            bt.logging.error(
                f"Failed to scrape tweets using query {query}: {traceback.format_exc()}."
            )
            # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
            return []

        # Return the parsed results, ignoring data that can't be parsed.
        x_contents = self._best_effort_parse_dataset(dataset)
        bt.logging.success(
            f"Completed scrape for {query}. Scraped {len(x_contents)} items."
        )

        return [XContent.to_data_entity(x_content) for x_content in x_contents]

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> List[XContent]:
        """Performs a best effort parsing of Apify dataset into List[XContent]

        Any errors are logged and ignored."""
        results: List[XContent] = []
        for data in dataset:
            try:
                results.append(XContent(**data))
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )
        return results

    @classmethod
    def _validate_tweet(cls, tweet: XContent, entity: DataEntity) -> ValidationResult:
        """Validates the tweet is valid by the definition provided by entity."""
        tweet_to_verify = None
        try:
            tweet_to_verify = XContent.from_data_entity(entity)
        except Exception:
            bt.logging.error(
                f"Failed to decode XContent from data entity bytes: {traceback.format_exc()}."
            )
            return ValidationResult(
                is_valid=False,
                reason="Failed to decode data entity",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        if tweet_to_verify != tweet:
            bt.logging.info(f"Tweets do not match: {tweet_to_verify} != {tweet}.")
            return ValidationResult(
                is_valid=False,
                reason="Tweet does not match",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # Wahey! A valid Tweet.
        # One final check. Does the tweet content match the data entity information?
        try:
            tweet_entity = XContent.to_data_entity(tweet)
            if not DataEntity.are_non_content_fields_equal(tweet_entity, entity):
                return ValidationResult(
                    is_valid=False,
                    reason="The DataEntity fields are incorrect based on the tweet.",
                    content_size_bytes_validated=entity.content_size_bytes,
                )
        except Exception:
            # This shouldn't really happen, but let's safeguard against it anyway to avoid us somehow accepting
            # corrupted or malformed data.
            bt.logging.error(
                f"Failed to convert XContent to DataEntity: {traceback.format_exc()}"
            )
            return ValidationResult(
                is_valid=False,
                reason="Failed to convert XContent to DataEntity.",
                content_size_bytes_validated=entity.content_size_bytes,
            )

        # At last, all checks have passed. The DataEntity is indeed valid. Nice work!
        return ValidationResult(
            is_valid=True,
            reason="Good job, you honest miner!",
            content_size_bytes_validated=entity.content_size_bytes,
        )


async def test_scrape():
    scraper = TwitterFlashScraper()

    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=3,
            date_range=DateRange(
                start=dt.datetime(2023, 12, 9, 10, 0, 0, tzinfo=dt.timezone.utc),
                end=dt.datetime(2023, 12, 9, 11, 0, 0, tzinfo=dt.timezone.utc),
            ),
            labels=[DataLabel(value="#bittensor"), DataLabel(value="#TAO")],
        )
    )

    print(f"Scraped {len(entities)} entities: {entities}")


async def test_validate():
    scraper = TwitterFlashScraper()

    true_entities = [
        DataEntity(
            uri="https://twitter.com/TcMMTsTc/status/1733441357090545731",
            datetime=dt.datetime(2023, 12, 9, 10, 59, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            content=b'{"username":"@TcMMTsTc","text":"\xe3\x81\xbc\xe3\x81\x8f\xe7\x9c\xa0\xe3\x81\x84\xe3\x81\xa7\xe3\x81\x99","url":"https://twitter.com/TcMMTsTc/status/1733441357090545731","timestamp":"2023-12-09T10:59:00Z","tweet_hashtags":[]}',
            content_size_bytes=218,
        ),
        DataEntity(
            uri="https://twitter.com/nirmaljajra2/status/1733439438473380254",
            datetime=dt.datetime(2023, 12, 9, 10, 52, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bittensor"),
            content=b'{"username":"@nirmaljajra2","text":"DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
            content_size_bytes=484,
        ),
    ]

    results = await scraper.validate(entities=true_entities)
    print(f"Validation results: {results}")

    # Now modify the entities to make them invalid and check validation fails.
    good_entity = true_entities[1]
    bad_entities = [
        good_entity.copy(
            update={"uri": "https://twitter.com/nirmaljajra2/status/abc123"}
        ),
        good_entity.copy(
            update={
                "content": b'{"username":"@nirmaljajra2","text":"Random-text-insertion-DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
            }
        ),
        good_entity.copy(
            update={"datetime": good_entity.datetime + dt.timedelta(seconds=1)}
        ),
        # Hashtag ordering needs to be deterministic. Verify changing the order of the hashtags makes the content non-equivalent.
        good_entity.copy(update={"label": DataLabel(value="#PAAl")}),
    ]

    for entity in bad_entities:
        results = await scraper.validate(entities=[entity])
        print(f"Expecting a failed validation. Result={results}")


if __name__ == "__main__":
    asyncio.run(test_scrape())
    asyncio.run(test_validate())
