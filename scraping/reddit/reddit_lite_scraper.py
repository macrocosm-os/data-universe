import asyncio
import random
import traceback
import bittensor as bt
from typing import Any, Dict, List
from common.data import DataEntity, DataLabel, DataSource, DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunError, ActorRunner, RunConfig
from scraping.reddit.model import RedditContent, RedditDataType
from scraping.reddit.utils import is_valid_reddit_url
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

    def __init__(self, runner: ActorRunner = ActorRunner):
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
                results.append(ValidationResult(is_valid=False, reason="Invalid URI"))
                continue

            # Parse out the RedditContent object that we're validating
            reddit_content_to_verify = None
            try:
                reddit_content_to_verify = RedditContent.from_data_entity(entity)
            except Exception:
                bt.logging.error(
                    f"Failed to decode RedditContent from data entity bytes: {traceback.format_exc()}"
                )
                return ValidationResult(
                    is_valid=False, reason="Failed to decode data entity"
                )

            run_input = self._get_validation_run_input(reddit_content_to_verify)
            run_config = RunConfig(
                actor_id=RedditLiteScraper.ACTOR_ID,
                debug_info=f"Validate {entity.uri}",
                max_items=1,
            )

            # Retrieve the Reddit Post/Comment from Apify.
            dataset: List[dict] = None
            try:
                dataset: List[dict] = await self.runner.run(run_config, run_input)
            except ActorRunError as e:
                bt.logging.error(f"Failed to validate entity: {traceback.format_exc()}")
                raise Scraper.ValidationError(f"Failed to run Apify Actor: {e}")

            # Parse the response
            items = self._best_effort_parse_dataset(dataset)
            if len(items) < 1:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Reddit post/comment not found or is invalid",
                    )
                )
                continue

            # We found the Reddit content. Validate it.
            actual_content = items[0]
            results.append(self._validate_reddit_content(actual_content, entity))

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""

        assert (
            not scrape_config.labels or len(scrape_config.labels) <= 1
        ), "Can only scrape 1 subreddit at a time."

        run_input = {
            **RedditLiteScraper.BASE_RUN_INPUT,
            "time": self._get_time_input(scrape_config.date_range.end),
            "sort": self._get_sort_input(scrape_config.date_range.end),
        }

        if scrape_config.labels:
            run_input["searches"] = [
                f"subreddit:{self._normalize_label(scrape_config.labels[0])}"
            ]
        else:
            # No label provided. Search all
            run_input["searches"] = ["https://www.reddit.com/"]

        # Construct the input to the runner.
        run_config = RunConfig(
            actor_id=RedditLiteScraper.ACTOR_ID,
            debug_info=f"Scrape {run_input['searches']}",
            max_items=scrape_config.entity_limit,
            timeout_secs=RedditLiteScraper.SCRAPE_TIMEOUT_SECS,
        )

        # Run the Actor and retrieve the scraped data.
        dataset: List[dict] = None
        try:
            dataset: List[dict] = await self.runner.run(run_config, run_input)
        except ActorRunError:
            bt.logging.error(
                f"Failed to scrape reddit using query {run_input['searches']}: {traceback.format_exc()}"
            )
            # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
            return []

        # Return the parsed results, ignoring data that can't be parsed.
        contents = self._best_effort_parse_dataset(dataset)
        return [content.to_data_entity() for content in contents]

    def _get_time_input(self, datetime: dt.datetime) -> str:
        """Returns the value of the 'time' key for a run input based on the targetted scrape time"""
        now = dt.datetime.now(tz=dt.timezone.utc)
        # For scraping requests that are almost in the past hour, look in the past 1 hour.
        if now - datetime < dt.timedelta(minutes=90):
            return "hour"
        if now - datetime < dt.timedelta(days=1):
            return "day"
        if now - datetime < dt.timedelta(days=7):
            return "week"
        if now - datetime < dt.timedelta(days=30):
            return "month"
        return "year"

    def _get_sort_input(self, datetime: dt.datetime) -> str:
        """Returns the sort to use for a scrape query based on the targeted timestamp."""
        # We are unable to scrape reddit with any date filters.
        # So instead, we'll use the "sort" field to help increase the chances that we get some data
        # from our targetted time window.
        now = dt.datetime.now(tz=dt.timezone.utc)
        if now - datetime < dt.timedelta(minutes=90):
            return "new"

        # For all other time-windows, we randomly pick one of the sort options. This in combination
        # with the chosen "time" input, should help get us data spread over time.
        return random.choice(["top", "hot", "relevance", "comments", "new"])

    def _normalize_label(self, label: DataLabel) -> str:
        """Returns the datalabel without the 'r/' prefix."""
        return DataLabel(value=label.value.removeprefix("r/"))

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> List[RedditContent]:
        """Performs a best effort parsing of Apify dataset into List[RedditContent]

        Any errors are logged and ignored."""
        results: List[RedditContent] = []
        for data in dataset:
            try:
                results.append(RedditContent(**data))
            except Exception:
                bt.logging.trace(
                    f"Failed to decode RedditContent from Apify response: {traceback.format_exc()}"
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
            run_input["maxPostCount"] = 1
        elif content.data_type == RedditDataType.COMMENT:
            run_input["searchPosts"] = False
            run_input["maxComments"] = 1
        else:
            assert (
                False
            ), "Someone forgot to update this code after adding a RedditDataType..."

        return run_input

    def _validate_reddit_content(
        self, actual_content: RedditContent, entity_to_validate: DataEntity
    ) -> ValidationResult:
        """Verifies the RedditContent is valid by the definition provided by entity."""
        content_to_validate = None
        try:
            content_to_validate = RedditContent.from_data_entity(entity_to_validate)
        except Exception:
            bt.logging.error(
                f"Failed to decode RedditContent from data entity bytes: {traceback.format_exc()}"
            )
            return ValidationResult(
                is_valid=False, reason="Failed to decode data entity"
            )

        if not actual_content.is_equivalent_to(content_to_validate):
            bt.logging.trace(
                f"RedditContent does not match: {actual_content} != {content_to_validate}"
            )
            return ValidationResult(is_valid=False, reason="Content does not match")

        # Wahey! The content is valid.
        # One final check. Does the Reddit content match the data entity information?
        try:
            actual_entity = actual_content.to_data_entity()
            if not actual_entity.matches_non_content_fields(entity_to_validate):
                return ValidationResult(
                    is_valid=False,
                    reason="The DataEntity fields are incorrect based on the Reddit content",
                )
        except Exception:
            # This shouldn't really happen, but let's safeguard against it anyway to avoid us somehow accepting
            # corrupted or malformed data.
            bt.logging.error(
                f"Failed to convert RedditContent to DataEntity: {traceback.format_exc()}"
            )
            return ValidationResult(
                is_valid=False,
                reason="Failed to convert RedditContent to DataEntity",
            )

        # At last, all checks have passed. The DataEntity is indeed valid. Nice work!
        return ValidationResult(is_valid=True, reason="Good job, you honest miner!")


async def test_scrape():
    scraper = RedditLiteScraper()

    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=3,
            date_range=DateRange(
                start=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=3),
                end=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=2),
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
    scraper = TwitterFlashScraper()

    true_entities = [
        DataEntity(
            uri="https://twitter.com/TcMMTsTc/status/1733441357090545731",
            datetime=dt.datetime(2023, 12, 9, 10, 59, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            content=b'{"username":"@TcMMTsTc","text":"\xe3\x81\xbc\xe3\x81\x8f\xe7\x9c\xa0\xe3\x81\x84\xe3\x81\xa7\xe3\x81\x99","replies":1,"retweets":0,"quotes":0,"likes":31,"url":"https://twitter.com/TcMMTsTc/status/1733441357090545731","timestamp":"2023-12-09T10:59:00Z","tweet_hashtags":[]}',
            content_size_bytes=218,
        ),
        DataEntity(
            uri="https://twitter.com/nirmaljajra2/status/1733439438473380254",
            datetime=dt.datetime(2023, 12, 9, 10, 52, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bittensor"),
            content=b'{"username":"@nirmaljajra2","text":"DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","replies":2,"retweets":0,"quotes":0,"likes":4,"url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
            content_size_bytes=484,
        ),
    ]

    # results = await scraper.validate(entities=true_entities)
    # print(f"Validation results: {results}")

    # Now modify the entities to make them invalid and check validation fails.
    good_entity = true_entities[1]
    bad_entities = [
        good_entity.model_copy(
            update={"uri": "https://twitter.com/nirmaljajra2/status/abc123"}
        ),
        good_entity.model_copy(
            update={
                "content": b'{"username":"@nirmaljajra2","text":"Random-text-insertion-DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","replies":2,"retweets":0,"quotes":0,"likes":4,"url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
            }
        ),
        good_entity.model_copy(
            update={"datetime": good_entity.datetime + dt.timedelta(seconds=1)}
        ),
        # Hashtag ordering needs to be deterministic. Verify changing the order of the hashtags makes the content non-equivalent.
        good_entity.model_copy(update={"label": DataLabel(value="#PAAl")}),
    ]

    for entity in bad_entities:
        results = await scraper.validate(entities=[entity])
        print(f"Expecting a failed validation. Result={results}")


if __name__ == "__main__":
    bt.logging()
    asyncio.run(test_scrape())
