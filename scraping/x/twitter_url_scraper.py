import asyncio
import re
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


class TwitterURLScraper(Scraper):
    """
    Scrapes tweets using the Twitter URL Scraper: https://console.apify.com/actors/KVJr35xjTw2XyvMeK.
    """

    ACTOR_ID = "KVJr35xjTw2XyvMeK"

    SCRAPE_TIMEOUT_SECS = 120

    BASE_RUN_INPUT = {
        "addUserInfo": False,
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
                **TwitterURLScraper.BASE_RUN_INPUT,
                "startUrls": [
                    {"url": entity.uri},
                ],
                "tweetsDesired": 1,
            }
            run_config = RunConfig(
                actor_id=TwitterURLScraper.ACTOR_ID,
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
            results.append(TwitterURLScraper._validate_tweet(actual_tweet, entity))

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""
        raise NotImplementedError("Scraping is not supported on this scraper.")

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> List[XContent]:
        """Performs a best effort parsing of Apify dataset into List[XContent]

        Any errors are logged and ignored."""
        results: List[XContent] = []
        for data in dataset:
            try:
                results.append(
                    XContent(
                        username=self.extract_user(data["url"]),
                        text=self.sanitize_text(data["full_text"]),
                        url=data["url"],
                        timestamp=dt.datetime.strptime(
                            data["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
                        ).replace(second=0, tzinfo=dt.timezone.utc),
                        tweet_hashtags=self.extract_hashtags(data["full_text"]),
                    )
                )
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )
        return results

    def extract_user(self, url: str) -> str:
        """Extracts the twitter user from the URL and returns it in the expected format."""
        pattern = r"https://twitter.com/(\w+)/status/.*"
        if re.match(pattern, url):
            return f"@{re.match(pattern, url).group(1)}"
        return ValueError(f"Unable to extract user from {url}")

    def extract_hashtags(self, text: str) -> List[str]:
        """Given a tweet, extracts the hashtags in the order they appear in the tweet."""
        hashtags = []
        for word in text.split():
            if word.startswith("#"):
                hashtags.append(word)
        return hashtags

    def sanitize_text(self, text: str) -> str:
        """Removes any image/media links from the tweet"""
        pattern = r"\s*https://t.co/[^\s]+\s*"  # Matches any link to a twitter image
        return re.sub(pattern, "", text)

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
    scraper = TwitterURLScraper()

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
    scraper = TwitterURLScraper()

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
    # asyncio.run(test_scrape())
    asyncio.run(test_validate())
