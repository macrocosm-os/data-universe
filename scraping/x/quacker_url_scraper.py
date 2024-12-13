import asyncio
import traceback
import bittensor as bt
from typing import List
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunner, RunConfig
from scraping.x.microworlds_scraper import test_scrape
from scraping.x.model import XContent
from scraping.x import utils
import datetime as dt


class QuackerUrlScraper(Scraper):
    """
    Scrapes tweets using the Quacker URL Scraper: https://console.apify.com/actors/KVJr35xjTw2XyvMeK.

    This scraper is not currently used.
    """

    ACTOR_ID = "KVJr35xjTw2XyvMeK"

    VALIDATE_TIMEOUT_SECS = 90

    BASE_RUN_INPUT = {
        "addUserInfo": False,
    }

    def __init__(self, runner: ActorRunner = ActorRunner()):
        self.runner = runner

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        bt.logging.info("Using Quacker URL Scraper as backup while APIDojo is nonfunctional.")
        """Validate the correctness of a DataEntity by URI."""
        if not entities:
            return []

        # Treat the entities as guilty until proven innocent.
        results = []

        # The Apify Actor is not as consistent at multiple tweet_urls at once. So we must perform each run separately.
        for entity in entities:
            # First check the URI is a valid Twitter URL.
            if not utils.is_valid_twitter_url(entity.uri):
                results.append(
                    ValidationResult(is_valid=False, reason="Invalid URI."),
                    content_size_bytes_validated=entity.content_size_bytes,
                )
                continue

        run_input = {
            **QuackerUrlScraper.BASE_RUN_INPUT,
            "startUrls": [{"url": entity.uri} for entity in entities],
            "tweetsDesired": len(entities),
        }
        run_config = RunConfig(
            actor_id=QuackerUrlScraper.ACTOR_ID,
            debug_info=f"Validate {[entity.uri for entity in entities]}",
            max_data_entities=len(entities),
            timeout_secs=QuackerUrlScraper.VALIDATE_TIMEOUT_SECS,
        )

        # Retrieve the tweet from Apify.
        dataset: List[dict] = None
        try:
            dataset: List[dict] = await self.runner.run(run_config, run_input)
        except (
            Exception
        ) as e:  # Catch all exceptions here to ensure we do not exit validation early.
            bt.logging.error(f"Failed to validate entities: {traceback.format_exc()}.")
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

        # Parse the response
        tweets = self._best_effort_parse_dataset(dataset)
        print(f"tweets: {tweets}")

        # We found the tweet. Validate it.
        for entity in entities:
            actual_tweet = None
            for tweet in tweets:
                if utils.normalize_url(tweet.url) == utils.normalize_url(entity.uri):
                    actual_tweet = tweet
                    break
            if actual_tweet is None:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Tweet not found or is invalid.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            results.append(
                utils.validate_tweet_content(
                    actual_tweet=actual_tweet,
                    entity=entity,
                    is_retweet=False # Quacker does not have an is_retweet field, give credit to all tweets for now 
                )
            )

        return results

    async def validate_hf(self, entities) -> bool:
        """Validate the correctness of a HFEntities by URL."""

        async def validate_hf_entity(entity) -> ValidationResult:
            for entity in entities:
                if not utils.is_valid_twitter_url(entity.get('url')):
                    return ValidationResult(
                        is_valid=False,
                        reason="Invalid URI.",
                        content_size_bytes_validated=0,
                    )

                attempt = 0
                max_attempts = 2

                while attempt < max_attempts:
                    # Increment attempt.
                    attempt += 1

                    # On attempt 1 we fetch the exact number of tweets. On retry we fetch more in case they are in replies.
                    tweet_count = 1 if attempt == 1 else 5

                    run_input = {
                        **QuackerUrlScraper.BASE_RUN_INPUT,
                        "startUrls": [entity.get('url')],
                        "maxItems": tweet_count,
                    }
                    run_config = RunConfig(
                        actor_id=QuackerUrlScraper.ACTOR_ID,
                        debug_info=f"Validate {entity.get('url')}",
                        max_data_entities=tweet_count,
                    )

                    # Retrieve the tweets from Apify.
                    dataset: List[dict] = None
                    try:
                        dataset: List[dict] = await self.runner.run(run_config, run_input)
                    except (
                            Exception
                    ) as e:  # Catch all exceptions here to ensure we do not exit validation early.
                        if attempt != max_attempts:
                            # Retrying.
                            continue
                        else:
                            bt.logging.error(
                                f"Failed to run actor: {traceback.format_exc()}."
                            )
                            # This is an unfortunate situation. We have no way to distinguish a genuine failure from
                            # one caused by malicious input. In my own testing I was able to make the Actor timeout by
                            # using a bad URI. As such, we have to penalize the miner here. If we didn't they could
                            # pass malicious input for chunks they don't have.
                            return ValidationResult(
                                is_valid=False,
                                reason="Failed to run Actor. This can happen if the URI is invalid, or APIfy is having an issue.",
                                content_size_bytes_validated=0,
                            )

                    # Parse the response
                    tweets = self._best_effort_parse_hf_dataset(dataset)
                    actual_tweet = None

                    for index, tweet in enumerate(tweets):
                        if utils.normalize_url(tweet['url']) == utils.normalize_url(entity.get('url')):
                            actual_tweet = tweet
                            break

                    bt.logging.debug(actual_tweet)
                    if actual_tweet is None:
                        # Only append a failed result if on final attempt.
                        if attempt == max_attempts:
                            return ValidationResult(
                                is_valid=False,
                                reason="Tweet not found or is invalid.",
                                content_size_bytes_validated=0,
                            )
                    else:
                        return utils.validate_hf_retrieved_tweet(
                            actual_tweet=actual_tweet,
                            tweet_to_verify=entity

                        )

        results = []
        for entity in entities:
            results.append(validate_hf_entity(entity))

        is_valid = utils.hf_tweet_validation(validation_results=results)
        return is_valid

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""
        raise NotImplementedError("This scraper only validates.")

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> List[XContent]:
        """Performs a best effort parsing of Apify dataset into List[XContent]

        Any errors are logged and ignored."""
        results: List[XContent] = []
        for data in dataset:
            try:
                results.append(
                    XContent(
                        username=utils.extract_user(data["url"]),
                        text=utils.sanitize_scraped_tweet(data["full_text"]),
                        url=utils.normalize_url(data["url"]),
                        timestamp=dt.datetime.strptime(
                            data["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
                        ).replace(tzinfo=dt.timezone.utc),
                        tweet_hashtags=utils.extract_hashtags(data["full_text"]),
                    )
                )
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )

        return results
    
    def _best_effort_parse_hf_dataset(self, dataset: List[dict]) -> List[dict]:
        """Performs a best effort parsing of Apify dataset into List[XContent]
        Any errors are logged and ignored."""
        if dataset == [{"zero_result": True}] or not dataset:  # Todo remove first statement if it's not necessary
            return []
        results: List[dict] = []
        i = 0
        for data in dataset:
            i = i+1
            if (
                    ("text" not in data)
                    or "url" not in data
                    or "createdAt" not in data
            ):
                continue

            text = data['text']
            url = data['url']
            results.append({"text": utils.sanitize_scraped_tweet(text),
                            "url": url,
                            "datetime": dt.datetime.strptime(
                            data["createdAt"], "%a %b %d %H:%M:%S %z %Y"
                        ),})

        return results


async def test_validate():
    scraper = QuackerUrlScraper()

    true_entities = [
        DataEntity(
            uri="https://twitter.com/HadsonNery/status/1752011223330124021",
            datetime=dt.datetime(2024, 1, 29, 16, 50, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#faleitoleve"),
            content='{"username":"@HadsonNery","text":"Se ele fosse brabo mesmo e eu estaria aqui defendendo ele, pq ele não foi direto no Davi já que a intenção dele era fazer o Davi comprar o barulho dela 🤷🏻\u200d♂️ MC fofoqueiro foi macetado pela CUNHÃ #faleitoleve","url":"https://twitter.com/HadsonNery/status/1752011223330124021","timestamp":"2024-01-29T16:50:00Z","tweet_hashtags":["#faleitoleve"]}',
            content_size_bytes=492,
        ),
        DataEntity(
            uri="https://twitter.com/TcMMTsTc/status/1733441357090545731",
            datetime=dt.datetime(2023, 12, 9, 10, 59, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=None,
            content=b'{"username":"@TcMMTsTc","text":"\xe3\x81\xbc\xe3\x81\x8f\xe7\x9c\xa0\xe3\x81\x84\xe3\x81\xa7\xe3\x81\x99","url":"https://twitter.com/TcMMTsTc/status/1733441357090545731","timestamp":"2023-12-09T10:59:00Z","tweet_hashtags":[]}',
            content_size_bytes=218,
        ),
        DataEntity(
            uri="https://twitter.com/mdniy/status/1743249601925185642",
            datetime=dt.datetime(2024, 1, 5, 12, 34, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=None,
            content='{"username":"@mdniy","text":"🗓January 6, 2024\\n0️⃣8️⃣ Days to Makar Sankranti 2024\\n📍Sun Temple, Surya Pahar, Goalpura, Assam\\n \\nDepartment of Yogic Science and Naturopathy, Mahapurusha Srimanta Sankaradeva Viswavidyalaya, Assam in collaboration with MDNIY is organizing mass Surya Namaskar Demonstration…","url":"https://twitter.com/mdniy/status/1743249601925185642","timestamp":"2024-01-05T12:34:00Z","tweet_hashtags":[]}',
            content_size_bytes=485,
        ),
        DataEntity(
            uri="https://twitter.com/rEQjoewd6WfNFL3/status/1743187684422799519",
            datetime=dt.datetime(2024, 1, 5, 8, 28, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=None,
            content='{"username":"@rEQjoewd6WfNFL3","text":"ありがとうございます\\n\\nそうなんです\\nほんと偶然です\\n聞いたときはビックリしました\\n\\nいえいえ、私の記念日だなんて\\nもったいないです\\n妹の記念日にしてください\\nぷぷっ","url":"https://twitter.com/rEQjoewd6WfNFL3/status/1743187684422799519","timestamp":"2024-01-05T08:28:00Z","tweet_hashtags":[]}',
            content_size_bytes=253,
        ),
        # DataEntity(
        #     uri="https://twitter.com/nirmaljajra2/status/1733439438473380254",
        #     datetime=dt.datetime(2023, 12, 9, 10, 52, tzinfo=dt.timezone.utc),
        #     source=DataSource.X,
        #     label=DataLabel(value="#bittensor"),
        #     content=b'{"username":"@nirmaljajra2","text":"DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
        #     content_size_bytes=484,
        # ),
        # DataEntity(
        #     uri="https://twitter.com/nirmaljajra2/status/1733439438473380254",
        #     datetime=dt.datetime(2023, 12, 9, 10, 52, 10, tzinfo=dt.timezone.utc),
        #     source=DataSource.X,
        #     label=DataLabel(value="#bittensor"),
        #     content=b'{"username":"@nirmaljajra2","text":"DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
        #     content_size_bytes=484,
        # ),
    ]

    results = await scraper.validate(entities=true_entities)
    print(f"Validation results: {results}")

    # Now modify the entities to make them invalid and check validation fails.
    # good_entity = true_entities[4]
    # bad_entities = [
    #     good_entity.copy(
    #         update={"uri": "https://twitter.com/nirmaljajra2/status/abc123"}
    #     ),
    #     good_entity.copy(
    #         update={
    #             "content": b'{"username":"@nirmaljajra2","text":"Random-text-insertion-DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
    #         }
    #     ),
    #     good_entity.copy(
    #         update={"datetime": good_entity.datetime + dt.timedelta(minutes=1)}
    #     ),
    #     # Hashtag ordering needs to be deterministic. Verify changing the order of the hashtags makes the content non-equivalent.
    #     good_entity.copy(update={"label": DataLabel(value="#PAAl")}),
    # ]

    # for entity in bad_entities:
    #     results = await scraper.validate(entities=[entity])
    #     print(f"Expecting a failed validation. Result={results}")


if __name__ == "__main__":
    bt.logging.set_trace(True)
    entities = asyncio.run(test_scrape())
    print(asyncio.run(QuackerUrlScraper().validate(entities)))
    # asyncio.run(test_validate())
