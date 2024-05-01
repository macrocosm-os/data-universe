import asyncio
import threading
import traceback
import bittensor as bt
from typing import List
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.global_counter import decrement_count, get_and_increment_count
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunner, RunConfig
from scraping.x.model import XContent
from scraping.x import utils
import datetime as dt

from datadog import statsd


class MicroworldsTwitterScraper(Scraper):
    """
    Scrapes tweets using the Microworlds Twitter Scraper: https://console.apify.com/actors/heLL6fUofdPgRXZie.
    """

    ACTOR_ID = "heLL6fUofdPgRXZie"

    SCRAPE_TIMEOUT_SECS = 120

    BASE_RUN_INPUT = {
        "maxRequestRetries": 5,
        "searchMode": "live",
    }

    # As of 2/5/24 this actor only takes 256 MB in the default config so we can run a full batch without hitting shared actor memory limits.
    concurrent_validates_semaphore = threading.BoundedSemaphore(20)

    def __init__(self, runner: ActorRunner = ActorRunner()):
        self.runner = runner

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a DataEntity by URI."""

        async def validate_entity(entity) -> ValidationResult:
            if not utils.is_valid_twitter_url(entity.uri):
                return ValidationResult(
                    is_valid=False,
                    reason="Invalid URI.",
                    content_size_bytes_validated=entity.content_size_bytes,
                )

            attempt = 0
            max_attempts = 2
            while attempt < max_attempts:
                # Increment attempt.
                attempt += 1

                # On attempt 1 we fetch the exact number of tweets. On retry we fetch more in case they are in replies.
                tweet_count = 1 if attempt == 1 else 5

                run_input = {
                    **MicroworldsTwitterScraper.BASE_RUN_INPUT,
                    "urls": [entity.uri],
                    "maxTweets": tweet_count,
                }
                run_config = RunConfig(
                    actor_id=MicroworldsTwitterScraper.ACTOR_ID,
                    debug_info=f"Validate {entity.uri}",
                    max_data_entities=tweet_count,
                )

                # Retrieve the tweets from Apify.
                dataset: List[dict] = None
                try:
                    active_count = get_and_increment_count()
                    statsd.gauge("active_request_count", active_count)
                    statsd.gauge("Active tasks", len(asyncio.all_tasks()))
                    with statsd.timed("twitter_microworlds_latency"):
                        dataset: List[dict] = await self.runner.run(
                            run_config, run_input
                        )
                        statsd.increment("twitter_microworlds", tags=["status:success"])
                except (
                    Exception
                ) as e:  # Catch all exceptions here to ensure we do not exit validation early.
                    if attempt != max_attempts:
                        # Retrying.
                        continue
                    else:
                        statsd.increment("twitter_microworlds", tags=["status:failure"])
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
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                finally:
                    decrement_count()

                # Parse the response
                tweets = self._best_effort_parse_dataset(dataset)

                actual_tweet = None
                for tweet in tweets:
                    if tweet.url == entity.uri:
                        actual_tweet = tweet
                        break
                if actual_tweet is None:
                    # Only append a failed result if on final attempt.
                    if attempt == max_attempts:
                        return ValidationResult(
                            is_valid=False,
                            reason="Tweet not found or is invalid.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                else:
                    return utils.validate_tweet_content(
                        actual_tweet=actual_tweet,
                        entity=entity,
                    )

        if not entities:
            return []

        # Since we are using the threading.semaphore we need to use it in a context outside of asyncio.
        bt.logging.trace("Acquiring semaphore for concurrent microworlds validations.")
        with MicroworldsTwitterScraper.concurrent_validates_semaphore:
            bt.logging.trace(
                "Acquired semaphore for concurrent microworlds validations."
            )
            results = await asyncio.gather(
                *[validate_entity(entity) for entity in entities]
            )

        valid = sum(1 for result in results if result.is_valid)
        invalid = sum(1 for result in results if not result.is_valid)
        statsd.increment("twitter_validate.valid", valid)
        statsd.increment("twitter_validate.invalid", invalid)

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
            **MicroworldsTwitterScraper.BASE_RUN_INPUT,
            "searchTerms": [query],
            "maxTweets": max_items,
        }

        run_config = RunConfig(
            actor_id=MicroworldsTwitterScraper.ACTOR_ID,
            debug_info=f"Scrape {query}",
            max_data_entities=scrape_config.entity_limit,
            timeout_secs=MicroworldsTwitterScraper.SCRAPE_TIMEOUT_SECS,
        )

        bt.logging.trace(f"Performing Twitter scrape for search terms: {query}.")

        # Run the Actor and retrieve the scraped data.
        dataset: List[dict] = None
        try:
            dataset: List[dict] = await self.runner.run(run_config, run_input)
        except Exception:
            bt.logging.error(
                f"Failed to scrape tweets using search terms {query}: {traceback.format_exc()}."
            )
            # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
            return []

        # Return the parsed results, ignoring data that can't be parsed.
        x_contents = self._best_effort_parse_dataset(dataset)
        bt.logging.success(
            f"Completed scrape for {query}. Scraped {len(x_contents)} items."
        )

        data_entities = []

        for x_content in x_contents:
            data_entities.append(XContent.to_data_entity(content=x_content))

        return data_entities

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> List[XContent]:
        """Performs a best effort parsing of Apify dataset into List[XContent]

        Any errors are logged and ignored."""
        if dataset == [{"zero_result": True}]:
            return []

        results: List[XContent] = []
        for data in dataset:
            try:
                # Check that we have the required fields.
                if (
                    ("full_text" not in data and "truncated_full_text" not in data)
                    or "url" not in data
                    or "created_at" not in data
                ):
                    continue

                # Truncated_full_text is only populated if "full_text" is truncated.
                text = (
                    data["truncated_full_text"]
                    if "truncated_full_text" in data and data["truncated_full_text"]
                    else data["full_text"]
                )

                # Microworlds returns cashtags separately under symbols.
                # These are returned as list of dicts where the indices key is the first/last index and text is the tag.
                # If there are no hashtags or cashtags they are empty lists.
                hashtags = (
                    data["entities"]["hashtags"]
                    if "entities" in data and "hashtags" in data["entities"]
                    else []
                )
                cashtags = (
                    data["entities"]["symbols"]
                    if "entities" in data and "symbols" in data["entities"]
                    else []
                )

                sorted_tags = sorted(hashtags + cashtags, key=lambda x: x["indices"][0])

                tags = ["#" + item["text"] for item in sorted_tags]

                results.append(
                    XContent(
                        username=utils.extract_user(data["url"]),
                        text=utils.sanitize_scraped_tweet(text),
                        url=data["url"],
                        timestamp=dt.datetime.strptime(
                            data["created_at"], "%a %b %d %H:%M:%S %z %Y"
                        ),
                        tweet_hashtags=tags,
                    )
                )
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )

        return results


async def test_scrape():
    scraper = MicroworldsTwitterScraper()

    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=5,
            date_range=DateRange(
                start=dt.datetime(2024, 1, 30, 0, 0, 0, tzinfo=dt.timezone.utc),
                end=dt.datetime(2024, 1, 30, 9, 0, 0, tzinfo=dt.timezone.utc),
            ),
            labels=[DataLabel(value="#bittensor"), DataLabel(value="#btc")],
        )
    )

    print(f"Scraped {len(entities)} entities: {entities}")

    return entities


async def test_validate():
    scraper = MicroworldsTwitterScraper()

    true_entities = [
        DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"]}',
            content_size_bytes=318,
        ),
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
        DataEntity(
            uri="https://twitter.com/Sid14290237375/status/1760088426400162274",
            datetime=dt.datetime(2024, 2, 20, 23, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#HowlongcanImakeahashtaganywayIg"),
            content='{"username":"@Sid14290237375","text":"Testing hashtags\\n\\n#HowlongcanImakeahashtaganywayIguessthatthiswillbeagoodtest","url":"https://twitter.com/Sid14290237375/status/1760088426400162274","timestamp":"2024-02-20T23:45:00Z","tweet_hashtags":["#HowlongcanImakeahashtaganywayIguessthatthiswillbeagoodtest"]}',
            content_size_bytes=356,
        ),
        # Entity with a latin capital I with a dot above that becomes 2 characters when .lower() is used on it.
        DataEntity(
            uri="https://twitter.com/DervisMusa/status/1761758719941988688",
            datetime=dt.datetime(2024, 2, 25, 14, 23, 5, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#i̇srailleticaretfilistinei̇hane"),
            content='{"username": "@DervisMusa", "text": "\\"\\u0130srail\'le ticaret, Filistin\'e ihanet!\\"\\n(\\u0627\\u0644\\u062a\\u062c\\u0627\\u0631\\u0629 \\u0645\\u0639 \\u0625\\u0633\\u0631\\u0627\\u0626\\u064a\\u0644 \\u062a\\u062e\\u0648\\u0646 \\u0641\\u0644\\u0633\\u0637\\u064a\\u0646)\\n\\nAllah kabul etsin. Aya\\u011f\\u0131n\\u0131za / y\\u00fcre\\u011finize sa\\u011fl\\u0131k. Herkese \\u00f6rnek olur in\\u015fallah.\\n\\n#\\u0130srailleTicaretFilistine\\u0130hanet\\n\\n#\\u0637\\u0648\\u0641\\u0627\\u0646_\\u0627\\u0644\\u0623\\u0642\\u0635\\u0649 \\n#\\u0641\\u0644\\u0633\\u0637\\u064a\\u0646 \\n#\\u063a\\u0632\\u0629_\\u062a\\u0646\\u062a\\u0635\\u0631 \\n#\\u0627\\u0644\\u064a\\u0645\\u0646\\n#Hamas\\n#deprem", "url": "https://twitter.com/DervisMusa/status/1761758719941988688", "timestamp": "2024-02-25T14:23:05+00:00", "tweet_hashtags": ["#\\u0130srailleTicaretFilistine\\u0130hanet", "#\\u0637\\u0648\\u0641\\u0627\\u0646_\\u0627\\u0644\\u0623\\u0642\\u0635\\u0649", "#\\u0641\\u0644\\u0633\\u0637\\u064a\\u0646", "#\\u063a\\u0632\\u0629_\\u062a\\u0646\\u062a\\u0635\\u0631", "#\\u0627\\u0644\\u064a\\u0645\\u0646", "#Hamas", "#deprem"]}',
            content_size_bytes=1072,
        ),
    ]

    results = await scraper.validate(entities=true_entities)
    print(f"Validation results: {results}")


async def test_multi_thread_validate():
    scraper = MicroworldsTwitterScraper()

    true_entities = [
        DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"]}',
            content_size_bytes=318,
        ),
        DataEntity(
            uri="https://twitter.com/HadsonNery/status/1752011223330124021",
            datetime=dt.datetime(2024, 1, 29, 16, 50, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#faleitoleve"),
            content='{"username":"@HadsonNery","text":"Se ele fosse brabo mesmo e eu estaria aqui defendendo ele, pq ele não foi direto no Davi já que a intenção dele era fazer o Davi comprar o barulho dela 🤷🏻\u200d♂️ MC fofoqueiro foi macetado pela CUNHÃ #faleitoleve","url":"https://twitter.com/HadsonNery/status/1752011223330124021","timestamp":"2024-01-29T16:50:00Z","tweet_hashtags":["#faleitoleve"]}',
            content_size_bytes=492,
        ),
    ]

    def sync_validate(entities: list[DataEntity]) -> None:
        """Synchronous version of eval_miner."""
        asyncio.run(scraper.validate(entities))

    threads = [
        threading.Thread(target=sync_validate, args=(true_entities,)) for _ in range(5)
    ]

    for thread in threads:
        thread.start()

    for t in threads:
        t.join(120)


if __name__ == "__main__":
    bt.logging.set_trace(True)
    asyncio.run(test_multi_thread_validate())
    asyncio.run(test_scrape())
    asyncio.run(test_validate())
