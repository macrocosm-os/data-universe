import asyncio
import traceback
import bittensor as bt
from typing import List
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunner, RunConfig
from scraping.x.model import XContent
from scraping.x import utils
import datetime as dt


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
                            content_size_bytes_validated=entity.content_size_bytes,
                        )

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
                    return utils.validate_tweet_content(actual_tweet, entity)

        if not entities:
            return []

        results = await asyncio.gather(
            *[validate_entity(entity) for entity in entities]
        )

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

        return [XContent.to_data_entity(x_content) for x_content in x_contents]

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
                results.append(
                    XContent(
                        username=utils.extract_user(data["url"]),
                        text=utils.sanitize_scraped_tweet(text),
                        url=data["url"],
                        timestamp=dt.datetime.strptime(
                            data["created_at"], "%a %b %d %H:%M:%S %z %Y"
                        ),
                        tweet_hashtags=utils.extract_hashtags(text),
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
    ]

    results = await scraper.validate(entities=true_entities)
    print(f"Validation results: {results}")


if __name__ == "__main__":
    bt.logging.set_trace(True)
    asyncio.run(test_scrape())
    asyncio.run(test_validate())