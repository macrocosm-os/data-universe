import asyncio
import traceback
import bittensor as bt
from typing import List
from common.data import DataEntity, DataLabel
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunError, ActorRunner, RunConfig
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
        raise NotImplementedError("Validation is not supported on this scraper.")

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""
        # Construct the query string.
        search_terms = []
        if scrape_config.labels:
            search_terms.extend([label.value for label in scrape_config.labels])
        else:
            # If no label is specified, just search for "e", the most common letter in the English alphabet.
            # I attempted using "#" instead, but that still returned empty results ¯\_(ツ)_/¯
            search_terms.append("e")

        # Construct the input to the runner.
        max_items = scrape_config.entity_limit or 150
        run_input = {
            **MicroworldsTwitterScraper.BASE_RUN_INPUT,
            "searchTerms": search_terms,
            "maxTweets": max_items,
        }
        run_config = RunConfig(
            actor_id=MicroworldsTwitterScraper.ACTOR_ID,
            debug_info=f"Scrape {search_terms}",
            max_data_entities=scrape_config.entity_limit,
            timeout_secs=MicroworldsTwitterScraper.SCRAPE_TIMEOUT_SECS,
        )

        bt.logging.trace(f"Performing Twitter scrape for search terms: {search_terms}.")

        # Run the Actor and retrieve the scraped data.
        dataset: List[dict] = None
        try:
            dataset: List[dict] = await self.runner.run(run_config, run_input)
        except ActorRunError:
            bt.logging.error(
                f"Failed to scrape tweets using search terms {search_terms}: {traceback.format_exc()}."
            )
            # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
            return []

        # Return the parsed results, ignoring data that can't be parsed.
        x_contents = self._best_effort_parse_dataset(dataset)
        bt.logging.success(
            f"Completed scrape for {search_terms}. Scraped {len(x_contents)} items."
        )

        return [XContent.to_data_entity(x_content) for x_content in x_contents]

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
                        url=data["url"],
                        timestamp=dt.datetime.strptime(
                            data["created_at"], "%a %b %d %H:%M:%S %z %Y"
                        ),
                        tweet_hashtags=utils.extract_hashtags(data["full_text"]),
                    )
                )
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )

        print(results)
        return results


async def test_scrape():
    scraper = MicroworldsTwitterScraper()

    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=5,
            date_range=DateRange(
                start=dt.datetime(2023, 12, 9, 10, 0, 0, tzinfo=dt.timezone.utc),
                end=dt.datetime(2023, 12, 9, 11, 0, 0, tzinfo=dt.timezone.utc),
            ),
            labels=[DataLabel(value="#bittensor"), DataLabel(value="#TAO")],
        )
    )

    print(f"Scraped {len(entities)} entities: {entities}")


# async def test_validate():
#     scraper = TwitterURLScraper()

#     true_entities = [
#         DataEntity(
#             uri="https://twitter.com/TcMMTsTc/status/1733441357090545731",
#             datetime=dt.datetime(2023, 12, 9, 10, 59, tzinfo=dt.timezone.utc),
#             source=DataSource.X,
#             content=b'{"username":"@TcMMTsTc","text":"\xe3\x81\xbc\xe3\x81\x8f\xe7\x9c\xa0\xe3\x81\x84\xe3\x81\xa7\xe3\x81\x99","url":"https://twitter.com/TcMMTsTc/status/1733441357090545731","timestamp":"2023-12-09T10:59:00Z","tweet_hashtags":[]}',
#             content_size_bytes=218,
#         ),
#         DataEntity(
#             uri="https://twitter.com/nirmaljajra2/status/1733439438473380254",
#             datetime=dt.datetime(2023, 12, 9, 10, 52, tzinfo=dt.timezone.utc),
#             source=DataSource.X,
#             label=DataLabel(value="#bittensor"),
#             content=b'{"username":"@nirmaljajra2","text":"DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
#             content_size_bytes=484,
#         ),
#     ]

#     results = await scraper.validate(entities=true_entities)
#     print(f"Validation results: {results}")

#     # Now modify the entities to make them invalid and check validation fails.
#     good_entity = true_entities[1]
#     bad_entities = [
#         good_entity.copy(
#             update={"uri": "https://twitter.com/nirmaljajra2/status/abc123"}
#         ),
#         good_entity.copy(
#             update={
#                 "content": b'{"username":"@nirmaljajra2","text":"Random-text-insertion-DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
#             }
#         ),
#         good_entity.copy(
#             update={"datetime": good_entity.datetime + dt.timedelta(seconds=1)}
#         ),
#         # Hashtag ordering needs to be deterministic. Verify changing the order of the hashtags makes the content non-equivalent.
#         good_entity.copy(update={"label": DataLabel(value="#PAAl")}),
#     ]

#     for entity in bad_entities:
#         results = await scraper.validate(entities=[entity])
#         print(f"Expecting a failed validation. Result={results}")


if __name__ == "__main__":
    asyncio.run(test_scrape())
    # asyncio.run(test_validate())
