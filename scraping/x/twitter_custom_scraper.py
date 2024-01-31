import asyncio
import threading
import traceback
import bittensor as bt
from typing import List, Optional
from common.data import DataEntity, DataLabel, DataSource
from scraping.global_counter import decrement_count, get_and_increment_count
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.x import utils
from scraping.x.model import XContent
import datetime as dt
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from datadog import statsd

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Pixel 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Pixel 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/605.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/605.1",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 YaBrowser/21.6.1.80 Yowser/2.5 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 YaBrowser/21.6.1.80 Yowser/2.5 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 YaBrowser/21.6.1.80 Yowser/2.5 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 YaBrowser/21.6.1.80 Yowser/2.5 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Pixel 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 YaBrowser/21.6.1.80 Yowser/2.5 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Pixel 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 YaBrowser/21.6.1.80 Yowser/2.5 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Chrome/91.0.4472.124 YaBrowser/21.6.1.80 Yowser/2.5 Mobile Safari/605.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Chrome/90.0.4430.212 YaBrowser/21.6.1.80 Yowser/2.5 Mobile Safari/605.1",
]


class TwitterCustomScraper(Scraper):
    """
    Scrapes tweets using Playwright.
    """

    index = 0
    lock = threading.Lock()

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a DataEntity by URI."""
        if not entities:
            return []

        # Treat the entities as guilty until proven innocent.
        results = []

        # Playwright does not support searching for multiple tweet_urls at once. So we must perform each run separately.
        for entity in entities:
            # First check the URI is a valid Twitter URL.
            if not utils.is_valid_twitter_url(entity.uri):
                results.append(
                    ValidationResult(is_valid=False, reason="Invalid URI."),
                    content_size_bytes_validated=entity.content_size_bytes,
                )
                continue

            html = None
            browser = None
            try:
                active_count = get_and_increment_count()
                statsd.gauge("active_request_count", active_count)
                statsd.gauge("Active tasks", len(asyncio.all_tasks()))
                with statsd.timed("playwright_request_time"):
                    async with async_playwright() as playwright:
                        i = 0
                        with TwitterCustomScraper.lock:
                            i = TwitterCustomScraper.index
                            TwitterCustomScraper.index = (
                                TwitterCustomScraper.index + 1
                            ) % len(user_agents)

                        chromium = playwright.chromium
                        browser = await chromium.launch()
                        context = await browser.new_context(user_agent=user_agents[i])
                        page = await context.new_page()
                        await page.goto(entity.uri)
                        await page.get_by_test_id("tweet").wait_for(timeout=15000)
                        html = await page.get_by_test_id("tweet").first.inner_html()
                        statsd.increment("twitter_microworlds", tags=["status:success"])
            except Exception as e:
                statsd.increment("twitter_microworlds", tags=["status:failure"])

                bt.logging.error(
                    f"Failed to validate entity {entity.uri}: {traceback.format_exc()}."
                )
                # This is an unfortunate situation. We have no way to distinguish a genuine failure from
                # one caused by malicious input. In my own testing I was able to make this timeout by
                # using a bad URI. As such, we have to penalize the miner here. If we didn't they could
                # pass malicious input for chunks they don't have.
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Failed to get Tweet. This can happen if the URI is invalid, or playwright is having an issue.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue
            finally:
                decrement_count()
                # Try to close the browser but swallow exceptions here.
                if browser:
                    try:
                        await browser.close()
                    except Exception as be:
                        bt.logging.trace(
                            f"Failed to close playwright browser: {traceback.format_exc()}."
                        )

            # Parse the response
            tweet = self._best_effort_parse_tweet_from_html(html, entity.uri)
            if tweet is None:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        reason="Tweet not found or is invalid.",
                        content_size_bytes_validated=entity.content_size_bytes,
                    )
                )
                continue

            # We found the tweet. Validate it.
            results.append(utils.validate_tweet_content(tweet, entity))

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""
        raise NotImplementedError(
            "Twitter custom scraper only supports validating at this time."
        )

    def _best_effort_parse_tweet_from_html(
        self, html: str, url: str
    ) -> Optional[XContent]:
        """Performs a best effort parsing of a tweets html into XContent"""

        tweet: XContent = None

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Get the username.
            username = utils.extract_user(url)

            # Get the text. We get the first text here in the case that there is a thread.
            tweet_text_element = soup.find("div", attrs={"data-testid": "tweetText"})

            tweet_text = ""

            # Only find span or images to avoid catching anchor tags and double printing hashtags.
            # TODO: consider if there is a more general approach with NavigableString check.
            for element in tweet_text_element.find_all(
                lambda tag: tag.name in ["span", "img"]
            ):
                # If this is an emoji with an alt text then include this in the text.
                if element.has_attr("alt"):
                    tweet_text += element["alt"]
                # Text defaults to empty string if not existing.
                tweet_text += element.text

            # Get the url.
            url = url

            # Get the last time element here in the case that there is a thread.
            time_element = soup.find_all("time")[-1]
            # Get the datetime attribute from the element and convert to the appropriate format.
            # It is already in utc but we need to add the utc timezone to match exactly.
            timestamp = dt.datetime.strptime(
                time_element["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=dt.timezone.utc)

            # TODO: Check for other kinds of tags?
            # Get Hashtags + Cashtags together to keep them in order.
            hashtags = [
                tag.text
                for tag in soup.find_all(
                    "a", {"href": lambda x: x and "ashtag_click" in x}
                )
            ]

            # Cashtags use $ instead of # so ensure first character is #.
            corrected_hashtags = ["#" + hashtag[1:] for hashtag in hashtags]
            unique_corrected_hashtags = list(dict.fromkeys(corrected_hashtags))

            tweet = XContent(
                username=username,
                text=tweet_text,
                url=url,
                timestamp=timestamp,
                tweet_hashtags=unique_corrected_hashtags,
            )
        except Exception:
            bt.logging.warning(
                f"Failed to decode XContent from twitter html response: {traceback.format_exc()}."
            )

        return tweet


async def test_validate():
    scraper = TwitterCustomScraper()

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
        # Test for elided long tweets.
        # DataEntity(
        #     uri="https://twitter.com/mdniy/status/1743249601925185642",
        #     datetime=dt.datetime(2024, 1, 5, 12, 34, tzinfo=dt.timezone.utc),
        #     source=DataSource.X,
        #     label=None,
        #     content='{"username":"@mdniy","text":"🗓January 6, 2024\\n0️⃣8️⃣ Days to Makar Sankranti 2024\\n📍Sun Temple, Surya Pahar, Goalpura, Assam\\n \\nDepartment of Yogic Science and Naturopathy, Mahapurusha Srimanta Sankaradeva Viswavidyalaya, Assam in collaboration with MDNIY is organizing mass Surya Namaskar Demonstration…","url":"https://twitter.com/mdniy/status/1743249601925185642","timestamp":"2024-01-05T12:34:00Z","tweet_hashtags":[]}',
        #     content_size_bytes=485,
        # ),
        DataEntity(
            uri="https://twitter.com/rEQjoewd6WfNFL3/status/1743187684422799519",
            datetime=dt.datetime(2024, 1, 5, 8, 28, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=None,
            content='{"username":"@rEQjoewd6WfNFL3","text":"ありがとうございます\\n\\nそうなんです\\nほんと偶然です\\n聞いたときはビックリしました\\n\\nいえいえ、私の記念日だなんて\\nもったいないです\\n妹の記念日にしてください\\nぷぷっ","url":"https://twitter.com/rEQjoewd6WfNFL3/status/1743187684422799519","timestamp":"2024-01-05T08:28:00Z","tweet_hashtags":[]}',
            content_size_bytes=253,
        ),
        DataEntity(
            uri="https://twitter.com/nirmaljajra2/status/1733439438473380254",
            datetime=dt.datetime(2023, 12, 9, 10, 52, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bittensor"),
            content=b'{"username":"@nirmaljajra2","text":"DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
            content_size_bytes=484,
        ),
        DataEntity(
            uri="https://twitter.com/nirmaljajra2/status/1733439438473380254",
            datetime=dt.datetime(2023, 12, 9, 10, 52, 10, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bittensor"),
            content=b'{"username":"@nirmaljajra2","text":"DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
            content_size_bytes=484,
        ),
    ]

    results = await scraper.validate(entities=true_entities)
    print(f"Validation results: {results}")

    # Now modify the entities to make them invalid and check validation fails.
    good_entity = true_entities[4]
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
            update={"datetime": good_entity.datetime + dt.timedelta(minutes=1)}
        ),
        # Hashtag ordering needs to be deterministic. Verify changing the order of the hashtags makes the content non-equivalent.
        good_entity.copy(update={"label": DataLabel(value="#PAAl")}),
    ]

    for entity in bad_entities:
        results = await scraper.validate(entities=[entity])
        print(f"Expecting a failed validation. Result={results}")


async def test_multi_validate():
    scraper = TwitterCustomScraper()

    entities = [
        DataEntity(
            uri="https://twitter.com/nirmaljajra2/status/1733439438473380254",
            datetime=dt.datetime(2023, 12, 9, 10, 52, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bittensor"),
            content=b'{"username":"@nirmaljajra2","text":"DMind has the biggest advantage of using #Bittensor APIs. \\n\\nIt means it is not controlled/Run by a centralized network but it is powered by AI P2P modules making it more decentralized\\n\\n$PAAl uses OpenAI API which is centralized \\n\\nA detailed comparison","url":"https://twitter.com/nirmaljajra2/status/1733439438473380254","timestamp":"2023-12-09T10:52:00Z","tweet_hashtags":["#Bittensor","#PAAl"]}',
            content_size_bytes=484,
        ),
        DataEntity(
            uri="https://twitter.com/TcMMTsTc/status/1733441357090545731",
            datetime=dt.datetime(2023, 12, 9, 10, 59, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=None,
            content=b'{"username":"@TcMMTsTc","text":"\xe3\x81\xbc\xe3\x81\x8f\xe7\x9c\xa0\xe3\x81\x84\xe3\x81\xa7\xe3\x81\x99","url":"https://twitter.com/TcMMTsTc/status/1733441357090545731","timestamp":"2023-12-09T10:59:00Z","tweet_hashtags":[]}',
            content_size_bytes=218,
        ),
    ]

    def validate_sync(entities):
        result = asyncio.run(scraper.validate(entities=entities))
        print(result)

    threads = [
        threading.Thread(target=validate_sync, args=(entities,)) for i in range(10)
    ]

    for t in threads:
        t.start()

    for i, t in enumerate(threads):
        t.join(timeout=300)
        print(f"Thread {i} finished.")


if __name__ == "__main__":
    asyncio.run(test_validate())
    asyncio.run(test_multi_validate())
