import asyncio
import threading
import traceback
import bittensor as bt
from typing import List, Tuple
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult, HFValidationResult
from scraping.x.model import XContent
from scraping.x import utils
import datetime as dt
import aiohttp
import json
import pytz  # Add this import for timezone handling


class MyApiTwitterScraper(Scraper):
    """
    Scrapes tweets using the My API Twitter Scraper.
    """
    SCRAPE_TIMEOUT_SECS = 120

    BASE_RUN_INPUT = {
        "maxRequestRetries": 5
    }

    API_URL = "http://45.250.255.84:8080/api/v1/data/twitter/tweets/recent"

    # As of 2/5/24 this actor only takes 256 MB in the default config so we can run a full batch without hitting shared actor memory limits.
    concurrent_validates_semaphore = threading.BoundedSemaphore(20)


    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """Validate the correctness of a list of HF retrieved data."""
        # Implement if needed
        return True
    
    async def validate_hf(self, entities) -> bool:
        """Validate the correctness of a list of HF retrieved data."""
        # Implement if needed
        return True

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config."""
        # Construct the query string
        date_format = "%Y-%m-%d_%H:%M:%S_UTC"
        query_parts = []

        # Add date range
        query_parts.append(
            f"since:{scrape_config.date_range.start.astimezone(tz=dt.timezone.utc).strftime(date_format)}"
        )
        query_parts.append(
            f"until:{scrape_config.date_range.end.astimezone(tz=dt.timezone.utc).strftime(date_format)}"
        )

        # Handle labels - separate usernames and keywords
        if scrape_config.labels:
            username_labels = []
            keyword_labels = []

            for label in scrape_config.labels:
                if label.value.startswith('@'):
                    username = label.value[1:]
                    username_labels.append(f"from:{username}")
                else:
                    keyword_labels.append(label.value)

            if username_labels:
                query_parts.append(f"({' OR '.join(username_labels)})")
            if keyword_labels:
                query_parts.append(f"({' OR '.join(keyword_labels)})")
        else:
            query_parts.append("e")

        query = " ".join(query_parts)

        payload = {
            "query": query,
            "count": scrape_config.entity_limit or 10,
        }

        bt.logging.info(f"Sending request to My API with payload: {payload}")
        print("@@X.payload: ", payload)
        print("==============================")

        max_retries = self.BASE_RUN_INPUT.get("maxRequestRetries", 5)
        for attempt in range(max_retries):
            bt.logging.info(f"@@attempt: {attempt}")
            print("@@attempt: ", attempt)
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.SCRAPE_TIMEOUT_SECS)) as session:
                    async with session.post(self.API_URL, json=payload) as response:
                        response_text = await response.text()
                        bt.logging.info(f"Response data: {response_text}")
                        
                        if response.status != 200:
                            bt.logging.error(f"API request failed with status {response.status}")
                            continue

                        try:
                            data = json.loads(response_text)
                            if not data or "data" not in data or data.get("data") is None:
                                bt.logging.warning("API returned empty or invalid data.")
                                continue
                        except json.JSONDecodeError as e:
                            bt.logging.error(f"Failed to parse JSON response: {e}")
                            continue

                        # Parse the dataset
                        tweets = []
                        for data in (data.get("data") or []):
                            if "Tweet" in data and data["Tweet"] is not None:
                                # Replace "twitter.com" with "x.com" in PermanentURL
                                if "PermanentURL" in data["Tweet"]:
                                    data["Tweet"]["PermanentURL"] = data["Tweet"]["PermanentURL"].replace("twitter.com", "x.com")
                                
                                if "Hashtags" not in data["Tweet"]:
                                    data["Tweet"]["Hashtags"] = []
                        return self._best_effort_parse_dataset(tweets)
            except Exception as e:
                bt.logging.error(f"Attempt {attempt + 1} failed: {traceback.format_exc()}")
                if attempt == max_retries - 1:
                    bt.logging.error("Max retries reached. Failing scrape.")
        return []

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> Tuple[List[XContent], List[bool]]:
        """Performs a best effort parsing of Masa dataset into List[XContent]

        Any errors are logged and ignored."""
        if not dataset:  # Todo remove first statement if it's not necessary
            return []

        results: List[XContent] = []
        is_retweets: List[bool] = []
        for data in dataset:
            try:
                if (
                        ("Text" not in data)
                        or "PermanentURL" not in data
                        or "Timestamp" not in data
                ):
                    continue

                hashtags = data.get("Hashtags", [])
                tags = [f"#{tag}" for tag in hashtags]

                is_retweet = data.get('IsRetweet', False)
                is_retweets.append(is_retweet)
                results.append(
                    XContent(
                        username=data['Username'],  # utils.extract_user(data["url"]),
                        text=utils.sanitize_scraped_tweet(data['Text']),
                        url=data["PermanentURL"],
                        timestamp=dt.datetime.fromtimestamp(data["Timestamp"], tz=dt.timezone.utc),
                        tweet_hashtags=tags,
                    )
                )
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )
        data_entities = []
        for x_content in results:
            data_entities.append(XContent.to_data_entity(content=x_content))
        
        return data_entities

    def _best_effort_parse_hf_dataset(self, dataset: List[dict]) -> List[dict]:
        """Performs a best effort parsing of Apify dataset into List[XContent]
        Any errors are logged and ignored."""
        if not dataset:  # Todo remove first statement if it's not necessary
            return []
        results: List[dict] = []
        i = 0
        for data in dataset:
            i = i + 1
            if (
                    ("Text" not in data)
                    or "PermanentURL" not in data
                    or "Timestamp" not in data
            ):
                continue

            text = data['Text']
            url = data['PermanentURL']
            results.append({
                "text": utils.sanitize_scraped_tweet(text),
                "url": url,
                "datetime": dt.datetime.fromtimestamp(data["Timestamp"], tz=dt.timezone.utc), 
            })

        return results


async def test_scrape():
    scraper = MyApiTwitterScraper()

    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=100,
            date_range=DateRange(
                start=dt.datetime(2024, 5, 27, 0, 0, 0, tzinfo=dt.timezone.utc),
                end=dt.datetime(2024, 5, 27, 9, 0, 0, tzinfo=dt.timezone.utc),
            ),
            labels=[DataLabel(value="#bittgergnergerojngoierjgensor")],
        )
    )

    return entities

if __name__ == "__main__":
    bt.logging.set_trace(True)
    # asyncio.run(test_multi_thread_validate())
    # asyncio.run(test_scrape())\
