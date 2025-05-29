import asyncio
import threading
import traceback
import bittensor as bt
from typing import List, Tuple
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult, HFValidationResult
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
                tweets, is_retweets = self._best_effort_parse_dataset(dataset)
                actual_tweet = None

                for index, tweet in enumerate(tweets):
                    if utils.normalize_url(tweet.url) == utils.normalize_url(entity.uri):
                        actual_tweet = tweet
                        is_retweet = is_retweets[index]
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
                        is_retweet=is_retweet,
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

        return results
    
    async def validate_hf(self, entities) -> HFValidationResult:
        """Validate the correctness of a HFEntities by URL."""

        async def validate_hf_entity(entity) -> ValidationResult:
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
                    **MicroworldsTwitterScraper.BASE_RUN_INPUT,
                    "startUrls": [entity.get('url')],
                    "maxItems": tweet_count,
                }
                run_config = RunConfig(
                    actor_id=MicroworldsTwitterScraper.ACTOR_ID,
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

        # Since we are using the threading.semaphore we need to use it in a context outside of asyncio.
        bt.logging.trace("Acquiring semaphore for concurrent apidojo validations.")

        with MicroworldsTwitterScraper.concurrent_validates_semaphore:
            bt.logging.trace(
                "Acquired semaphore for concurrent apidojo validations."
            )
            results = await asyncio.gather(
                *[validate_hf_entity(entity) for entity in entities]
            )

        bt.logging.info(f'HF validation result: {results}')
        is_valid, valid_percent = utils.hf_tweet_validation(validation_results=results)
        return HFValidationResult(is_valid=is_valid, validation_percentage=valid_percent, reason=f"Validation Percentage = {valid_percent}")

    async def validate_hf(self, entities) -> HFValidationResult:
        """Validate the correctness of a HFEntities by URL."""

        async def validate_hf_entity(entity) -> ValidationResult:
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
                    **MicroworldsTwitterScraper.BASE_RUN_INPUT,
                    "startUrls": [entity.get('url')],
                    "maxItems": tweet_count,
                }
                run_config = RunConfig(
                    actor_id=MicroworldsTwitterScraper.ACTOR_ID,
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

        # Since we are using the threading.semaphore we need to use it in a context outside of asyncio.
        bt.logging.trace("Acquiring semaphore for concurrent apidojo validations.")

        with MicroworldsTwitterScraper.concurrent_validates_semaphore:
            bt.logging.trace(
                "Acquired semaphore for concurrent apidojo validations."
            )
            results = await asyncio.gather(
                *[validate_hf_entity(entity) for entity in entities]
            )

        is_valid = utils.hf_tweet_validation(validation_results=results)
        return is_valid

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

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> Tuple[List[XContent], List[bool]]:
        """Performs a best effort parsing of Apify dataset into List[XContent]
        Any errors are logged and ignored."""
        if not dataset or dataset == [{"zero_result": True}]:
            return [], []

        results: List[XContent] = []
        is_retweets: List[bool] = []

        for data in dataset:
            try:
                # Check that we have the required fields
                if not all(field in data for field in ["full_text", "user", "created_at"]):
                    bt.logging.warning("Missing required fields in data")
                    continue

                text = data['full_text']

                # Extract hashtags from text or user entities if available
                tags = []
                user_data = data.get('user', {})
                user_entities = user_data.get('entities', {})
                if 'hashtags' in user_entities:
                    tags = ["#" + item['text'] for item in user_entities['hashtags']]
                
                # Extract media URLs - check multiple possible locations
                media_urls = []
                
                # check entities.media (deprecated in microworlds scraper)
                if 'entities' in data and 'media' in data['entities']:
                    for media_item in data['entities']['media']:
                        if 'media_url_https' in media_item:
                            media_urls.append(media_item['media_url_https'])

                is_retweet = data.get('retweeted', False)
                is_retweets.append(is_retweet)
                
                # Extract user information - user_id_str if available, or id_str
                user_id = data.get('user_id_str') or user_data.get('id_str')
                user_display_name = user_data.get('name', '')
                user_verified = user_data.get('verified', False)
                username = user_data.get('screen_name', '')
                
                # Extract tweet metadata
                tweet_id = data.get('id_str')
                
                # Extract reply information
                is_reply = data.get('in_reply_to_status_id_str', None)
                is_quote = data.get('is_quote_status', None)
                
                # Get conversation ID
                conversation_id = data.get('conversation_id_str')
                
                # Get in-reply-to information
                in_reply_to_user_id = data.get('in_reply_to_user_id_str')

                # Create URL if not present (construct from tweet ID)
                url = data.get('url')
                if not url and tweet_id and username:
                    url = f"https://twitter.com/{username}/status/{tweet_id}"

                results.append(
                    XContent(
                        username=username,
                        text=utils.sanitize_scraped_tweet(text),
                        url=url,
                        timestamp=dt.datetime.strptime(
                            data["created_at"], "%a %b %d %H:%M:%S %z %Y"
                        ),
                        tweet_hashtags=tags,
                        media=media_urls if media_urls else None,
                        # Enhanced fields
                        user_id=user_id,
                        user_display_name=user_display_name,
                        user_verified=user_verified,
                        # Non-dynamic tweet metadata
                        tweet_id=tweet_id,
                        is_reply=is_reply,
                        is_quote=is_quote,
                        # Additional metadata
                        conversation_id=conversation_id,
                        in_reply_to_user_id=in_reply_to_user_id,
                    )
                )
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )

        return results, is_retweets


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
        # DataEntity(
        #     uri="https://twitter.com/0xedeon/status/1790788053960667309",
        #     datetime=dt.datetime(2024, 5, 15, 16, 55, 17, tzinfo=dt.timezone.utc),
        #     source=DataSource.X,
        #     label=DataLabel(value="#cryptocurrency"),
        #     content='{"username": "@0xedeon", "text": "Deux frères ont manipulé les protocoles Ethereum pour voler 25M $ selon le Département de la Justice 🕵️‍♂️💰 #Cryptocurrency #JusticeDept", "url": "https://twitter.com/0xedeon/status/1790788053960667309", "timestamp": "2024-05-15T16:55:00+00:00", "tweet_hashtags": ["#Cryptocurrency", "#JusticeDept"]}',
        #     content_size_bytes=391
        # ),
        DataEntity(
            uri="https://x.com/100Xpotential/status/1790785842967101530",
            datetime=dt.datetime(2024, 5, 15, 16, 46, 30, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#catcoin"),
            content='{"username": "@100Xpotential", "text": "As i said green candles incoming 🚀🫡👇👇\\n\\nAround 15% price surge in #CatCoin 📊💸🚀🚀\\n\\n𝐂𝐨𝐦𝐦𝐞𝐧𝐭 |  𝐋𝐢𝐤𝐞 |  𝐑𝐞𝐭𝐰𝐞𝐞𝐭 |  𝐅𝐨𝐥𝐥𝐨𝐰\\n\\n#Binance #Bitcoin #PiNetwork #Blockchain #NFT #BabyDoge #Solana #PEPE #Crypto #1000x #cryptocurrency #Catcoin #100x", "url": "https://twitter.com/100Xpotential/status/1790785842967101530", "timestamp": "2024-05-15T16:46:00+00:00", "tweet_hashtags": ["#CatCoin", "#Binance", "#Bitcoin", "#PiNetwork", "#Blockchain", "#NFT", "#BabyDoge", "#Solana", "#PEPE", "#Crypto", "#1000x", "#cryptocurrency", "#Catcoin", "#100x"]}',
            content_size_bytes=933
        ),
        DataEntity(
            uri="https://x.com/20nineCapitaL/status/1789488160688541878",
            datetime=dt.datetime(2024, 5, 12, 2, 49, 59, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@20nineCapitaL", "text": "Yup! We agreed to. \\n\\n@MetaMaskSupport #Bitcoin #Investors #DigitalAssets #EthereumETF #Airdrops", "url": "https://twitter.com/20nineCapitaL/status/1789488160688541878", "timestamp": "2024-05-12T02:49:00+00:00", "tweet_hashtags": ["#Bitcoin", "#Investors", "#DigitalAssets", "#EthereumETF", "#Airdrops"]}',
            content_size_bytes=345
        ),
        DataEntity(
            uri="https://x.com/AAAlviarez/status/1790787185047658838",
            datetime=dt.datetime(2024, 5, 15, 16, 51, 50, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#web3‌‌"),
            content='{"username": "@AAAlviarez", "text": "1/3🧵\\n\\nOnce a month dozens of #web3‌‌  users show our support to one of the projects that is doing an excellent job in services and #cryptocurrency adoption.\\n\\nDo you know what Leo Power Up Day is all about?", "url": "https://twitter.com/AAAlviarez/status/1790787185047658838", "timestamp": "2024-05-15T16:51:00+00:00", "tweet_hashtags": ["#web3‌‌", "#cryptocurrency"]}',
            content_size_bytes=439
        ),
        DataEntity(
            uri="https://x.com/AGariaparra/status/1789488091453091936",
            datetime=dt.datetime(2024, 5, 12, 2, 49, 42, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AGariaparra", "text": "J.P Morgan, Wells Fargo hold #Bitcoin now: Why are they interested in BTC? - AMBCrypto", "url": "https://twitter.com/AGariaparra/status/1789488091453091936", "timestamp": "2024-05-12T02:49:00+00:00", "tweet_hashtags": ["#Bitcoin"]}',
            content_size_bytes=269
        ),
        DataEntity(
            uri="https://x.com/AGariaparra/status/1789488427546939525",
            datetime=dt.datetime(2024, 5, 12, 2, 51, 2, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AGariaparra", "text": "We Asked ChatGPT if #Bitcoin Will Enter a Massive Bull Run in 2024", "url": "https://twitter.com/AGariaparra/status/1789488427546939525", "timestamp": "2024-05-12T02:51:00+00:00", "tweet_hashtags": ["#Bitcoin"]}',
            content_size_bytes=249
        ),
        DataEntity(
            uri="https://x.com/AMikulanecs/status/1784324497895522673",
            datetime=dt.datetime(2024, 4, 27, 20, 51, 26, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#felix"),
            content='{"username": "@AMikulanecs", "text": "$FELIX The new Dog with OG Vibes... \\nWe have a clear vision for success.\\nNew Dog $FELIX \\n➡️Follow @FelixInuETH \\n➡️Join➡️Visit#memecoins #BTC #MemeCoinSeason #Bullrun2024 #Ethereum #altcoin #Crypto #meme #SOL #BaseChain #Binance", "url": "https://twitter.com/AMikulanecs/status/1784324497895522673", "timestamp": "2024-04-27T20:51:00+00:00", "tweet_hashtags": ["#FELIX", "#FELIX", "#memecoins", "#BTC", "#MemeCoinSeason", "#Bullrun2024", "#Ethereum", "#altcoin", "#Crypto", "#meme", "#SOL", "#BaseChain", "#Binance"]}',
            content_size_bytes=588
        ),
        DataEntity(
            uri="https://x.com/AdamEShelton/status/1789490040751411475",
            datetime=dt.datetime(2024, 5, 12, 2, 57, 27, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AdamEShelton", "text": "#bitcoin  love", "url": "https://twitter.com/AdamEShelton/status/1789490040751411475", "timestamp": "2024-05-12T02:57:00+00:00", "tweet_hashtags": ["#bitcoin"]}',
            content_size_bytes=199
        ),
        DataEntity(
            uri="https://x.com/AfroWestor/status/1789488798406975580",
            datetime=dt.datetime(2024, 5, 12, 2, 52, 31, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AfroWestor", "text": "Given is for Prince and princess form inheritances  to kingdom. \\n\\nWe the #BITCOIN family we Gain profits for ever. \\n\\nSo if you embrace #BTC that means you have a Kingdom to pass on for ever.", "url": "https://twitter.com/AfroWestor/status/1789488798406975580", "timestamp": "2024-05-12T02:52:00+00:00", "tweet_hashtags": ["#BITCOIN", "#BTC"]}',
            content_size_bytes=383
        ),
        DataEntity(
            uri="https://x.com/AlexEmidio7/status/1789488453979189327",
            datetime=dt.datetime(2024, 5, 12, 2, 51, 9, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AlexEmidio7", "text": "Bip47 V3 V4 #Bitcoin", "url": "https://twitter.com/AlexEmidio7/status/1789488453979189327", "timestamp": "2024-05-12T02:51:00+00:00", "tweet_hashtags": ["#Bitcoin"]}',
            content_size_bytes=203
        ),
    ]

    results = await scraper.validate(entities=true_entities)
    print(f"Validation results: {results}")


async def test_multi_thread_validate():
    scraper = MicroworldsTwitterScraper()

    true_entities = [
        DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"]}',
            content_size_bytes=281,
        ),
        DataEntity(
            uri="https://twitter.com/HadsonNery/status/1752011223330124021",
            datetime=dt.datetime(2024, 1, 29, 16, 50, 1, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#faleitoleve"),
            content='{"username":"@HadsonNery","text":"Se ele fosse brabo mesmo e eu estaria aqui defendendo ele, pq ele não foi direto no Davi já que a intenção dele era fazer o Davi comprar o barulho dela 🤷🏻\u200d♂️ MC fofoqueiro foi macetado pela CUNHÃ #faleitoleve","url":"https://twitter.com/HadsonNery/status/1752011223330124021","timestamp":"2024-01-29T16:50:00Z","tweet_hashtags":["#faleitoleve"]}',
            content_size_bytes=455,
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
    # asyncio.run(test_multi_thread_validate())
    # asyncio.run(test_scrape())
    asyncio.run(test_validate())
