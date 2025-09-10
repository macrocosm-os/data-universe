import asyncio
import threading
import traceback
import bittensor as bt
from typing import List, Tuple, Optional
from common.data import DataEntity, DataLabel, DataSource
from common.protocol import KeywordMode
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunner, RunConfig
from scraping.x.model import XContent
from scraping.x import utils
import datetime as dt


class ApiDojoTwitterScraper(Scraper):
    """
    Scrapes tweets using the Apidojo Twitter Scraper: https://console.apify.com/actors/61RPP7dywgiy0JPD0.
    """

    ACTOR_ID = "61RPP7dywgiy0JPD0"

    SCRAPE_TIMEOUT_SECS = 120

    BASE_RUN_INPUT = {"maxRequestRetries": 5}

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
                    **ApiDojoTwitterScraper.BASE_RUN_INPUT,
                    "startUrls": [entity.uri],
                    "maxItems": tweet_count,
                }
                run_config = RunConfig(
                    actor_id=ApiDojoTwitterScraper.ACTOR_ID,
                    debug_info=f"Validate {entity.uri}",
                    max_data_entities=tweet_count,
                )

                # Retrieve the tweets from Apify.
                dataset: List[dict] = None
                try:
                    dataset: List[dict] = await self.runner.run(run_config, run_input)
                except Exception as e:  # Catch all exceptions here to ensure we do not exit validation early.
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
                tweets, is_retweets, author_datas, view_counts = (
                    self._best_effort_parse_dataset(dataset)
                )

                actual_tweet = None
                actual_author_data = None
                actual_view_count = 0

                for index, tweet in enumerate(tweets):
                    if utils.normalize_url(tweet.url) == utils.normalize_url(
                        entity.uri
                    ):
                        actual_tweet = tweet
                        is_retweet = is_retweets[index]
                        actual_author_data = author_datas[index]
                        actual_view_count = view_counts[index]
                        break

                bt.logging.debug(actual_tweet)
                if actual_tweet is None:
                    # Only append a failed result if on final attempt.
                    if attempt == max_attempts:
                        return ValidationResult(
                            is_valid=False,
                            reason="Tweet not found or is invalid.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                else:
                    return self._validate_tweet_content(
                        actual_tweet=actual_tweet,
                        entity=entity,
                        is_retweet=is_retweet,
                        author_data=actual_author_data,
                        view_count=actual_view_count,
                    )

        if not entities:
            return []

        # Since we are using the threading.semaphore we need to use it in a context outside of asyncio.
        bt.logging.trace("Acquiring semaphore for concurrent apidojo validations.")

        with ApiDojoTwitterScraper.concurrent_validates_semaphore:
            bt.logging.trace("Acquired semaphore for concurrent apidojo validations.")
            results = await asyncio.gather(
                *[validate_entity(entity) for entity in entities]
            )

        return results

    async def scrape(
        self, scrape_config: ScrapeConfig, allow_low_engagement: bool = False
    ) -> List[DataEntity]:
        """Scrapes a batch of Tweets according to the scrape config.

        Args:
            scrape_config: Configuration for scraping
            allow_low_engagement: If True, disables spam/engagement filtering (for OnDemand API)
        """
        # Construct the query string.
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
                if label.value.startswith("@"):
                    # Remove @ for the API query
                    username = label.value[1:]
                    username_labels.append(f"from:{username}")
                else:
                    keyword_labels.append(label.value)

            # Add usernames with OR between them
            if username_labels:
                query_parts.append(f"({' OR '.join(username_labels)})")

            # Add keywords with OR between them if there are any
            if keyword_labels:
                query_parts.append(f"({' OR '.join(keyword_labels)})")
        else:
            # HACK: The search query doesn't work if only a time range is provided.
            # If no label is specified, just search for "e", the most common letter in the English alphabet.
            query_parts.append("e")

        # Join all parts with spaces
        query = " ".join(query_parts)

        # Construct the input to the runner.
        max_items = scrape_config.entity_limit or 150
        run_input = {
            **ApiDojoTwitterScraper.BASE_RUN_INPUT,
            "searchTerms": [query],
            "maxTweets": max_items,
        }

        run_config = RunConfig(
            actor_id=ApiDojoTwitterScraper.ACTOR_ID,
            debug_info=f"Scrape {query}",
            max_data_entities=scrape_config.entity_limit,
            timeout_secs=ApiDojoTwitterScraper.SCRAPE_TIMEOUT_SECS,
        )

        bt.logging.success(f"Performing Twitter scrape for search terms: {query}.")

        # Run the Actor and retrieve the scraped data.
        dataset: List[dict] = None
        try:
            dataset: List[dict] = await self.runner.run(run_config, run_input)
        except Exception:
            bt.logging.error(
                f"Failed to scrape tweets using search terms {query}: {traceback.format_exc()}."
            )
            return []

        # Return the parsed results, optionally disabling engagement filtering
        check_engagement = (
            not allow_low_engagement
        )  # Disable filtering if allow_low_engagement=True
        x_contents, is_retweets, _, _ = self._best_effort_parse_dataset(
            dataset, check_engagement=check_engagement
        )

        bt.logging.success(
            f"Completed scrape for {query}. Scraped {len(x_contents)} items."
        )

        data_entities = []
        for x_content in x_contents:
            data_entities.append(XContent.to_data_entity(content=x_content))

        return data_entities

    def _best_effort_parse_dataset(
        self, dataset: List[dict], check_engagement: bool = True
    ) -> Tuple[List[XContent], List[bool], List[dict], List[int]]:
        """Performs a best effort parsing of Apify dataset into List[XContent]
        Any errors are logged and ignored."""

        if dataset == [{"zero_result": True}] or not dataset:
            return [], [], [], []

        results: List[XContent] = []
        is_retweets: List[bool] = []
        author_datas: List[dict] = []
        view_counts: List[int] = []

        for data in dataset:
            try:
                # Check that we have the required fields.
                if not all(field in data for field in ["text", "url", "createdAt"]):
                    continue

                # Filter spam accounts and low engagement tweets if check_engagement is True
                if check_engagement:
                    if "author" in data and utils.is_spam_account(data["author"]):
                        bt.logging.debug(
                            f"Filtered spam account: {data.get('author', {}).get('userName', 'unknown')}"
                        )
                        continue

                    if utils.is_low_engagement_tweet(data):
                        bt.logging.debug(
                            f"Filtered low engagement tweet: {data.get('url', 'unknown')}"
                        )
                        continue

                # Extract reply information (tuple of (user_id, username))
                reply_info = self._extract_reply_info(data)

                # Extract user information
                user_info = self._extract_user_info(data)

                # Extract hashtags and media
                tags = self._extract_tags(data)
                media_urls = self._extract_media_urls(data)

                is_retweet = data.get("isRetweet", False)
                is_retweets.append(is_retweet)

                # Extract engagement data for validation
                author_datas.append(data.get("author", {}))
                view_counts.append(data.get("viewCount", 0))

                # Extract engagement metrics
                engagement_metrics = self._extract_engagement_metrics(data)

                # Extract additional user profile data
                user_profile_data = self._extract_user_profile_data(data)

                results.append(
                    XContent(
                        username=data["author"]["userName"],
                        text=utils.sanitize_scraped_tweet(data["text"]),
                        url=data["url"],
                        timestamp=dt.datetime.strptime(
                            data["createdAt"], "%a %b %d %H:%M:%S %z %Y"
                        ),
                        tweet_hashtags=tags,
                        media=media_urls if media_urls else None,
                        # Enhanced fields
                        user_id=user_info["id"],
                        user_display_name=user_info["display_name"],
                        user_verified=user_info["verified"],
                        # Non-dynamic tweet metadata
                        tweet_id=data.get("id"),
                        is_reply=data.get("isReply", None),
                        is_quote=data.get("isQuote", None),
                        # Additional metadata
                        conversation_id=data.get("conversationId"),
                        in_reply_to_user_id=reply_info[0],
                        # ===== NEW FIELDS =====
                        # Static tweet metadata
                        language=data.get("lang"),
                        in_reply_to_username=reply_info[1],
                        quoted_tweet_id=data.get("quotedStatusId"),
                        # Dynamic engagement metrics
                        like_count=engagement_metrics["like_count"],
                        retweet_count=engagement_metrics["retweet_count"],
                        reply_count=engagement_metrics["reply_count"],
                        quote_count=engagement_metrics["quote_count"],
                        view_count=engagement_metrics["view_count"],
                        bookmark_count=engagement_metrics["bookmark_count"],
                        # User profile data
                        user_blue_verified=user_profile_data["user_blue_verified"],
                        user_description=user_profile_data["user_description"],
                        user_location=user_profile_data["user_location"],
                        profile_image_url=user_profile_data["profile_image_url"],
                        cover_picture_url=user_profile_data["cover_picture_url"],
                        user_followers_count=user_profile_data["user_followers_count"],
                        user_following_count=user_profile_data["user_following_count"],
                    )
                )
            except Exception:
                bt.logging.warning(
                    f"Failed to decode XContent from Apify response: {traceback.format_exc()}."
                )

        return results, is_retweets, author_datas, view_counts

    def _extract_reply_info(self, data: dict) -> Tuple[Optional[str], Optional[str]]:
        """Extract reply information, returning (user_id, username) or (None, None)"""
        if not data.get("isReply", False):
            return None, None

        user_id = data.get("inReplyToUserId")
        username = None

        if "inReplyToUser" in data and isinstance(data["inReplyToUser"], dict):
            username = data["inReplyToUser"].get("userName")

        return user_id, username

    def _extract_user_info(self, data: dict) -> dict:
        """Extract user information from tweet"""
        if "author" not in data or not isinstance(data["author"], dict):
            return {"id": None, "display_name": None, "verified": False}

        author = data["author"]
        return {
            "id": author.get("id"),
            "display_name": author.get("name"),
            "verified": any(
                [
                    author.get("isVerified", False),
                    author.get("isBlueVerified", False),
                    author.get("verified", False),
                ]
            ),
        }

    def _extract_tags(self, data: dict) -> List[str]:
        """Extract and format hashtags and cashtags from tweet"""
        entities = data.get("entities", {})
        hashtags = entities.get("hashtags", [])
        cashtags = entities.get("symbols", [])

        # Combine and sort by index
        all_tags = sorted(hashtags + cashtags, key=lambda x: x["indices"][0])

        return ["#" + item["text"] for item in all_tags]

    def _extract_media_urls(self, data: dict) -> List[str]:
        """Extract media URLs from tweet"""
        media_urls = []
        media_data = data.get("media", [])

        if not isinstance(media_data, list):
            return media_urls

        for media_item in media_data:
            if isinstance(media_item, dict) and "media_url_https" in media_item:
                media_urls.append(media_item["media_url_https"])
            elif isinstance(media_item, str):
                media_urls.append(media_item)

        return media_urls

    def _extract_engagement_metrics(self, data: dict) -> dict:
        """Extract engagement metrics from tweet data"""
        return {
            "like_count": data.get("likeCount"),
            "retweet_count": data.get("retweetCount"),
            "reply_count": data.get("replyCount"),
            "quote_count": data.get("quoteCount"),
            "view_count": data.get("viewCount"),
            "bookmark_count": data.get("bookmarkCount"),
        }

    def _extract_user_profile_data(self, data: dict) -> dict:
        """Extract user profile data from tweet author information"""
        author = data.get("author", {})
        return {
            "user_blue_verified": author.get("isBlueVerified"),
            "user_description": author.get("description"),
            "user_location": author.get("location"),
            "profile_image_url": author.get("profilePicture"),
            "cover_picture_url": author.get("coverPicture"),
            "user_followers_count": author.get("followers"),
            "user_following_count": author.get("following"),
        }

    def _validate_tweet_content(
        self,
        actual_tweet: XContent,
        entity: DataEntity,
        is_retweet: bool,
        author_data: dict = None,
        view_count: int = None,
    ) -> ValidationResult:
        """Validates the tweet with spam and engagement filtering."""
        # First check spam/engagement if data is available
        if author_data is not None and view_count is not None:
            # Check if account is spam (low followers/new account)
            if utils.is_spam_account(author_data):
                return ValidationResult(
                    is_valid=False,
                    reason="Tweet from spam account (low followers/new account).",
                    content_size_bytes_validated=entity.content_size_bytes,
                )

            # Check if tweet has low engagement # todo remove it for on_demand
            tweet_data = {"viewCount": view_count}
            if utils.is_low_engagement_tweet(tweet_data):
                return ValidationResult(
                    is_valid=False,
                    reason="Tweet has low engagement (insufficient views).",
                    content_size_bytes_validated=entity.content_size_bytes,
                )

        # Delegate to the core validation logic in utils
        return utils.validate_tweet_content(
            actual_tweet=actual_tweet,
            entity=entity,
            is_retweet=is_retweet,
            author_data=author_data,
            view_count=view_count,
        )

    async def on_demand_scrape(
        self,
        usernames: List[str] = None,
        keywords: List[str] = None,
        keyword_mode: KeywordMode = "all",
        start_date: dt.datetime = None,
        end_date: dt.datetime = None,
        limit: int = 150,
        allow_low_engagement: bool = True,
    ) -> List[DataEntity]:
        """
        OnDemand scraping method for API requests with flexible filtering and AND/OR logic.

        Args:
            usernames: List of usernames to scrape (with or without @, OR logic between them)
            keywords: List of keywords to search for
            keyword_mode: "any" (OR logic) or "all" (AND logic) for keyword matching
            start_date: Start date for scraping
            end_date: End date for scraping
            limit: Maximum number of tweets to return
            allow_low_engagement: If True, includes low engagement posts (default for OnDemand)

        Returns:
            List of DataEntity objects
        """
        # Return empty list if all key search parameters are None
        if all(param is None for param in [usernames, keywords, start_date, end_date]):
            bt.logging.trace("All search parameters are None, returning empty list")
            return []

        # Set default date range if not provided
        if not start_date:
            start_date = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
        if not end_date:
            end_date = dt.datetime.now(dt.timezone.utc)

        # Construct the query string directly without ScrapeConfig
        date_format = "%Y-%m-%d_%H:%M:%S_UTC"
        query_parts = []

        # Add date range
        query_parts.append(
            f"since:{start_date.astimezone(tz=dt.timezone.utc).strftime(date_format)}"
        )
        query_parts.append(
            f"until:{end_date.astimezone(tz=dt.timezone.utc).strftime(date_format)}"
        )

        # Add usernames with OR logic between them
        if usernames:
            username_queries = [f"from:{username.removeprefix('@')}" for username in usernames]
            query_parts.append(f"({' OR '.join(username_queries)})")

        # Add keywords with specified logic (AND or OR)
        if keywords:
            quoted_keywords = [f'"{keyword}"' for keyword in keywords]
            if keyword_mode == "all":
                query_parts.append(f"({' AND '.join(quoted_keywords)})")
            else:  # keyword_mode == "any"
                query_parts.append(f"({' OR '.join(quoted_keywords)})")

        # If no specific criteria provided, add default search term
        if not query_parts or (not usernames and not keywords):
            query_parts.append("e")  # Most common letter in English

        query = " ".join(query_parts)

        # Construct the input to the runner
        run_input = {
            **ApiDojoTwitterScraper.BASE_RUN_INPUT,
            "searchTerms": [query],
            "maxTweets": limit,
        }

        run_config = RunConfig(
            actor_id=ApiDojoTwitterScraper.ACTOR_ID,
            debug_info=f"On-demand scrape {query}",
            max_data_entities=limit,
            timeout_secs=ApiDojoTwitterScraper.SCRAPE_TIMEOUT_SECS,
        )

        bt.logging.success(f"Performing on-demand Twitter scrape for: {query}")

        # Run the Actor and retrieve the scraped data
        try:
            dataset: List[dict] = await self.runner.run(run_config, run_input)
        except Exception as e:
            bt.logging.error(f"Failed to scrape tweets using query {query}: {str(e)}")
            return []

        # Return the parsed results, optionally disabling engagement filtering
        check_engagement = (
            not allow_low_engagement
        )  # Disable filtering if allow_low_engagement=True
        x_contents, is_retweets, _, _ = self._best_effort_parse_dataset(
            dataset, check_engagement=check_engagement
        )

        bt.logging.success(
            f"Completed on-demand scrape for {query}. Scraped {len(x_contents)} items."
        )

        data_entities = []
        for x_content in x_contents:
            data_entities.append(XContent.to_data_entity(content=x_content))

        return data_entities


async def test_scrape():
    scraper = ApiDojoTwitterScraper()

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


async def test_validate():
    scraper = ApiDojoTwitterScraper()

    true_entities = [
        DataEntity(
            uri="https://x.com/0xedeon/status/1790788053960667309",
            datetime=dt.datetime(2024, 5, 15, 16, 55, 17, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#cryptocurrency"),
            content='{"username": "@0xedeon", "text": "Deux frères ont manipulé les protocoles Ethereum pour voler 25M $ selon le Département de la Justice 🕵️‍♂️💰 #Cryptocurrency #JusticeDept", "url": "https://x.com/0xedeon/status/1790788053960667309", "timestamp": "2024-05-15T16:55:00+00:00", "tweet_hashtags": ["#Cryptocurrency", "#JusticeDept"]}',
            content_size_bytes=391,
        ),
        DataEntity(
            uri="https://x.com/100Xpotential/status/1790785842967101530",
            datetime=dt.datetime(2024, 5, 15, 16, 46, 30, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#catcoin"),
            content='{"username": "@100Xpotential", "text": "As i said green candles incoming 🚀🫡👇👇\\n\\nAround 15% price surge in #CatCoin 📊💸🚀🚀\\n\\n𝐂𝐨𝐦𝐦𝐞𝐧𝐭 |  𝐋𝐢𝐤𝐞 |  𝐑𝐞𝐭𝐰𝐞𝐞𝐭 |  𝐅𝐨𝐥𝐥𝐨𝐰\\n\\n#Binance #Bitcoin #PiNetwork #Blockchain #NFT #BabyDoge #Solana #PEPE #Crypto #1000x #cryptocurrency #Catcoin #100x", "url": "https://x.com/100Xpotential/status/1790785842967101530", "timestamp": "2024-05-15T16:46:00+00:00", "tweet_hashtags": ["#CatCoin", "#Binance", "#Bitcoin", "#PiNetwork", "#Blockchain", "#NFT", "#BabyDoge", "#Solana", "#PEPE", "#Crypto", "#1000x", "#cryptocurrency", "#Catcoin", "#100x"]}',
            content_size_bytes=933,
        ),
        DataEntity(
            uri="https://x.com/20nineCapitaL/status/1789488160688541878",
            datetime=dt.datetime(2024, 5, 12, 2, 49, 59, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@20nineCapitaL", "text": "Yup! We agreed to. \\n\\n@MetaMaskSupport #Bitcoin #Investors #DigitalAssets #EthereumETF #Airdrops", "url": "https://x.com/20nineCapitaL/status/1789488160688541878", "timestamp": "2024-05-12T02:49:00+00:00", "tweet_hashtags": ["#Bitcoin", "#Investors", "#DigitalAssets", "#EthereumETF", "#Airdrops"]}',
            content_size_bytes=345,
        ),
        DataEntity(
            uri="https://x.com/AAAlviarez/status/1790787185047658838",
            datetime=dt.datetime(2024, 5, 15, 16, 51, 50, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#web3‌‌"),
            content='{"username": "@AAAlviarez", "text": "1/3🧵\\n\\nOnce a month dozens of #web3‌‌  users show our support to one of the projects that is doing an excellent job in services and #cryptocurrency adoption.\\n\\nDo you know what Leo Power Up Day is all about?", "url": "https://x.com/AAAlviarez/status/1790787185047658838", "timestamp": "2024-05-15T16:51:00+00:00", "tweet_hashtags": ["#web3‌‌", "#cryptocurrency"]}',
            content_size_bytes=439,
        ),
        DataEntity(
            uri="https://x.com/AGariaparra/status/1789488091453091936",
            datetime=dt.datetime(2024, 5, 12, 2, 49, 42, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AGariaparra", "text": "J.P Morgan, Wells Fargo hold #Bitcoin now: Why are they interested in BTC? - AMBCrypto", "url": "https://x.com/AGariaparra/status/1789488091453091936", "timestamp": "2024-05-12T02:49:00+00:00", "tweet_hashtags": ["#Bitcoin"]}',
            content_size_bytes=269,
        ),
        DataEntity(
            uri="https://x.com/AGariaparra/status/1789488427546939525",
            datetime=dt.datetime(2024, 5, 12, 2, 51, 2, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AGariaparra", "text": "We Asked ChatGPT if #Bitcoin Will Enter a Massive Bull Run in 2024", "url": "https://x.com/AGariaparra/status/1789488427546939525", "timestamp": "2024-05-12T02:51:00+00:00", "tweet_hashtags": ["#Bitcoin"]}',
            content_size_bytes=249,
        ),
        DataEntity(
            uri="https://x.com/AMikulanecs/status/1784324497895522673",
            datetime=dt.datetime(2024, 4, 27, 20, 51, 26, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#felix"),
            content='{"username": "@AMikulanecs", "text": "$FELIX The new Dog with OG Vibes... \\nWe have a clear vision for success.\\nNew Dog $FELIX \\n➡️Follow @FelixInuETH \\n➡️Join➡️Visit#memecoins #BTC #MemeCoinSeason #Bullrun2024 #Ethereum #altcoin #Crypto #meme #SOL #BaseChain #Binance", "url": "https://x.com/AMikulanecs/status/1784324497895522673", "timestamp": "2024-04-27T20:51:00+00:00", "tweet_hashtags": ["#FELIX", "#FELIX", "#memecoins", "#BTC", "#MemeCoinSeason", "#Bullrun2024", "#Ethereum", "#altcoin", "#Crypto", "#meme", "#SOL", "#BaseChain", "#Binance"]}',
            content_size_bytes=588,
        ),
        DataEntity(
            uri="https://x.com/AdamEShelton/status/1789490040751411475",
            datetime=dt.datetime(2024, 5, 12, 2, 57, 27, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AdamEShelton", "text": "#bitcoin  love", "url": "https://x.com/AdamEShelton/status/1789490040751411475", "timestamp": "2024-05-12T02:57:00+00:00", "tweet_hashtags": ["#bitcoin"]}',
            content_size_bytes=199,
        ),
        DataEntity(
            uri="https://x.com/AfroWestor/status/1789488798406975580",
            datetime=dt.datetime(2024, 5, 12, 2, 52, 31, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AfroWestor", "text": "Given is for Prince and princess form inheritances  to kingdom. \\n\\nWe the #BITCOIN family we Gain profits for ever. \\n\\nSo if you embrace #BTC that means you have a Kingdom to pass on for ever.", "url": "https://x.com/AfroWestor/status/1789488798406975580", "timestamp": "2024-05-12T02:52:00+00:00", "tweet_hashtags": ["#BITCOIN", "#BTC"]}',
            content_size_bytes=383,
        ),
        DataEntity(
            uri="https://x.com/AlexEmidio7/status/1789488453979189327",
            datetime=dt.datetime(2024, 5, 12, 2, 51, 9, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#bitcoin"),
            content='{"username": "@AlexEmidio7", "text": "Bip47 V3 V4 #Bitcoin", "url": "https://x.com/AlexEmidio7/status/1789488453979189327", "timestamp": "2024-05-12T02:51:00+00:00", "tweet_hashtags": ["#Bitcoin"]}',
            content_size_bytes=203,
        ),
    ]
    results = await scraper.validate(entities=true_entities)
    for result in results:
        print(result)


async def test_shadowban_detection():
    """Test shadowban detection for HelenRoach32601 account"""
    scraper = ApiDojoTwitterScraper()

    # Test the suspected shadowbanned account
    username = "HelenRoach32601"

    run_input = {
        **ApiDojoTwitterScraper.BASE_RUN_INPUT,
        "searchTerms": [f"from:{username}"],
        "maxTweets": 5,
    }

    run_config = RunConfig(
        actor_id=ApiDojoTwitterScraper.ACTOR_ID,
        debug_info=f"Shadowban test for {username}",
        max_data_entities=5,
        timeout_secs=60,
    )

    bt.logging.success(f"Testing shadowban detection for @{username}")

    try:
        dataset: List[dict] = await scraper.runner.run(run_config, run_input)

        if not dataset or dataset == [{"zero_result": True}]:
            bt.logging.success(
                f"✅ SHADOWBANNED: @{username} - No results from 'from:username' search"
            )
            return True  # Account is shadowbanned
        else:
            bt.logging.info(
                f"❌ NOT SHADOWBANNED: @{username} - Found {len(dataset)} results"
            )
            for tweet in dataset[:2]:  # Show first 2 results
                bt.logging.info(f"  - Tweet: {tweet.get('text', '')[:100]}...")
            return False  # Account is not shadowbanned

    except Exception as e:
        bt.logging.error(f"Error testing shadowban for @{username}: {e}")
        return None  # Unknown status


async def test_multi_thread_validate():
    scraper = ApiDojoTwitterScraper()

    true_entities = [
        DataEntity(
            uri="https://x.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"]}',
            content_size_bytes=318,
        ),
        DataEntity(
            uri="https://x.com/HadsonNery/status/1752011223330124021",
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


async def test_on_demand_scraping():
    """Test OnDemand scraping functionality with filtering options."""
    print("🧪 Testing OnDemand scraping...")

    scraper = ApiDojoTwitterScraper()

    # Test 1: OnDemand with low engagement allowed (default)
    print("Testing with low engagement allowed...")
    entities_with_low = await scraper.on_demand_scrape(
        keywords=["bitcoin"], limit=5, allow_low_engagement=False
    )

    # Test 2: OnDemand with low engagement filtered
    print("Testing with low engagement filtered...")
    entities_filtered = await scraper.on_demand_scrape(
        keywords=["bitcoin"], limit=5, allow_low_engagement=False
    )

    print(f"✅ With low engagement: {len(entities_with_low)} tweets")
    print(f"✅ Filtered engagement: {len(entities_filtered)} tweets")
    print(f"✅ OnDemand API ready with filtering control!")


if __name__ == "__main__":
    bt.logging.set_trace(True)
    # asyncio.run(test_new_fields_validation())
    asyncio.run(test_on_demand_scraping())
    # asyncio.run(test_multi_thread_validate())
    # asyncio.run(test_scrape())
    # asyncio.run(test_validate())
