import asyncio
import aiohttp
import traceback
import datetime as dt
import bittensor as bt
from typing import List, Optional
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.reddit.model import RedditContent, RedditDataType, DELETED_USER
from scraping.reddit.utils import (
    is_valid_reddit_url,
    validate_reddit_content,
    normalize_label,
    normalize_permalink,
    extract_media_urls,
)
from common.protocol import KeywordMode


class RedditJsonScraper(Scraper):
    """
    Scrapes Reddit data using Reddit's public JSON API (no authentication required).
    This scraper accesses publicly available data through Reddit's .json endpoints.
    """

    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
    BASE_URL = "https://www.reddit.com"

    # Rate limiting settings
    REQUEST_TIMEOUT = 10  # seconds
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """
        Validate a list of DataEntity objects using Reddit's public JSON API.
        """
        if not entities:
            return []

        results: List[ValidationResult] = []

        async with aiohttp.ClientSession(headers={"User-Agent": self.USER_AGENT}) as session:
            for entity in entities:
                # 1) Basic URI sanity check
                if not is_valid_reddit_url(entity.uri):
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="Invalid URI.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )
                    continue

                # 2) Decode RedditContent blob
                try:
                    ent_content = RedditContent.from_data_entity(entity)
                except Exception:
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="Failed to decode data entity.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )
                    continue

                # 3) Fetch live data from Reddit's JSON API
                try:
                    live_content = await self._fetch_content_from_url(session, ent_content.url, ent_content.data_type)
                except Exception as e:
                    bt.logging.error(f"Failed to retrieve content for {entity.uri}: {e}")
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="Failed to retrieve submission/comment from Reddit.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )
                    continue

                # 4) Live content object exists?
                if not live_content:
                    results.append(
                        ValidationResult(
                            is_valid=False,
                            reason="Reddit content not found or invalid.",
                            content_size_bytes_validated=entity.content_size_bytes,
                        )
                    )
                    continue

                # 5) Field-by-field validation
                validation_result = validate_reddit_content(
                    actual_content=live_content,
                    entity_to_validate=entity,
                )

                results.append(validation_result)

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of reddit posts/comments according to the scrape config."""
        bt.logging.trace(
            f"Reddit JSON scraper performing scrape with config: {scrape_config}."
        )

        assert (
            not scrape_config.labels or len(scrape_config.labels) <= 1
        ), "Can only scrape 1 subreddit at a time."

        # Strip the r/ from the config or use 'all' if no label is provided.
        subreddit_name = (
            normalize_label(scrape_config.labels[0]) if scrape_config.labels else "all"
        )

        bt.logging.trace(
            f"Running Reddit JSON scraper with subreddit: {subreddit_name}."
        )

        # Get the search parameters
        limit = min(scrape_config.entity_limit, 100)  # Reddit API max is 100
        sort = self._get_sort_for_date_range(scrape_config.date_range.end)

        contents = []
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": self.USER_AGENT}) as session:
                # Fetch posts from the subreddit
                # IMPORTANT: raw_json=1 returns unescaped text (e.g., ">" instead of "&gt;")
                # This matches PRAW output format for consistent validation with miners
                url = f"{self.BASE_URL}/r/{subreddit_name}/{sort}.json?limit={limit}&raw_json=1"
                posts = await self._fetch_posts(session, url)

                for post_data in posts:
                    content = self._parse_post(post_data)
                    if content:
                        contents.append(content)

        except Exception as e:
            bt.logging.error(
                f"Failed to scrape reddit using subreddit {subreddit_name}: {traceback.format_exc()}."
            )
            return []

        # Filter out NSFW content with media
        filtered_contents = []
        for content in contents:
            if content.is_nsfw and content.media:
                bt.logging.trace(f"Skipping NSFW content with media: {content.url}")
                continue
            filtered_contents.append(content)

        bt.logging.success(
            f"Completed scrape for subreddit {subreddit_name}. Scraped {len(filtered_contents)} items "
            f"(filtered out {len(contents) - len(filtered_contents)} NSFW+media posts)."
        )

        # Convert to DataEntity objects
        data_entities = []
        for content in filtered_contents:
            data_entities.append(RedditContent.to_data_entity(content=content))

        return data_entities

    async def on_demand_scrape(
        self,
        usernames: List[str] = None,
        subreddit: str = "all",
        keywords: List[str] = None,
        keyword_mode: KeywordMode = "all",
        start_datetime: dt.datetime = None,
        end_datetime: dt.datetime = None,
        limit: int = 100
    ) -> List[DataEntity]:
        """
        Scrapes Reddit data based on specific search criteria using public JSON API.

        Args:
            usernames: List of target usernames - content from any of these users will be included (OR logic)
            subreddit: Target specific subreddit (without r/ prefix)
            keywords: List of keywords to search for
            keyword_mode: "any" (OR logic) or "all" (AND logic) for keyword matching
            start_datetime: Earliest datetime for content (UTC)
            end_datetime: Latest datetime for content (UTC)
            limit: Maximum number of items to return (max 100 per request)

        Returns:
            List of DataEntity objects matching the criteria
        """

        # Return empty list if all key search parameters are None
        if all(param is None for param in [usernames, keywords, start_datetime, end_datetime]) and subreddit == "all":
            bt.logging.trace("All search parameters are None, returning empty list")
            return []

        bt.logging.trace(
            f"On-demand scrape with usernames={usernames}, subreddit={subreddit}, "
            f"keywords={keywords}, keyword_mode={keyword_mode}, start={start_datetime}, end={end_datetime}"
        )

        contents = []
        limit = min(limit, 100)  # Reddit API max is 100

        try:
            async with aiohttp.ClientSession(headers={"User-Agent": self.USER_AGENT}) as session:

                # Case 1: Search by usernames
                if usernames:
                    for username in usernames:
                        try:
                            # Get user's posts
                            # raw_json=1 returns unescaped text to match PRAW output
                            posts_url = f"{self.BASE_URL}/user/{username}/submitted.json?limit={limit}&raw_json=1"
                            posts = await self._fetch_posts(session, posts_url)

                            for post_data in posts:
                                content = self._parse_post(post_data)
                                if content and self._matches_criteria(content, keywords, keyword_mode, start_datetime, end_datetime):
                                    contents.append(content)

                            # Get user's comments
                            comments_url = f"{self.BASE_URL}/user/{username}/comments.json?limit={limit}&raw_json=1"
                            comments = await self._fetch_posts(session, comments_url)

                            for comment_data in comments:
                                content = self._parse_comment(comment_data)
                                if content and self._matches_criteria(content, keywords, keyword_mode, start_datetime, end_datetime):
                                    contents.append(content)
                        except Exception as e:
                            bt.logging.warning(f"Failed to scrape user '{username}': {e}")
                            continue

                # Case 2: Search by subreddit (with optional keywords)
                else:
                    subreddit_name = subreddit.removeprefix("r/") if subreddit.startswith('r/') else subreddit

                    # If we have keywords, use Reddit's search functionality
                    # raw_json=1 returns unescaped text to match PRAW output
                    if keywords:
                        if keyword_mode == "all":
                            search_query = ' AND '.join(f'"{keyword}"' for keyword in keywords)
                        else:  # keyword_mode == "any"
                            search_query = ' OR '.join(f'"{keyword}"' for keyword in keywords)

                        url = f"{self.BASE_URL}/r/{subreddit_name}/search.json?q={search_query}&restrict_sr=1&limit={limit}&sort=new&raw_json=1"
                    else:
                        # No keywords, just get recent posts
                        url = f"{self.BASE_URL}/r/{subreddit_name}/new.json?limit={limit}&raw_json=1"

                    posts = await self._fetch_posts(session, url)

                    for post_data in posts:
                        # Check if it's a post or comment based on kind
                        kind = post_data.get("kind", "")
                        if kind == "t3":  # Post
                            content = self._parse_post(post_data)
                        elif kind == "t1":  # Comment
                            content = self._parse_comment(post_data)
                        else:
                            content = self._parse_post(post_data)  # Default to post parsing

                        if content and self._matches_criteria(content, keywords, keyword_mode, start_datetime, end_datetime):
                            contents.append(content)

        except Exception as e:
            bt.logging.error(f"Failed to perform on-demand scrape: {e}")
            bt.logging.error(traceback.format_exc())
            return []

        # Filter out NSFW content with media
        filtered_contents = []
        for content in contents:
            if content.is_nsfw and content.media:
                bt.logging.trace(f"Skipping NSFW content with media: {content.url}")
                continue
            filtered_contents.append(content)

        bt.logging.success(
            f"On-demand scrape completed. Found {len(filtered_contents)} items "
            f"(filtered out {len(contents) - len(filtered_contents)} NSFW+media posts)."
        )

        # Convert to DataEntity objects
        data_entities = []
        for content in filtered_contents:
            data_entities.append(RedditContent.to_data_entity(content=content))

        return data_entities

    async def _fetch_posts(self, session: aiohttp.ClientSession, url: str) -> List[dict]:
        """
        Fetch posts from Reddit's JSON API with retry logic.

        Returns:
            List of post/comment data dictionaries
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                async with session.get(url, timeout=self.REQUEST_TIMEOUT) as response:
                    if response.status == 429:
                        # Rate limited, wait and retry
                        retry_after = int(response.headers.get("Retry-After", self.RETRY_DELAY))
                        bt.logging.warning(f"Rate limited, waiting {retry_after}s before retry...")
                        await asyncio.sleep(retry_after)
                        continue

                    if response.status != 200:
                        bt.logging.warning(f"Got status {response.status} from {url}")
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_DELAY)
                            continue
                        return []

                    data = await response.json()

                    # Reddit JSON API returns data in "data" -> "children" structure
                    if isinstance(data, dict) and "data" in data:
                        children = data["data"].get("children", [])
                        return children
                    elif isinstance(data, list) and len(data) > 0:
                        # Sometimes Reddit returns a list (e.g., for comments)
                        if "data" in data[0]:
                            return data[0]["data"].get("children", [])

                    return []

            except asyncio.TimeoutError:
                bt.logging.warning(f"Timeout fetching {url}, attempt {attempt + 1}/{self.MAX_RETRIES}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    continue
            except Exception as e:
                bt.logging.error(f"Error fetching {url}: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    continue

        return []

    async def _fetch_content_from_url(
        self,
        session: aiohttp.ClientSession,
        url: str,
        data_type: RedditDataType
    ) -> Optional[RedditContent]:
        """
        Fetch and parse a specific post or comment from its URL.
        """
        # Normalize URL: remove .json if present, remove existing query params
        clean_url = url
        if clean_url.rstrip('/').endswith('.json'):
            clean_url = clean_url.rstrip('/')[:-5] + '/'
        if '?' in clean_url:
            clean_url = clean_url.split('?')[0]

        # Add .json and raw_json=1 parameter
        # raw_json=1 returns unescaped text (e.g., ">" instead of "&gt;") to match PRAW output
        # Reddit accepts /.json format (e.g., /comments/abc/.json)
        json_url = f"{clean_url}.json?raw_json=1"

        try:
            async with session.get(json_url, timeout=self.REQUEST_TIMEOUT) as response:
                if response.status != 200:
                    return None

                data = await response.json()

                if data_type == RedditDataType.POST:
                    # For posts, data is a list where [0] contains the post
                    if isinstance(data, list) and len(data) > 0:
                        children = data[0].get("data", {}).get("children", [])
                        if children:
                            return self._parse_post(children[0])
                elif data_type == RedditDataType.COMMENT:
                    # For comments, we need to navigate to find the specific comment
                    # data[0] contains the parent post, data[1] contains comments
                    if isinstance(data, list) and len(data) > 1:
                        # Get parent post's NSFW status (comments inherit from parent)
                        parent_post_data = data[0].get("data", {}).get("children", [{}])[0].get("data", {})
                        parent_nsfw = parent_post_data.get("over_18", False)

                        children = data[1].get("data", {}).get("children", [])
                        if children:
                            return self._parse_comment(children[0], parent_nsfw=parent_nsfw)

        except Exception as e:
            bt.logging.error(f"Error fetching content from {url}: {e}")

        return None

    def _parse_post(self, post_data: dict) -> Optional[RedditContent]:
        """
        Parse a Reddit post from JSON API response.
        """
        try:
            data = post_data.get("data", {})

            # Extract media URLs
            media_urls = []
            if data.get("url"):
                # Check if it's an image/video URL
                url = data["url"]
                if any(url.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.webm']):
                    media_urls.append(url)
                elif 'reddit_video' in str(data.get("media", {})):
                    if data.get("media", {}).get("reddit_video", {}).get("fallback_url"):
                        media_urls.append(data["media"]["reddit_video"]["fallback_url"])

            # Check for gallery
            if data.get("gallery_data"):
                for item in data.get("media_metadata", {}).values():
                    if item.get("s", {}).get("u"):
                        media_urls.append(item["s"]["u"].replace("&amp;", "&"))

            # Clean media URLs
            media_urls = [url.split('?')[0].replace('preview.redd.it', 'i.redd.it') for url in media_urls]

            username = data.get("author", DELETED_USER)
            if username == "[deleted]":
                username = DELETED_USER

            return RedditContent(
                id=data.get("name", ""),
                url=f"{self.BASE_URL}{normalize_permalink(data.get('permalink', ''))}",
                username=username,
                communityName=data.get("subreddit_name_prefixed", ""),
                body=data.get("selftext", ""),
                createdAt=dt.datetime.utcfromtimestamp(data.get("created_utc", 0)).replace(
                    tzinfo=dt.timezone.utc
                ),
                dataType=RedditDataType.POST,
                title=data.get("title", ""),
                parentId=None,
                media=media_urls if media_urls else None,
                is_nsfw=data.get("over_18", False),
                score=data.get("score"),
                upvote_ratio=data.get("upvote_ratio"),
                num_comments=data.get("num_comments"),
                scrapedAt=dt.datetime.now(dt.timezone.utc),
            )
        except Exception as e:
            bt.logging.trace(f"Failed to parse post: {e}")
            return None

    def _parse_comment(self, comment_data: dict, parent_nsfw: bool = False) -> Optional[RedditContent]:
        """
        Parse a Reddit comment from JSON API response.

        Args:
            comment_data: The comment data from Reddit JSON API
            parent_nsfw: NSFW status inherited from parent post (comments don't have their own over_18 field)
        """
        try:
            data = comment_data.get("data", {})

            username = data.get("author", DELETED_USER)
            if username == "[deleted]":
                username = DELETED_USER

            return RedditContent(
                id=data.get("name", ""),
                url=f"{self.BASE_URL}{normalize_permalink(data.get('permalink', ''))}",
                username=username,
                communityName=data.get("subreddit_name_prefixed", ""),
                body=data.get("body", ""),
                createdAt=dt.datetime.utcfromtimestamp(data.get("created_utc", 0)).replace(
                    tzinfo=dt.timezone.utc
                ),
                dataType=RedditDataType.COMMENT,
                title=None,
                parentId=data.get("parent_id"),
                media=None,
                is_nsfw=parent_nsfw,  # Inherit NSFW from parent post
                score=data.get("score"),
                upvote_ratio=None,
                num_comments=None,
                scrapedAt=dt.datetime.now(dt.timezone.utc),
            )
        except Exception as e:
            bt.logging.trace(f"Failed to parse comment: {e}")
            return None

    def _matches_criteria(
        self,
        content: RedditContent,
        keywords: List[str] = None,
        keyword_mode: KeywordMode = "all",
        start_datetime: dt.datetime = None,
        end_datetime: dt.datetime = None
    ) -> bool:
        """
        Check if content matches the specified criteria.
        """
        if start_datetime:
            if start_datetime.tzinfo is None:
                start_datetime = start_datetime.replace(tzinfo=dt.timezone.utc)
            if content.created_at < start_datetime:
                return False

        if end_datetime:
            if end_datetime.tzinfo is None:
                end_datetime = end_datetime.replace(tzinfo=dt.timezone.utc)
            if content.created_at > end_datetime:
                return False

        # Check keywords based on keyword_mode
        if keywords:
            searchable_text = ""
            if content.title:
                searchable_text += content.title.lower() + " "
            if content.body:
                searchable_text += content.body.lower()

            if keyword_mode == "all":
                if not all(keyword.lower() in searchable_text for keyword in keywords):
                    return False
            else:  # keyword_mode == "any"
                if not any(keyword.lower() in searchable_text for keyword in keywords):
                    return False

        return True

    def _get_sort_for_date_range(self, end_date: dt.datetime) -> str:
        """
        Determine the sort order based on the date range.
        """
        now = dt.datetime.now(tz=dt.timezone.utc)
        days_ago = (now - end_date).days

        if days_ago <= 1:
            return "new"
        else:
            return "top"


async def test_scrape():
    """Test the basic scrape functionality."""
    scraper = RedditJsonScraper()

    print("=" * 60)
    print("TESTING BASIC SCRAPE")
    print("=" * 60)

    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=5,
            date_range=DateRange(
                start=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=1),
                end=dt.datetime.now(tz=dt.timezone.utc),
            ),
            labels=[DataLabel(value="r/python")],
        )
    )

    print(f"\nScraped r/python: {len(entities)} entities")
    if entities:
        print(f"Sample URI: {entities[0].uri}")
        print(f"Sample datetime: {entities[0].datetime}")


async def test_on_demand_scrape():
    """Test the on_demand_scrape functionality."""
    scraper = RedditJsonScraper()

    print("\n" + "=" * 60)
    print("TESTING ON-DEMAND SCRAPE")
    print("=" * 60)

    # Test 1: Search by subreddit
    print("\n1. Testing subreddit search (r/python)...")
    entities = await scraper.on_demand_scrape(subreddit="r/python", limit=5)
    print(f"   Result: {len(entities)} entities from r/python")
    if entities:
        print(f"   Sample: {entities[0].uri}")

    # Test 2: Search by username
    print("\n2. Testing username search (spez)...")
    entities = await scraper.on_demand_scrape(usernames=["spez"], limit=3)
    print(f"   Result: {len(entities)} entities from user 'spez'")
    if entities:
        print(f"   Sample: {entities[0].uri}")

    # Test 3: Search with keywords
    print("\n3. Testing keyword search in r/python...")
    entities = await scraper.on_demand_scrape(
        subreddit="r/python",
        keywords=["django"],
        limit=3
    )
    print(f"   Result: {len(entities)} entities with 'django'")
    if entities:
        print(f"   Sample: {entities[0].uri}")

    print("\n" + "=" * 60)
    print("TESTS COMPLETED")
    print("=" * 60)


async def test_validation():
    """Test validation functionality and print all DataEntity details."""
    scraper = RedditJsonScraper()

    print("\n" + "=" * 60)
    print("TESTING VALIDATION")
    print("=" * 60)

    # First, scrape some data
    print("\n1. Scraping r/python to get test entities...")
    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=3,
            date_range=DateRange(
                start=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=1),
                end=dt.datetime.now(tz=dt.timezone.utc),
            ),
            labels=[DataLabel(value="r/python")],
        )
    )

    print(f"   Scraped {len(entities)} entities")

    # Print all DataEntity details
    print("\n2. Printing all DataEntity details:")
    print("-" * 60)
    for i, entity in enumerate(entities, 1):
        print(f"\n   Entity #{i}:")
        print(f"   URI: {entity.uri}")
        print(f"   Datetime: {entity.datetime}")
        print(f"   Source: {entity.source}")
        print(f"   Label: {entity.label}")
        print(f"   Content Size: {entity.content_size_bytes} bytes")

        # Decode and print the content
        try:
            content = RedditContent.from_data_entity(entity)
            print(f"   Content ID: {content.id}")
            print(f"   Username: {content.username}")
            print(f"   Community: {content.community}")
            print(f"   Data Type: {content.data_type}")
            print(f"   Title: {content.title[:80] + '...' if content.title and len(content.title) > 80 else content.title}")
            print(f"   Body: {content.body[:100] + '...' if content.body and len(content.body) > 100 else content.body}")
            print(f"   Created At: {content.created_at}")
            print(f"   Score: {content.score}")
            print(f"   Upvote Ratio: {content.upvote_ratio}")
            print(f"   Num Comments: {content.num_comments}")
            print(f"   NSFW: {content.is_nsfw}")
            print(f"   Media: {content.media}")
        except Exception as e:
            print(f"   Failed to decode content: {e}")

    # Validate the entities
    print("\n" + "-" * 60)
    print("3. Validating entities...")
    results = await scraper.validate(entities)

    print(f"\n   Validation Results:")
    for i, (entity, result) in enumerate(zip(entities, results), 1):
        print(f"\n   Entity #{i}: {entity.uri}")
        print(f"   Valid: {result.is_valid}")
        print(f"   Reason: {result.reason if hasattr(result, 'reason') and result.reason else 'N/A'}")
        print(f"   Content Size Validated: {result.content_size_bytes_validated} bytes")

    # Test with a known good entity (from bittensor_ subreddit)
    print("\n" + "-" * 60)
    print("4. Testing with a known good entity...")

    test_entity = DataEntity(
        uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/",
        datetime=dt.datetime(2023, 12, 5, 15, 59, 13, tzinfo=dt.timezone.utc),
        source=DataSource.REDDIT,
        label=DataLabel(value="r/bittensor_"),
        content=b'{"id": "t3_18bf67l", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Hey all!!\\n\\nHow do we add TAO to MetaMask? Online gives me these network configurations and still doesn\\u2019t work? \\n\\nHow are you all storing TAO? I wanna purchase on MEXC, but holding off until I can store it!  \\ud83d\\ude11 \\n\\nThanks in advance!!!\\n\\n=====\\n\\nhere is a manual way.\\nNetwork Name\\nTao Network\\n\\nRPC URL\\nhttp://rpc.testnet.tao.network\\n\\nChain ID\\n558\\n\\nCurrency Symbol\\nTAO", "createdAt": "2023-12-05T15:59:13+00:00", "dataType": "post", "title": "How do you add TAO to MetaMask?", "parentId": null}',
        content_size_bytes=775,
    )

    print(f"   Test Entity URI: {test_entity.uri}")

    validation_results = await scraper.validate([test_entity])
    print(f"   Validation Result:")
    print(f"   Valid: {validation_results[0].is_valid}")
    print(f"   Reason: {validation_results[0].reason if hasattr(validation_results[0], 'reason') and validation_results[0].reason else 'N/A'}")

    print("\n" + "=" * 60)
    print("VALIDATION TESTS COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_scrape())
    asyncio.run(test_on_demand_scrape())
    asyncio.run(test_validation())
