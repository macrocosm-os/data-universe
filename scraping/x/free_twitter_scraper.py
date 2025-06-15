import asyncio
import json
import re
import time
import datetime as dt
from typing import List, Tuple, Optional, Dict, Any
import aiohttp
import bittensor as bt
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult, HFValidationResult
from scraping.x.model import XContent
from scraping.x import utils
from scraping.x.on_demand_model import EnhancedXContent


class FreeTwitterScraper:
    """
    A free Twitter/X scraper that doesn't require Apify or paid services.
    """

    BASE_URL = "https://twitter.com"
    SEARCH_URL = "https://twitter.com/i/api/2/search/adaptive.json"
    USER_TWEETS_URL = "https://twitter.com/i/api/2/timeline/profile/{user_id}.json"
    TWEET_DETAIL_URL = "https://twitter.com/i/api/2/timeline/conversation/{tweet_id}.json"

    def __init__(self):
        self.session = None
        self.guest_token = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://twitter.com/",
            "Origin": "https://twitter.com",
            "DNT": "1",
            "Connection": "keep-alive",
            "TE": "Trailers",
        }

    async def __aenter__(self):
        await self.initialize_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_session()

    async def initialize_session(self):
        """Initialize a session with guest token"""
        self.session = aiohttp.ClientSession()
        await self._get_guest_token()

    async def close_session(self):
        """Close the session"""
        if self.session:
            await self.session.close()

    async def _get_guest_token(self):
        """Get a guest token for API access"""
        try:
            async with self.session.get(
                    "https://twitter.com",
                    headers=self.headers,
                    allow_redirects=True
            ) as response:
                html = await response.text()
                # Try to find guest token in JavaScript
                match = re.search(r'"gt=(\d+);', html)
                if match:
                    self.guest_token = match.group(1)
                else:
                    # Fallback: try to get token from API
                    async with self.session.post(
                            "https://api.twitter.com/1.1/guest/activate.json",
                            headers=self.headers
                    ) as token_resp:
                        token_data = await token_resp.json()
                        self.guest_token = token_data.get('guest_token')

                if self.guest_token:
                    self.headers['x-guest-token'] = self.guest_token
                else:
                    bt.logging.warning("Failed to obtain guest token")
        except Exception as e:
            bt.logging.error(f"Error getting guest token: {str(e)}")
            raise

    async def _make_request(self, url: str, params: dict = None) -> dict:
        """Make a request to Twitter API"""
        if not self.session:
            await self.initialize_session()

        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.session.get(
                        url,
                        headers=self.headers,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        bt.logging.warning("Rate limited, waiting...")
                        await asyncio.sleep(5 * (attempt + 1))
                        continue
                    else:
                        bt.logging.error(f"Request failed with status {response.status}")
                        return None
            except Exception as e:
                bt.logging.error(f"Request error: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(1)
        return None

    async def search_tweets(self, query: str, limit: int = 20) -> List[dict]:
        """Search for tweets using a query"""
        params = {
            'q': query,
            'count': limit,
            'tweet_search_mode': 'live',
            'query_source': 'typed_query',
            'pc': '1',
            'spelling_corrections': '1',
            'include_ext_edit_control': 'true',
            'ext': 'mediaStats,highlightedLabel,voiceInfo'
        }

        data = await self._make_request(self.SEARCH_URL, params)
        if not data:
            return []

        return self._parse_tweet_data(data)

    async def get_user_tweets(self, username: str, limit: int = 20) -> List[dict]:
        """Get tweets from a specific user"""
        # First get user ID
        user_info = await self._get_user_info(username)
        if not user_info:
            return []

        params = {
            'count': limit,
            'userId': user_info['id'],
            'include_tweet_replies': '1',
            'include_ext_alt_text': 'true',
            'include_ext_edit_control': 'true',
            'ext': 'mediaStats,highlightedLabel,voiceInfo'
        }

        url = self.USER_TWEETS_URL.format(user_id=user_info['id'])
        data = await self._make_request(url, params)
        if not data:
            return []

        return self._parse_tweet_data(data)

    async def _get_user_info(self, username: str) -> Optional[dict]:
        """Get user info by username"""
        if username.startswith('@'):
            username = username[1:]

        try:
            async with self.session.get(
                    f"{self.BASE_URL}/{username}",
                    headers=self.headers,
                    allow_redirects=True
            ) as response:
                html = await response.text()
                # Find user data in JavaScript
                match = re.search(r'data-user-id="(\d+)"', html)
                if match:
                    return {'id': match.group(1), 'username': username}

                # Alternative method: try to find in JSON data
                match = re.search(r'window.__INITIAL_STATE__\s*=\s*({.+?});', html)
                if match:
                    data = json.loads(match.group(1))
                    if 'entities' in data and 'users' in data['entities']:
                        for user in data['entities']['users'].values():
                            if user['screen_name'].lower() == username.lower():
                                return {
                                    'id': user['id_str'],
                                    'username': user['screen_name'],
                                    'name': user['name'],
                                    'verified': user.get('verified', False),
                                    'followers_count': user.get('followers_count', 0),
                                    'following_count': user.get('friends_count', 0)
                                }
        except Exception as e:
            bt.logging.error(f"Error getting user info: {str(e)}")
        return None

    def _parse_tweet_data(self, data: dict) -> List[dict]:
        """Parse raw tweet data from API response"""
        tweets = []

        # Extract tweets from different possible locations in response
        tweet_entries = []
        if 'globalObjects' in data and 'tweets' in data['globalObjects']:
            tweet_entries = list(data['globalObjects']['tweets'].values())
        elif 'timeline' in data and 'instructions' in data['timeline']:
            for instruction in data['timeline']['instructions']:
                if 'addEntries' in instruction:
                    for entry in instruction['addEntries']['entries']:
                        if 'content' in entry and 'item' in entry['content']:
                            if 'content' in entry['content']['item'] and 'tweet' in entry['content']['item']['content']:
                                tweet_id = entry['content']['item']['content']['tweet']['id']
                                if 'globalObjects' in data and 'tweets' in data['globalObjects']:
                                    if tweet_id in data['globalObjects']['tweets']:
                                        tweet_entries.append(data['globalObjects']['tweets'][tweet_id])

        for tweet in tweet_entries:
            try:
                parsed = self._parse_single_tweet(tweet, data.get('globalObjects', {}).get('users', {}))
                if parsed:
                    tweets.append(parsed)
            except Exception as e:
                bt.logging.error(f"Error parsing tweet: {str(e)}")
                continue

        return tweets

    def _parse_single_tweet(self, tweet: dict, users: dict) -> dict:
        """Parse a single tweet object"""
        user = users.get(tweet['user_id_str'], {})

        # Extract media
        media = []
        if 'extended_entities' in tweet and 'media' in tweet['extended_entities']:
            for item in tweet['extended_entities']['media']:
                media.append({
                    'media_url_https': item['media_url_https'],
                    'type': item['type']
                })

        # Extract hashtags and cashtags
        entities = tweet.get('entities', {})
        hashtags = [f"#{tag['text']}" for tag in entities.get('hashtags', [])]
        cashtags = [f"${tag['text']}" for tag in entities.get('symbols', [])]
        sorted_tags = hashtags + cashtags

        # Parse timestamp
        created_at = dt.datetime.strptime(
            tweet['created_at'], '%a %b %d %H:%M:%S %z %Y'
        ) if 'created_at' in tweet else dt.datetime.now(dt.timezone.utc)

        return {
            'id': tweet['id_str'],
            'text': tweet['full_text'] if 'full_text' in tweet else tweet.get('text', ''),
            'createdAt': created_at.strftime('%a %b %d %H:%M:%S %z %Y'),
            'url': f"https://twitter.com/{user.get('screen_name', '')}/status/{tweet['id_str']}",
            'likeCount': tweet.get('favorite_count', 0),
            'retweetCount': tweet.get('retweet_count', 0),
            'replyCount': tweet.get('reply_count', 0),
            'quoteCount': tweet.get('quote_count', 0),
            'viewCount': tweet.get('view_count', 0),
            'isRetweet': 'retweeted_status' in tweet,
            'isReply': tweet.get('in_reply_to_status_id_str', None) is not None,
            'isQuote': 'quoted_status_id_str' in tweet,
            'conversationId': tweet.get('conversation_id_str', tweet['id_str']),
            'inReplyToUserId': tweet.get('in_reply_to_user_id_str'),
            'media': media,
            'entities': entities,
            'author': {
                'id': user.get('id_str', ''),
                'userName': user.get('screen_name', ''),
                'name': user.get('name', ''),
                'isBlueVerified': user.get('is_blue_verified', False),
                'isVerified': user.get('verified', False),
                'followers': user.get('followers_count', 0),
                'following': user.get('friends_count', 0)
            },
            'tweet_hashtags': sorted_tags
        }


class EnhancedFreeTwitterScraper(FreeTwitterScraper):
    """
    An enhanced version of the free Twitter scraper that collects more detailed Twitter data
    using the EnhancedXContent model.
    """

    def __init__(self):
        super().__init__()
        self.enhanced_contents = []

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """
        Scrape tweets based on the configuration and return DataEntity objects.
        """
        # Construct the query string
        date_format = "%Y-%m-%d"
        query_parts = []

        # Add date range if available
        if scrape_config.date_range:
            query_parts.append(
                f"since:{scrape_config.date_range.start.astimezone(tz=dt.timezone.utc).strftime(date_format)}")
            query_parts.append(
                f"until:{scrape_config.date_range.end.astimezone(tz=dt.timezone.utc).strftime(date_format)}")

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

            # Add usernames with OR between them
            if username_labels:
                query_parts.append(f"({' OR '.join(username_labels)})")

            # Add keywords with OR between them if there are any
            if keyword_labels:
                query_parts.append(f"({' OR '.join(keyword_labels)})")
        else:
            # Fallback search term if no labels provided
            query_parts.append("bitcoin OR crypto OR blockchain")

        # Join all parts with spaces
        query = " ".join(query_parts)
        limit = scrape_config.entity_limit or 20

        bt.logging.success(f"Performing Twitter scrape for search terms: {query}")

        try:
            # Perform the search
            dataset = await self.search_tweets(query, limit=limit)

            # Parse the results
            x_contents, is_retweets = self._best_effort_parse_dataset(dataset)
            self.enhanced_contents = self._parse_enhanced_content(dataset)

            bt.logging.success(
                f"Completed scrape for {query}. Scraped {len(x_contents)} items."
            )

            # Convert to DataEntity objects
            data_entities = []
            for x_content in x_contents:
                data_entities.append(XContent.to_data_entity(content=x_content))

            return data_entities

        except Exception as e:
            bt.logging.error(
                f"Failed to scrape tweets using search terms {query}: {str(e)}"
            )
            return []

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> Tuple[List[XContent], List[bool]]:
        """
        Parse the dataset into XContent objects.

        Returns:
            Tuple[List[XContent], List[bool]]: (parsed_content, is_retweets)
        """
        if not dataset:
            return [], []

        contents = []
        is_retweets = []

        for data in dataset:
            try:
                # Basic content parsing
                username = f"@{data.get('author', {}).get('userName', '')}"
                text = utils.sanitize_scraped_tweet(data.get('text', ''))
                url = data.get('url', '')

                # Parse timestamp
                timestamp = None
                if 'createdAt' in data:
                    try:
                        timestamp = dt.datetime.strptime(
                            data["createdAt"], "%a %b %d %H:%M:%S %z %Y"
                        )
                    except ValueError:
                        timestamp = dt.datetime.now(dt.timezone.utc)

                # Hashtags
                hashtags = data.get('tweet_hashtags', [])

                # Create XContent
                content = XContent(
                    username=username,
                    text=text,
                    url=url,
                    timestamp=timestamp,
                    tweet_hashtags=hashtags
                )

                contents.append(content)
                is_retweets.append(data.get('isRetweet', False))

            except Exception as e:
                bt.logging.warning(
                    f"Failed to decode XContent from scraped data: {str(e)}"
                )
                continue

        return contents, is_retweets

    def _parse_enhanced_content(self, dataset: List[dict]) -> List[EnhancedXContent]:
        """
        Parse the dataset into EnhancedXContent objects with all available metadata.
        """
        if not dataset:
            return []

        results: List[EnhancedXContent] = []
        for data in dataset:
            try:
                # Extract user information
                author = data.get('author', {})
                user_id = author.get('id')
                username = author.get('userName')
                display_name = author.get('name')
                verified = author.get('isBlueVerified', False) or author.get('isVerified', False)
                followers_count = author.get('followers', 0)
                following_count = author.get('following', 0)

                # Extract tweet metadata
                tweet_id = data.get('id')
                like_count = data.get('likeCount', 0)
                retweet_count = data.get('retweetCount', 0)
                reply_count = data.get('replyCount', 0)
                quote_count = data.get('quoteCount', 0)
                view_count = data.get('viewCount', 0)
                is_retweet = data.get('isRetweet', False)
                is_reply = data.get('isReply', False)
                is_quote = data.get('isQuote', False)

                # Extract conversation and reply data
                conversation_id = data.get('conversationId')
                in_reply_to_user_id = data.get('inReplyToUserId')

                # Extract hashtags
                hashtags = data.get('tweet_hashtags', [])

                # Extract media content
                media_urls = []
                media_types = []
                for media_item in data.get('media', []):
                    media_urls.append(media_item.get('media_url_https', ''))
                    media_types.append(media_item.get('type', 'photo'))

                # Parse timestamp
                timestamp = None
                if 'createdAt' in data:
                    try:
                        timestamp = dt.datetime.strptime(
                            data["createdAt"], "%a %b %d %H:%M:%S %z %Y"
                        )
                    except ValueError:
                        timestamp = dt.datetime.now(dt.timezone.utc)

                # Create the enhanced content object
                enhanced_content = EnhancedXContent(
                    # Basic fields
                    username=f"@{username}" if username else "",
                    text=utils.sanitize_scraped_tweet(data.get('text', '')),
                    url=data.get('url', ''),
                    timestamp=timestamp,
                    tweet_hashtags=hashtags,

                    # Enhanced user fields
                    user_id=user_id,
                    user_display_name=display_name,
                    user_verified=verified,
                    user_followers_count=followers_count,
                    user_following_count=following_count,

                    # Enhanced tweet metadata
                    tweet_id=tweet_id,
                    like_count=like_count,
                    retweet_count=retweet_count,
                    reply_count=reply_count,
                    quote_count=quote_count,
                    is_retweet=is_retweet,
                    is_reply=is_reply,
                    is_quote=is_quote,

                    # Media content
                    media_urls=media_urls,
                    media_types=media_types,

                    # Additional metadata
                    conversation_id=conversation_id,
                    in_reply_to_user_id=in_reply_to_user_id,
                )
                results.append(enhanced_content)

            except Exception as e:
                bt.logging.warning(
                    f"Failed to decode EnhancedXContent from scraped data: {str(e)}"
                )
                continue

        return results

    def get_enhanced_content(self) -> List[EnhancedXContent]:
        """
        Returns the enhanced content from the last scrape operation.
        """
        return self.enhanced_contents

    async def scrape_enhanced(self, scrape_config: ScrapeConfig) -> List[EnhancedXContent]:
        """
        Scrape and return enhanced content directly.
        """
        await self.scrape(scrape_config)
        return self.get_enhanced_content()

    async def get_enhanced_data_entities(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """
        Scrape and return DataEntity objects with EnhancedXContent as their content.
        """
        await self.scrape(scrape_config)
        return [
            EnhancedXContent.to_data_entity(content=content)
            for content in self.get_enhanced_content()
        ]


async def test_free_scraper():
    """Test function for the free scraper."""
    async with EnhancedFreeTwitterScraper() as scraper:
        # Test with keyword
        print("\n===== TESTING WITH KEYWORD: bitcoin =====")
        keyword_config = ScrapeConfig(
            entity_limit=5,
            date_range=DateRange(
                start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1),
                end=dt.datetime.now(dt.timezone.utc)
            ),
            labels=[DataLabel(value="bitcoin")]
        )

        keyword_results = await scraper.scrape(keyword_config)
        print(f"Got {len(keyword_results)} results for keyword 'bitcoin'")

        # Test with username
        print("\n===== TESTING WITH USERNAME: @elonmusk =====")
        username_config = ScrapeConfig(
            entity_limit=5,
            date_range=DateRange(
                start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1),
                end=dt.datetime.now(dt.timezone.utc)
            ),
            labels=[DataLabel(value="@elonmusk")]
        )

        username_results = await scraper.scrape(username_config)
        print(f"Got {len(username_results)} results for username '@elonmusk'")

        # Get enhanced content
        enhanced_content = scraper.get_enhanced_content()
        if enhanced_content:
            print("\nEnhanced content sample:")
            print(f"Username: {enhanced_content[0].username}")
            print(f"Text: {enhanced_content[0].text[:100]}...")
            print(f"Likes: {enhanced_content[0].like_count}")
            print(f"Retweets: {enhanced_content[0].retweet_count}")

        return {
            "keyword_results": keyword_results,
            "username_results": username_results,
            "enhanced_content_sample": enhanced_content[0] if enhanced_content else None
        }


if __name__ == "__main__":
    asyncio.run(test_free_scraper())