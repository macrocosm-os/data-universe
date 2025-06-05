import asyncio
import threading
import traceback
import bittensor as bt
from typing import List, Tuple, Optional, Dict, Any
from common import constants
from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult, HFValidationResult
from scraping.apify import ActorRunner, RunConfig
from scraping.x.model import XContent
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.x import utils
import datetime as dt
import json

# Import the EnhancedXContent class
from scraping.x.on_demand_model import EnhancedXContent


class EnhancedApiDojoTwitterScraper(ApiDojoTwitterScraper):
    """
    An enhanced version of ApiDojoTwitterScraper that collects more detailed Twitter data
    using the EnhancedXContent model.
    """

    def __init__(self, runner: ActorRunner = None):
        # Initialize the parent class
        super().__init__(runner=runner or ActorRunner())

    def _best_effort_parse_dataset(self, dataset: List[dict]) -> Tuple[List[XContent], List[bool]]:
        """
        Enhanced version that parses the full dataset into both standard XContent (for backward compatibility)
        and EnhancedXContent objects.

        Returns:
            Tuple[List[XContent], List[bool]]: (standard_parsed_content, is_retweets)
        """
        # Call the parent class method to get standard parsed content
        standard_contents, is_retweets = super()._best_effort_parse_dataset(dataset)

        # Also parse into enhanced content and store it in a class attribute
        self.enhanced_contents = self._parse_enhanced_content(dataset)

        return standard_contents, is_retweets

    def _parse_enhanced_content(self, dataset: List[dict]) -> List[EnhancedXContent]:
        """
        Parse the dataset into EnhancedXContent objects with all available metadata.

        Args:
            dataset (List[dict]): The raw dataset from ApiDojo Twitter Scraper.

        Returns:
            List[EnhancedXContent]: List of parsed EnhancedXContent objects.
        """
        if dataset == [{"zero_result": True}] or not dataset:
            return []

        results: List[EnhancedXContent] = []
        for data in dataset:
            try:
                # Debug the structure of the data
                if 'media' in data:
                    if isinstance(data['media'], list) and data['media']:
                        if isinstance(data['media'][0], str):
                            # Fix for string media items: convert to dict format expected by from_apify_response
                            fixed_media = []
                            for media_url in data['media']:
                                fixed_media.append({'media_url_https': media_url, 'type': 'photo'})
                            data['media'] = fixed_media

                # Extract user information from author field
                author = data.get('author', {})
                user_id = author.get('id')
                username = author.get('userName')
                display_name = author.get('name')
                verified = author.get('isBlueVerified', False) or author.get('isVerified', False)
                followers_count = author.get('followers')
                following_count = author.get('following')

                # Extract tweet metadata
                tweet_id = data.get('id')
                like_count = data.get('likeCount')
                retweet_count = data.get('retweetCount')
                reply_count = data.get('replyCount')
                quote_count = data.get('quoteCount')
                view_count = data.get('viewCount')
                is_retweet = data.get('isRetweet', False)
                is_reply = data.get('isReply', False)
                is_quote = data.get('isQuote', False)

                # Extract conversation and reply data
                conversation_id = data.get('conversationId')
                in_reply_to_user_id = data.get('inReplyToUserId')

                # Extract hashtags and media
                hashtags = []
                cashtags = []
                if 'entities' in data:
                    if 'hashtags' in data['entities']:
                        hashtags = ["#" + item['text'] for item in data['entities']['hashtags']]
                    if 'symbols' in data['entities']:
                        cashtags = ["$" + item['text'] for item in data['entities']['symbols']]

                # Sort hashtags and cashtags by index if available
                sorted_tags = []
                if 'entities' in data and 'hashtags' in data['entities']:
                    if 'symbols' in data['entities']:
                        # Try to sort by indices if available
                        try:
                            hashtag_items = [
                                {'text': item['text'], 'indices': item.get('indices', [0, 0]), 'type': 'hashtag'}
                                for item in data['entities']['hashtags']]
                            cashtag_items = [
                                {'text': item['text'], 'indices': item.get('indices', [0, 0]), 'type': 'symbol'}
                                for item in data['entities']['symbols']]
                            combined = hashtag_items + cashtag_items

                            # Sort by first index
                            sorted_items = sorted(combined, key=lambda x: x['indices'][0])
                            sorted_tags = ["#" + item['text'] if item['type'] == 'hashtag' else "$" + item['text']
                                           for item in sorted_items]
                        except (KeyError, IndexError, TypeError):
                            # If sorting fails, just combine the lists
                            sorted_tags = hashtags + cashtags
                    else:
                        sorted_tags = hashtags
                else:
                    sorted_tags = hashtags + cashtags

                # Extract media content
                media_urls = []
                media_types = []

                if 'media' in data:
                    for media_item in data['media']:
                        if isinstance(media_item, dict):
                            media_url = media_item.get('media_url_https')
                            if media_url:
                                media_urls.append(media_url)
                                media_types.append(media_item.get('type', 'photo'))
                        elif isinstance(media_item, str):
                            media_urls.append(media_item)
                            media_types.append('photo')

                # Create timestamp from createdAt
                timestamp = None
                if 'createdAt' in data:
                    try:
                        timestamp = dt.datetime.strptime(
                            data["createdAt"], "%a %b %d %H:%M:%S %z %Y"
                        )
                    except ValueError:
                        # Try alternative formats if the first one fails
                        try:
                            timestamp = dt.datetime.fromisoformat(data["createdAt"])
                        except ValueError:
                            timestamp = dt.datetime.now(dt.timezone.utc)

                # Create the enhanced content object
                enhanced_content = EnhancedXContent(
                    # Basic fields
                    username=f"@{username}" if username else "",
                    text=utils.sanitize_scraped_tweet(data.get('text', '')),
                    url=data.get('url', ''),
                    timestamp=timestamp,
                    tweet_hashtags=sorted_tags,

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
                    f"Failed to decode EnhancedXContent from Apify response: {traceback.format_exc()}."
                )
                # Try simpler parsing as fallback
                try:
                    # Alternative parsing approach for problematic data
                    text = data.get('text', '')
                    url = data.get('url', '')
                    created_at = data.get('createdAt', '')
                    author = data.get('author', {})
                    username = author.get('userName', '')

                    # Get basic tweet metadata
                    tweet_id = data.get('id', None)
                    like_count = data.get('likeCount', None)
                    retweet_count = data.get('retweetCount', None)
                    reply_count = data.get('replyCount', None)

                    # Handle hashtags extraction
                    hashtags = []
                    if 'entities' in data and 'hashtags' in data['entities']:
                        hashtags = ["#" + item['text'] for item in data['entities']['hashtags']]

                    # Create minimal enhanced content
                    enhanced_content = EnhancedXContent(
                        username=f"@{username}" if username else "",
                        text=utils.sanitize_scraped_tweet(text),
                        url=url,
                        timestamp=dt.datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
                        if created_at else dt.datetime.now(dt.timezone.utc),
                        tweet_hashtags=hashtags,
                        tweet_id=tweet_id,
                        like_count=like_count,
                        retweet_count=retweet_count,
                        reply_count=reply_count
                    )
                    results.append(enhanced_content)
                    bt.logging.debug(f"Used fallback parsing for tweet: {url}")
                except Exception as fallback_error:
                    bt.logging.error(f"Fallback parsing also failed: {str(fallback_error)}")
        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """
        Enhanced scrape method that uses EnhancedXContent under the hood but returns
        standard DataEntity objects for compatibility.
        """
        # Construct the query string with special handling for usernames and keywords
        date_format = "%Y-%m-%d_%H:%M:%S_UTC"

        query_parts = []

        # Add date range
        query_parts.append(
            f"since:{scrape_config.date_range.start.astimezone(tz=dt.timezone.utc).strftime(date_format)}")
        query_parts.append(f"until:{scrape_config.date_range.end.astimezone(tz=dt.timezone.utc).strftime(date_format)}")

        # Handle labels - separate usernames and keywords
        if scrape_config.labels:
            username_labels = []
            keyword_labels = []

            for label in scrape_config.labels:
                if label.value.startswith('@'):
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

        # Parse the results using both standard and enhanced methods
        x_contents, is_retweets = self._best_effort_parse_dataset(dataset)

        bt.logging.success(
            f"Completed scrape for {query}. Scraped {len(x_contents)} items."
        )

        data_entities = []
        for x_content in x_contents:
            data_entities.append(XContent.to_data_entity(content=x_content))

        return data_entities

    def get_enhanced_content(self) -> List[EnhancedXContent]:
        """
        Returns the enhanced content from the last scrape operation.

        Returns:
            List[EnhancedXContent]: The enhanced content objects with additional metadata.
        """
        if not hasattr(self, 'enhanced_contents'):
            return []
        return self.enhanced_contents

    async def scrape_enhanced(self, scrape_config: ScrapeConfig) -> List[EnhancedXContent]:
        """
        Scrape and return enhanced content directly.

        Args:
            scrape_config (ScrapeConfig): The scrape configuration.

        Returns:
            List[EnhancedXContent]: The enhanced content objects with additional metadata.
        """
        # Perform standard scrape to populate enhanced_contents
        await self.scrape(scrape_config)

        # Return the enhanced content
        return self.get_enhanced_content()

    async def get_enhanced_data_entities(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """
        Scrape and return DataEntity objects with EnhancedXContent as their content.

        Args:
            scrape_config (ScrapeConfig): The scrape configuration.

        Returns:
            List[DataEntity]: DataEntity objects with EnhancedXContent content.
        """
        # Perform the scrape to populate enhanced_contents
        await self.scrape(scrape_config)

        # Convert the enhanced content to DataEntity objects
        data_entities = []
        for content in self.get_enhanced_content():
            data_entities.append(EnhancedXContent.to_data_entity(content=content))

        return data_entities


async def test_enhanced_scraper():
    """Test function for the enhanced scraper."""
    scraper = EnhancedApiDojoTwitterScraper()

    # Test with keyword (TAO)
    print("\n===== TESTING WITH KEYWORD: TAO =====")
    keyword_config = ScrapeConfig(
        entity_limit=5,
        date_range=DateRange(
            start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1),
            end=dt.datetime.now(dt.timezone.utc)
        ),
        labels=[DataLabel(value="TAO")]
    )

    # Get enhanced content for keyword
    await scraper.scrape(keyword_config)
    keyword_enhanced_content = scraper.get_enhanced_content()
    print(f"Enhanced content for keyword 'TAO': {len(keyword_enhanced_content)}")

    # Print detailed enhanced content information
    if keyword_enhanced_content:
        print("\nExample enhanced content for keyword 'TAO':")
        print_enriched_content(keyword_enhanced_content[0])

    # Test with username (@elonmusk)
    print("\n===== TESTING WITH USERNAME: @elonmusk =====")
    username_config = ScrapeConfig(
        entity_limit=5,
        date_range=DateRange(
            start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1),
            end=dt.datetime.now(dt.timezone.utc)
        ),
        labels=[DataLabel(value="@elonmusk")]
    )

    # Get enhanced content for username
    await scraper.scrape(username_config)
    username_enhanced_content = scraper.get_enhanced_content()
    print(f"Enhanced content for username '@elonmusk': {len(username_enhanced_content)}")

    # Print detailed enhanced content information
    if username_enhanced_content:
        print("\nExample enhanced content for username '@elonmusk':")
        print_enriched_content(username_enhanced_content[0])

    # Test API endpoint-like usage
    print("\n===== TESTING ON-DEMAND API STYLE USAGE =====")

    # Simulate processing a request similar to the on_demand_data_request API endpoint
    async def simulate_api_request(source, keywords=None, usernames=None):
        source_enum = DataSource[source.upper()]

        scrape_config = ScrapeConfig(
            entity_limit=5,
            date_range=DateRange(
                start=dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1),
                end=dt.datetime.now(dt.timezone.utc)
            ),
            labels=[DataLabel(value=k) for k in keywords] if keywords else
            [DataLabel(value=u) for u in usernames] if usernames else []
        )

        await scraper.scrape(scrape_config)
        enhanced_content = scraper.get_enhanced_content()

        # Format response similar to API
        processed_data = []
        for content in enhanced_content:
            processed_data.append(content.to_api_response())

        return {
            "status": "success",
            "data": processed_data,
            "meta": {
                "source": source,
                "keywords": keywords,
                "usernames": usernames,
                "items_returned": len(processed_data)
            }
        }

    # Test with keyword
    keyword_api_response = await simulate_api_request("X", keywords=["TAO"])
    print(f"API response for keyword 'TAO': {len(keyword_api_response['data'])} items")

    # Test with username
    username_api_response = await simulate_api_request("X", usernames=["@elonmusk"])
    print(f"API response for username '@elonmusk': {len(username_api_response['data'])} items")

    # Print complete data structure for one item
    if len(username_api_response['data']) > 0:
        print("\nCOMPLETE API RESPONSE STRUCTURE FOR ONE ITEM:")
        print(json.dumps(username_api_response['data'][0], indent=2))

    return {
        "keyword_content": keyword_enhanced_content,
        "username_content": username_enhanced_content,
        "api_style_responses": {
            "keyword": keyword_api_response,
            "username": username_api_response
        }
    }


def print_enriched_content(content: EnhancedXContent):
    """Prints all available fields in an enhanced content object"""
    print(f"BASIC CONTENT:")
    print(f"  Username: {content.username}")
    print(f"  Text: {content.text[:100]}..." if len(content.text) > 100 else f"  Text: {content.text}")
    print(f"  URL: {content.url}")
    print(f"  Timestamp: {content.timestamp}")
    print(f"  Hashtags: {content.tweet_hashtags}")

    print(f"\nUSER INFO:")
    print(f"  User ID: {content.user_id}")
    print(f"  Display Name: {content.user_display_name}")
    print(f"  Verified: {content.user_verified}")
    print(f"  Followers: {content.user_followers_count}")
    print(f"  Following: {content.user_following_count}")

    print(f"\nTWEET METADATA:")
    print(f"  Tweet ID: {content.tweet_id}")
    print(f"  Likes: {content.like_count}")
    print(f"  Retweets: {content.retweet_count}")
    print(f"  Replies: {content.reply_count}")
    print(f"  Quotes: {content.quote_count}")
    print(f"  Is Retweet: {content.is_retweet}")
    print(f"  Is Reply: {content.is_reply}")
    print(f"  Is Quote: {content.is_quote}")

    print(f"\nCONVERSATION INFO:")
    print(f"  Conversation ID: {content.conversation_id}")
    print(f"  In Reply To User ID: {content.in_reply_to_user_id}")

    print(f"\nMEDIA CONTENT:")
    print(f"  Media URLs: {content.media_urls}")
    print(f"  Media Types: {content.media_types}")

    print(f"\nAPI RESPONSE EXAMPLE:")
    print(json.dumps(content.to_api_response(), indent=2))


if __name__ == "__main__":
    asyncio.run(test_enhanced_scraper())