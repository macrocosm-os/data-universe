import os
import random
import asyncio
import asyncpraw
import traceback
import datetime as dt
import bittensor as bt

from common.data import DataEntity, DataLabel, DataSource
from scraping.reddit.utils import (
    is_valid_reddit_url,
    validate_reddit_content,
    validate_media_content,
    validate_nsfw_content,
    get_time_input,
    get_custom_sort_input,
    normalize_label,
    normalize_permalink,
    extract_media_urls
)

from common.date_range import DateRange
from scraping.reddit import model
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.reddit.model import RedditContent, RedditDataType
from typing import List
from dotenv import load_dotenv


load_dotenv()


class RedditCustomScraper(Scraper):
    """
    Scrapes Reddit data using a personal reddit account.
    """

    USER_AGENT = f"User-Agent: python: {os.getenv('REDDIT_USERNAME')}"

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        """
        Validate a list of DataEntity objects.

        * For comments, it checks the parent submission (and subreddit) NSFW flag.
        """
        if not entities:
            return []

        results: List[ValidationResult] = []

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

            # 3) Fetch live data from Reddit
            try:
                async with asyncpraw.Reddit(
                        client_id=os.getenv("REDDIT_CLIENT_ID"),
                        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
                        username=os.getenv("REDDIT_USERNAME"),
                        password=os.getenv("REDDIT_PASSWORD"),
                        user_agent=self.USER_AGENT,
                ) as reddit:

                    # ---- A) POST branch ----
                    if ent_content.data_type == RedditDataType.POST:
                        submission = await reddit.submission(url=ent_content.url)
                        await submission.load()  # ensure attrs

                        live_content = self._best_effort_parse_submission(submission)

                    # ---- B) COMMENT branch ----
                    else:
                        comment = await reddit.comment(url=ent_content.url)
                        await comment.load()

                        parent = comment.submission
                        await parent.load()  # full parent
                        subreddit = comment.subreddit
                        await subreddit.load()  # full subreddit

                        live_content = self._best_effort_parse_comment(comment)

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

            # 6) Media validation (strict check to prevent fake media URLs)
            if validation_result.is_valid:
                media_validation_result = validate_media_content(ent_content, live_content, entity)
                if not media_validation_result.is_valid:
                    validation_result = media_validation_result

            # 7) NSFW validation (check NSFW content - no date restrictions)
            if validation_result.is_valid:
                nsfw_validation_result = validate_nsfw_content(ent_content, live_content, entity)
                if not nsfw_validation_result.is_valid:
                    validation_result = nsfw_validation_result

            results.append(validation_result)

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        """Scrapes a batch of reddit posts/comments according to the scrape config."""
        bt.logging.trace(
            f"Reddit custom scraper peforming scrape with config: {scrape_config}."
        )

        assert (
                not scrape_config.labels or len(scrape_config.labels) <= 1
        ), "Can only scrape 1 subreddit at a time."

        # Strip the r/ from the config or use 'all' if no label is provided.
        subreddit_name = (
            normalize_label(scrape_config.labels[0]) if scrape_config.labels else "all"
        )

        bt.logging.trace(
            f"Running custom Reddit scraper with search: {subreddit_name}."
        )

        # Randomize between fetching submissions and comments to reduce api calls.
        fetch_submissions = bool(random.getrandbits(1))

        # Get the search terms for the reddit query.
        search_limit = scrape_config.entity_limit
        search_sort = get_custom_sort_input(scrape_config.date_range.end)
        search_time = get_time_input(scrape_config.date_range.end)

        # In either case we parse the response into a list of RedditContents.
        contents = None
        try:
            async with asyncpraw.Reddit(
                    client_id=os.getenv("REDDIT_CLIENT_ID"),
                    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
                    username=os.getenv("REDDIT_USERNAME"),
                    password=os.getenv("REDDIT_PASSWORD"),
                    user_agent=RedditCustomScraper.USER_AGENT,
            ) as reddit:
                subreddit = await reddit.subreddit(subreddit_name)

                if fetch_submissions:
                    submissions = None
                    match search_sort:
                        case "new":
                            submissions = subreddit.new(limit=search_limit)
                        case "top":
                            submissions = subreddit.top(
                                limit=search_limit, time_filter=search_time
                            )
                        case "hot":
                            submissions = subreddit.hot(limit=search_limit)

                    contents = [
                        self._best_effort_parse_submission(submission)
                        async for submission in submissions
                    ]
                else:
                    comments = subreddit.comments(limit=search_limit)

                    contents = [
                        self._best_effort_parse_comment(comment)
                        async for comment in comments
                    ]
        except Exception:
            bt.logging.error(
                f"Failed to scrape reddit using subreddit {subreddit_name}, limit {search_limit}, time {search_time}, sort {search_sort}: {traceback.format_exc()}."
            )
            # TODO: Raise a specific exception, in case the scheduler wants to have some logic for retries.
            return []

        # Return the parsed results, ignoring data that can't be parsed.
        parsed_contents = [content for content in contents if content != None]
        
        # Filter out NSFW content with media (same validation logic as in validate_nsfw_content)
        filtered_contents = []
        for content in parsed_contents:
            # Skip NSFW content with media - not valid for the subnet
            if content.is_nsfw and content.media:
                bt.logging.trace(f"Skipping NSFW content with media: {content.url}")
                continue
            filtered_contents.append(content)

        bt.logging.success(
            f"Completed scrape for subreddit {subreddit_name}. Scraped {len(filtered_contents)} items (filtered out {len(parsed_contents) - len(filtered_contents)} NSFW+media posts)."
        )

        data_entities = []
        for content in filtered_contents:
            data_entities.append(RedditContent.to_data_entity(content=content))

        return data_entities

    async def on_demand_scrape(
        self,
        usernames: List[str] = None,
        subreddit: str = None,
        keywords: List[str] = None,
        start_datetime: dt.datetime = None,
        end_datetime: dt.datetime = None,
        limit: int = 100
    ) -> List[DataEntity]:
        """
        Scrapes Reddit data based on specific search criteria.
        
        Args:
            usernames: List of target usernames - content from any of these users will be included (OR logic)
            subreddit: Target specific subreddit (without r/ prefix)
            keywords: List of keywords that must ALL be present in the content
            start_datetime: Earliest datetime for content (UTC)
            end_datetime: Latest datetime for content (UTC)
            limit: Maximum number of items to return (applies per username if usernames provided)
        
        Returns:
            List of DataEntity objects matching the criteria
        """
        
        # Return empty list if all parameters are None
        if all(param is None for param in [usernames, subreddit, keywords, start_datetime, end_datetime]):
            bt.logging.trace("All parameters are None, returning empty list")
            return []
        
        bt.logging.trace(
            f"On-demand scrape with usernames={usernames}, subreddit={subreddit}, "
            f"keywords={keywords}, start={start_datetime}, end={end_datetime}"
        )
        
        contents = []
        
        try:
            async with asyncpraw.Reddit(
                client_id=os.getenv("REDDIT_CLIENT_ID"),
                client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
                username=os.getenv("REDDIT_USERNAME"),
                password=os.getenv("REDDIT_PASSWORD"),
                user_agent=self.USER_AGENT,
            ) as reddit:
                
                # Case 1: Search by usernames
                if usernames:
                    for username in usernames:
                        try:
                            user = await reddit.redditor(username)
                            
                            # Get user's posts (submissions)
                            async for submission in user.submissions.new(limit=limit):
                                content = self._best_effort_parse_submission(submission)
                                if content and self._matches_criteria(content, keywords, start_datetime, end_datetime):
                                    contents.append(content)
                            
                            # Get user's comments
                            async for comment in user.comments.new(limit=limit):
                                content = self._best_effort_parse_comment(comment)
                                if content and self._matches_criteria(content, keywords, start_datetime, end_datetime):
                                    contents.append(content)
                        except Exception as e:
                            bt.logging.warning(f"Failed to scrape user '{username}': {e}")
                            continue
                
                # Case 2: Search by subreddit (with optional keywords)
                elif subreddit:
                    subreddit_name = subreddit.removeprefix("r/") if subreddit.startswith('r/') else subreddit
                    sub = await reddit.subreddit(subreddit_name)
                    
                    # If we have keywords, use Reddit's search functionality
                    if keywords:
                        search_query = ' AND '.join(f'"{keyword}"' for keyword in keywords)
                        search_results = sub.search(search_query, sort='new', limit=limit)
                        
                        async for item in search_results:
                            if hasattr(item, 'title'): 
                                content = self._best_effort_parse_submission(item)
                            else: 
                                content = self._best_effort_parse_comment(item)
                            
                            if content and self._matches_criteria(content, keywords, start_datetime, end_datetime):
                                contents.append(content)
                    else:
                        # No keywords, just get recent posts from subreddit
                        async for submission in sub.new(limit=limit):
                            content = self._best_effort_parse_submission(submission)
                            if content and self._matches_criteria(content, keywords, start_datetime, end_datetime):
                                contents.append(content)
                
                # Case 3: Search by keywords across all of Reddit
                elif keywords:
                    search_query = ' AND '.join(f'"{keyword}"' for keyword in keywords)
                    all_subreddit = await reddit.subreddit('all')
                    search_results = all_subreddit.search(search_query, sort='new', limit=limit)
                    
                    async for item in search_results:
                        if hasattr(item, 'title'):  
                            content = self._best_effort_parse_submission(item)
                        else:  
                            content = self._best_effort_parse_comment(item)
                        
                        if content and self._matches_criteria(content, keywords, start_datetime, end_datetime):
                            contents.append(content)
                
                # Case 4: Just date range filtering (get recent content from r/all)
                else:
                    sub = await reddit.subreddit('all')
                    async for submission in sub.new(limit=limit):
                        content = self._best_effort_parse_submission(submission)
                        if content and self._matches_criteria(content, keywords, start_datetime, end_datetime):
                            contents.append(content)
        
        except Exception as e:
            bt.logging.error(f"Failed to perform on-demand scrape: {e}")
            bt.logging.error(traceback.format_exc())
            return []
        
        # Filter out NSFW content with media (same as in regular scrape method)
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

    def _matches_criteria(
        self,
        content: RedditContent,
        keywords: List[str] = None,
        start_datetime: dt.datetime = None,
        end_datetime: dt.datetime = None
    ) -> bool:
        """
        Check if content matches the specified criteria.
        
        Args:
            content: RedditContent object to check
            keywords: List of keywords that must ALL be present
            start_datetime: Earliest datetime for content
            end_datetime: Latest datetime for content
        
        Returns:
            True if content matches all criteria, False otherwise
        """
        
        if start_datetime:
            # If start_datetime is naive, assume it's UTC
            if start_datetime.tzinfo is None:
                start_datetime = start_datetime.replace(tzinfo=dt.timezone.utc)
            if content.created_at < start_datetime:
                return False
        
        if end_datetime:
            # If end_datetime is naive, assume it's UTC
            if end_datetime.tzinfo is None:
                end_datetime = end_datetime.replace(tzinfo=dt.timezone.utc)
            if content.created_at > end_datetime:
                return False
        
        # Check keywords - all must be present in title + body (case insensitive)
        if keywords:
            # Combine title and body for searching
            searchable_text = ""
            if content.title:
                searchable_text += content.title.lower() + " "
            if content.body:
                searchable_text += content.body.lower()
            
            # All keywords must be present
            for keyword in keywords:
                if keyword.lower() not in searchable_text:
                    return False
        
        return True

    def _best_effort_parse_submission(
            self, submission: asyncpraw.models.Submission
    ) -> RedditContent:
        """Performs a best effort parsing of a Reddit submission into a RedditContent

        Any errors are logged and ignored."""
        content = None

        try:
            # Extract media URLs once
            media_urls = extract_media_urls(submission)

            user = submission.author.name if submission.author else model.DELETED_USER
            content = RedditContent(
                id=submission.name,
                url="https://www.reddit.com"
                    + normalize_permalink(submission.permalink),
                username=user,
                communityName=submission.subreddit_name_prefixed,
                body=submission.selftext,
                createdAt=dt.datetime.utcfromtimestamp(submission.created_utc).replace(
                    tzinfo=dt.timezone.utc
                ),
                dataType=RedditDataType.POST,
                # Post only fields
                title=submission.title,
                # Comment only fields
                parentId=None,
                # Media fields
                media=media_urls if media_urls else None,
                is_nsfw=submission.over_18,
                # Score fields
                score=getattr(submission, 'score', None),
                upvote_ratio=getattr(submission, 'upvote_ratio', None),
                num_comments=getattr(submission, 'num_comments', None),
            )
        except Exception:
            bt.logging.trace(
                f"Failed to decode RedditContent from Reddit Submission: {traceback.format_exc()}."
            )

        return content

    def _best_effort_parse_comment(
            self, comment: asyncpraw.models.Comment
    ) -> RedditContent:
        """Performs a best effort parsing of a Reddit comment into a RedditContent

        Any errors are logged and ignored."""
        content = None

        try:
            user = comment.author.name if comment.author else model.DELETED_USER
            # Comments typically don't have media, but check parent submission for NSFW
            parent_nsfw = getattr(comment.submission, 'over_18', False) if hasattr(comment, 'submission') else False
            subreddit_nsfw = getattr(comment.subreddit, 'over18', False) if hasattr(comment, 'subreddit') else False
            content = RedditContent(
                id=comment.name,
                url="https://www.reddit.com" + normalize_permalink(comment.permalink),
                username=user,
                communityName=comment.subreddit_name_prefixed,
                body=comment.body,
                createdAt=dt.datetime.utcfromtimestamp(comment.created_utc).replace(
                    tzinfo=dt.timezone.utc
                ),
                dataType=RedditDataType.COMMENT,
                # Post only fields
                title=None,
                # Comment only fields
                parentId=comment.parent_id,
                # Media fields
                media=None,  # Comments don't have media
                is_nsfw=parent_nsfw or subreddit_nsfw,
                # Score fields
                score=getattr(comment, 'score', None),
                upvote_ratio=None,  # Not available for comments
                num_comments=None,  # Not applicable for comments
            )
        except Exception:
            bt.logging.trace(
                f"Failed to decode RedditContent from Reddit Submission: {traceback.format_exc()}."
            )

        return content


async def test_scrape():
    scraper = RedditCustomScraper()

    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=3,
            date_range=DateRange(
                start=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=2),
                end=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=2),
            ),
            labels=[DataLabel(value="r/bittensor_")],
        )
    )

    # Scrape some older content without a label.
    start = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=2)
    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=3,
            date_range=DateRange(
                start=start,
                end=start + dt.timedelta(hours=1),
            ),
        )
    )


async def test_validate():
    scraper = RedditCustomScraper()

    # This test covers a top level comment, a submission, and a nested comment with both the correct parent id and the submission id in order.
    # Previous versions of the custom scraper incorrectly got the submission id as the parent id for nested comments.
    true_entities = [
        DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3vd3n/",
            datetime=dt.datetime(2023, 12, 5, 16, 29, 27, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t1_kc3vd3n", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3vd3n/", "username": "one-bad-dude", "communityName": "r/bittensor_", "body": "Its not an EVM chain or ERC-20 token. Its a subnet/substrate of Polkadot ecosystem. So you need the polkadot.js wallet.", "createdAt": "2023-12-05T16:29:27+00:00", "dataType": "comment", "title": null, "parentId": "t3_18bf67l"}',
            content_size_bytes=476,
        ),
        DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/",
            datetime=dt.datetime(2023, 12, 5, 15, 59, 13, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t3_18bf67l", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Hey all!!\\n\\nHow do we add TAO to MetaMask? Online gives me these network configurations and still doesn\\u2019t work? \\n\\nHow are you all storing TAO? I wanna purchase on MEXC, but holding off until I can store it!  \\ud83d\\ude11 \\n\\nThanks in advance!!!\\n\\n=====\\n\\nhere is a manual way.\\nNetwork Name\\nTao Network\\n\\nRPC URL\\nhttp://rpc.testnet.tao.network\\n\\nChain ID\\n558\\n\\nCurrency Symbol\\nTAO", "createdAt": "2023-12-05T15:59:13+00:00", "dataType": "post", "title": "How do you add TAO to MetaMask?", "parentId": null}',
            content_size_bytes=775,
        ),
        DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            datetime=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:16+00:00", "dataType": "comment", "parentId": "t1_kc3vd3n"}',
            content_size_bytes=392,
        ),
        DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            datetime=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:16+00:00", "dataType": "comment", "parentId": "t3_18bf67l"}',
            content_size_bytes=392,
        ),
    ]
    results = await scraper.validate(entities=true_entities)
    print(f"Expecting Pass. Validation results: {results}")

    # Now modify the entities to make them invalid and check validation fails.
    good_entity = true_entities[1]
    good_comment_entity = true_entities[2]
    bad_entities = [
        # Change url.
        good_entity.copy(
            update={
                "uri": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask-abc123/"
            }
        ),
        # Change title.
        good_entity.copy(
            update={
                "content": b'{"id": "t3_18bf67l", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Hey all!!\\n\\nHow do we add TAO to MetaMask? Online gives me these network configurations and still doesn\\u2019t work? \\n\\nHow are you all storing TAO? I wanna purchase on MEXC, but holding off until I can store it!  \\ud83d\\ude11 \\n\\nThanks in advance!!!\\n\\n=====\\n\\nhere is a manual way.\\nNetwork Name\\nTao Network\\n\\nRPC URL\\nhttp://rpc.testnet.tao.network\\n\\nChain ID\\n558\\n\\nCurrency Symbol\\nTAO", "createdAt": "2023-12-05T15:59:13+00:00", "dataType": "post", "title": "How do you add TAO to MetaMask??!!?", "parent_id": null}',
            }
        ),
        # Change created_at.
        good_entity.copy(
            update={"datetime": good_entity.datetime + dt.timedelta(seconds=1)}
        ),
        # Change label.
        good_entity.copy(update={"label": DataLabel(value="bittensor_")}),
        # Change comment parent id.
        good_comment_entity.copy(
            update={
                "content": b'{"id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:16+00:00", "dataType": "comment", "parentId": "extra-long-parent-id"}'
            }
        ),
        # Change submission parent id.
        good_entity.copy(
            update={
                "content": b'{"id": "t3_18bf67l", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Hey all!!\\n\\nHow do we add TAO to MetaMask? Online gives me these network configurations and still doesn\\u2019t work? \\n\\nHow are you all storing TAO? I wanna purchase on MEXC, but holding off until I can store it!  \\ud83d\\ude11 \\n\\nThanks in advance!!!\\n\\n=====\\n\\nhere is a manual way.\\nNetwork Name\\nTao Network\\n\\nRPC URL\\nhttp://rpc.testnet.tao.network\\n\\nChain ID\\n558\\n\\nCurrency Symbol\\nTAO", "createdAt": "2023-12-05T15:59:13+00:00", "dataType": "post", "title": "How do you add TAO to MetaMask?", "parentId": "extra-long-parent-id"}'
            }
        ),
    ]

    for entity in bad_entities:
        results = await scraper.validate(entities=[entity])
        print(f"Expecting a failed validation. Result={results}")


async def test_u_deleted():
    """Verifies that the RedditCustomScraper can handle deleted users."""
    comment = DataEntity(
        uri="https://www.reddit.com/r/AskReddit/comments/ablzuq/people_who_havent_pooped_in_2019_yet_why_are_you/ed1j7is/",
        datetime=dt.datetime(2019, 1, 1, 22, 59, 9, tzinfo=dt.timezone.utc),
        source=1,
        label=DataLabel(value="r/askreddit"),
        content=b'{"id": "t1_ed1j7is", "url": "https://www.reddit.com/r/AskReddit/comments/ablzuq/people_who_havent_pooped_in_2019_yet_why_are_you/ed1j7is/", "username": "[deleted]", "communityName": "r/AskReddit", "body": "Aw man what a terrible way to spend NYE! I hope you feel better soon bud!", "createdAt": "2019-01-01T22:59:09+00:00", "dataType": "comment", "title": null, "parentId": "t1_ed1dqvy"}',
        content_size_bytes=387,
    )

    scraper = RedditCustomScraper()
    result = await scraper.validate(entities=[comment])
    print(f"Expecting a passed validation: {result}")


async def test_on_demand_scrape():
    """Test the on_demand_scrape method with various parameter combinations."""
    scraper = RedditCustomScraper()
    
    print("=" * 60)
    print("TESTING ON-DEMAND SCRAPE")
    print("=" * 60)
    
    # Test 1: All parameters None (should return empty list)
    print("\n1. Testing with all parameters None...")
    entities = await scraper.on_demand_scrape()
    print(f"   Result: {len(entities)} entities (expected: 0)")
    
    # Test 2: Search by subreddit only
    print("\n2. Testing subreddit search (r/bittensor_)...")
    entities = await scraper.on_demand_scrape(subreddit="r/bittensor_", limit=5)
    print(f"   Result: {len(entities)} entities from r/bittensor_")
    if entities:
        print(f"   Sample: {entities[0].uri}")
    
    # Test 3: Search by keywords only
    print("\n3. Testing keyword search ('bittensor', 'TAO')...")
    entities = await scraper.on_demand_scrape(keywords=["bittensor", "TAO"], limit=3)
    print(f"   Result: {len(entities)} entities with both keywords")
    if entities:
        print(f"   Sample: {entities[0].uri}")
    
    # Test 4: Search by subreddit + keywords
    print("\n4. Testing subreddit + keywords (r/cryptocurrency + 'bitcoin')...")
    entities = await scraper.on_demand_scrape(
        subreddit="r/cryptocurrency", 
        keywords=["bitcoin"], 
        limit=3
    )
    print(f"   Result: {len(entities)} entities from r/cryptocurrency with 'bitcoin'")
    if entities:
        print(f"   Sample: {entities[0].uri}")
    
    # Test 5: Search by single username
    print("\n5. Testing single username search...")
    entities = await scraper.on_demand_scrape(usernames=["spez"], limit=3)
    print(f"   Result: {len(entities)} entities from user 'spez'")
    if entities:
        print(f"   Sample: {entities[0].uri}")
    
    # Test 6: Search by multiple usernames =
    print("\n6. Testing multiple usernames search...")
    entities = await scraper.on_demand_scrape(usernames=["spez", "AutoModerator"], limit=5)
    print(f"   Result: {len(entities)} entities from users 'spez' OR 'AutoModerator'")
    if entities:
        print(f"   Sample: {entities[0].uri}")
        # Show which users were found
        found_users = set()
        for entity in entities[:3]:  # Check first 3 entities
            try:
                content = RedditContent.from_data_entity(entity)
                found_users.add(content.username)
            except:
                pass
        print(f"   Found content from users: {found_users}")
    
    # Test 7: Search by usernames + keywords (both conditions must be met)
    print("\n7. Testing usernames + keywords...")
    entities = await scraper.on_demand_scrape(
        usernames=["spez", "AutoModerator"],
        keywords=["reddit"], 
        limit=3
    )
    print(f"   Result: {len(entities)} entities from specified users containing 'reddit'")
    if entities:
        print(f"   Sample: {entities[0].uri}")
    
    # Test 8: Date range filtering
    print("\n8. Testing date range filtering (last 7 days)...")
    end_time = dt.datetime.now(tz=dt.timezone.utc)
    start_time = end_time - dt.timedelta(days=7)
    entities = await scraper.on_demand_scrape(
        subreddit="r/python",
        start_datetime=start_time,
        end_datetime=end_time,
        limit=3
    )
    print(f"   Result: {len(entities)} entities from r/python in last 7 days")
    if entities:
        print(f"   Sample: {entities[0].uri}")
        print(f"   Sample date: {entities[0].datetime}")
    
    # Test 9: Complex search (usernames + keywords + date range)
    print("\n9. Testing complex search (usernames + keywords + date range)...")
    end_time = dt.datetime.now(tz=dt.timezone.utc)
    start_time = end_time - dt.timedelta(days=30)
    entities = await scraper.on_demand_scrape(
        usernames=["spez", "AutoModerator"],
        keywords=["announcement"],
        start_datetime=start_time,
        end_datetime=end_time,
        limit=2
    )
    print(f"   Result: {len(entities)} entities matching complex criteria")
    if entities:
        print(f"   Sample: {entities[0].uri}")
        print(f"   Sample date: {entities[0].datetime}")
    
    # Test 10: Invalid/non-existent usernames (should handle gracefully)
    print("\n10. Testing with invalid usernames...")
    entities = await scraper.on_demand_scrape(
        usernames=["nonexistentuser12345", "anotherfakeuser99999"], 
        limit=3
    )
    print(f"   Result: {len(entities)} entities from invalid usernames (expected: 0 or very few)")
    
    print("\n" + "=" * 60)
    print("ON-DEMAND SCRAPE TESTS COMPLETED")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_scrape())
    asyncio.run(test_validate())
    asyncio.run(test_u_deleted())
    asyncio.run(test_on_demand_scrape())