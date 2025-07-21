import random
import json
import asyncio
from typing import Dict, List, Tuple, Any, Optional
import bittensor as bt
from common.data import DataSource, DataLabel, DataEntity
from common.protocol import OnDemandRequest
from common.organic_protocol import OrganicRequest
from common import constants, utils
from scraping.provider import ScraperProvider
from scraping.x.enhanced_apidojo_scraper import EnhancedApiDojoTwitterScraper
from scraping.x.on_demand_model import EnhancedXContent
from scraping.reddit.model import RedditContent
from scraping.scraper import ScrapeConfig
from common.date_range import DateRange
import datetime as dt
import json


class OrganicQueryProcessor:
    """Handles organic query processing, cross-validation, and miner evaluation"""
    
    def __init__(self, validator):
        self.validator = validator
        self.wallet = validator.wallet
        self.metagraph = validator.metagraph
        self.evaluator = validator.evaluator
        
        # Configuration constants
        self.NUM_MINERS_TO_QUERY = 5
        self.CROSS_VALIDATION_SAMPLE_SIZE = 10
        self.MIN_POST_THRESHOLD = 0.8  # 80% of requested posts
        self.SEVERE_UNDERPERFORMANCE_THRESHOLD = 0.5  # 50% of requested posts
    
    async def process_organic_query(self, synapse: OrganicRequest) -> OrganicRequest:
        """
        Main entry point for processing organic queries
        """
        bt.logging.info(f"Processing organic query for source: {synapse.source}")
        
        try:
            # Step 1: Select miners for querying
            selected_miners = self._select_miners()
            if not selected_miners:
                return self._create_error_response(synapse, "Not enough miners available")
            
            # Step 2: Query miners
            miner_responses, miner_data_counts = await self._query_miners(synapse, selected_miners)
            
            # Step 3: Apply basic penalties (timeouts, empty responses)
            non_responsive_uids, empty_uids = self._apply_basic_penalties(selected_miners, miner_responses)
            
            # Step 4: Perform backup validation scrape to determine actual data availability
            backup_scrape_count = await self._perform_backup_scrape(synapse)
            bt.logging.info(f"Backup scrape found {backup_scrape_count} posts available")
            
            # Step 5: Apply insufficient post count penalties (only if backup scrape shows more data exists)
            insufficient_miners = self._apply_post_count_penalties(
                synapse.limit, miner_data_counts, backup_scrape_count
            )
            
            # Step 6: Perform cross-validation
            validation_results = await self._perform_cross_validation(synapse, miner_responses)
            
            # Step 7: Calculate final scores with all penalties applied
            miner_scores = self._apply_validation_penalties(miner_responses, validation_results)
            
            # Step 8: Select best data and format response
            return self._create_success_response(
                synapse, miner_responses, miner_scores, {
                    'selected_miners': selected_miners,
                    'non_responsive_uids': non_responsive_uids,
                    'empty_uids': empty_uids,
                    'insufficient_miners': insufficient_miners,
                    'validation_results': validation_results,
                    'backup_scrape_count': backup_scrape_count
                }
            )
            
        except Exception as e:
            bt.logging.error(f"Error in organic query processing: {str(e)}")
            return self._create_error_response(synapse, str(e))
    
    def _select_miners(self) -> List[int]:
        """Select diverse set of miners for querying"""
        miner_uids = utils.get_miner_uids(self.metagraph, self.validator.uid, 10000)
        miner_scores = [(uid, float(self.metagraph.I[uid])) for uid in miner_uids]
        miner_scores.sort(key=lambda x: x[1], reverse=True)
        
        # Take top 60% of miners (but at least 5 if available)
        top_count = max(5, int(len(miner_scores) * 0.6))
        top_miners = miner_scores[:top_count]
        
        if len(top_miners) < 2:
            return []
        
        # Select diverse miners
        selected_miners = []
        selected_coldkeys = set()
        
        while len(selected_miners) < self.NUM_MINERS_TO_QUERY and top_miners:
            idx = random.randint(0, len(top_miners) - 1)
            uid, _ = top_miners.pop(idx)
            coldkey = self.metagraph.coldkeys[uid]
            
            if coldkey not in selected_coldkeys or len(selected_coldkeys) < 2:
                selected_miners.append(uid)
                selected_coldkeys.add(coldkey)
        
        # Fill remaining slots if needed
        if len(selected_miners) < 1:
            for uid in utils.get_miner_uids(self.metagraph, self.validator.uid, 10000):
                if uid not in selected_miners:
                    selected_miners.append(uid)
                    if len(selected_miners) >= self.NUM_MINERS_TO_QUERY:
                        break
        
        bt.logging.info(f"Selected {len(selected_miners)} miners for query: {selected_miners}")
        return selected_miners
    
    async def _query_miners(self, synapse: OrganicRequest, selected_miners: List[int]) -> Tuple[Dict[int, List], Dict[int, int]]:
        """Query selected miners and return their responses"""
        on_demand_synapse = OnDemandRequest(
            source=DataSource[synapse.source.upper()],
            usernames=synapse.usernames,
            keywords=synapse.keywords,
            start_date=synapse.start_date,
            end_date=synapse.end_date,
            limit=synapse.limit,
            version=constants.PROTOCOL_VERSION
        )
        
        miner_responses = {}
        miner_data_counts = {}
        
        async with bt.dendrite(wallet=self.wallet) as dendrite:
            axons = [self.metagraph.axons[uid] for uid in selected_miners]
            responses = await dendrite.forward(
                axons=axons,
                synapse=on_demand_synapse,
                timeout=30
            )
            
            for i, response in enumerate(responses):
                if i < len(selected_miners):
                    uid = selected_miners[i]
                    hotkey = self.metagraph.hotkeys[uid]
                    
                    if response is not None and hasattr(response, 'data'):
                        data = getattr(response, 'data', [])
                        data_count = len(data) if data else 0
                        
                        miner_responses[uid] = data
                        miner_data_counts[uid] = data_count
                        
                        bt.logging.info(f"Miner {uid} ({hotkey}) returned {data_count} items")
                    else:
                        bt.logging.warning(f"Miner {uid} ({hotkey}) failed to respond properly")
        
        return miner_responses, miner_data_counts
    
    def _apply_basic_penalties(self, selected_miners: List[int], miner_responses: Dict[int, List]) -> Tuple[List[int], List[int]]:
        """Apply penalties for timeouts and empty responses"""
        # Non-responsive miners
        non_responsive_uids = [uid for uid in selected_miners if uid not in miner_responses]
        for uid in non_responsive_uids:
            bt.logging.info(f"Applying penalty to non-responsive miner {uid}")
            self.evaluator.scorer.apply_ondemand_penalty(uid, 1)
        
        # Empty response miners
        empty_uids = [uid for uid, rows in miner_responses.items() if len(rows) == 0]
        
        return non_responsive_uids, empty_uids
    
    async def _perform_backup_scrape(self, synapse: OrganicRequest) -> int:
        """
        Perform a backup scrape to determine how much data actually exists.
        This is used to validate whether miners should be penalized for insufficient data.
        """
        bt.logging.info("Performing backup scrape to validate data availability...")
        
        try:
            scraper = self._get_scraper(synapse.source)
            if not scraper:
                bt.logging.warning("No scraper available for backup validation")
                return 0
            
            # Create scrape config matching the original request
            labels = []
            if synapse.keywords:
                labels.extend([DataLabel(value=k) for k in synapse.keywords])
            if synapse.usernames:
                # Ensure usernames have @ prefix for X
                labels.extend([DataLabel(value=f"@{u.strip('@')}" if not u.startswith('@') else u) 
                              for u in synapse.usernames])
            
            start_date = utils.parse_iso_date(synapse.start_date) if synapse.start_date else dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
            end_date = utils.parse_iso_date(synapse.end_date) if synapse.end_date else dt.datetime.now(dt.timezone.utc)
            
            backup_config = ScrapeConfig(
                entity_limit=synapse.limit,  # Use the same limit as requested
                date_range=DateRange(
                    start=start_date,
                    end=end_date
                ),
                labels=labels,
            )
            
            # Perform backup scrape
            if synapse.source.upper() == 'X':
                enhanced_content = await scraper.scrape_enhanced(backup_config)
                backup_data_count = len(enhanced_content)
            else:
                backup_data = await scraper.scrape(backup_config)
                backup_data_count = len(backup_data) if backup_data else 0
            
            bt.logging.info(f"Backup scrape completed: found {backup_data_count} posts")
            return backup_data_count
            
        except Exception as e:
            bt.logging.error(f"Error during backup scrape: {str(e)}")
            return 0
    
    def _apply_post_count_penalties(self, requested_limit: int, miner_data_counts: Dict[int, int], 
                                  backup_scrape_count: int) -> List[int]:
        """
        Apply penalties for insufficient post counts, but ONLY if backup scrape shows more data exists.
        This prevents punishing miners when the requested data simply doesn't exist.
        """
        bt.logging.info("Evaluating miners for insufficient post counts...")
        
        min_acceptable_posts = int(requested_limit * self.MIN_POST_THRESHOLD)
        
        # Only penalize if backup scrape found more data than the threshold
        backup_exceeds_threshold = backup_scrape_count >= min_acceptable_posts
        
        bt.logging.info(f"Backup scrape found {backup_scrape_count} posts. Threshold is {min_acceptable_posts}. Will penalize: {backup_exceeds_threshold}")
        
        insufficient_miners = []
        
        if not backup_exceeds_threshold:
            bt.logging.info("Backup scrape shows insufficient data exists - skipping post count penalties")
            return insufficient_miners
        
        for uid, post_count in miner_data_counts.items():
            if post_count < min_acceptable_posts:
                insufficient_miners.append(uid)
                bt.logging.info(f"Miner {uid} provided {post_count}/{requested_limit} posts (below {self.MIN_POST_THRESHOLD*100}% threshold) - backup scrape confirms more data exists")
        
        # Apply penalties only when we know more data exists
        for uid in insufficient_miners:
            bt.logging.info(f"Applying full penalty to miner {uid} - backup scrape confirms missed available data")
            self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
        
        return insufficient_miners
    
    async def _perform_cross_validation(self, synapse: OrganicRequest, miner_responses: Dict[int, List]) -> Dict[str, bool]:
        """Perform cross-validation on pooled miner responses"""
        bt.logging.info("Starting cross-validation process...")
        
        # Pool all responses
        all_posts, post_to_miners = self._pool_responses(miner_responses)
        
        # Select posts for validation
        posts_to_validate = self._select_validation_posts(all_posts, post_to_miners)
        
        # Perform actual validation
        validation_results = await self._validate_posts(synapse, posts_to_validate)
        
        bt.logging.info(f"Cross-validation completed: {sum(validation_results.values())}/{len(validation_results)} passed")
        return validation_results
    
    def _pool_responses(self, miner_responses: Dict[int, List]) -> Tuple[List, Dict[str, List[int]]]:
        """Pool all miner responses and track duplicates"""
        all_posts = []
        post_to_miners = {}
        
        for uid, posts in miner_responses.items():
            if not posts:
                continue
            
            for post in posts:
                post_id = self._get_post_id(post)
                
                if post_id not in post_to_miners:
                    post_to_miners[post_id] = []
                    all_posts.append(post)
                
                post_to_miners[post_id].append(uid)
        
        bt.logging.info(f"Found {len(all_posts)} unique posts with {sum(len(miners) - 1 for miners in post_to_miners.values())} duplicates")
        return all_posts, post_to_miners
    
    def _select_validation_posts(self, all_posts: List, post_to_miners: Dict[str, List[int]]) -> List:
        """Select posts for validation randomly from the unique pool"""
        validation_sample_size = min(self.CROSS_VALIDATION_SAMPLE_SIZE, len(all_posts))
        
        if validation_sample_size == 0:
            return []
        
        # Simple random sampling from all unique posts
        posts_to_validate = random.sample(all_posts, validation_sample_size)
        
        bt.logging.info(f"Selected {len(posts_to_validate)} posts for validation (random sampling)")
        return posts_to_validate
    
    def _validate_requested_fields(self, synapse: OrganicRequest, post) -> bool:
        """
        Validate that the returned data matches the requested fields (usernames, keywords, time range).
        
        Args:
            synapse: The organic request with validation criteria
            post: The post data (either dict or DataEntity)
            
        Returns:
            bool: True if the post matches the request criteria, False otherwise
        """
        try:
            if synapse.source.upper() == 'X':
                # Convert to EnhancedXContent for validation
                x_content = self._convert_to_enhanced_x_content(post)
                if not x_content:
                    return False
                
                # Validate username if specified
                if synapse.usernames:
                    requested_usernames = [u.strip('@').lower() for u in synapse.usernames]
                    post_username = x_content.username.strip('@').lower()
                    if post_username not in requested_usernames:
                        bt.logging.debug(f"Username mismatch: post has {post_username}, requested {requested_usernames}")
                        return False
                
                # Validate keywords if specified
                if synapse.keywords:
                    post_text = x_content.text.lower()
                    keyword_found = any(keyword.lower() in post_text for keyword in synapse.keywords)
                    if not keyword_found:
                        bt.logging.debug(f"Keyword mismatch: none of {synapse.keywords} found in post text")
                        return False
                
                # Validate time range
                if not self._validate_time_range(synapse, x_content.timestamp):
                    return False
                
                # Validate tweet metadata completeness
                if not self._validate_x_metadata_completeness(x_content):
                    return False
                    
            else: 
                # Reddit validation
                reddit_content = self._convert_to_reddit_content(post)
                if not reddit_content:
                    return False
                
                # Validate username if specified
                if synapse.usernames:
                    requested_usernames = [u.lower() for u in synapse.usernames]
                    post_username = reddit_content.username.lower()
                    if post_username not in requested_usernames:
                        bt.logging.debug(f"Reddit username mismatch: post has {post_username}, requested {requested_usernames}")
                        return False
                
                # Validate subreddits (labels) if specified
                if synapse.keywords:  # In Reddit context, keywords could be subreddit names or text keywords
                    # Check if keywords match subreddit names (removing r/ prefix)
                    post_community = reddit_content.community.lower().removeprefix('r/')
                    subreddit_match = any(keyword.lower().removeprefix('r/') == post_community for keyword in synapse.keywords)
                    
                    # Also check if keywords appear in post content (body + title)
                    content_text = (reddit_content.body or '').lower()
                    if reddit_content.title:
                        content_text += ' ' + reddit_content.title.lower()
                    
                    keyword_in_content = any(keyword.lower() in content_text for keyword in synapse.keywords)
                    
                    if not (subreddit_match or keyword_in_content):
                        bt.logging.debug(f"Reddit keyword mismatch: none of {synapse.keywords} found in subreddit '{post_community}' or content")
                        return False
                
                # Validate time range
                if not self._validate_time_range(synapse, reddit_content.created_at):
                    return False
                
            return True
            
        except Exception as e:
            bt.logging.error(f"Error validating requested fields: {str(e)}")
            return False
    
    async def _validate_posts(self, synapse: OrganicRequest, posts_to_validate: List) -> Dict[str, bool]:
        """Validate selected posts using appropriate scraper"""
        validation_results = {}
        
        if not posts_to_validate:
            return validation_results
        
        try:
            scraper = self._get_scraper(synapse.source)
            if not scraper:
                bt.logging.warning("No scraper available for validation")
                return validation_results
            
            # Convert posts to validation format
            posts_for_validation = []
            for post in posts_to_validate:
                if isinstance(post, dict):
                    posts_for_validation.append(post)
                else:
                    post_dict = {
                        'uri': getattr(post, 'uri', None),
                        'datetime': getattr(post, 'datetime', None),
                        'source': getattr(post, 'source', None),
                        'content': getattr(post, 'content', None)
                    }
                    posts_for_validation.append(post_dict)
            
            # Perform request matching validation first
            bt.logging.info(f"Performing request field validation on {len(posts_for_validation)} posts")
            request_validated_posts = []
            for post in posts_for_validation:
                if self._validate_requested_fields(synapse, post):
                    request_validated_posts.append(post)
                else:
                    # Post failed request matching validation - mark as invalid
                    post_id = self._get_post_id(post) if post in posts_to_validate else str(hash(str(post)))
                    validation_results[post_id] = False
                    bt.logging.info(f"Post {post_id} failed request field validation")
            
            bt.logging.info(f"Request field validation: {len(request_validated_posts)}/{len(posts_for_validation)} posts passed")

            # Perform standard validation only on posts that passed request validation
            if request_validated_posts:
                validation_check_results = await scraper.validate(request_validated_posts)
                
                # Map results back to post IDs for posts that passed request validation
                for i, result in enumerate(validation_check_results):
                    if i < len(request_validated_posts):
                        # Find the original post in posts_to_validate
                        validated_post = request_validated_posts[i]
                        original_post_index = posts_for_validation.index(validated_post)
                        if original_post_index < len(posts_to_validate):
                            post_id = self._get_post_id(posts_to_validate[original_post_index])
                            validation_results[post_id] = result.is_valid if hasattr(result, 'is_valid') else bool(result)
            
        except Exception as e:
            bt.logging.error(f"Error during validation: {str(e)}")
        
        return validation_results
    
    def _convert_to_enhanced_x_content(self, post) -> Optional[EnhancedXContent]:
        """
        Convert a post (dict or DataEntity) to EnhancedXContent for validation.
        
        Args:
            post: The post data (either dict or DataEntity)
            
        Returns:
            EnhancedXContent: Converted content object, or None if conversion fails
        """
        try:
            if isinstance(post, dict):
                # Try to parse as JSON content first
                if 'content' in post and isinstance(post['content'], str):
                    try:
                        content_data = json.loads(post['content'])
                        # Create EnhancedXContent from parsed JSON
                        return EnhancedXContent(
                            username=content_data.get('username', ''),
                            text=content_data.get('text', ''),
                            url=content_data.get('url', ''),
                            timestamp=dt.datetime.fromisoformat(content_data.get('timestamp', dt.datetime.now().isoformat())),
                            tweet_hashtags=content_data.get('tweet_hashtags', [])
                        )
                    except (json.JSONDecodeError, ValueError):
                        pass
                
                # Handle dict with direct fields
                return EnhancedXContent(
                    username=post.get('username', ''),
                    text=post.get('text', ''),
                    url=post.get('url', post.get('uri', '')),
                    timestamp=self._parse_timestamp(post.get('timestamp', post.get('datetime'))),
                    tweet_hashtags=post.get('tweet_hashtags', [])
                )
            
            elif hasattr(post, 'content'):
                # DataEntity object
                if isinstance(post.content, bytes):
                    content_str = post.content.decode('utf-8')
                else:
                    content_str = post.content
                    
                try:
                    content_data = json.loads(content_str)
                    return EnhancedXContent(
                        username=content_data.get('username', ''),
                        text=content_data.get('text', ''),
                        url=content_data.get('url', ''),
                        timestamp=self._parse_timestamp(content_data.get('timestamp')),
                        tweet_hashtags=content_data.get('tweet_hashtags', [])
                    )
                except json.JSONDecodeError:
                    # Try using EnhancedXContent.from_data_entity
                    return EnhancedXContent.from_data_entity(post)
                    
        except Exception as e:
            bt.logging.warning(f"Failed to convert post to EnhancedXContent: {str(e)}")
            return None
            
        return None
    
    def _convert_to_reddit_content(self, post) -> Optional[RedditContent]:
        """
        Convert a post (dict or DataEntity) to RedditContent for validation.
        
        Args:
            post: The post data (either dict or DataEntity)
            
        Returns:
            RedditContent: Converted content object, or None if conversion fails
        """
        try:
            if isinstance(post, dict):
                # Try to parse as JSON content first
                if 'content' in post and isinstance(post['content'], str):
                    try:
                        content_data = json.loads(post['content'])
                        # Create RedditContent from parsed JSON
                        return RedditContent(
                            id=content_data.get('id', ''),
                            url=content_data.get('url', ''),
                            username=content_data.get('username', ''),
                            community=content_data.get('communityName', content_data.get('community', '')),
                            body=content_data.get('body', ''),
                            created_at=self._parse_timestamp(content_data.get('createdAt', content_data.get('created_at'))),
                            data_type=content_data.get('dataType', content_data.get('data_type', 'post')),
                            title=content_data.get('title'),
                            parent_id=content_data.get('parentId', content_data.get('parent_id'))
                        )
                    except (json.JSONDecodeError, ValueError):
                        pass
                
                # Handle dict with direct fields
                return RedditContent(
                    id=post.get('id', ''),
                    url=post.get('url', post.get('uri', '')),
                    username=post.get('username', ''),
                    community=post.get('communityName', post.get('community', '')),
                    body=post.get('body', ''),
                    created_at=self._parse_timestamp(post.get('createdAt', post.get('created_at', post.get('datetime')))),
                    data_type=post.get('dataType', post.get('data_type', 'post')),
                    title=post.get('title'),
                    parent_id=post.get('parentId', post.get('parent_id'))
                )
            
            elif hasattr(post, 'content'):
                # DataEntity object
                if isinstance(post.content, bytes):
                    content_str = post.content.decode('utf-8')
                else:
                    content_str = post.content
                    
                try:
                    content_data = json.loads(content_str)
                    return RedditContent(
                        id=content_data.get('id', ''),
                        url=content_data.get('url', ''),
                        username=content_data.get('username', ''),
                        community=content_data.get('communityName', content_data.get('community', '')),
                        body=content_data.get('body', ''),
                        created_at=self._parse_timestamp(content_data.get('createdAt', content_data.get('created_at'))),
                        data_type=content_data.get('dataType', content_data.get('data_type', 'post')),
                        title=content_data.get('title'),
                        parent_id=content_data.get('parentId', content_data.get('parent_id'))
                    )
                except json.JSONDecodeError:
                    # Try using RedditContent.from_data_entity
                    return RedditContent.from_data_entity(post)
                    
        except Exception as e:
            bt.logging.warning(f"Failed to convert post to RedditContent: {str(e)}")
            return None
            
        return None
    
    def _parse_timestamp(self, timestamp_str) -> dt.datetime:
        """
        Parse timestamp string to datetime object.
        
        Args:
            timestamp_str: Timestamp as string or datetime object
            
        Returns:
            dt.datetime: Parsed datetime, defaults to current time if parsing fails
        """
        if isinstance(timestamp_str, dt.datetime):
            return timestamp_str
            
        if not timestamp_str:
            return dt.datetime.now(dt.timezone.utc)
            
        try:
            # Try ISO format first
            return dt.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError:
            try:
                # Try common Twitter format
                return dt.datetime.strptime(timestamp_str, "%a %b %d %H:%M:%S %z %Y")
            except ValueError:
                bt.logging.warning(f"Failed to parse timestamp: {timestamp_str}")
                return dt.datetime.now(dt.timezone.utc)
    
    def _validate_time_range(self, synapse: OrganicRequest, post_timestamp: dt.datetime) -> bool:
        """
        Validate that the post timestamp falls within the requested time range.
        
        Args:
            synapse: The organic request with time range
            post_timestamp: The timestamp of the post
            
        Returns:
            bool: True if timestamp is within range, False otherwise
        """
        try:
            if synapse.start_date:
                start_dt = utils.parse_iso_date(synapse.start_date)
                if post_timestamp < start_dt:
                    bt.logging.debug(f"Post timestamp {post_timestamp} is before start date {start_dt}")
                    return False
                    
            if synapse.end_date:
                end_dt = utils.parse_iso_date(synapse.end_date)
                if post_timestamp > end_dt:
                    bt.logging.debug(f"Post timestamp {post_timestamp} is after end date {end_dt}")
                    return False
                    
            return True
            
        except Exception as e:
            bt.logging.error(f"Error validating time range: {str(e)}")
            return False
    
    def _validate_x_metadata_completeness(self, x_content: EnhancedXContent) -> bool:
        """
        Validate that X content has all required tweet metadata fields present.
        
        Args:
            x_content: The EnhancedXContent object to validate
            
        Returns:
            bool: True if all required metadata is present, False otherwise
        """
        try:
            # All tweet metadata fields are required for organic responses
            required_fields = [
                ('tweet_id', 'Tweet ID'),
                ('like_count', 'Like count'),
                ('retweet_count', 'Retweet count'),
                ('reply_count', 'Reply count'),
                ('quote_count', 'Quote count'),
                ('is_retweet', 'Is retweet flag'),
                ('is_reply', 'Is reply flag'),
                ('is_quote', 'Is quote flag')
            ]
            
            missing_fields = []
            
            # Check all required fields
            for field_name, display_name in required_fields:
                field_value = getattr(x_content, field_name, None)
                if field_value is None:
                    missing_fields.append(display_name)
            
            # Fail validation if any required fields are missing
            if missing_fields:
                bt.logging.info(f"Tweet {x_content.url} missing required metadata: {missing_fields}")
                return False
            
            # Additional validation: ensure numeric fields are actually numeric
            numeric_fields = ['like_count', 'retweet_count', 'reply_count', 'quote_count']
            for field_name in numeric_fields:
                field_value = getattr(x_content, field_name, None)
                if field_value is not None and not isinstance(field_value, (int, float)):
                    try:
                        # Try to convert to int
                        int(field_value)
                    except (ValueError, TypeError):
                        bt.logging.info(f"Tweet {x_content.url} has invalid {field_name}: {field_value} (not numeric)")
                        return False
            
            # Additional validation: ensure boolean fields are actually boolean
            boolean_fields = ['is_retweet', 'is_reply', 'is_quote']
            for field_name in boolean_fields:
                field_value = getattr(x_content, field_name, None)
                if field_value is not None and not isinstance(field_value, bool):
                    bt.logging.info(f"Tweet {x_content.url} has invalid {field_name}: {field_value} (not boolean)")
                    return False
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error validating X metadata completeness: {str(e)}")
            return False
    
    def _get_scraper(self, source: str):
        """Get appropriate scraper for the data source"""
        try:
            if source.upper() == 'X':
                return EnhancedApiDojoTwitterScraper()
            else:
                scraper_id = self.evaluator.PREFERRED_SCRAPERS.get(DataSource[source.upper()])
                if scraper_id:
                    return ScraperProvider().get(scraper_id)
        except Exception as e:
            bt.logging.error(f"Error getting scraper for {source}: {str(e)}")
        return None
    
    def _apply_validation_penalties(self, miner_responses: Dict[int, List], validation_results: Dict[str, bool]) -> Dict[int, int]:
        """Calculate final scores incorporating all penalties"""
        miner_scores = {}
        
        for uid in miner_responses.keys():
            if not miner_responses[uid]:
                miner_scores[uid] = 0
                continue
            
            # Apply validation penalty
            miner_failed_validation = False
            validated_posts_count = 0
            
            for post in miner_responses[uid]:
                post_id = self._get_post_id(post)
                if post_id in validation_results:
                    validated_posts_count += 1
                    if not validation_results[post_id]:
                        miner_failed_validation = True
                        bt.logging.info(f"Miner {uid} failed validation for post {post_id}")
                        break
            
            if miner_failed_validation:
                miner_scores[uid] = 0
                bt.logging.info(f"Miner {uid} score zeroed due to failed validation")
                self.evaluator.scorer.apply_ondemand_penalty(uid, 1)
        
        bt.logging.info(f"Final miner scores: {miner_scores}")
        return miner_scores
    
    def _create_success_response(self, synapse: OrganicRequest, miner_responses: Dict[int, List], 
                               miner_scores: Dict[int, int], metadata: Dict) -> OrganicRequest:
        """Create successful response with best miner data"""
        miners_with_valid_data = {uid: score for uid, score in miner_scores.items() if score > 0}
        
        if not miners_with_valid_data:
            return self._create_empty_response(synapse, metadata)
        
        # Select best miner
        best_uid = max(miners_with_valid_data.keys(), key=lambda uid: miner_scores[uid])
        best_data = miner_responses[best_uid]
        
        bt.logging.info(f"Selected miner {best_uid} with score {miner_scores[best_uid]}")
        
        # Process data
        processed_data = self._process_response_data(synapse, best_data)
        
        # Remove duplicates
        unique_data = self._remove_duplicates(processed_data)
        
        synapse.status = "success"
        synapse.data = unique_data[:synapse.limit]
        synapse.meta = {
            "miners_queried": len(metadata['selected_miners']),
            "miners_responded": len(miner_responses),
            "non_responsive_miners": len(metadata['non_responsive_uids']),
            "empty_response_miners": len(metadata['empty_uids']),
            "insufficient_post_miners": len(metadata['insufficient_miners']),
            "backup_scrape_count": metadata['backup_scrape_count'],
            "backup_scrape_exceeded_threshold": metadata['backup_scrape_count'] >= int(synapse.limit * self.MIN_POST_THRESHOLD),
            "cross_validation_performed": len(metadata['validation_results']) > 0,
            "validation_success_rate": f"{sum(metadata['validation_results'].values())}/{len(metadata['validation_results'])}" if metadata['validation_results'] else "0/0",
            "best_miner_uid": best_uid,
            "best_miner_hotkey": self.metagraph.hotkeys[best_uid],
            "cross_validation_scores": miner_scores,
            "items_returned": len(unique_data),
            "post_count_penalties_applied": "only when backup scrape confirmed more data exists"
        }
        
        return synapse
    
    def _create_empty_response(self, synapse: OrganicRequest, metadata: Dict) -> OrganicRequest:
        """Create response when no valid data is available"""
        synapse.status = "success"
        synapse.data = []
        synapse.meta = {
            "miners_queried": len(metadata['selected_miners']),
            "miners_responded": len(metadata.get('miner_responses', {})),
            "backup_scrape_count": metadata.get('backup_scrape_count', 0),
            "consensus": "no_valid_data",
            "cross_validation_performed": len(metadata['validation_results']) > 0
        }
        return synapse
    
    def _create_error_response(self, synapse: OrganicRequest, error_msg: str) -> OrganicRequest:
        """Create error response"""
        synapse.status = "error"
        synapse.meta = {"error": error_msg}
        synapse.data = []
        return synapse
    
    def _process_response_data(self, synapse: OrganicRequest, data: List) -> List[Dict]:
        """Process raw response data into standardized format"""
        processed_data = []
        
        for item in data:
            if isinstance(item, dict):
                processed_data.append(item)
            else:
                # Handle DataEntity objects
                if synapse.source.upper() == 'X':
                    try:
                        if hasattr(item, 'content') and item.content:
                            content_str = item.content.decode('utf-8') if isinstance(item.content, bytes) else item.content
                            try:
                                item_dict = json.loads(content_str)
                                processed_data.append(item_dict)
                                continue
                            except json.JSONDecodeError:
                                pass
                    except Exception as e:
                        bt.logging.error(f"Error processing X content: {str(e)}")
                
                # Standard processing
                item_dict = {
                    'uri': getattr(item, 'uri', None),
                    'datetime': getattr(item, 'datetime', None).isoformat() if hasattr(item, 'datetime') and item.datetime else None,
                    'source': DataSource(getattr(item, 'source')).name if hasattr(item, 'source') else None,
                    'label': getattr(item, 'label').value if hasattr(item, 'label') and item.label else None,
                    'content': item.content.decode('utf-8') if hasattr(item, 'content') and isinstance(item.content, bytes) else
                              getattr(item, 'content', None)
                }
                processed_data.append(item_dict)
        
        return processed_data
    
    def _remove_duplicates(self, data: List[Dict]) -> List[Dict]:
        """Remove duplicate items from processed data"""
        seen = set()
        unique_data = []
        
        for item in data:
            item_str = str(sorted(item.items()))
            if item_str not in seen:
                seen.add(item_str)
                unique_data.append(item)
        
        return unique_data
    
    def _get_post_id(self, post) -> str:
        """Generate consistent post identifier"""
        if isinstance(post, dict):
            return post.get('uri') or str(hash(str(sorted(post.items()))))
        else:
            return getattr(post, 'uri', None) or str(hash(str(post)))