import random
import json
import asyncio
import statistics
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
from vali_utils.miner_evaluator import MinerEvaluator


class OrganicQueryProcessor:
    """Handles organic query processing, cross-validation, and miner evaluation"""
    
    def __init__(self, 
                 wallet: bt.wallet,
                 metagraph: bt.metagraph, 
                 evaluator: MinerEvaluator):
        self.wallet = wallet
        self.metagraph = metagraph
        self.evaluator = evaluator
        
        # constants
        self.NUM_MINERS_TO_QUERY = 5
        self.CROSS_VALIDATION_SAMPLE_SIZE = 10
        self.MIN_CONSENSUS = 0.3    # if consensus is <30% of request size, consensus penalties skipped
    
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
            non_responsive_uids, empty_uids, early_response = await self._apply_basic_penalties(synapse, selected_miners, miner_responses)
            
            # If early response (all miners empty), return immediately
            if early_response:
                return early_response
            
            # Step 4: Apply consensus-based volume penalties 
            insufficient_miners = self._apply_consensus_volume_penalties(miner_data_counts, synapse.limit)
            
            # Step 5: Perform cross-validation and get pooled data
            validation_results, pooled_data = await self._perform_cross_validation(synapse, miner_responses)
            
            # Step 6: Calculate final scores with all penalties applied
            miner_scores = self._apply_validation_penalties(miner_responses, validation_results)
            
            # Step 7: Format response
            return self._create_success_response(
                synapse, miner_responses, miner_scores, pooled_data, {
                    'selected_miners': selected_miners,
                    'non_responsive_uids': non_responsive_uids,
                    'empty_uids': empty_uids,
                    'insufficient_miners': insufficient_miners,
                    'validation_results': validation_results
                }
            )
            
        except Exception as e:
            bt.logging.error(f"Error in organic query processing: {str(e)}")
            return self._create_error_response(synapse, str(e))
    
    def _select_miners(self) -> List[int]:
        """Select diverse set of miners for querying"""
        miner_uids = utils.get_miner_uids(self.metagraph, self.evaluator.vpermit_rao_limit)
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
            for uid in utils.get_miner_uids(self.metagraph, self.evaluator.vpermit_rao_limit):
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
                        bt.logging.error(f"Miner {uid} ({hotkey}) failed to respond properly")
        
        return miner_responses, miner_data_counts
    
    async def _apply_basic_penalties(self, 
                                     synapse: OrganicRequest,
                                     selected_miners: List[int], 
                                     miner_responses: Dict[int, List]) -> Tuple[List[int], List[int], Optional[OrganicRequest]]:
        """Apply penalties for timeouts and empty responses, with data check rescrape"""
        # Non-responsive miners
        non_responsive_uids = [uid for uid in selected_miners if uid not in miner_responses]
        for uid in non_responsive_uids:
            bt.logging.info(f"Applying penalty to non-responsive miner {uid}")
            self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
        
        # Empty response miners
        empty_uids = [uid for uid, rows in miner_responses.items() if len(rows) == 0]
        
        # Check if ALL responding miners returned empty data
        responding_miners = [uid for uid in selected_miners if uid in miner_responses]
        all_empty = len(responding_miners) > 0 and all(len(miner_responses[uid]) == 0 for uid in responding_miners)
        
        if all_empty:
            bt.logging.info("All miners returned empty results - performing data check rescrape")
            verification_data = await self._perform_verification_rescrape(synapse)
            
            if verification_data:
                bt.logging.info(f"Verification found {len(verification_data)} items - applying penalties and returning verification data")
                # Apply penalties to all miners that returned empty results when data exists
                for uid in empty_uids:
                    bt.logging.info(f"Applying penalty to miner {uid} for returning empty results when data exists")
                    self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
                
                # Return verification data as response
                processed_data = self._process_response_data(synapse, verification_data)
                synapse.status = "success"
                synapse.data = processed_data[:synapse.limit]
                synapse.meta = {
                    "verification_rescrape": True,
                    "items_returned": len(processed_data),
                    "miners_queried": len(selected_miners),
                    "all_miners_empty": True
                }
                return non_responsive_uids, empty_uids, synapse
            else:
                bt.logging.info("Verification found no data - empty response is legitimate, exiting early")
                # Return empty response using existing method and exit early
                metadata = {
                    'selected_miners': selected_miners,
                    'miner_responses': miner_responses,
                    'verification_rescrape': True,
                    'no_data_available': True
                }
                empty_response = self._create_empty_response(synapse, metadata)
                empty_response.meta.update({
                    "verification_rescrape": True,
                    "all_miners_empty": True,
                    "no_data_available": True
                })
                return non_responsive_uids, empty_uids, empty_response
            
        for uid in empty_uids:
                    bt.logging.info(f"Applying penalty to miner {uid} for returning empty results when data exists")
                    self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
        return non_responsive_uids, empty_uids, None
    
    def _apply_consensus_volume_penalties(self, miner_data_counts: Dict[int, int], requested_limit: int) -> List[int]:
        """
        Apply volume-based penalties using consensus validation with dynamic penalty scaling.
        Uses mult_factor to scale penalties based on degree of underperformance.
        """
        if not miner_data_counts or len(miner_data_counts) < 2:
            return []
        
        # Filter out miners with 0 posts for consensus calculation
        non_zero_counts = [count for count in miner_data_counts.values() if count > 0]
        
        if len(non_zero_counts) < 2:
            bt.logging.info("Not enough miners with data for consensus - skipping volume penalties")
            return []
        
        # Calculate consensus metrics from miners who actually found data
        median_count = statistics.median(non_zero_counts)
        mean_count = statistics.mean(non_zero_counts) 
        consensus_count = max(median_count, mean_count)
        
        bt.logging.info(f"Volume consensus: {consensus_count:.1f} posts (median: {median_count}, mean: {mean_count:.1f})")
        
        # Only apply penalties if consensus shows meaningful data availability
        min_consensus_threshold = requested_limit * self.MIN_CONSENSUS  # At least 30% of request
        if consensus_count < min_consensus_threshold:
            bt.logging.info("Consensus shows limited data available - skipping volume penalties")
            return []
        
        penalized_miners = []
        
        for uid, post_count in miner_data_counts.items():
            if post_count > 0 and post_count < consensus_count: 
                # Calculate mult_factor based on degree of underperformance
                # Scale from 0.0 (at consensus) to 1.0 (at zero posts)
                underperformance_ratio = 1.0 - (post_count / consensus_count)
                
                # Apply penalty only if underperformance is significant (>20%)
                if underperformance_ratio > 0.2:
                    # Scale mult_factor: 0.2 underperformance = 0.1 mult_factor, 1.0 underperformance = 1.0 mult_factor
                    mult_factor = min((underperformance_ratio - 0.2) / 0.8, 1.0)
                    
                    penalized_miners.append(uid)
                    bt.logging.info(f"Miner {uid}: {post_count} posts vs consensus {consensus_count:.1f} "
                                   f"({underperformance_ratio:.1%} underperformance, {mult_factor:.2f} penalty)")
                    
                    self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=mult_factor)
        
        bt.logging.info(f"Applied consensus volume penalties to {len(penalized_miners)} miners")
        return penalized_miners
    
    async def _perform_cross_validation(self, synapse: OrganicRequest, miner_responses: Dict[int, List]) -> Tuple[Dict[str, bool], List]:
        """Perform cross-validation on pooled miner responses"""
        bt.logging.info("Starting cross-validation process...")
        
        # Pool all responses
        all_posts, post_to_miners = self._pool_responses(miner_responses)
        
        # Select posts for validation
        posts_to_validate = self._select_validation_posts(all_posts, post_to_miners)
        
        # Perform actual validation
        validation_results = await self._validate_posts(synapse, posts_to_validate)
        
        bt.logging.info(f"Cross-validation completed: {sum(validation_results.values())}/{len(validation_results)} passed")
        return validation_results, all_posts
    
    async def _perform_verification_rescrape(self, synapse: OrganicRequest) -> Optional[List]:
        """Perform verification rescrape using the same logic as miners"""
        try:
            # Initialize scraper based on source
            if synapse.source.upper() == 'X':
                scraper = EnhancedApiDojoTwitterScraper()
            else:
                scraper_id = self.evaluator.PREFERRED_SCRAPERS.get(DataSource[synapse.source.upper()])
                scraper = ScraperProvider().get(scraper_id) if scraper_id else None
            
            if not scraper:
                bt.logging.warning(f"No scraper available for verification of {synapse.source}")
                return None
            
            # Create verification config (limited scope)
            labels = []
            if synapse.keywords:
                labels.extend([DataLabel(value=k) for k in synapse.keywords])
            if synapse.usernames:
                labels.extend([DataLabel(value=f"@{u.strip('@')}" if not u.startswith('@') else u) for u in synapse.usernames])
            
            start_date = utils.parse_iso_date(synapse.start_date) if synapse.start_date else dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
            end_date = utils.parse_iso_date(synapse.end_date) if synapse.end_date else dt.datetime.now(dt.timezone.utc)
            
            verify_config = ScrapeConfig(
                entity_limit=synapse.limit,  
                date_range=DateRange(start=start_date, end=end_date),
                labels=labels,
            )
            
            # Perform scraping based on source
            if synapse.source.upper() == 'X':
                await scraper.scrape(verify_config)
                enhanced_content = scraper.get_enhanced_content()
                # Convert EnhancedXContent to dictionaries using to_api_response
                verification_data = [content.to_api_response() for content in enhanced_content]
            elif synapse.source.upper() == 'REDDIT':
                verification_data = await scraper.on_demand_scrape(usernames=synapse.usernames,
                                                                   subreddit=synapse.keywords[0] if synapse.keywords else None,
                                                                   keywords=synapse.keywords[1:] if len(synapse.keywords) > 1 else None,
                                                                   start_datetime=start_date,
                                                                   end_datetime=end_date)
            
            return verification_data if verification_data else None
            
        except Exception as e:
            bt.logging.error(f"Error during verification rescrape: {str(e)}")
            return None
    
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
    
    async def _validate_posts(self, synapse: OrganicRequest, posts_to_validate: List) -> Dict[str, bool]:
        """
        Performs request field matching, enhanced field validation, and content validation on posts. 
        """
        validation_results = {}
        
        if not posts_to_validate:
            return validation_results
        
        # Create validation tasks for concurrent execution
        validation_tasks = []
        post_ids = []
        
        for post in posts_to_validate:
            post_id = self._get_post_id(post)
            post_ids.append(post_id)
            
            # convert to RedditContent or XContent 
            content_model = self._convert_post_to_content_model(post, synapse.source)
            if content_model is None:
                validation_results[post_id] = False
                bt.logging.error(f"Post {post_id} failed content model conversion")
                continue
            
            # Create async task for validation
            task = self._validate_content_model(synapse, content_model, post_id)
            validation_tasks.append(task)
        
        # Run all validations concurrently
        if validation_tasks:
            validation_task_results = await asyncio.gather(*validation_tasks, return_exceptions=True)
            
            # Process results
            task_index = 0
            for i, post_id in enumerate(post_ids):
                if post_id in validation_results:
                    continue  # Already failed conversion
                    
                result = validation_task_results[task_index]
                task_index += 1
                
                if isinstance(result, Exception):
                    bt.logging.error(f"Validation error for {post_id}: {str(result)}")
                    validation_results[post_id] = False
                else:
                    validation_results[post_id] = result
        
        return validation_results
    
    def _extract_content_data(self, post) -> Optional[Dict[str, Any]]:
        """Shared logic for extracting data from various post formats"""
        try:
            if isinstance(post, dict):
                # Handle dict with nested JSON content
                if 'content' in post and isinstance(post['content'], str):
                    try:
                        json_data = json.loads(post['content'])
                        return {**post, **json_data}
                    except json.JSONDecodeError:
                        pass
                return post
            
            elif hasattr(post, 'content'):
                content_str = post.content.decode('utf-8') if isinstance(post.content, bytes) else post.content
                try:
                    return json.loads(content_str)
                except json.JSONDecodeError:
                    return None
                    
        except Exception as e:
            bt.logging.warning(f"Error extracting content data: {str(e)}")
            return None
    
    def _convert_to_enhanced_x_content(self, post) -> Optional[EnhancedXContent]:
        """Convert post to EnhancedXContent - simplified version"""
        try:
            data = self._extract_content_data(post)
            if not data:
                # Fallback to from_data_entity if it's a DataEntity
                if hasattr(post, 'content'):
                    return EnhancedXContent.from_data_entity(post)
                return None
            
            # Extract user data (nested under 'user' key)
            user_data = data.get('user', {})
            
            # Extract tweet data (nested under 'tweet' key)  
            tweet_data = data.get('tweet', {})
            
            # Handle in_reply_to nested data
            in_reply_to_data = tweet_data.get('in_reply_to', {})
            
            return EnhancedXContent(
                username=user_data.get('username', ''),
                text=data.get('content', data.get('text', '')),  # 'content' field contains the tweet text
                url=data.get('uri', data.get('url', '')),  # 'uri' field contains the URL
                timestamp=self._parse_timestamp(data.get('datetime', data.get('timestamp'))),  # 'datetime' field
                tweet_hashtags=tweet_data.get('hashtags', data.get('tweet_hashtags', [])),
                # Metadata fields from tweet object
                tweet_id=tweet_data.get('id', data.get('tweet_id')),
                like_count=tweet_data.get('like_count'),
                retweet_count=tweet_data.get('retweet_count'),  
                reply_count=tweet_data.get('reply_count'),
                quote_count=tweet_data.get('quote_count'),
                is_retweet=tweet_data.get('is_retweet'),
                is_reply=tweet_data.get('is_reply'),
                is_quote=tweet_data.get('is_quote'),
                # Additional user metadata
                user_id=user_data.get('id'),
                user_display_name=user_data.get('display_name'),
                user_verified=user_data.get('verified'),
                user_followers_count=user_data.get('followers_count'),
                user_following_count=user_data.get('following_count'),
                # Additional tweet metadata - extract from top-level media array
                media_urls=[item.get('url') for item in data.get('media', []) if item.get('url')],
                media_types=[item.get('type') for item in data.get('media', []) if item.get('type')],
                conversation_id=tweet_data.get('conversation_id'),
                in_reply_to_user_id=in_reply_to_data.get('user_id')
            )
        except Exception as e:
            bt.logging.error(f"Failed to convert to EnhancedXContent: {str(e)}")
            return None
    
    def _convert_to_reddit_content(self, post) -> Optional[RedditContent]:
        """Convert post to RedditContent - simplified version"""
        try:
            data = self._extract_content_data(post)
            if not data:
                # Fallback to from_data_entity if it's a DataEntity
                if hasattr(post, 'content'):
                    return RedditContent.from_data_entity(post)
                return None
            
            return RedditContent(
                id=data.get('id', ''),
                url=data.get('url', data.get('uri', '')),
                username=data.get('username', ''),
                communityName=data.get('communityName', data.get('community', '')),
                body=data.get('body', ''),
                createdAt=self._parse_timestamp(post.datetime), # non-obfuscated datetime
                dataType=data.get('dataType', data.get('data_type', 'post')),
                title=data.get('title', None),

                parentId=data.get('parentId', data.get('parent_id', None)),
                media=data.get('media', None),
                is_nsfw=data.get('is_nsfw', data.get('over_18', None))
            )
        except Exception as e:
            bt.logging.error(f"Failed to convert to RedditContent: {str(e)}")
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
                bt.logging.error(f"Failed to parse timestamp: {timestamp_str}")
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
            
            # ensure numeric fields are actually numeric
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
            
            # ensure boolean fields are actually boolean
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
    
    def _convert_post_to_content_model(self, post, source: str):
        """Single conversion method based on source"""
        if source.upper() == 'X':
            return self._convert_to_enhanced_x_content(post)
        else:
            return self._convert_to_reddit_content(post)
    
    async def _validate_content_model(self, synapse: OrganicRequest, content_model, post_id: str) -> bool:
        """
        Three-phase validation:
        1. Request field validation 
        2. Metadata completeness validation 
        3. Scraper validation 
        """
        try:
            # Phase 1: Request field validation 
            if not self._validate_request_fields(synapse, content_model):
                bt.logging.error(f"Post {post_id} failed request field validation")
                return False
            
            # Phase 2: Metadata completeness validation (X only)
            if synapse.source.upper() == 'X':
                if not self._validate_x_metadata_completeness(content_model):
                    bt.logging.error(f"Post {post_id} failed metadata completeness validation")
                    return False
            
            # Phase 3: Scraper validation (only if previous validation passes)
            scraper_result = await self._validate_with_scraper(synapse, content_model, post_id)
            return scraper_result
            
        except Exception as e:
            bt.logging.error(f"Validation error for {post_id}: {str(e)}")
            return False
    
    def _validate_request_fields(self, synapse: OrganicRequest, content_model) -> bool:
        """
        Validates whether the returned content fields match the request fields.
        """
        try:
            if synapse.source.upper() == 'X':
                return self._validate_x_request_fields(synapse, content_model)
            else:
                return self._validate_reddit_request_fields(synapse, content_model)
        except Exception as e:
            bt.logging.error(f"Error in request field validation: {str(e)}")
            return False
    
    def _validate_x_request_fields(self, synapse: OrganicRequest, x_content) -> bool:
        """X request field validation with EnhancedXContent"""
        # Username validation
        if synapse.usernames:
            requested_usernames = [u.strip('@').lower() for u in synapse.usernames]
            post_username = x_content.username.strip('@').lower()
            if post_username not in requested_usernames:
                bt.logging.debug(f"Username mismatch: {post_username} not in {requested_usernames}")
                return False
        
        # Keyword validation
        if synapse.keywords:
            post_text = x_content.text.lower()
            if not all(keyword.lower() in post_text for keyword in synapse.keywords):
                bt.logging.debug(f"Not all keywords found in post {post_text}")
                return False
        
        # Time range validation
        if not self._validate_time_range(synapse, x_content.timestamp):
            return False
        
        return True
    
    def _validate_reddit_request_fields(self, synapse: OrganicRequest, reddit_content) -> bool:
        """Reddit request field validation with RedditContent"""
        # Username validation
        if synapse.usernames:
            requested_usernames = [u.lower() for u in synapse.usernames]
            if reddit_content.username.lower() not in requested_usernames:
                bt.logging.debug(f"Reddit username mismatch: {reddit_content.username}")
                return False
        
        # Keywords validation (subreddit or content)
        if synapse.keywords:
            post_community = reddit_content.community.lower().removeprefix('r/')
            subreddit_match = any(keyword.lower().removeprefix('r/') == post_community 
                                for keyword in synapse.keywords)
            
            content_text = (reddit_content.body or '').lower()
            if reddit_content.title:
                content_text += ' ' + reddit_content.title.lower()
            
            keyword_in_content = all(keyword.lower() in content_text for keyword in synapse.keywords)
            
            if not (subreddit_match or keyword_in_content):
                bt.logging.debug(f"Reddit keyword mismatch in subreddit '{post_community}' and content")
                return False
        
        # Time range validation
        if not self._validate_time_range(synapse, reddit_content.created_at):
            return False
        
        return True
    
    async def _validate_with_scraper(self, synapse: OrganicRequest, content_model, post_id: str) -> bool:
        """
        Scraper validation working directly with content model.
        Converts to DataEntity using proper to_data_entity() methods.
        """
        try:
            scraper = self._get_scraper(synapse.source)
            if not scraper:
                bt.logging.warning(f"No scraper available for {synapse.source}")
                return False
            
            # Convert content model to DataEntity using existing methods
            data_entity = self._content_model_to_data_entity(content_model)
            if not data_entity:
                bt.logging.error(f"Failed to convert content model to DataEntity for {post_id}")
                return False
            
            # Call scraper validation
            results = await scraper.validate([data_entity])
            if results and len(results) > 0:
                result = results[0]
                is_valid = result.is_valid if hasattr(result, 'is_valid') else bool(result)
                if not is_valid:
                    bt.logging.error(f"Post {post_id} failed scraper validation: {getattr(result, 'reason', 'Unknown')}")
                return is_valid
            else:
                bt.logging.error(f"No scraper validation results for {post_id}")
                return False
                
        except Exception as e:
            bt.logging.error(f"Scraper validation error for {post_id}: {str(e)}")
            return False
    
    def _content_model_to_data_entity(self, content_model):
        """
        Convert content model to DataEntity using existing to_data_entity methods.
        Uses EnhancedXContent.to_data_entity() or RedditContent.to_data_entity().
        """
        try:
            if hasattr(content_model, '__class__') and hasattr(content_model.__class__, 'to_data_entity'):
                return content_model.__class__.to_data_entity(content_model)
            else:
                bt.logging.error(f"Content model {type(content_model)} has no to_data_entity method")
                return None
                
        except Exception as e:
            bt.logging.error(f"Failed to convert content model to DataEntity: {str(e)}")
            return None
    
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
                        bt.logging.error(f"Miner {uid} failed validation for post {post_id}")
                        break
            
            if miner_failed_validation:
                miner_scores[uid] = 0
                self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
        
        bt.logging.info(f"Final miner scores: {miner_scores}")
        return miner_scores
    
    def _create_success_response(self, synapse: OrganicRequest, miner_responses: Dict[int, List], 
                               miner_scores: Dict[int, int], pooled_data: List, metadata: Dict) -> OrganicRequest:
        """Create successful response with pooled data from all miners"""
        miners_with_valid_data = {uid: score for uid, score in miner_scores.items() if score > 0}
        
        if not miners_with_valid_data and not pooled_data:
            return self._create_empty_response(synapse, metadata)
        
        # Process pooled data from all miners (already deduplicated)
        processed_data = self._process_response_data(synapse, pooled_data)
        
        synapse.status = "success"
        synapse.data = processed_data[:synapse.limit]
        synapse.meta = {
            "miners_queried": len(metadata['selected_miners']),
            "miners_responded": len(miner_responses),
            "non_responsive_miners": len(metadata['non_responsive_uids']),
            "empty_response_miners": len(metadata['empty_uids']),
            "insufficient_post_miners": len(metadata['insufficient_miners']),
            "validation_success_rate": f"{sum(metadata['validation_results'].values())}/{len(metadata['validation_results'])}" if metadata['validation_results'] else "0/0",
            "items_returned": len(processed_data)
        }
        
        return synapse
    
    def _create_empty_response(self, synapse: OrganicRequest, metadata: Dict) -> OrganicRequest:
        """Create response when no valid data is available"""
        synapse.status = "success"
        synapse.data = []
        synapse.meta = {
            "miners_queried": len(metadata['selected_miners']),
            "miners_responded": len(metadata.get('miner_responses', {})),
            "consensus": "no_valid_data"
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
    
    def _get_post_id(self, post) -> str:
        """Generate consistent post identifier"""
        if isinstance(post, dict):
            return post.get('uri') or str(hash(str(sorted(post.items()))))
        else:
            return getattr(post, 'uri', None) or str(hash(str(post)))