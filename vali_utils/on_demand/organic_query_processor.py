import random
import json
import asyncio
import statistics
from typing import Dict, List, Tuple, Optional
import bittensor as bt
from common.data import DataSource, DataLabel, DataEntity
from common.protocol import OnDemandRequest
from common.organic_protocol import OrganicRequest
from common.constants import X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE
from common import constants, utils
from scraping.provider import ScraperProvider
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from scraping.x.model import XContent
from scraping.reddit.model import RedditContent
from scraping.youtube.model import YouTubeContent
from scraping.scraper import ScrapeConfig
from common.date_range import DateRange
import datetime as dt
from vali_utils.miner_evaluator import MinerEvaluator
from vali_utils.on_demand import utils as on_demand_utils
from vali_utils.on_demand.output_models import create_organic_output_dict, validate_metadata_completeness
from vali_utils.metrics import ORGANIC_MINER_RESULTS


class OrganicQueryProcessor:
    """Handles organic query processing, validation, and miner evaluation"""
    
    def __init__(self, 
                 wallet: bt.wallet,
                 metagraph: bt.metagraph, 
                 evaluator: MinerEvaluator):
        self.wallet = wallet
        self.metagraph = metagraph
        self.evaluator = evaluator
        
        # constants
        self.NUM_MINERS_TO_QUERY = 5
        self.PER_MINER_VALIDATION_SAMPLE_SIZE = 2
        self.MIN_CONSENSUS = 0.3    # if consensus is <30% of request size, consensus penalties skipped
        
        # Volume verification constants
        self.VOLUME_VERIFICATION_RATE = 0.1    # 10% chance to perform volume verification rescrape
        self.VOLUME_CONSENSUS_THRESHOLD = 0.8  # 80% of requested limit threshold
    

    def update_metagraph(self, metagraph = bt.metagraph):
        """Updates metagraph for the Organic Query Processor."""
        bt.logging.info("Updating metagraph for OrganicQueryProcessor.")
        self.metagraph = metagraph


    async def process_organic_query(self, synapse: OrganicRequest) -> OrganicRequest:
        """
        Main entry point for processing organic queries
        """
        request_start_time = dt.datetime.now(dt.timezone.utc)
        request_type = "URL" if synapse.url else "username/keyword"
        bt.logging.info(f"Processing organic query for source: {synapse.source} (type: {request_type})")
        
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
            
            # Step 4: Calculate consensus, check for volume verification, and apply volume-based penalties
            consensus_count = self._calculate_volume_consensus(miner_data_counts)
            volume_verification_response = await self._apply_volume_penalties(synapse, miner_data_counts, selected_miners, consensus_count)
            if volume_verification_response:
                return volume_verification_response
            
            # Step 5: Perform validation 
            validation_results = await self._perform_validation(synapse, miner_responses)
            
            # Step 6: Calculate final scores with all penalties applied
            miner_scores, failed_miners, successful_miners = self._apply_validation_penalties(miner_responses, validation_results)
            
            # Step 7: Pool only valid responses (excludes failed miners)
            pooled_data = self._pool_valid_responses(miner_responses, failed_miners)
            
            # Step 7.5: If no valid data pooled, perform rescrape fallback
            verification_data = None
            if not pooled_data:
                bt.logging.info("No valid data pooled from miners - performing rescrape fallback")
                verification_data = await self._perform_verification_rescrape(synapse)
                if verification_data:
                    bt.logging.info(f"Rescrape fallback found {len(verification_data)} items")
                    pooled_data = verification_data
                else:
                    bt.logging.info("Rescrape fallback found no data")
            
            # Step 8: Apply delayed penalties to empty miners (only if data was actually available)
            penalized_empty_uids = self._apply_delayed_empty_penalties(empty_uids, pooled_data, verification_data)
            
            # Step 8.5: Log summary of all penalized miners for this request
            all_penalized_uids = failed_miners + penalized_empty_uids
            if all_penalized_uids:
                penalized_with_hotkeys = [f"{uid}:{self.metagraph.hotkeys[uid]}" for uid in all_penalized_uids]
                bt.logging.info(f"Failed Miners for {synapse.source} request at {request_start_time}: {penalized_with_hotkeys}")
            
            # Step 9: Format response
            return self._create_success_response(
                synapse, miner_responses, miner_scores, pooled_data, {
                    'selected_miners': selected_miners,
                    'non_responsive_uids': non_responsive_uids,
                    'empty_uids': empty_uids,
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
        
        # Take top 75% of miners (but at least 5 if available)
        top_count = max(5, int(len(miner_scores) * 0.75))
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
        
        selected_miners_with_hotkeys = [f"{uid}:{self.metagraph.hotkeys[uid]}" for uid in selected_miners]
        bt.logging.info(f"Selected {len(selected_miners)} miners for query: {selected_miners_with_hotkeys}")
        return selected_miners
    

    async def _query_miners(self, synapse: OrganicRequest, selected_miners: List[int]) -> Tuple[Dict[int, List], Dict[int, int]]:
        """Query selected miners and return their responses"""
        on_demand_synapse = OnDemandRequest(
            source=DataSource[synapse.source.upper()],
            usernames=synapse.usernames,
            keywords=synapse.keywords,
            url=synapse.url,
            keyword_mode=synapse.keyword_mode,
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
                        
                        # Early format validation
                        if data_count > 0:
                            if self._validate_miner_data_format(synapse, data, uid):
                                miner_responses[uid] = data
                                miner_data_counts[uid] = data_count
                                bt.logging.info(f"Miner {uid} ({hotkey}) returned {data_count} valid items")
                            else:
                                # Format validation failed - treat as empty response
                                miner_responses[uid] = []
                                miner_data_counts[uid] = 0
                                bt.logging.warning(f"Miner {uid} ({hotkey}) failed format validation - treating as empty")
                                ORGANIC_MINER_RESULTS.labels(miner_uid=uid, result_type="failure_format_validation").inc()
                        else:
                            miner_responses[uid] = data
                            miner_data_counts[uid] = data_count
                            bt.logging.info(f"Miner {uid} ({hotkey}) returned {data_count} items")
                    else:
                        bt.logging.error(f"Miner {uid} ({hotkey}) failed to respond properly")
        
        return miner_responses, miner_data_counts
    

    def _validate_miner_data_format(self, synapse: OrganicRequest, data: List, miner_uid: int) -> bool:
        """
        Validate miner data format by testing conversion to appropriate content model.
        Validates all items in the response and checks for duplicates within the response.
        
        Args:
            synapse: The organic request with source info
            data: List of data items from the miner 
            miner_uid: UID of the miner for logging
            
        Returns:
            bool: True if all items have valid format and no duplicates, False otherwise
        """
        if not data:
            return True  # Empty is valid
            
        source = synapse.source.upper()
        seen_post_ids = set()

        for i, item in enumerate(data):
            try:
                # Validate basic DataEntity structure
                if not isinstance(item, DataEntity):
                    bt.logging.debug(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]}: Item {i} is not a DataEntity")
                    return False
                    
                if not item.uri or not item.content:
                    bt.logging.debug(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]}: Item {i} missing required fields (uri, content)")
                    return False
                
                # Check for duplicates within this miner's response
                post_id = self._get_post_id(item)
                if post_id in seen_post_ids:
                    bt.logging.info(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} has duplicate posts in response (item {i}): {post_id}")
                    return False
                seen_post_ids.add(post_id)
                
                # Test conversion to appropriate content model based on source
                if source == 'X':
                    x_content = XContent.from_data_entity(item)
                    if on_demand_utils.is_nested_format(item) and dt.datetime.now(tz=dt.timezone.utc) < X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE:
                        item = XContent.to_data_entity(x_content)
                    
                elif source == 'REDDIT':
                    # Parse the content JSON to validate structure
                    reddit_content = RedditContent.from_data_entity(item)
                    
                else:   # source == 'YOUTUBE'
                    youtube_content = YouTubeContent.from_data_entity(item)
                    
            except Exception as e:
                bt.logging.info(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} format validation failed on item {i}: {str(e)}")
                return False
        
        bt.logging.trace(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]}: Successfully validated all {len(data)} items for formatting and duplicates.")
        return True
    

    async def _apply_basic_penalties(self, 
                                     synapse: OrganicRequest,
                                     selected_miners: List[int], 
                                     miner_responses: Dict[int, List]) -> Tuple[List[int], List[int], Optional[OrganicRequest]]:
        """Apply penalties for timeouts and empty responses, with data check rescrape"""
        # Non-responsive miners
        non_responsive_uids = [uid for uid in selected_miners if uid not in miner_responses]
        for uid in non_responsive_uids:
            bt.logging.info(f"Applying penalty to non-responsive miner {uid}:{self.metagraph.hotkeys[uid]}")
            self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
            ORGANIC_MINER_RESULTS.labels(miner_uid=uid, result_type="failure_non_responsive").inc()
        
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
                    bt.logging.info(f"Applying penalty to miner {uid}:{self.metagraph.hotkeys[uid]} for returning empty results when data exists")
                    self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
                    ORGANIC_MINER_RESULTS.labels(miner_uid=uid, result_type="failure_empty_when_data_exists").inc()
                
                # Return verification data as response
                processed_data = [self._create_entity_dictionary(item) for item in verification_data]
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
            
        # Note: Empty miner penalties are delayed until the end of processing
        # to avoid penalizing miners who correctly return 0 rows when no data exists
        return non_responsive_uids, empty_uids, None


    def _calculate_volume_consensus(self, miner_data_counts: Dict[int, int]) -> Optional[float]:
        """
        Calculate volume consensus from miner data counts.
        Returns the higher of mean and median of non-zero responses, or None if insufficient data.
        """
        if not miner_data_counts or len(miner_data_counts) < 2:
            return None
        
        # Filter out miners with 0 posts for consensus calculation
        non_zero_counts = [count for count in miner_data_counts.values() if count > 0]
        
        if len(non_zero_counts) < 2:
            return None
        
        # Calculate consensus metrics from miners who actually found data
        median_count = statistics.median(non_zero_counts)
        mean_count = statistics.mean(non_zero_counts) 
        consensus_count = max(median_count, mean_count)
        
        bt.logging.info(f"Volume consensus: {consensus_count:.1f} posts (median: {median_count}, mean: {mean_count:.1f})")
        return consensus_count


    def _apply_consensus_volume_penalties(self, miner_data_counts: Dict[int, int], requested_limit: Optional[int], consensus_count: Optional[float]) -> List[int]:
        """
        Apply consensus volume penalties when verification is not triggered.
        """
        if consensus_count is None:
            bt.logging.info("Not enough miners with data for consensus - skipping volume penalties")
            return []

        # Only apply penalties if consensus shows meaningful data availability
        # Skip this check if requested_limit is None (no limit specified)
        if requested_limit is not None:
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
                    bt.logging.info(f"Miner {uid}:{self.metagraph.hotkeys[uid]}: {post_count} posts vs consensus {consensus_count:.1f} "
                                   f"({underperformance_ratio:.1%} underperformance, {mult_factor:.2f} penalty)")

                    self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=mult_factor)
                    ORGANIC_MINER_RESULTS.labels(miner_uid=uid, result_type="failure_volume_underperformance_consensus").inc()
        
        bt.logging.info(f"Applied consensus volume penalties to {len(penalized_miners)} miners")
        return penalized_miners

    
    def _apply_delayed_empty_penalties(self, empty_uids: List[int], pooled_data: List, verification_data: Optional[List]) -> List[int]:
        """
        Apply penalties to empty miners only if data was actually available.
        This prevents penalizing miners who correctly return 0 rows when no legitimate data exists.
        
        Args:
            empty_uids: List of miner UIDs that returned empty responses
            pooled_data: Final pooled data from valid miners  
            verification_data: Data from verification rescrape (if performed)
            
        Returns:
            List of miner UIDs that were penalized for empty responses
        """
        if not empty_uids:
            return []
            
        has_valid_pooled_data = pooled_data and len(pooled_data) > 0
        has_verification_data = verification_data and len(verification_data) > 0
        
        if has_valid_pooled_data or has_verification_data:
            # Data for this request actually exists, so empty miners should be penalized
            for uid in empty_uids:
                bt.logging.info(f"Applying delayed penalty to empty miner {uid}:{self.metagraph.hotkeys[uid]} - data was available")
                self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
                ORGANIC_MINER_RESULTS.labels(miner_uid=uid, result_type="failure_empty_when_data_exists").inc()
            return empty_uids
        else:
            bt.logging.info(f"No penalties for {len(empty_uids)} empty miners - no data was actually available")
            return []


    async def _apply_volume_penalties(self, 
                                      synapse: OrganicRequest, 
                                      miner_data_counts: Dict[int, int], 
                                      selected_miners: List[int], 
                                      consensus_count: Optional[float]) -> Optional[OrganicRequest]:
        """
        Check if volume consensus is below threshold and potentially perform verification rescrape.
        Returns early response if verification is triggered, None otherwise.
        """
        if consensus_count is None:
            bt.logging.info("No consensus available for volume verification check")
            return None
        
        # Check if consensus is below threshold
        threshold_count = synapse.limit * self.VOLUME_CONSENSUS_THRESHOLD
        
        if consensus_count >= threshold_count:
            bt.logging.info(f"Volume consensus {consensus_count:.1f} meets threshold {threshold_count:.1f}")
            return None
        
        bt.logging.info(f"Volume consensus {consensus_count:.1f} below threshold {threshold_count:.1f}")
        
        should_apply_concensus_penalty_instead_of_rescrape_verification = random.random() > self.VOLUME_VERIFICATION_RATE
        if should_apply_concensus_penalty_instead_of_rescrape_verification:
            bt.logging.info("Volume verification not triggered, applying volume consensus penalties...")
            self._apply_consensus_volume_penalties(miner_data_counts, synapse.limit, consensus_count)
            return None
        
        bt.logging.info("Volume verification triggered - performing verification rescrape")
        
        # Perform verification rescrape
        verification_data = await self._perform_verification_rescrape(synapse)
        
        if verification_data is None:
            bt.logging.info("Volume verification failed - no data found")
            return None
        
        verification_count = len(verification_data)
        bt.logging.info(f"Volume verification found {verification_count} items")
        
        # Apply scaled penalties based on verification results
        bt.logging.info(f"Applying scaled penalties based on verification count ({verification_count})")
        
        for uid, miner_count in miner_data_counts.items():
            if miner_count > 0:
                # Check for fake data padding if verification rescrape wasn't capped
                if verification_count < synapse.limit and miner_count > verification_count:
                        bt.logging.info(f"Miner {uid}:{self.metagraph.hotkeys[uid]}: fake data padding detected - {miner_count} posts vs {verification_count} verified, applying full penalty")
                        self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
                        ORGANIC_MINER_RESULTS.labels(miner_uid=uid, result_type="failure_fake_data_padding").inc()
                        continue
                
                # Apply scaled penalties based on underperformance
                underperformance_ratio = max(0, (verification_count - miner_count) / verification_count)
                mult_factor = min(underperformance_ratio, 1.0)

                if mult_factor > 0.1:  # Only penalize underperformance of > 10%
                    bt.logging.info(f"Miner {uid}:{self.metagraph.hotkeys[uid]}: {miner_count} posts vs {verification_count} verified "
                                   f"({underperformance_ratio:.1%} underperformance, {mult_factor:.2f} penalty)")
                    self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=mult_factor)
                    ORGANIC_MINER_RESULTS.labels(miner_uid=uid, result_type="failure_volume_underperformance_verification").inc()
        
        # Return verification data as response
        processed_data = [self._create_entity_dictionary(item) for item in verification_data]
        synapse.status = "success"
        synapse.data = processed_data[:synapse.limit]
        synapse.meta = {
            "volume_verification_triggered": True,
            "verification_data_count": verification_count,
            "volume_consensus": consensus_count,
            "threshold_count": threshold_count,
            "items_returned": len(processed_data),
            "miners_queried": len(selected_miners)
        }
        
        return synapse
    

    async def _perform_validation(self, synapse: OrganicRequest, miner_responses: Dict[int, List]) -> Dict[str, bool]:
        """Perform OnDemand validation: selecting random posts per miner with non-empty responses"""
        validation_results = {}
        validation_tasks = []
        miner_post_keys = []
        
        # For each miner with non-empty responses, select up to 2 random posts
        for uid, posts in miner_responses.items():
            if not posts:
                continue
                
            # Select up to {2} random posts from this miner
            num_to_validate = min(self.PER_MINER_VALIDATION_SAMPLE_SIZE, len(posts))
            selected_posts = random.sample(posts, num_to_validate)
            
            bt.logging.info(f"Validating {num_to_validate} posts from miner {uid}:{self.metagraph.hotkeys[uid]}")
            
            for post in selected_posts:
                post_id = self._get_post_id(post)
                # Create unique key combining miner and post to avoid cross-miner contamination
                miner_post_key = f"{uid}:{post_id}"
                miner_post_keys.append(miner_post_key)
                
                # Create async task for validation
                task = self._validate_entity(synapse=synapse, entity=post, post_id=post_id, miner_uid=uid)
                validation_tasks.append(task)
        
        # Run all validations concurrently
        if validation_tasks:
            validation_task_results = await asyncio.gather(*validation_tasks, return_exceptions=True)
            
            # Process results
            for i, miner_post_key in enumerate(miner_post_keys):
                result = validation_task_results[i]
                
                if isinstance(result, Exception):
                    bt.logging.error(f"Validation error for {miner_post_key}: {str(result)}")
                    validation_results[miner_post_key] = False
                else:
                    validation_results[miner_post_key] = result
        
        bt.logging.info(f"Simplified validation completed: {sum(validation_results.values())}/{len(validation_results)} passed")
        return validation_results
    

    async def _perform_verification_rescrape(self, synapse: OrganicRequest) -> Optional[List]:
        """Perform verification rescrape using the same logic as miners"""
        try:
            # Initialize scraper based on source
            if synapse.source.upper() == 'X':
                scraper = ApiDojoTwitterScraper()
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
                verification_data = await scraper.on_demand_scrape(usernames=synapse.usernames,
                                                                   keywords=synapse.keywords,
                                                                   url=synapse.url,
                                                                   keyword_mode=synapse.keyword_mode,
                                                                   start_datetime=start_date,
                                                                   end_datetime=end_date,
                                                                   limit=synapse.limit)
            elif synapse.source.upper() == 'REDDIT':
                verification_data = await scraper.on_demand_scrape(usernames=synapse.usernames,
                                                                   subreddit=synapse.keywords[0] if synapse.keywords else None,
                                                                   keywords=synapse.keywords[1:] if len(synapse.keywords) > 1 else None,
                                                                   keyword_mode=synapse.keyword_mode,
                                                                   start_datetime=start_date,
                                                                   end_datetime=end_date,
                                                                   limit=synapse.limit)
            elif synapse.source.upper() == 'YOUTUBE':
                # Determine YouTube scraping mode: channel or video URL
                valid_usernames = [u.strip() for u in synapse.usernames if u and u.strip()]

                if valid_usernames:
                    # Channel mode
                    channel_identifier = valid_usernames[0]
                    bt.logging.info(f"YouTube verification: channel scraping @{channel_identifier}")
                    verification_data = await scraper.scrape(
                        channel_url=f"https://www.youtube.com/@{channel_identifier.lstrip('@')}",
                        max_videos=synapse.limit or 10,
                        start_date=start_date.isoformat(),
                        end_date=end_date.isoformat(),
                        language="en"
                    )
                elif synapse.url:
                    # Video URL mode
                    bt.logging.info(f"YouTube verification: video URL scraping {synapse.url}")
                    verification_data = await scraper.scrape(
                        youtube_url=synapse.url,
                        language="en"
                    )
                else:
                    bt.logging.error("YouTube verification needs either username (channel) or url (video URL)")
                    return None
            
            return verification_data if verification_data else None
            
        except Exception as e:
            bt.logging.error(f"Error during verification rescrape: {str(e)}")
            return None
    

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
    

    def _validate_metadata_completeness(self, entity: DataEntity, post_id: str = None, miner_uid: int = None) -> bool:
        """
        Generalized metadata completeness validation using Pydantic output models.
        Works for all data sources (X, Reddit, YouTube).
        
        Args:
            entity: DataEntity to validate
            post_id: Optional identifier for logging
            miner_uid: Optional miner UID for logging
            
        Returns:
            bool: True if all required metadata is present, False otherwise
        """
        try:
            is_valid, missing_fields = validate_metadata_completeness(entity)
            
            if not is_valid:
                source = DataSource(entity.source).name.upper()
                post_identifier = post_id or entity.uri or "unknown"
                bt.logging.info(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} - {source} post {post_identifier} missing required metadata: {missing_fields}")
                return False
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} - Error validating metadata completeness: {str(e)}")
            return False
    

    async def _validate_entity(self, synapse: OrganicRequest, entity: DataEntity, post_id: str, miner_uid: int) -> bool:
        """
        Three-phase validation:
        1. Request field validation 
        2. Metadata completeness validation 
        3. Scraper validation 
        """
        try:
            entity_for_validation = entity

            # Phase 1: Request field validation 
            if not self._validate_request_fields(synapse, entity, miner_uid):
                bt.logging.error(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} post {post_id} failed request field validation")
                return False
            
            # Phase 2: Metadata completeness validation (all sources)
            if not self._validate_metadata_completeness(entity, post_id, miner_uid):
                bt.logging.error(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} post {post_id} failed metadata completeness validation")
                return False
            
            # Convert to proper format for scraper validation if needed
            if synapse.source.upper() == 'X' and on_demand_utils.is_nested_format(entity):
                x_content = XContent.from_data_entity(entity)
                entity_for_validation = XContent.to_data_entity(content=x_content)
            else:
                entity_for_validation = entity
            
            # Phase 3: Scraper validation (only if previous validation passes)
            scraper_result = await self._validate_with_scraper(synapse, entity_for_validation, post_id)
            return scraper_result
            
        except Exception as e:
            bt.logging.error(f"Miner {miner_uid}:{self.metagraph.hotkeys[miner_uid]} validation error for {post_id}: {str(e)}")
            return False
    

    def _validate_request_fields(self, synapse: OrganicRequest, entity: DataEntity, miner_uid: int) -> bool:
        """
        Validates whether the returned content fields match the request fields.
        """
        try:
            if synapse.source.upper() == 'X':
                return self._validate_x_request_fields(synapse, x_entity=entity, miner_uid=miner_uid)
            elif synapse.source.upper() == 'REDDIT':
                return self._validate_reddit_request_fields(synapse, reddit_entity=entity, miner_uid=miner_uid)
            elif synapse.source.upper() == 'YOUTUBE':
                return self._validate_youtube_request_fields(synapse, youtube_entity=entity, miner_uid=miner_uid)
        except Exception as e:
            bt.logging.error(f"Error in request field validation: {str(e)}")
            return False
    

    def _validate_x_request_fields(self, synapse: OrganicRequest, x_entity: DataEntity, miner_uid: int) -> bool:
        """X request field validation with the X DataEntity"""
        hotkey = self.metagraph.hotkeys[miner_uid]
        x_content_dict = json.loads(x_entity.content.decode('utf-8'))

        # URL validation - if URL is provided, validate that the returned post matches
        if synapse.url:
            from scraping.x import utils
            post_url = x_content_dict.get("url", "")
            # Normalize both URLs for comparison
            if utils.normalize_url(post_url) != utils.normalize_url(synapse.url):
                bt.logging.debug(f"Miner {miner_uid}:{hotkey} URL mismatch: {post_url} != {synapse.url}")
                return False
            # For URL mode, validate the URL matches and time range
            if not self._validate_time_range(synapse, x_entity.datetime):
                bt.logging.debug(f"Miner {miner_uid}:{hotkey} failed time range validation")
                return False
            return True

        # Username validation - handle both nested and flat formats
        if synapse.usernames:
            requested_usernames = [u.strip('@').lower() for u in synapse.usernames]

            # Check if this is nested format (backward compatibility)
            if 'user' in x_content_dict:
                # Nested format
                user_dict = x_content_dict.get("user", {})
                post_username = user_dict.get("username", "").strip('@').lower()
            else:
                # Flat format (current)
                post_username = x_content_dict.get("username", "").strip('@').lower()

            if not post_username or post_username not in requested_usernames:
                bt.logging.debug(f"Miner {miner_uid}:{hotkey} username mismatch: {post_username} not in: {requested_usernames}")
                return False

        # Keyword validation - text is at top level in both formats
        if synapse.keywords:
            post_text = x_content_dict.get("text", "")
            post_text = post_text.lower()

            # Apply keyword matching based on keyword_mode
            keyword_mode = synapse.keyword_mode
            if keyword_mode == 'all':
                if not all(keyword.lower() in post_text for keyword in synapse.keywords):
                    bt.logging.debug(f"Miner {miner_uid}:{hotkey} not all keywords ({synapse.keywords}) found in post: {post_text}")
                    return False
            else:  # keyword_mode == 'any'
                if not any(keyword.lower() in post_text for keyword in synapse.keywords):
                    bt.logging.debug(f"Miner {miner_uid}:{hotkey} none of the keywords ({synapse.keywords}) found in post: {post_text}")
                    return False

        # Time range validation
        if not self._validate_time_range(synapse, x_entity.datetime):
            bt.logging.debug(f"Miner {miner_uid}:{hotkey} failed time range validation")
            return False

        return True
    

    def _validate_reddit_request_fields(self, synapse: OrganicRequest, reddit_entity: DataEntity, miner_uid: int) -> bool:
        """Reddit request field validation with the Reddit DataEntity"""
        hotkey = self.metagraph.hotkeys[miner_uid]
        bt.logging.debug(f"Miner {miner_uid}:{hotkey} - Starting Reddit request field validation")
        reddit_content_dict = json.loads(reddit_entity.content.decode('utf-8'))
        # Username validation
        if synapse.usernames:
            requested_usernames = [u.lower() for u in synapse.usernames]
            post_username = reddit_content_dict.get("username")
            if not post_username or post_username.lower() not in requested_usernames:
                bt.logging.debug(f"Miner {miner_uid}:{hotkey} Reddit username mismatch: {post_username} not in: {requested_usernames}")
                return False
        
        # Keywords validation (subreddit or content)
        if synapse.keywords:
            post_community = reddit_content_dict.get("communityName")
            if post_community:
                post_community = post_community.lower().removeprefix('r/')
                subreddit_match = any(keyword.lower().removeprefix('r/') == post_community 
                                    for keyword in synapse.keywords)
            else:
                subreddit_match = False
            
            body_text = reddit_content_dict.get("body") or ""
            title_text = reddit_content_dict.get("title") or ""
            content_text = (body_text + ' ' + title_text).lower().strip()
            
            # Apply keyword matching based on keyword_mode
            keyword_mode = synapse.keyword_mode
            if keyword_mode == 'all':
                keyword_in_content = all(keyword.lower() in content_text for keyword in synapse.keywords) if content_text else False
            else:  # keyword_mode == 'any'  
                keyword_in_content = any(keyword.lower() in content_text for keyword in synapse.keywords) if content_text else False
            
            if not (subreddit_match or keyword_in_content):
                bt.logging.debug(f"Miner {miner_uid}:{hotkey} Reddit keyword mismatch in subreddit: '{post_community}' and content: '{content_text}'")
                return False
        
        # Time range validation using non-obfuscated datetime
        if not self._validate_time_range(synapse, reddit_entity.datetime):
            bt.logging.debug(f"Miner {miner_uid}:{hotkey} failed time range validation")
            return False
        
        return True
    

    def _validate_youtube_request_fields(self, synapse: OrganicRequest, youtube_entity: DataEntity, miner_uid: int) -> bool:
        """YouTube request field validation with the Youtube DataEntity"""
        hotkey = self.metagraph.hotkeys[miner_uid]
        bt.logging.debug(f"Miner {miner_uid}:{hotkey} - Starting YouTube request field validation")
        youtube_content_dict = json.loads(youtube_entity.content.decode('utf-8'))

        # URL validation - if URL is provided, validate that the returned video matches
        if synapse.url:
            video_url = youtube_content_dict.get("video_url", "")
            # Normalize both URLs for comparison
            if on_demand_utils.normalize_youtube_url(video_url) != on_demand_utils.normalize_youtube_url(synapse.url):
                bt.logging.debug(f"Miner {miner_uid}:{hotkey} YouTube URL mismatch: {video_url} != {synapse.url}")
                return False
            # For URL mode, we only validate the URL matches and time range
            if not self._validate_time_range(synapse, youtube_entity.datetime):
                bt.logging.debug(f"Miner {miner_uid}:{hotkey} failed time range validation")
                return False
            return True

        # Username (channel) validation
        if synapse.usernames:
            requested_channels = [u.strip('@').lower() for u in synapse.usernames]
            requested_channel = requested_channels[0]   # take only the first requested channel
            channel_name = youtube_content_dict.get("channel_name")
            if not channel_name or channel_name.lower() != requested_channel.lower():
                bt.logging.debug(f"Miner {miner_uid}:{hotkey} Channel mismatch: {channel_name} is not the requested channel: {requested_channel}")
                return False

        # Time range validation
        if not self._validate_time_range(synapse, youtube_entity.datetime):
            bt.logging.debug(f"Miner {miner_uid}:{hotkey} failed time range validation")
            return False

        return True


    async def _validate_with_scraper(self, synapse: OrganicRequest, data_entity: DataEntity, post_id: str) -> bool:
        """
        Scraper validation working directly with content model.
        Converts to DataEntity using proper to_data_entity() methods.
        """
        try:
            scraper = self._get_scraper(synapse.source)
            if not scraper:
                bt.logging.warning(f"No scraper available for {synapse.source}")
                return False
            
            # Call scraper validation
            if synapse.source.upper() == "X": 
                results = await scraper.validate(entities=[data_entity], allow_low_engagement=True)
            else:
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
    

    def _get_scraper(self, source: str):
        """Get appropriate scraper for the data source"""
        try:
            if source.upper() == 'X':
                return ApiDojoTwitterScraper()
            else:
                scraper_id = self.evaluator.PREFERRED_SCRAPERS.get(DataSource[source.upper()])
                if scraper_id:
                    return ScraperProvider().get(scraper_id)
        except Exception as e:
            bt.logging.error(f"Error getting scraper for {source}: {str(e)}")
        return None
    

    def _apply_validation_penalties(self, miner_responses: Dict[int, List], validation_results: Dict[str, bool]) -> Tuple[Dict[int, int], List[int], List[int]]:
        """Calculate final scores incorporating all penalties and return failed/successful miners"""
        miner_scores = {}
        failed_miners = []
        successful_miners = []
        
        for uid in miner_responses.keys():
            if not miner_responses[uid]:
                miner_scores[uid] = 0
                failed_miners.append(uid)
                continue
            
            # Apply validation penalty based on miner-specific validation results
            miner_failed_validation = False
            validated_posts_count = 0
            
            for post in miner_responses[uid]:
                post_id = self._get_post_id(post)
                # Use miner-specific key to check validation results
                miner_post_key = f"{uid}:{post_id}"
                if miner_post_key in validation_results:
                    validated_posts_count += 1
                    if not validation_results[miner_post_key]:
                        miner_failed_validation = True
                        bt.logging.error(f"Miner {uid}:{self.metagraph.hotkeys[uid]} failed validation for post {post_id}")
                        break
            
            if miner_failed_validation:
                miner_scores[uid] = 0
                failed_miners.append(uid)
                self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
                ORGANIC_MINER_RESULTS.labels(miner_uid=uid, result_type="failure_content_validation").inc()
            else:
                # Miner passed validation - track success
                successful_miners.append(uid)
                ORGANIC_MINER_RESULTS.labels(miner_uid=uid, result_type="success").inc()

        bt.logging.info(f"Final miner scores: {miner_scores}")
        bt.logging.info(f"Failed validation miners: {failed_miners}")
        bt.logging.info(f"Successful validation miners: {successful_miners}")
        return miner_scores, failed_miners, successful_miners

    def calculate_ondemand_reward_multipliers(
        self,
        job_created_at: dt.datetime,
        submission_timestamp: dt.datetime,
        returned_count: int,
        requested_limit: Optional[int],
        consensus_count: Optional[float] = None
    ) -> Tuple[float, float]:
        """
        Calculate speed and volume multipliers for on-demand rewards.

        Speed: Linear scale 0-2 minutes (1.0 → 0.1)
        Volume: rows_returned / limit (capped at 1.0), or
                consensus-relative ratio if limit is None

        Args:
            job_created_at: When the job was created
            submission_timestamp: When the miner submitted
            returned_count: Number of rows returned
            requested_limit: Number of rows requested (None means no limit)
            consensus_count: Consensus count from peers (used for unlimited requests)

        Returns:
            (speed_multiplier, volume_multiplier)
        """
        # Calculate speed multiplier
        if submission_timestamp is None or job_created_at is None:
            bt.logging.warning("Missing timestamp data for reward calculation, using default speed=0.5")
            speed_multiplier = 0.5
        else:
            upload_time_seconds = (submission_timestamp - job_created_at).total_seconds()
            upload_time_minutes = upload_time_seconds / 60.0

            # Speed window matches job expiry (2 minutes for all request types)
            speed_window_minutes = 2.0

            if upload_time_minutes <= speed_window_minutes:
                # Linear scale: 1.0 at 0 min → 0.1 at window end
                speed_multiplier = max(0.1, 1.0 - (upload_time_minutes / speed_window_minutes))
            else:
                # Floor at 0.1
                speed_multiplier = 0.1

            bt.logging.trace(
                f"Upload time: {upload_time_minutes:.2f} minutes "
                f"({upload_time_seconds:.0f}s), window: {speed_window_minutes:.0f} min "
                f"-> speed multiplier: {speed_multiplier:.3f}"
            )

        # Calculate volume multiplier
        if requested_limit is None:
            # No limit specified - use consensus-relative volume multiplier
            if consensus_count and consensus_count > 0:
                # Scale based on consensus: at consensus = 1.0, below = proportional
                volume_multiplier = min(1.0, returned_count / consensus_count)
                bt.logging.trace(
                    f"Returned {returned_count} rows vs consensus {consensus_count:.0f} (no limit) "
                    f"-> volume multiplier: {volume_multiplier:.3f}"
                )
            else:
                # No consensus available, use default full multiplier
                volume_multiplier = 1.0
                bt.logging.trace(
                    f"Returned {returned_count} rows (no limit, no consensus) "
                    f"-> volume multiplier: {volume_multiplier:.3f}"
                )
        elif returned_count == 0:
            volume_multiplier = 0.0
            bt.logging.trace(
                f"Returned {returned_count}/{requested_limit} rows "
                f"-> volume multiplier: {volume_multiplier:.3f}"
            )
        else:
            volume_multiplier = min(1.0, returned_count / requested_limit)
            bt.logging.trace(
                f"Returned {returned_count}/{requested_limit} rows "
                f"-> volume multiplier: {volume_multiplier:.3f}"
            )

        return speed_multiplier, volume_multiplier

    def _pool_valid_responses(self, miner_responses: Dict[int, List], failed_miners: List[int]) -> List:
        """Pool only responses from miners who passed validation"""
        failed_miner_set = set(failed_miners)
        seen_posts = set()
        pooled_data = []
        
        for uid, posts in miner_responses.items():
            if uid in failed_miner_set or not posts:
                continue
                
            for post in posts:
                post_id = self._get_post_id(post)
                if post_id not in seen_posts:
                    seen_posts.add(post_id)
                    pooled_data.append(post)
        
        bt.logging.info(f"Pooled {len(pooled_data)} unique posts from {len([uid for uid in miner_responses.keys() if uid not in failed_miner_set])} valid miners")
        return pooled_data
    

    def _create_success_response(self, synapse: OrganicRequest, miner_responses: Dict[int, List], 
                               miner_scores: Dict[int, int], pooled_data: List, metadata: Dict) -> OrganicRequest:
        """Create successful response with pooled data from all miners"""
        miners_with_valid_data = {uid: score for uid, score in miner_scores.items() if score > 0}
        
        if not miners_with_valid_data and not pooled_data:
            return self._create_empty_response(synapse, metadata)
        
        # Process pooled data from all miners (already deduplicated)
        processed_data = [self._create_entity_dictionary(item) for item in pooled_data]
        
        synapse.status = "success"
        synapse.data = processed_data[:synapse.limit]
        synapse.meta = {
            "miners_queried": len(metadata['selected_miners']),
            "miners_responded": len(miner_responses),
            "non_responsive_miners": len(metadata['non_responsive_uids']),
            "empty_response_miners": len(metadata['empty_uids']),
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
    

    def _create_entity_dictionary(self, data_entity: DataEntity) -> Dict:
        """Create entity dictionary using source-specific Pydantic output models"""
        try:
            return create_organic_output_dict(data_entity)
        except Exception as e:
            bt.logging.error(f"Error creating output dictionary: {str(e)}")
            # Fallback to basic dictionary
            return {
                "uri": data_entity.uri,
                "datetime": data_entity.datetime.isoformat() if data_entity.datetime else None,
                "source": DataSource(data_entity.source).name,
                "label": data_entity.label.value if data_entity.label else None,
                "content_size_bytes": data_entity.content_size_bytes,
                "error": "Failed to create structured output"
            }


    def _get_post_id(self, post) -> str:
        """Generate consistent post identifier"""
        if isinstance(post, dict):
            return post.get('uri') or str(hash(str(sorted(post.items()))))
        else:
            return getattr(post, 'uri', None) or str(hash(str(post)))
