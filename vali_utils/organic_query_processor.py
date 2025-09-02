import random
import json
import asyncio
import statistics
from typing import Dict, List, Tuple, Optional
import bittensor as bt
from common.data import DataSource, DataLabel, DataEntity
from common.protocol import OnDemandRequest
from common.organic_protocol import OrganicRequest
from common.constants import X_ON_DEMAND_CONTENT_EXPIRATION_DATE
from common import constants, utils
from scraping.provider import ScraperProvider
from scraping.x.enhanced_apidojo_scraper import EnhancedApiDojoTwitterScraper
from scraping.x.on_demand_model import EnhancedXContent
from scraping.youtube.model import YouTubeContent
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
            
            # Step 4: Calculate consensus, check for volume verification, and apply volume-based penalties
            consensus_count = self._calculate_volume_consensus(miner_data_counts)
            volume_verification_response = await self._apply_volume_penalties(synapse, miner_data_counts, selected_miners, consensus_count)
            if volume_verification_response:
                return volume_verification_response
            
            # Step 5: Perform cross-validation 
            validation_results = await self._perform_cross_validation(synapse, miner_responses)
            
            # Step 6: Calculate final scores with all penalties applied
            miner_scores, failed_miners = self._apply_validation_penalties(miner_responses, validation_results)
            
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
            self._apply_delayed_empty_penalties(empty_uids, pooled_data, verification_data)
            
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


    def _apply_consensus_volume_penalties(self, miner_data_counts: Dict[int, int], requested_limit: int, consensus_count: Optional[float]) -> List[int]:
        """
        Apply consensus volume penalties when verification is not triggered.
        """
        if consensus_count is None:
            bt.logging.info("Not enough miners with data for consensus - skipping volume penalties")
            return []
        
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

    
    def _apply_delayed_empty_penalties(self, empty_uids: List[int], pooled_data: List, verification_data: Optional[List]) -> None:
        """
        Apply penalties to empty miners only if data was actually available.
        This prevents penalizing miners who correctly return 0 rows when no legitimate data exists.
        
        Args:
            empty_uids: List of miner UIDs that returned empty responses
            pooled_data: Final pooled data from valid miners  
            verification_data: Data from verification rescrape (if performed)
        """
        if not empty_uids:
            return
            
        has_valid_pooled_data = pooled_data and len(pooled_data) > 0
        has_verification_data = verification_data and len(verification_data) > 0
        
        if has_valid_pooled_data or has_verification_data:
            # Data for this request actually exists, so empty miners should be penalized
            for uid in empty_uids:
                bt.logging.info(f"Applying delayed penalty to empty miner {uid} - data was available")
                self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
        else:
            bt.logging.info(f"No penalties for {len(empty_uids)} empty miners - no data was actually available")


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
                        bt.logging.info(f"Miner {uid}: fake data padding detected - {miner_count} posts vs {verification_count} verified, applying full penalty")
                        self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
                        continue
                
                # Apply scaled penalties based on underperformance
                underperformance_ratio = max(0, (verification_count - miner_count) / verification_count)
                mult_factor = min(underperformance_ratio, 1.0)
                
                if mult_factor > 0.1:  # Only penalize underperformance of > 10%
                    bt.logging.info(f"Miner {uid}: {miner_count} posts vs {verification_count} verified "
                                   f"({underperformance_ratio:.1%} underperformance, {mult_factor:.2f} penalty)")
                    self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=mult_factor)
        
        # Return verification data as response
        processed_data = self._process_response_data(synapse, verification_data)
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
    

    async def _perform_cross_validation(self, synapse: OrganicRequest, miner_responses: Dict[int, List]) -> Dict[str, bool]:
        """Perform cross-validation on pooled miner responses"""
        bt.logging.info("Starting cross-validation process...")
        
        # Pool all responses
        all_posts, post_to_miners = self._pool_responses(miner_responses)
        
        # Select posts for validation
        posts_to_validate = self._select_validation_posts(all_posts, post_to_miners, miner_responses)
        
        # Perform actual validation
        validation_results = await self._validate_posts(synapse, posts_to_validate)
        
        bt.logging.info(f"Cross-validation completed: {sum(validation_results.values())}/{len(validation_results)} passed")
        return validation_results
    

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
                # Convert EnhancedXContent to DataEntities
                verification_data = [EnhancedXContent.to_enhanced_data_entity(content=content) for content in enhanced_content]
            elif synapse.source.upper() == 'REDDIT':
                verification_data = await scraper.on_demand_scrape(usernames=synapse.usernames,
                                                                   subreddit=synapse.keywords[0] if synapse.keywords else None,
                                                                   keywords=synapse.keywords[1:] if len(synapse.keywords) > 1 else None,
                                                                   start_datetime=start_date,
                                                                   end_datetime=end_date)
            elif synapse.source.upper() == 'YOUTUBE':
                yt_label = DataLabel(value=YouTubeContent.create_channel_label(synapse.usernames[0]))
                verify_config = ScrapeConfig(
                    entity_limit=synapse.limit,  
                    date_range=DateRange(start=start_date, end=end_date),
                    labels=[yt_label],
                )
                verification_data = await scraper.scrape(verify_config)
            
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
    

    def _select_validation_posts(self, all_posts: List, post_to_miners: Dict[str, List[int]], miner_responses: Dict[int, List]) -> List:
        """Select posts for validation: first ensure miner coverage, then use weighted distribution for remaining slots"""
        validation_sample_size = min(self.CROSS_VALIDATION_SAMPLE_SIZE, len(all_posts))
        
        if validation_sample_size == 0:
            return []
        
        posts_to_validate = []
        selected_post_ids = set()
        
        # Miner coverage - randomly select one post from each miner with non-empty responses
        miners_with_data = [uid for uid, responses in miner_responses.items() if responses]
        coverage_slots = min(validation_sample_size, len(miners_with_data))
        
        bt.logging.info(f"Allocating {coverage_slots} slots for miner coverage from {len(miners_with_data)} miners with data")
        
        for uid in miners_with_data[:coverage_slots]:
            miner_posts = miner_responses[uid]
            if miner_posts:
                # Randomly select from this miner's posts that haven't been selected yet
                available_posts = [p for p in miner_posts if self._get_post_id(p) not in selected_post_ids]
                if available_posts:
                    selected_post = random.choice(available_posts)
                    posts_to_validate.append(selected_post)
                    selected_post_ids.add(self._get_post_id(selected_post))
        
        # Weighted selection for remaining slots targeting suspicious posts
        remaining_slots = validation_sample_size - len(posts_to_validate)
        
        if remaining_slots > 0:
            bt.logging.info(f"Using weighted selection for {remaining_slots} remaining slots")
            
            # Create list of (post, miner_count) tuples for remaining posts
            remaining_posts = []
            for post in all_posts:
                post_id = self._get_post_id(post)
                if post_id not in selected_post_ids:
                    miner_count = len(post_to_miners.get(post_id, []))
                    remaining_posts.append((post, miner_count))
            
            if remaining_posts:
                # Group remaining posts by miner count
                posts_by_count = {}
                for post, count in remaining_posts:
                    if count not in posts_by_count:
                        posts_by_count[count] = []
                    posts_by_count[count].append(post)
                
                total_miners = len(miner_responses)
                
                # Create weighted pool - only include posts not already selected
                weighted_posts = []
                for count, posts in posts_by_count.items():
                    weight = max(1, total_miners - count + 1)
                    for post in posts:
                        post_id = self._get_post_id(post)
                        if post_id not in selected_post_ids:
                            weighted_posts.extend([post] * weight)
                
                # Sample from weighted pool without replacement
                for _ in range(remaining_slots):
                    if not weighted_posts:
                        break
                        
                    selected_post = random.choice(weighted_posts)
                    post_id = self._get_post_id(selected_post)
                    
                    posts_to_validate.append(selected_post)
                    selected_post_ids.add(post_id)
                    
                    weighted_posts = [p for p in weighted_posts if self._get_post_id(p) != post_id]
        
        # Log the final distribution
        count_distribution = {}
        coverage_distribution = {}
        for i, post in enumerate(posts_to_validate):
            post_id = self._get_post_id(post)
            count = len(post_to_miners.get(post_id, []))
            count_distribution[count] = count_distribution.get(count, 0) + 1
            
            # Track which posts came from coverage vs weighted selection
            if i < coverage_slots:
                coverage_distribution[count] = coverage_distribution.get(count, 0) + 1
        
        # Verify no duplicates in final selection
        final_post_ids = [self._get_post_id(p) for p in posts_to_validate]
        assert len(final_post_ids) == len(set(final_post_ids)), f"Duplicate posts in validation selection: {len(final_post_ids)} total, {len(set(final_post_ids))} unique"
        
        return posts_to_validate


    async def _validate_posts(self, synapse: OrganicRequest, posts_to_validate: List[DataEntity]) -> Dict[str, bool]:
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
            bt.logging.info(f"Post for validation: {post}")
            post_id = self._get_post_id(post)
            post_ids.append(post_id)
            
            # Create async task for validation
            task = self._validate_entity(synapse=synapse, entity=post, post_id=post_id)
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
    

    async def _validate_entity(self, synapse: OrganicRequest, entity: DataEntity, post_id: str) -> bool:
        """
        Three-phase validation:
        1. Request field validation 
        2. Metadata completeness validation 
        3. Scraper validation 
        """
        try:
            entity_for_validation = entity

            # Phase 1: Request field validation 
            if not self._validate_request_fields(synapse, entity):
                bt.logging.error(f"Post {post_id} failed request field validation")
                return False
            
            # Phase 2: Metadata completeness validation (X only)
            if synapse.source.upper() == 'X':
                x_content = EnhancedXContent.from_data_entity(entity)
                if not self._validate_x_metadata_completeness(x_content=x_content):
                    bt.logging.error(f"Post {post_id} failed metadata completeness validation")
                    return False
                entity_for_validation = EnhancedXContent.to_data_entity(content=x_content)
            
            # Phase 3: Scraper validation (only if previous validation passes)
            scraper_result = await self._validate_with_scraper(synapse, entity_for_validation, post_id)
            return scraper_result
            
        except Exception as e:
            bt.logging.error(f"Validation error for {post_id}: {str(e)}")
            return False
    

    def _validate_request_fields(self, synapse: OrganicRequest, entity: DataEntity) -> bool:
        """
        Validates whether the returned content fields match the request fields.
        """
        try:
            if synapse.source.upper() == 'X':
                return self._validate_x_request_fields(synapse, x_entity=entity)
            elif synapse.source.upper() == 'REDDIT':
                return self._validate_reddit_request_fields(synapse, reddit_entity=entity)
            elif synapse.source.upper() == 'YOUTUBE':
                return self._validate_youtube_request_fields(synapse, youtube_entity=entity)
        except Exception as e:
            bt.logging.error(f"Error in request field validation: {str(e)}")
            return False
    

    def _validate_x_request_fields(self, synapse: OrganicRequest, x_entity: DataEntity) -> bool:
        """X request field validation with the X DataEntity"""
        x_content_dict = json.loads(x_entity.content.decode('utf-8'))
        # Username validation
        if synapse.usernames:
            requested_usernames = [u.strip('@').lower() for u in synapse.usernames]
            user_dict = x_content_dict.get("user", {})
            post_username = user_dict.get("username", "").strip('@').lower()
            if not post_username or post_username not in requested_usernames:
                bt.logging.debug(f"Username mismatch: {post_username} not in: {requested_usernames}")
                return False
        
        # Keyword validation
        if synapse.keywords:
            post_text = x_content_dict.get("text")
            now = dt.datetime.now(dt.timezone.utc)
            if now <= X_ON_DEMAND_CONTENT_EXPIRATION_DATE:
                if not post_text:
                    bt.logging.debug("'text' field not found, using 'content' as fallback. This fallback will expire Aug 25 2025.")
                    post_text = x_content_dict.get("content", "")
                
            post_text = post_text.lower()
            if not post_text or not all(keyword.lower() in post_text for keyword in synapse.keywords):
                bt.logging.debug(f"Not all keywords ({synapse.keywords}) found in post: {post_text}")
                return False
        
        # Time range validation
        if not self._validate_time_range(synapse, x_entity.datetime):
            return False
        
        return True
    

    def _validate_reddit_request_fields(self, synapse: OrganicRequest, reddit_entity: DataEntity) -> bool:
        """Reddit request field validation with the Reddit DataEntity"""
        reddit_content_dict = json.loads(reddit_entity.content.decode('utf-8'))
        # Username validation
        if synapse.usernames:
            requested_usernames = [u.lower() for u in synapse.usernames]
            post_username = reddit_content_dict.get("username")
            if not post_username or post_username.lower() not in requested_usernames:
                bt.logging.debug(f"Reddit username mismatch: {post_username} not in: {requested_usernames}")
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
            
            keyword_in_content = all(keyword.lower() in content_text for keyword in synapse.keywords) if content_text else False
            
            if not (subreddit_match or keyword_in_content):
                bt.logging.debug(f"Reddit keyword mismatch in subreddit: '{post_community}' and content: '{content_text}'")
                return False
        
        # Time range validation using non-obfuscated datetime
        if not self._validate_time_range(synapse, reddit_entity.datetime):
            return False
        
        return True
    

    def _validate_youtube_request_fields(self, synapse: OrganicRequest, youtube_entity: DataEntity) -> bool:
        """YouTube request field validation with the Youtube DataEntity"""
        youtube_content_dict = json.loads(youtube_entity.content.decode('utf-8'))

        # Username validation
        if synapse.usernames:
            requested_channels = [u.strip('@').lower() for u in synapse.usernames]
            requested_channel = requested_channels[0]   # take only the first requested channel
            channel_name = youtube_content_dict.get("channel_name")
            if not channel_name or channel_name.lower() != requested_channel.lower():
                bt.logging.debug(f"Channel mismatch: {channel_name} is not the requested channel: {requested_channel}")
                return False
            
        # Time range validation
        if not self._validate_time_range(synapse, youtube_entity.datetime):
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
                return EnhancedApiDojoTwitterScraper()
            else:
                scraper_id = self.evaluator.PREFERRED_SCRAPERS.get(DataSource[source.upper()])
                if scraper_id:
                    return ScraperProvider().get(scraper_id)
        except Exception as e:
            bt.logging.error(f"Error getting scraper for {source}: {str(e)}")
        return None
    

    def _apply_validation_penalties(self, miner_responses: Dict[int, List], validation_results: Dict[str, bool]) -> Tuple[Dict[int, int], List[int]]:
        """Calculate final scores incorporating all penalties and return failed miners"""
        miner_scores = {}
        failed_miners = []
        
        for uid in miner_responses.keys():
            if not miner_responses[uid]:
                miner_scores[uid] = 0
                failed_miners.append(uid)
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
                failed_miners.append(uid)
                self.evaluator.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
        
        bt.logging.info(f"Final miner scores: {miner_scores}")
        bt.logging.info(f"Failed validation miners: {failed_miners}")
        return miner_scores, failed_miners
    

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
        processed_data = self._process_response_data(synapse, pooled_data)
        
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
        """Create entity dictionary with nested structure instead of flattened"""
        entity_dict = {}

        # Top-level entity fields
        entity_dict["uri"] = data_entity.uri
        entity_dict["datetime"] = data_entity.datetime
        entity_dict["source"] = DataSource(data_entity.source).name
        entity_dict["label"] = data_entity.label.value if data_entity.label else None
        entity_dict["content_size_bytes"] = data_entity.content_size_bytes

        try:
            content_dict = json.loads(data_entity.content.decode("utf-8"))
            # Response content based off of the Data Source's given fields
            for item in content_dict:
                entity_dict[item] = content_dict.get(item)

        except Exception as e:
            bt.logging.error(f"Error decoding content from DataEntity. Content: {data_entity.content}")
            entity_dict["content"] = data_entity.content

        return entity_dict


    def _process_response_data(self, synapse: OrganicRequest, data: List) -> List[Dict]:
        """Process raw response data into standardized format"""
        processed_data = []
        
        for item in data:
            if isinstance(item, DataEntity):
                processed_data.append(self._create_entity_dictionary(data_entity=item))

            elif isinstance(item, Dict):
                processed_data.append(item)
        
        return processed_data
    

    def _get_post_id(self, post) -> str:
        """Generate consistent post identifier"""
        if isinstance(post, dict):
            return post.get('uri') or str(hash(str(sorted(post.items()))))
        else:
            return getattr(post, 'uri', None) or str(hash(str(post)))
