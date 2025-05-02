from common.organic_protocol import OrganicRequest
from vali_utils.api.models import QueryRequest
import datetime as dt
from fastapi import APIRouter, HTTPException, Depends
import bittensor as bt
import datetime as dt
from pathlib import Path
import pandas as pd
import random
import json
from common.data import DataSource, TimeBucket, DataEntityBucketId, DataLabel, DataEntity
from common import constants
from common.protocol import OnDemandRequest
from common import utils  # Import your utils

from scraping.scraper import ScrapeConfig
from common.date_range import DateRange
from scraping.provider import ScraperProvider
from scraping.scraper import ValidationResult
from scraping.x.enhanced_apidojo_scraper import EnhancedApiDojoTwitterScraper
from vali_utils.miner_evaluator import MinerEvaluator


async def process_organic_query(self, synapse: OrganicRequest) -> OrganicRequest:
    """Handler for organic queries through the axon"""
    try:
        bt.logging.info(f"Processing organic query for source: {synapse.source}")

        # Convert to QueryRequest format
        request = QueryRequest(
            source=synapse.source,
            usernames=synapse.usernames,
            keywords=synapse.keywords,
            start_date=synapse.start_date,
            end_date=synapse.end_date,
            limit=synapse.limit
        )

        # Execute core query logic
        result = await handle_data_query(self, request)

        # Map result to synapse response
        synapse.status = result.get("status", "error")
        synapse.data = result.get("data", [])
        synapse.meta = result.get("meta", {})

    except Exception as e:
        bt.logging.error(f"Error in organic query: {str(e)}")
        synapse.status = "error"
        synapse.meta = {"error": str(e)}
        synapse.data = []

    return synapse


def organic_blacklist(self, synapse) -> tuple[bool, str]:
    """Filter for organic requests"""
    if hasattr(self.config, 'organic_whitelist') and self.config.organic_whitelist:
        if synapse.dendrite.hotkey not in self.config.organic_whitelist:
            return True, "Sender not in whitelist"

    try:
        caller_uid = self.metagraph.hotkeys.index(synapse.dendrite.hotkey)
        min_stake = getattr(self.config, 'organic_min_stake', 100.0)
        if self.metagraph.S[caller_uid] < min_stake:
            return True, f"Insufficient stake (minimum: {min_stake})"
    except ValueError:
        return True, "Hotkey not found in metagraph"

    return False, "Request accepted"


async def handle_data_query(validator, request: QueryRequest):
    """
    Handle data queries targeting multiple miners with validation and incentives.
    Now supports enhanced X content with rich metadata.
    """
    try:
        # Constants for miner selection and validation
        NUM_MINERS_TO_QUERY = 5
        VALIDATION_PROBABILITY = 0.05
        MIN_PENALTY = 0.01
        MAX_PENALTY = 0.05

        # Get all miner UIDs and sort by incentive
        miner_uids = utils.get_miner_uids(validator.metagraph, validator.uid, 10_000)
        miner_scores = [(uid, float(validator.metagraph.I[uid])) for uid in miner_uids]
        miner_scores.sort(key=lambda x: x[1], reverse=True)

        # Take top 60% of miners (but at least 5 if available)
        top_count = max(5, int(len(miner_scores) * 0.6))
        top_miners = miner_scores[:top_count]

        if len(top_miners) < 2:
            return {
                "status": "error",
                "meta": {"error": "Not enough miners available to service request"},
                "data": []
            }

        # Select a diverse set of miners
        selected_miners = []
        selected_coldkeys = set()

        # First, try to get a diverse set from top miners
        while len(selected_miners) < NUM_MINERS_TO_QUERY and top_miners:
            idx = random.randint(0, len(top_miners) - 1)
            uid, _ = top_miners.pop(idx)
            coldkey = validator.metagraph.coldkeys[uid]

            # Only add if we haven't selected too many miners from this coldkey
            if coldkey not in selected_coldkeys or len(selected_coldkeys) < 2:
                selected_miners.append(uid)
                selected_coldkeys.add(coldkey)

        # If we couldn't get enough diverse miners, just take what we have
        if len(selected_miners) < 1:
            bt.logging.warning(f"Could only select {len(selected_miners)} diverse miners")
            # Get any miners to make up the numbers
            for uid in miner_uids:
                if uid not in selected_miners:
                    selected_miners.append(uid)
                    if len(selected_miners) >= NUM_MINERS_TO_QUERY:
                        break

        bt.logging.info(f"Selected {len(selected_miners)} miners for on-demand query")

        # Create request synapse
        synapse = OnDemandRequest(
            source=DataSource[request.source.upper()],
            usernames=request.usernames,
            keywords=request.keywords,
            start_date=request.start_date,
            end_date=request.end_date,
            limit=request.limit
        )

        # Query selected miners
        bt.logging.info(f"Querying miners: {selected_miners}")
        miner_responses = {}
        miner_data_counts = {}

        async with bt.dendrite(wallet=validator.wallet) as dendrite:
            axons = [validator.metagraph.axons[uid] for uid in selected_miners]
            responses = await dendrite.forward(
                axons=axons,
                synapse=synapse,
                timeout=30
            )

            # Process responses
            for i, response in enumerate(responses):
                if i < len(selected_miners) and response is not None:
                    uid = selected_miners[i]
                    hotkey = validator.metagraph.hotkeys[uid]

                    # Check if response has data
                    data = getattr(response, 'data', [])
                    data_count = len(data) if data else 0

                    miner_responses[uid] = response
                    miner_data_counts[uid] = data_count

                    bt.logging.info(f"Miner {uid} ({hotkey}) returned {data_count} items")

        if not miner_responses:
            return {
                "status": "error",
                "meta": {"error": "No miners responded to the query"},
                "data": []
            }

        # Step 1: Check if we have a consensus on "no data"
        no_data_consensus = all(count == 0 for count in miner_data_counts.values())
        if no_data_consensus:
            bt.logging.info("All miners returned no data - performing verification check")

            # Perform a quick check to verify if data should exist
            try:
                # For X data, use exactly the same approach as miners
                if synapse.source == DataSource.X:
                    # Initialize the enhanced scraper directly as miners do
                    scraper = EnhancedApiDojoTwitterScraper()
                elif synapse.source == DataSource.REDDIT:
                    # For other sources, use the standard provider
                    scraper_id = MinerEvaluator.PREFERRED_SCRAPERS.get(synapse.source)
                    if not scraper_id:
                        bt.logging.warning(f"No preferred scraper for source {synapse.source}")
                        # Return empty result with consensus
                        return {
                            "status": "success",
                            "data": [],
                            "meta": {
                                "miners_queried": len(selected_miners),
                                "consensus": "no_data"
                            }
                        }
                    scraper = ScraperProvider().get(scraper_id)
                else:
                    bt.logging.warning(f"No preferred scraper for source {synapse.source}")
                    return {
                        "status": "success",
                        "data": [],
                        "meta": {
                            "miners_queried": len(selected_miners),
                            "consensus": "no_data"
                        }
                    }

                if not scraper:
                    bt.logging.warning(f"Could not initialize scraper for {synapse.source}")
                    return {
                        "status": "success",
                        "data": [],
                        "meta": {
                            "miners_queried": len(selected_miners),
                            "consensus": "no_data"
                        }
                    }

                # Create scrape config with limited scope (only check for a few items)
                # For X data, combine keywords and usernames with appropriate label formatting
                labels = []
                if synapse.keywords:
                    labels.extend([DataLabel(value=k) for k in synapse.keywords])
                if synapse.usernames:
                    # Ensure usernames have @ prefix
                    labels.extend([DataLabel(value=f"@{u.strip('@')}" if not u.startswith('@') else u) for u in
                                   synapse.usernames])

                # Create scrape config matching the miner's configuration
                verify_config = ScrapeConfig(
                    entity_limit=request.limit,  # Use requested limit
                    date_range=DateRange(
                        start=dt.datetime.fromisoformat(synapse.start_date) if synapse.start_date else dt.datetime.now(
                            dt.timezone.utc) - dt.timedelta(days=1),
                        end=dt.datetime.fromisoformat(synapse.end_date) if synapse.end_date else dt.datetime.now(
                            dt.timezone.utc)
                    ),
                    labels=labels,
                )

                # For X source, replicate exactly what miners do in handle_on_demand
                if synapse.source == DataSource.X:
                    await scraper.scrape(verify_config)

                    # Get enhanced content
                    enhanced_content = scraper.get_enhanced_content()

                    # IMPORTANT: Convert EnhancedXContent to DataEntity to maintain protocol compatibility
                    # while keeping the rich data in serialized form
                    verification_data = []
                    for content in enhanced_content:
                        # Convert to DataEntity but store full rich content in serialized form
                        api_response = content.to_api_response()
                        data_entity = DataEntity(
                            uri=content.url,
                            datetime=content.timestamp,
                            source=DataSource.X,
                            label=DataLabel(
                                value=content.tweet_hashtags[0].lower()) if content.tweet_hashtags else None,
                            # Store the full enhanced content as serialized JSON in the content field
                            content=json.dumps(api_response).encode('utf-8'),
                            content_size_bytes=len(json.dumps(api_response))
                        )
                        verification_data.append(data_entity)
                else:
                    # For other sources, use standard scrape
                    verification_data = await scraper.scrape(verify_config)

                # If we found data but miners returned none, they should be penalized
                if verification_data:
                    bt.logging.warning(f"Miners returned no data, but validator found {len(verification_data)} items")

                    # Apply penalty to all miners for this request
                    for uid in selected_miners:
                        validation_results = [
                            ValidationResult(is_valid=False, reason="Failed to return existing data",
                                             content_size_bytes_validated=5)
                        ]

                        bt.logging.info(f"Applying 5% penalty to miner {uid} for not returning data that exists")

                        # Update the miner's score with the mixed results
                        validator.evaluator.scorer._update_credibility(uid, validation_results)

                    # Process the verification data to match exactly what miners would return
                    processed_data = []
                    for item in verification_data:
                        if synapse.source == DataSource.X:
                            # For X data, miners store the API response as JSON in the content field
                            # We need to decode it and parse it as JSON to match what miners return
                            try:
                                json_content = item.content.decode('utf-8') if isinstance(item.content,
                                                                                          bytes) else item.content
                                parsed_content = json.loads(json_content)
                                processed_data.append(parsed_content)
                            except Exception as e:
                                bt.logging.error(f"Error parsing X content: {str(e)}")
                                # Fallback if parsing fails
                                processed_data.append({
                                    'uri': item.uri,
                                    'datetime': item.datetime.isoformat(),
                                    'source': DataSource(item.source).name,
                                    'label': item.label.value if item.label else None,
                                    'content': str(item.content)[:1000]  # Truncate for safety
                                })
                        else:
                            # For other sources, use standard format
                            item_dict = {
                                'uri': item.uri,
                                'datetime': item.datetime.isoformat(),
                                'source': DataSource(item.source).name,
                                'label': item.label.value if item.label else None,
                                'content': item.content.decode('utf-8') if isinstance(item.content, bytes) else str(
                                    item.content)
                            }
                            processed_data.append(item_dict)

                    return {
                        "status": "warning",
                        "data": processed_data[:request.limit],
                        "meta": {
                            "miners_queried": len(selected_miners),
                            "miners_responded": len(miner_responses),
                            "verification_message": "Miners returned no data, but data was found. Results are from validator's direct check.",
                            "items_returned": len(processed_data)
                        }
                    }
            except Exception as e:
                bt.logging.error(f"Error during verification check: {str(e)}")

            # If verification failed or no data was found, return original empty result
            return {
                "status": "success",
                "data": [],
                "meta": {
                    "miners_queried": len(selected_miners),
                    "consensus": "no_data"
                }
            }

        # Step 2: Check consistency of responses from miners that returned data
        miners_with_data = [uid for uid, count in miner_data_counts.items() if count > 0]

        if not miners_with_data:
            return {
                "status": "error",
                "meta": {"error": "No miners returned valid data"},
                "data": []
            }

        # Get the median data count as a reference point
        data_counts = [count for count in miner_data_counts.values() if count > 0]
        median_count = sorted(data_counts)[len(data_counts) // 2]

        # Define consistency threshold (within 30% of median is considered consistent)
        consistency_threshold = 0.3

        consistent_miners = {}
        inconsistent_miners = {}

        for uid in miners_with_data:
            count = miner_data_counts[uid]
            # Check if count is within threshold of median
            if median_count > 0 and abs(count - median_count) / median_count <= consistency_threshold:
                consistent_miners[uid] = miner_responses[uid]
            else:
                inconsistent_miners[uid] = miner_responses[uid]

        bt.logging.info(
            f"Found {len(consistent_miners)} consistent miners and {len(inconsistent_miners)} inconsistent miners")

        # Step 3: Should we validate? (random chance or if consistency is poor)
        should_validate = random.random() < VALIDATION_PROBABILITY or len(consistent_miners) < len(
            miners_with_data) // 2

        # Step 4: Validation and penalties
        validated_miners = {}

        if should_validate:
            bt.logging.info("Performing validation on returned data")
            # We'll only validate a subset of data to minimize API usage
            for uid, response in list(consistent_miners.items()) + list(inconsistent_miners.items()):
                try:
                    # Only validate up to 2 items per miner to reduce API cost
                    data_to_validate = response.data[:2] if hasattr(response, 'data') and response.data else []

                    if not data_to_validate:
                        continue

                    # Use scraper to validate data
                    scraper_id = MinerEvaluator.PREFERRED_SCRAPERS.get(synapse.source)
                    if not scraper_id:
                        bt.logging.warning(f"No preferred scraper for source {synapse.source}")
                        continue

                    scraper = ScraperProvider().get(scraper_id)
                    if not scraper:
                        bt.logging.warning(f"Could not initialize scraper {scraper_id}")
                        continue

                    # Validate the data
                    validation_results = await scraper.validate(data_to_validate)

                    # Calculate validation success rate
                    valid_count = sum(1 for r in validation_results if r.is_valid)
                    validation_rate = valid_count / len(validation_results) if validation_results else 0

                    validated_miners[uid] = validation_rate

                    bt.logging.info(f"Miner {uid} validation rate: {validation_rate:.2f}")

                    # Apply penalty to miners with poor validation
                    if validation_rate < 0.5:  # Less than 50% valid
                        # This is a soft penalty - calculated based on how bad the data is
                        # Scale from MIN_PENALTY to MAX_PENALTY based on validation_rate
                        penalty = MIN_PENALTY + (0.5 - validation_rate) * 2 * (MAX_PENALTY - MIN_PENALTY)

                        # Ensure penalty doesn't make credibility negative
                        current_cred = validator.evaluator.scorer.get_miner_credibility(uid)
                        safe_penalty = min(current_cred * 0.2, penalty)  # Never reduce by more than 20%

                        bt.logging.info(f"Applying penalty of {safe_penalty:.4f} to miner {uid}")

                        # Create a validation result for the scorer
                        validation_result = ValidationResult(
                            is_valid=False,
                            reason="Failed on-demand data validation",
                            content_size_bytes_validated=sum(e.content_size_bytes for e in data_to_validate)
                        )

                        # Update the miner's score with this result
                        index = validator.storage.read_miner_index(validator.metagraph.hotkeys[uid])
                        validator.evaluator.scorer.on_miner_evaluated(uid, index, [validation_result])

                        # Remove this miner from consistent miners if it was there
                        if uid in consistent_miners:
                            del consistent_miners[uid]
                            inconsistent_miners[uid] = response

                except Exception as e:
                    bt.logging.error(f"Error validating data from miner {uid}: {str(e)}")

        # Step 5: Select best data to return to user
        best_data = []
        best_meta = {}

        # First preference: data from validated miners with high validation rates
        if validated_miners:
            # Find the miner with the highest validation rate
            best_uid = max(validated_miners.items(), key=lambda x: x[1])[0]
            best_miner_response = miner_responses[best_uid]
            best_data = best_miner_response.data if hasattr(best_miner_response, 'data') else []
            best_meta = {
                "source": "validated",
                "miner_uid": best_uid,
                "miner_hotkey": validator.metagraph.hotkeys[best_uid],
                "validation_rate": validated_miners[best_uid]
            }

        # Second preference: data from consistent miners (if no validation was done or no miners passed validation)
        elif consistent_miners:
            # Pick the miner with the most data (but within reason)
            best_uid = max(consistent_miners.keys(), key=lambda uid: miner_data_counts[uid])
            best_miner_response = consistent_miners[best_uid]
            best_data = best_miner_response.data if hasattr(best_miner_response, 'data') else []
            best_meta = {
                "source": "consistent",
                "miner_uid": best_uid,
                "miner_hotkey": validator.metagraph.hotkeys[best_uid]
            }

        # Last resort: take data from any miner that returned something
        elif miners_with_data:
            # Take the miner with median data count
            median_uid = sorted(miners_with_data, key=lambda uid: miner_data_counts[uid])[len(miners_with_data) // 2]
            best_miner_response = miner_responses[median_uid]
            best_data = best_miner_response.data if hasattr(best_miner_response, 'data') else []
            best_meta = {
                "source": "inconsistent",
                "miner_uid": median_uid,
                "miner_hotkey": validator.metagraph.hotkeys[median_uid]
            }

        # Process the data for return
        processed_data = []
        for item in best_data:
            # For X content from miners, item.content already contains the serialized API response
            # which needs to be parsed as JSON
            if synapse.source == DataSource.X:
                try:
                    # Extract the content as string and parse it as JSON
                    if hasattr(item, 'content') and item.content:
                        content_str = item.content.decode('utf-8') if isinstance(item.content, bytes) else item.content
                        try:
                            item_dict = json.loads(content_str)
                            processed_data.append(item_dict)
                            continue
                        except json.JSONDecodeError:
                            # If JSON parsing fails, fall through to the standard processing
                            pass
                except Exception as e:
                    bt.logging.error(f"Error processing X content: {str(e)}")

            # Standard processing for non-X content or if JSON parsing failed
            item_dict = {
                'uri': item.uri if hasattr(item, 'uri') else None,
                'datetime': item.datetime.isoformat() if hasattr(item, 'datetime') else None,
                'source': DataSource(item.source).name if hasattr(item, 'source') else None,
                'label': item.label.value if hasattr(item, 'label') and item.label else None,
                'content': item.content.decode('utf-8') if hasattr(item, 'content') and isinstance(item.content,
                                                                                                   bytes) else
                item.content if hasattr(item, 'content') else None
            }
            processed_data.append(item_dict)

        # Remove duplicates by converting to string representation
        seen = set()
        unique_data = []
        for item in processed_data:
            item_str = str(sorted(item.items()))
            if item_str not in seen:
                seen.add(item_str)
                unique_data.append(item)

        # Add summary info to meta
        best_meta.update({
            "miners_queried": len(selected_miners),
            "miners_responded": len(miner_responses),
            "consistent_miners": len(consistent_miners),
            "inconsistent_miners": len(inconsistent_miners),
            "validated_miners": len(validated_miners) if should_validate else 0,
            "items_returned": len(unique_data)
        })

        return {
            "status": "success",
            "data": unique_data[:request.limit],
            "meta": best_meta
        }

    except Exception as e:
        bt.logging.error(f"Error processing request: {str(e)}")
        raise HTTPException(500, str(e))
