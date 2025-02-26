from fastapi import APIRouter, HTTPException, Depends
import bittensor as bt
import datetime as dt
import random
from common.data import DataSource, TimeBucket, DataEntityBucketId, DataLabel
from common import constants
from common.protocol import OnDemandRequest, GetDataEntityBucket
from common import utils  # Import your utils
from vali_utils.api.models import QueryRequest, QueryResponse, HealthResponse, LabelSize, AgeSize, LabelBytes, \
    DesirabilityRequest
from vali_utils.api.auth.auth import require_master_key, verify_api_key
from vali_utils.api.utils import endpoint_error_handler, select_validation_samples
from scraping.provider import ScraperProvider
from scraping.scraper import ValidationResult
from vali_utils.miner_evaluator import MinerEvaluator

from dynamic_desirability.desirability_uploader import run_uploader_from_gravity
from dynamic_desirability.desirability_retrieval import get_hotkey_json_submission
from typing import List, Optional
router = APIRouter()


def get_validator():
    """Dependency to get validator instance"""
    if not hasattr(get_validator, 'api'):
        raise HTTPException(503, "API server not initialized")
    return get_validator.api.validator


@router.post("/on_demand_data_request", response_model=QueryResponse)
@endpoint_error_handler
async def query_data(request: QueryRequest,
                     validator=Depends(get_validator),
                     api_key: str = Depends(verify_api_key)):
    """
    Handle data queries targeting multiple miners with validation and incentives.

    1. Selects multiple miners from top performers
    2. Queries them with the same request
    3. Validates data with a small probability
    4. Rewards/penalizes miners based on data quality
    5. Returns the best data to the user
    """
    try:
        # Constants for miner selection and validation
        NUM_MINERS_TO_QUERY = 5  # Number of miners to query
        VALIDATION_PROBABILITY = 0.05  # 5% chance to validate (to minimize API usage)
        MIN_PENALTY = 0.01  # Minimum credibility penalty for bad data
        MAX_PENALTY = 0.05  # Maximum credibility penalty for bad data

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

        # Select a diverse set of miners (don't pick all from the same hotkey owner if possible)
        selected_miners = []
        selected_coldkeys = set()

        # First, try to get a diverse set from top 30%
        while len(selected_miners) < NUM_MINERS_TO_QUERY and top_miners:
            idx = random.randint(0, len(top_miners) - 1)
            uid, _ = top_miners.pop(idx)
            coldkey = validator.metagraph.coldkeys[uid]

            # Only add if we haven't selected too many miners from this coldkey
            if coldkey not in selected_coldkeys or len(selected_coldkeys) < 2:
                selected_miners.append(uid)
                selected_coldkeys.add(coldkey)

        # If we couldn't get enough diverse miners, just take what we have
        if len(selected_miners) < 2:
            bt.logging.warning(f"Could only select {len(selected_miners)} diverse miners")
            # Get any miners to make up the numbers
            while len(selected_miners) < min(NUM_MINERS_TO_QUERY, len(miner_uids)):
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
            bt.logging.info("All miners returned no data - likely a valid empty result")
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
            # Convert DataEntity to dict
            item_dict = {
                'uri': item.uri,
                'datetime': item.datetime.isoformat(),
                'source': DataSource(item.source).name,
                'label': item.label.value if item.label else None,
                'content': item.content.decode('utf-8') if isinstance(item.content, bytes) else item.content
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


@router.get("/query_bucket/{source}")
@endpoint_error_handler
async def query_bucket(
        source: str,
        label: Optional[str] = None,
        start_bucket: Optional[int] = None,
        end_bucket: Optional[int] = None,
        validator=Depends(get_validator),
        api_key: str = Depends(verify_api_key)
):
    try:
        # Validate source
        try:
            source_id = DataSource[source.upper()].value
        except KeyError:
            raise HTTPException(400, f"Invalid source: {source}")

        bt.logging.info(f"Starting query for source: {source}, label: {label}")

        # Time handling
        now = dt.datetime.now(dt.timezone.utc)
        current_bucket = TimeBucket.from_datetime(now)

        if not end_bucket:
            end_bucket = current_bucket.id - 1
        if not start_bucket:
            start_bucket = end_bucket - 24  # Just look back 24 buckets instead of converting to hours

        bt.logging.info(f"Querying buckets {start_bucket} to {end_bucket}")

        # Single optimized query to get both latest bucket and miner in one go
        query = """
            WITH latest_bucket AS (
                SELECT MAX(timeBucketId) as bucket_id
                FROM MinerIndex
                WHERE source = ? 
                AND timeBucketId BETWEEN ? AND ?
                LIMIT 1
            )
            SELECT m.hotkey, m.credibility, mi.contentSizeBytes, mi.timeBucketId
            FROM MinerIndex mi
            JOIN Miner m ON mi.minerId = m.minerId
            JOIN latest_bucket lb
            WHERE mi.source = ?
            AND mi.timeBucketId = lb.bucket_id
        """
        params = [source_id, start_bucket, end_bucket, source_id]

        if label:
            query += " AND mi.labelId = ?"
            label_id = validator.evaluator.storage.label_dict.get_or_insert(label.strip().casefold())
            params.append(label_id)

        query += " ORDER BY m.credibility DESC LIMIT 1"

        with validator.evaluator.storage.lock:
            connection = validator.evaluator.storage._create_connection()
            cursor = connection.cursor()

            bt.logging.info("Executing SQL query...")
            cursor.execute(query, params)
            result = cursor.fetchone()
            connection.close()

            if not result:
                return {
                    "status": "error",
                    "message": "No data found in specified time range"
                }

            target_hotkey, credibility, expected_size, latest_bucket = result
            bt.logging.info(f"Found miner with bucket {latest_bucket}")

            # Find miner's UID
            uid = validator.metagraph.hotkeys.index(target_hotkey)
            axon = validator.metagraph.axons[uid]

            # Create bucket request
            bucket_id = DataEntityBucketId(
                time_bucket=TimeBucket(id=latest_bucket),
                source=DataSource(source_id),
                label=DataLabel(value=label) if label else None
            )

            # Query miner
            bt.logging.info(f"Querying miner {uid} for bucket {latest_bucket}")
            async with bt.dendrite(wallet=validator.wallet) as dendrite:
                response = await dendrite.forward(
                    axons=[axon],
                    synapse=GetDataEntityBucket(
                        data_entity_bucket_id=bucket_id,
                        version=constants.PROTOCOL_VERSION,
                    ),
                    timeout=30
                )

            if not response:
                return {
                    "status": "error",
                    "message": "No response from miner"
                }

            data = []
            for entity in response[0].data_entities:
                data.append({
                    'uri': entity.uri,
                    'datetime': entity.datetime.isoformat(),
                    'source': DataSource(entity.source).name,
                    'label': entity.label.value if entity.label else None,
                    'content': entity.content.decode('utf-8')
                })

            return {
                "status": "success",
                "miner": {
                    "hotkey": target_hotkey,
                    "uid": uid
                },
                "bucket": {
                    "id": latest_bucket,
                    "start": TimeBucket.to_date_range(TimeBucket(id=latest_bucket)).start.isoformat(),
                    "end": TimeBucket.to_date_range(TimeBucket(id=latest_bucket)).end.isoformat(),
                    "source": source.upper(),
                    "label": label,
                    "expected_size": expected_size
                },
                "data": data
            }

    except Exception as e:
        bt.logging.error(f"Error querying bucket: {str(e)}")
        raise HTTPException(500, str(e))




@router.get("/health", response_model=HealthResponse)
@endpoint_error_handler
async def health_check(validator=Depends(get_validator),
                       api_key: str = Depends(verify_api_key)):
    """Health check endpoint"""
    miner_uids = utils.get_miner_uids(validator.metagraph, validator.uid, 10_000)
    return {
        "status": "healthy" if validator.is_healthy() else "unhealthy",
        "timestamp": dt.datetime.utcnow(),
        "miners_available": len(miner_uids),
        "netuid": validator.config.netuid,
        "hotkey": validator.wallet.hotkey.ss58_address,
        "version": "1.0.0"
    }


@router.get("/get_top_labels_by_source/{source}", response_model=List[LabelSize])
@endpoint_error_handler
async def get_label_sizes(
        source: str,
        validator=Depends(get_validator),
        api_key: str = Depends(verify_api_key)):
    """Get content size information by label for a specific source"""
    try:
        # Validate source
        try:
            source_id = DataSource[source.upper()].value
        except KeyError:
            raise HTTPException(400, f"Invalid source: {source}")

        with validator.evaluator.storage.lock:
            connection = validator.evaluator.storage._create_connection()
            cursor = connection.cursor()

            # Direct query from MinerIndex and Miner tables
            cursor.execute("""
                SELECT 
                    labelId,
                    SUM(contentSizeBytes) as contentSizeBytes,
                    SUM(contentSizeBytes * credibility) as adjContentSizeBytes
                FROM MinerIndex
                JOIN Miner USING (minerId)
                WHERE source = ?
                GROUP BY labelId
                ORDER BY adjContentSizeBytes DESC
                LIMIT 1000
            """, [source_id])

            # Translate labelIds to label values using label_dict
            labels = [
                LabelSize(
                    label_value=validator.evaluator.storage.label_dict.get_by_id(row[0]),
                    content_size_bytes=int(row[1]),
                    adj_content_size_bytes=int(row[2])
                )
                for row in cursor.fetchall()
            ]

            connection.close()
            return labels

    except Exception as e:
        bt.logging.error(f"Error getting label sizes: {str(e)}")
        raise HTTPException(500, str(e))


@router.get("/ages", response_model=List[AgeSize])
@endpoint_error_handler
async def get_age_sizes(
        source: str,
        validator=Depends(get_validator),
        api_key: str = Depends(verify_api_key)

):
    """Get content size information by age bucket for a specific source from Miner and MinerIndex validator tables"""
    try:
        # Validate source
        try:
            source_id = DataSource[source.upper()].value
        except KeyError:
            raise HTTPException(400, f"Invalid source: {source}")

        with validator.evaluator.storage.lock:
            connection = validator.evaluator.storage._create_connection()
            cursor = connection.cursor()

            cursor.execute("""
                SELECT 
                    timeBucketId,
                    SUM(contentSizeBytes) as contentSizeBytes,
                    SUM(contentSizeBytes * credibility) as adjContentSizeBytes
                FROM Miner
                JOIN MinerIndex USING (minerId)
                WHERE source = ?
                GROUP BY timeBucketId
                ORDER BY timeBucketId DESC
                LIMIT 1000
            """, [source_id])

            ages = [
                AgeSize(
                    time_bucket_id=row[0],
                    content_size_bytes=int(row[1]),
                    adj_content_size_bytes=int(row[2])
                )
                for row in cursor.fetchall()
            ]
            connection.close()
            return ages
    except Exception as e:
        raise HTTPException(500, f"Error retrieving age sizes: {str(e)}")


@router.post("/set_desirabilities")
@endpoint_error_handler
async def set_desirabilities(
        request: DesirabilityRequest,
        validator=Depends(get_validator),
        api_key: str = Depends(require_master_key)):
    try:
        success, message = run_uploader_from_gravity(validator.config, request.desirabilities)
        if not success:
            bt.logging.error(f"Could not set desirabilities error message\n: {message}")
            raise HTTPException(status_code=400, detail=message)
        return {"status": "success", "message": message}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        bt.logging.error(f"Error setting desirabilities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/get_desirabilities")
@endpoint_error_handler
async def get_desirability_list(
        hotkey: str = None,
        validator=Depends(get_validator),
        api_key: str = Depends(verify_api_key)
):
    """If hotkey specified, return the current unscaled json submission for a specific validator hotkey. 
       Otherwise, return the current aggregate desirability list."""
    try:
        subtensor = validator.subtensor
        netuid = validator.evaluator.config.netuid
        metagraph = validator.evaluator.metagraph
        return get_hotkey_json_submission(subtensor=subtensor, netuid=netuid, metagraph=metagraph, hotkey=hotkey)
    except Exception as e:
        bt.logging.error(f"Error getting desirabilities: {str(e)}")
        raise HTTPException(500, f"Error retrieving desirabilities: {str(e)}")


@router.get("/get_bytes_by_label", response_model=LabelBytes)
@endpoint_error_handler
async def get_bytes_by_label(
        label: str,
        validator=Depends(get_validator),
        api_key: str = Depends(verify_api_key)):
    """
    Returns the total sum of contentSizeBytes and adjusted bytes for the given label.
    """
    try:
        # Normalize label as done in the storage system
        normalized_label = label.strip().casefold()
        if not normalized_label:
            return LabelBytes(label=label, total_bytes=0, adj_total_bytes=0.0)

        with validator.evaluator.storage.lock:
            connection = validator.evaluator.storage._create_connection()
            cursor = connection.cursor()

            # Get label ID from label dictionary
            label_id = validator.evaluator.storage.label_dict.get_or_insert(normalized_label)

            # Query both raw bytes and adjusted bytes (weighted by credibility)
            cursor.execute("""
                SELECT 
                    SUM(contentSizeBytes) as total_bytes,
                    SUM(contentSizeBytes * credibility) as adj_total_bytes
                FROM MinerIndex
                JOIN Miner USING (minerId)
                WHERE labelId = ?
            """, [label_id])

            row = cursor.fetchone()
            total_bytes = row[0] if row and row[0] is not None else 0
            adj_total_bytes = row[1] if row and row[1] is not None else 0.0

            connection.close()

            return LabelBytes(
                label=label,
                total_bytes=int(total_bytes),
                adj_total_bytes=int(adj_total_bytes)
            )

    except Exception as e:
        bt.logging.error(f"Error getting bytes for label {label}: {str(e)}")
        raise HTTPException(500, f"Error retrieving bytes for label: {str(e)}")