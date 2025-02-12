from fastapi import APIRouter, HTTPException, Depends
import bittensor as bt
import datetime as dt
from common.data import DataSource, TimeBucket, DataEntityBucketId, DataLabel
from common import constants
from common.protocol import OnDemandRequest, GetDataEntityBucket
from common import utils  # Import your utils
from vali_utils.api.models import QueryRequest, QueryResponse, HealthResponse, LabelSize, AgeSize, LabelBytes, \
    DesirabilityRequest
from vali_utils.api.auth.auth import require_master_key, verify_api_key
from vali_utils.api.utils import endpoint_error_handler, select_validation_samples

from dynamic_desirability.desirability_uploader import run_uploader_from_gravity

from dynamic_desirability.desirability_retrieval import get_hotkey_json_submission
from typing import List, Optional
import random

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
    """Handle data queries targeting test miner"""
    try:

        # Get all miner UIDs and sort by stake
        miner_uids = utils.get_miner_uids(validator.metagraph, validator.uid)
        miner_scores = [(uid, float(validator.metagraph.I[uid])) for uid in miner_uids]
        miner_scores.sort(key=lambda x: x[1], reverse=True)

        # Take top 50%
        top_miners = miner_scores[:len(miner_scores) // 2]

        # Select random miner from top 50%
        uid, _ = random.choice(top_miners)
        hotkey = validator.metagraph.hotkeys[uid]

        bt.logging.info(f"Selected miner {uid} from top {len(top_miners)} miners")

        # Create request synapse
        synapse = OnDemandRequest(
            source=DataSource[request.source.upper()],
            usernames=request.usernames,
            keywords=request.keywords,
            start_date=request.start_date,
            end_date=request.end_date,
            limit=request.limit
        )

        # Query test miner
        async with bt.dendrite(wallet=validator.wallet) as dendrite:
            axon = validator.metagraph.axons[uid]
            responses = await dendrite.forward(
                axons=[axon],
                synapse=synapse,
                timeout=30
            )

            # Get first response
            if not responses:
                raise HTTPException(503, "No response received from miner")

            bt.logging.info(f"Raw response from miner: {responses}")

            response = responses[0]  # This is the synapse
            bt.logging.info(f"Raw response from miner: {response}")

            # Access the data directly from the synapse data field
            all_data = response.data if response.data else []

            # Convert DataEntity objects to dictionaries
            processed_data = []
            for item in all_data:
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

            return {
                "status": "success",
                "data": unique_data[:request.limit],
                "meta": {
                    "miner_hotkey": TEST_MINER_HOTKEY,
                    "miner_uid": uid,
                    "items_returned": len(unique_data)
                }
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
    miner_uids = utils.get_miner_uids(validator.metagraph, validator.uid)
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