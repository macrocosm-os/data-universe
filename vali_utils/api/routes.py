from fastapi import APIRouter, HTTPException, Depends
import bittensor as bt
import datetime as dt
from pathlib import Path
import pandas as pd
import time
import json
from common.data import DataSource, TimeBucket, DataEntityBucketId, DataLabel, DataEntity
from common import constants
from common.protocol import OnDemandRequest, GetDataEntityBucket
from common.organic_protocol import OrganicRequest
from common import utils  # Import your utils
from vali_utils.api.models import QueryRequest, QueryResponse, HealthResponse, LabelSize, AgeSize, LabelBytes, \
    DesirabilityRequest, HfReposResponse
from vali_utils.api.auth.auth import require_master_key, verify_api_key
from vali_utils.api.utils import endpoint_error_handler, query_validator
from scraping.scraper import ScrapeConfig
from common.date_range import DateRange
from scraping.provider import ScraperProvider
from scraping.scraper import ValidationResult
from scraping.x.enhanced_apidojo_scraper import EnhancedApiDojoTwitterScraper
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
async def query_data(
    request: QueryRequest,
    validator=Depends(get_validator),
    api_key: str = Depends(verify_api_key)
):
    """
    Handle data queries targeting validators with fallback support.
    
    This endpoint:
    1. Gets available validators from the registry
    2. Tries each validator in turn until a successful response is received
    3. Updates validator status based on response (success/failure)
    4. Returns processed data or error information
    """
    bt.logging.info(f"Processing on-demand data request for source: {request.source}")
    
    # Get validator registry from the manager
    validator_registry = validator.validator_registry
    
    # Get available validators
    available_validators = validator_registry.get_available_validators()
    
    if not available_validators:
        bt.logging.error("No validators available to process request")
        raise HTTPException(
            status_code=503,
            detail="Service unavailable: No validators are currently available."
        )
    
    # Prepare the OrganicRequest from the QueryRequest
    try:
        organic_request = OrganicRequest(
            source=request.source.upper(),
            usernames=request.usernames or [],
            keywords=request.keywords or [],
            start_date=request.start_date,
            end_date=request.end_date,
            limit=request.limit or 100 # default request is 100 items
        )
    except Exception as e:
        bt.logging.error(f"Error creating OrganicRequest: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Invalid request parameters: {str(e)}")
    
    # Go through validators until successful response received
    response = None
    last_error = None
    status = None
    
    for uid in available_validators:
        queried_validator = validator_registry.validators[uid]
        
        bt.logging.info(f"Querying validator {queried_validator.uid} at {queried_validator.axon}")
        
        try:
            # Parse host and port from axon string
            if ':' in queried_validator.axon:
                host, port_str = queried_validator.axon.rsplit(':', 1)
                port = int(port_str)
            else:
                host = queried_validator.axon
                port = 8091  # Default port
            
            wallet = validator.wallet
            
            # Query the validator
            response = await query_validator(
                wallet=wallet,
                validator_host=host,
                validator_port=port,
                validator_hotkey=queried_validator.hotkey,
                source=request.source,
                keywords=request.keywords or [],
                usernames=request.usernames or [],
                start_date=request.start_date,
                end_date=request.end_date,
                limit=request.limit or 100
            )
            
            # Check if we got a valid response
            status = response.get('status') if isinstance(response, dict) else getattr(response, 'status', 'unknown')
            if response and status:
                # Update validator status based on response
                validator_registry.update_validators(uid, status)
                
                # If successful, break the loop
                if status == "success" or status == "warning":
                    bt.logging.info(f"Validator {uid} returned successful response")
                    break
                else:
                    bt.logging.warning(f"Validator {uid} returned error response: {status}")
                    last_error = f"Validator error: {response.meta.get('error', 'Unknown error')}" if hasattr(response, 'meta') else "Unknown validator error"
            else:
                validator_registry.update_validators(uid, "error")
                bt.logging.warning(f"Invalid response from validator {uid}")
                last_error = f"Invalid response from validator: {response}"
                
        except Exception as e:
            validator_registry.update_validators(uid, "error")
            bt.logging.error(f"Error querying validator {uid}: {str(e)}")
            last_error = str(e)
    
    # If we didn't get a successful response from any validator
    if not response or not status or status not in ["success", "warning"]:
        bt.logging.error(f"All validators failed to process request. Status: {status}")
        raise HTTPException(
            status_code=502,
            detail=f"Failed to get valid response from any validator. Last error: {last_error}"
        )
    
    # Process the successful response
    try:
        # Extract data and metadata
        data = response.get('data', []) if isinstance(response, dict) else getattr(response, 'data', [])
        meta = response.get('meta', {}) if isinstance(response, dict) else getattr(response, 'meta', {})
        
        # Construct and return the response
        return QueryResponse(
            status=status,
            data=data,
            meta={
                **meta,
                "processing_time_ms": time.time() * 1000 - time.time() * 1000,  # Replace with actual timing logic
                "api_version": "1.0"
            }
        )
        
    except Exception as e:
        bt.logging.error(f"Error processing validator response: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing response: {str(e)}")


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


@router.get("/list_repo_names", response_model=HfReposResponse)
@endpoint_error_handler
async def list_hf_repo_names(
        validator=Depends(get_validator),
        api_key: str = Depends(verify_api_key)):
    """
    Returns a list of repository names from the hf_validation.parquet file,
    excluding "no_dataset_provided".
    """
    try:
        parquet_path = validator.config.hf_results_path
        
        df = pd.read_parquet(parquet_path)
        
        # Extract unique repo names, excluding "no_dataset_provided"
        repo_names = [name for name in df['repo_name'].unique() if name != "no_dataset_provided"]
        repo_names.sort()
        
        return {
            "count": len(repo_names),
            "repo_names": repo_names
        }
    except FileNotFoundError:
        bt.logging.error("hf_validation.parquet file not found")
        raise HTTPException(404, "Dataset file not found")
    except Exception as e:
        bt.logging.error(f"Error retrieving repository names: {str(e)}")
        raise HTTPException(500, f"Error retrieving repository names: {str(e)}")


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


@router.get("/monitoring/system-status")
@endpoint_error_handler
async def system_health_check(
    _: bool = Depends(require_master_key)
):
    """Internal health check endpoint for monitoring"""
    return {"status": "healthy", "timestamp": dt.datetime.utcnow().isoformat()}