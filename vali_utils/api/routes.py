# vali_utils/api/routes.py

from fastapi import APIRouter, HTTPException, Depends
import bittensor as bt
import datetime as dt
from common.data import DataSource
from common.protocol import OnDemandRequest
from common import utils  # Import your utils
from vali_utils.api.models import QueryRequest, QueryResponse, HealthResponse, LabelSize, AgeSize, LabelBytes, DesirabilityItem
from vali_utils.api.auth.auth import require_master_key, verify_api_key
from vali_utils.api.utils import endpoint_error_handler

from dynamic_desirability.desirability_uploader import run_uploader_from_gravity
from typing import List
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
    """Handle data queries"""
    try:
        # Get miner UIDs using your utility function
        # This excludes validators and the current validator's UID
        miner_uids = utils.get_miner_uids(validator.metagraph, validator.uid)

        # Sort by stake and get top 50%
        miners = sorted(
            miner_uids,
            key=lambda uid: validator.metagraph.I[uid],
            reverse=True
        )[:len(miner_uids) // 2]

        if not miners:
            raise HTTPException(503, "No miners available")

        # Create request synapse
        synapse = OnDemandRequest(
            source=DataSource[request.source.upper()],
            usernames=request.usernames,
            keywords=request.keywords,
            start_date=request.start_date,
            end_date=request.end_date,
            limit=request.limit
        )

        # Query random miner
        responses = []
        async with bt.dendrite(wallet=validator.wallet) as dendrite:
            uid = random.choice(miner_uids)
            # Check if miner is qualified using your utility
            if utils.is_miner(uid, validator.metagraph):
                axon = validator.metagraph.axons[uid]
                try:
                    response = await dendrite.forward(
                        axons=[axon],
                        synapse=synapse,
                        timeout=30
                    )
                    if response and response.data:
                        responses.append(response.data)
                except Exception as e:
                    bt.logging.error(f"Error querying miner {uid}: {str(e)}")


        # TODO ADD VALIDATION

        # Process data
        all_data = []
        seen = set()
        for response in responses:
            for item in response:
                item_hash = hash(str(item))
                if item_hash not in seen:
                    seen.add(item_hash)
                    all_data.append(item)

        return {
            "status": "success",
            "data": all_data[:request.limit],
            "meta": {
                "total_responses": len(responses),
                "unique_items": len(all_data),
                "miners_queried": len(miners),
                "total_miners": len(miner_uids)
            }
        }

    except Exception as e:
        bt.logging.error(f"Error processing request: {str(e)}")
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
        request: List[DesirabilityItem],
        validator=Depends(get_validator),
        api_key: str = Depends(require_master_key)):

    try:
        request_data = [item.model_dump() for item in request]
        success, message = await run_uploader_from_gravity(validator.config, request_data)
        if not success:
            bt.logging.error(f"Could not set desirabilities error message\n: {message}")
            raise HTTPException(status_code=400, detail=message)
        return {"status": "success", "message": message}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        bt.logging.error(f"Error setting desirabilities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


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