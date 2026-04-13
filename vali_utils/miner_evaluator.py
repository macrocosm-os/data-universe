import os
import gc
import copy
import asyncio
import datetime
import random
import traceback
import threading
import time

from common import constants
from common.data_v2 import ScorableMinerIndex
from common.metagraph_syncer import MetagraphSyncer
import common.utils as utils
import datetime as dt
import bittensor as bt
from common.data import (
    CompressedMinerIndex,
    DataEntityBucket,
    DataEntity,
    DataSource,
)
from common.protocol import GetDataEntityBucket, GetMinerIndex
from rewards.data_value_calculator import DataValueCalculator
from scraping.provider import ScraperProvider
from scraping.scraper import ScraperId, ValidationResult
from storage.validator.sqlite_memory_validator_storage import (
    SqliteMemoryValidatorStorage,
)
from storage.validator.s3_validator_storage import S3ValidationStorage

from vali_utils.miner_iterator import MinerIterator
from vali_utils import metrics, utils as vali_utils

from typing import List, Optional
from vali_utils.validator_s3_access import ValidatorS3Access
from vali_utils.s3_utils import validate_s3_miner_data, get_s3_validation_summary, S3ValidationResult
from vali_utils.s3_logging_utils import log_s3_validation_table

import httpx

from common.api_client import (
    DataUniverseApiClient,
    ListJobsWithSubmissionsForValidationRequest,
    OnDemandMinerUpload,
)
from rewards.miner_scorer import MinerScorer
from vali_utils.on_demand.od_job_cache import ODJobCache
from vali_utils.on_demand.on_demand_validation import OnDemandValidator


class MinerEvaluator:
    """MinerEvaluator is responsible for evaluating miners and updating their scores."""

    SCORER_FILENAME = "scorer.pickle"

    # Mapping of scrapers to use based on the data source to validate.
    PREFERRED_SCRAPERS = {
        DataSource.X: ScraperId.X_APIDOJO,
        DataSource.REDDIT: ScraperId.REDDIT_MC,
    }

    def __init__(self, config: bt.config, uid: int, metagraph_syncer: MetagraphSyncer, s3_reader: ValidatorS3Access):
        self.config = config
        self.uid = uid
        self.metagraph_syncer = metagraph_syncer
        self.metagraph = self.metagraph_syncer.get_metagraph(config.netuid)
        self.metagraph_syncer.register_listener(
            self._on_metagraph_updated, netuids=[config.netuid]
        )
        self.vpermit_rao_limit = self.config.vpermit_rao_limit
        self.wallet = bt.wallet(config=self.config)

        # Set up initial scoring weights for validation
        self.scorer = MinerScorer(self.metagraph.n, DataValueCalculator())

        # Setup dependencies.
        self.miner_iterator = MinerIterator(
            utils.get_miner_uids(self.metagraph, self.vpermit_rao_limit)
        )
        self.scraper_provider = ScraperProvider()
        self.storage = SqliteMemoryValidatorStorage()
        self.s3_storage = S3ValidationStorage(self.config.s3_results_path)
        self.s3_reader = s3_reader
        # OD job cache — set by validator.py after construction.
        # eval_miner() drains cached OD results per-miner.
        self.od_cache: Optional[ODJobCache] = None
        # OD validator — set by validator.py after construction.
        # Used for spot-check validation at eval time.
        self.on_demand_validator = None

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.lock = threading.RLock()
        self.is_setup = False

    def get_scorer(self) -> MinerScorer:
        """Returns the scorer used by the evaluator."""
        return self.scorer

    def eval_miner_sync(self, uid: int) -> None:
        """Synchronous version of eval_miner."""
        asyncio.run(self.eval_miner(uid))

    async def _evaluate_od(self, uid: int, hotkey: str) -> None:
        """Drain cached OD results for this miner and apply scores.

        Called at the start of eval_miner() so OD rewards/penalties are applied
        in the same per-miner evaluation flow as P2P and S3.

        1. Apply rewards/penalties for all cached results (speed-based).
        2. Spot-check: pick one result, fetch the actual submission from the API,
           download it, and validate a sample entity with the scraper.
           If validation fails → apply penalty and flip the reward.
        """
        if self.od_cache is None:
            return

        results = self.od_cache.get_and_drain(hotkey)
        if not results:
            return

        # Apply all cached results in a single pass
        passed_count = 0
        failed_count = 0
        spot_check_candidate = None

        for r in results:
            if r.passed_validation is True:
                self.scorer.apply_ondemand_reward(
                    uid=uid,
                    speed_multiplier=r.speed_multiplier,
                    volume_multiplier=r.volume_multiplier,
                )
                passed_count += 1
                spot_check_candidate = r
            elif r.passed_validation is False:
                self.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
                failed_count += 1

        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: Applied {len(results)} cached OD results "
            f"(rewarded={passed_count}, penalized={failed_count})"
        )

        # Spot-check: validate one submission to catch fake data
        if spot_check_candidate and self.on_demand_validator is not None:
            await self._spot_check_od_submission(uid, hotkey, spot_check_candidate)

    async def _spot_check_od_submission(self, uid: int, hotkey: str, result) -> None:
        """Download one OD submission and validate a sample entity.

        Fetches the job from the API to get the presigned URL, downloads it,
        and runs scraper validation on a sample. If it fails, apply a penalty.
        """
        try:
            base_url = self.config.s3_auth_url
            verify_ssl = "localhost" not in base_url

            async with DataUniverseApiClient(
                base_url=base_url,
                verify_ssl=verify_ssl,
                keypair=self.wallet.hotkey,
                timeout=60,
            ) as client:
                # Fetch the specific job — use a narrow time window
                resp = await client.validator_list_jobs_with_submissions(
                    req=ListJobsWithSubmissionsForValidationRequest(
                        expired_since=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=60),
                        expired_until=dt.datetime.now(dt.timezone.utc),
                        limit=10,
                    ),
                )

            # Find this miner's submission for this job
            submission = None
            job = None
            for jws in resp.jobs_with_submissions:
                if jws.job.id == result.job_id:
                    job = jws.job
                    for sub in jws.submissions:
                        if sub.miner_hotkey == hotkey and sub.s3_presigned_url:
                            submission = sub
                            break
                    break

            if not submission or not job:
                bt.logging.debug(
                    f"UID:{uid} - OD spot-check: job {result.job_id} not found or expired"
                )
                return

            # Download the submission data
            async with httpx.AsyncClient(timeout=30.0) as http:
                dl_resp = await http.get(submission.s3_presigned_url, follow_redirects=True)
                if dl_resp.status_code != 200:
                    bt.logging.debug(f"UID:{uid} - OD spot-check: download failed ({dl_resp.status_code})")
                    return

                miner_upload = OnDemandMinerUpload.model_validate(dl_resp.json())

            if not miner_upload.data_entities:
                # Before penalizing, check if data actually exists for this query.
                # If the job asks for something that doesn't exist, empty is correct.
                ctx = OnDemandValidator.build_validation_context(job)
                data_exists = await self.on_demand_validator.check_data_exists(ctx)
                if data_exists:
                    bt.logging.info(
                        f"UID:{uid} - OD spot-check: empty submission but data exists for job {result.job_id}"
                    )
                    self.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
                else:
                    bt.logging.info(
                        f"UID:{uid} - OD spot-check: empty submission, data doesn't exist — no penalty for job {result.job_id}"
                    )
                return

            # Cap entities to job limit — don't reward more than requested
            entities = miner_upload.data_entities
            if job.limit and len(entities) > job.limit:
                bt.logging.info(
                    f"UID:{uid} - OD spot-check: {hotkey[:16]} returned {len(entities)} "
                    f"but limit is {job.limit} — capping"
                )
                entities = entities[:job.limit]

            # Recalculate actual volume multiplier from real row count.
            # The poller trusted volume_mult=1.0; if actual count is lower,
            # apply a corrective penalty proportional to the shortfall.
            actual_count = len(entities)
            if job.limit and actual_count < job.limit:
                actual_volume = actual_count / job.limit
                shortfall = 1.0 - actual_volume  # e.g. 0.4 if they returned 60%
                if shortfall > 0.2:
                    # Significant shortfall — penalize proportionally
                    self.scorer.apply_ondemand_penalty(uid=uid, mult_factor=shortfall)
                    bt.logging.info(
                        f"UID:{uid} - OD spot-check: volume shortfall {actual_count}/{job.limit} "
                        f"({shortfall:.0%}), penalty applied"
                    )

            ctx = OnDemandValidator.build_validation_context(job)

            # Validate one random entity
            entity = random.choice(entities)
            post_id = self.on_demand_validator._get_post_id(entity)
            is_valid = await self.on_demand_validator._validate_entity(ctx, entity, post_id, uid)

            if not is_valid:
                bt.logging.warning(
                    f"UID:{uid} - HOTKEY:{hotkey}: OD spot-check FAILED for job {result.job_id}, post {post_id}"
                )
                self.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)
            else:
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: OD spot-check passed for job {result.job_id}"
                )

        except Exception as e:
            bt.logging.debug(f"UID:{uid} - OD spot-check error (non-fatal): {e}")

    async def eval_miner(self, uid: int) -> None:
        """Evaluates a miner and updates their score.

        Specifically:
            1. Gets the latest index from the miner
            2. Chooses a random data entity bucket to query
            3. Performs basic validation on the data entity bucket (right labels, matching size, etc.)
            4. Samples data from the data entity bucket and verifies the data is correct
            5. Passes the validation result to the scorer to update the miner's score.
        """
        t_start = time.perf_counter()

        axon_info = None
        hotkey = None
        with self.lock:
            axon_info = self.metagraph.axons[uid]
            hotkey = self.metagraph.hotkeys[uid]

        bt.logging.info(f"UID:{uid} - HOTKEY:{hotkey}: Evaluating miner.")

        # Apply any cached OD results before the main P2P/S3 evaluation
        await self._evaluate_od(uid, hotkey)

        # Query the miner for the latest index.
        index = await self._update_and_get_miner_index(hotkey, uid, axon_info)
        if not index:
            # The miner hasn't provided an index yet, so we can't validate them. Count as a failed validation.
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Failed to get an index for miner. Counting as a failed validation."
            )
            self.scorer.on_miner_evaluated(
                uid,
                None,
                [
                    ValidationResult(
                        is_valid=False,
                        reason="No available miner index.",
                        content_size_bytes_validated=0,  # Since there is just one failed result size doesn't matter.
                    )
                ]
            )

            metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='unavailable miner index').observe(time.perf_counter() - t_start)
            return

        current_block = int(self.metagraph.block)
        s3_validation_info = self.s3_storage.get_validation_info(hotkey)
        s3_validation_result = None

        if not s3_validation_info or (current_block - s3_validation_info['block']) > 600:  # ~2 hrs
            s3_validation_result = await self._perform_s3_validation(uid, hotkey, current_block)

        # From that index, find a data entity bucket to sample and get it from the miner.
        chosen_data_entity_bucket: DataEntityBucket = (
            vali_utils.choose_data_entity_bucket_to_query(index)
        )
        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: Querying miner for Bucket ID: {chosen_data_entity_bucket.id}."
        )

        responses = None
        async with bt.dendrite(wallet=self.wallet) as dendrite:
            responses = await dendrite.forward(
                axons=[axon_info],
                synapse=GetDataEntityBucket(
                    data_entity_bucket_id=chosen_data_entity_bucket.id,
                    version=constants.PROTOCOL_VERSION,
                ),
                timeout=140,
            )
        
        data_entity_bucket = vali_utils.get_single_successful_response(
            responses, GetDataEntityBucket
        )

        # Treat a failed response the same way we treat a failed validation.
        # If we didn't, the miner could just not respond to queries for data entity buckets it doesn't have.
        if data_entity_bucket is None:
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Miner returned an invalid/failed response for Bucket ID: {chosen_data_entity_bucket.id}."
            )
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason="Response failed or is invalid.",
                        content_size_bytes_validated=0,  # Since there is just one failed result size doesn't matter.
                    )
                ]
            )

            metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='invalid response').observe(time.perf_counter() - t_start)
            return

        # Perform basic validation on the entities.
        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: Performing basic validation on Bucket ID: {chosen_data_entity_bucket.id} containing "
            f"{chosen_data_entity_bucket.size_bytes} bytes across {len(data_entity_bucket.data_entities)} entities."
        )

        data_entities: List[DataEntity] = data_entity_bucket.data_entities
        (valid, reason) = vali_utils.are_entities_valid(
            data_entities, chosen_data_entity_bucket
        )
        if not valid:
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Failed basic entity validation on Bucket ID: {chosen_data_entity_bucket.id} with reason: {reason}"
            )
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason=reason,
                        content_size_bytes_validated=0,  # Since there is just one failed result size doesn't matter.
                    )
                ]
            )

            metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='invalid data entity bucket').observe(time.perf_counter() - t_start)
            return

        # Perform uniqueness validation on the entity contents.
        # If we didn't, the miner could just return the same data over and over again.
        unique = vali_utils.are_entities_unique(data_entities)
        if not unique:
            bt.logging.info(
                f"UID:{uid} - HOTKEY:{hotkey}: Failed enitity uniqueness checks on Bucket ID: {chosen_data_entity_bucket.id}."
            )
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason="Duplicate entities found.",
                        content_size_bytes_validated=0,  # Since there is just one failed result size doesn't matter.
                    )
                ]
            )

            metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='duplicate entities').observe(time.perf_counter() - t_start)
            return

        # Basic validation and uniqueness passed. Now sample some entities for data correctness.
        entities_to_validate: List[DataEntity] = vali_utils.choose_entities_to_verify(
            data_entities
        )

        entity_uris = [entity.uri for entity in entities_to_validate]

        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: Basic validation on Bucket ID: {chosen_data_entity_bucket.id} passed. Validating uris: {entity_uris}."
        )

        scraper = self.scraper_provider.get(
            MinerEvaluator.PREFERRED_SCRAPERS[chosen_data_entity_bucket.id.source]
        )
        validation_results = await scraper.validate(entities_to_validate)

        bt.logging.success(
            f"UID:{uid} - HOTKEY:{hotkey}: Data validation on selected entities finished with results: {validation_results}"
        )

        self.scorer.on_miner_evaluated(uid, index, validation_results)

        # Force garbage collection to free miner index objects (can be 350K+ buckets per miner)
        del index
        gc.collect()

        metrics.MINER_EVALUATOR_EVAL_MINER_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address, miner_hotkey=hotkey, status='ok').observe(time.perf_counter() - t_start)

        if s3_validation_result:
            # Log validation result
            if s3_validation_result.is_valid:
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: Miner {uid} passed S3 validation. "
                    f"Validation: {s3_validation_result.validation_percentage:.1f}%, "
                    f"Jobs: {s3_validation_result.total_active_jobs}, Files: {s3_validation_result.recent_files_count}, "
                    f"Coverage: {s3_validation_result.job_coverage_rate:.1f}%, "
                    f"Effective size: {s3_validation_result.effective_size_bytes/(1024*1024):.1f}MB, "
                    f"Job match: {s3_validation_result.job_match_rate:.1f}%"
                )
            else:
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: Miner {uid} did not pass S3 validation. "
                    f"Reason: {s3_validation_result.reason}"
                )

            # Update scorer with competition-based effective_size
            self.scorer.update_s3_effective_size(
                uid=uid,
                effective_size=s3_validation_result.effective_size_bytes,
                validation_passed=s3_validation_result.is_valid,
            )

    async def _perform_s3_validation(
        self, uid: int, hotkey: str, current_block: int
    ) -> Optional[S3ValidationResult]:
        """
        Performs S3 validation using DuckDB-based sampled validation.

        Returns:
            An S3ValidationResult with validation details or None if no S3 data is found.
        """
        bt.logging.info(f"UID:{uid} - HOTKEY:{hotkey}: Starting comprehensive S3 validation")

        try:
            # Use S3 auth URL from config
            s3_auth_url = self.config.s3_auth_url
            
            s3_validation_result = await validate_s3_miner_data(
                self.wallet, s3_auth_url, hotkey,
                config=self.config, s3_reader=self.s3_reader
            )
            
            # Log results with rich table
            summary = get_s3_validation_summary(s3_validation_result)
            bt.logging.info(f"{hotkey}: {summary}")

            # Display rich table with detailed metrics
            try:
                log_s3_validation_table(
                    result=s3_validation_result,
                    uid=uid,
                    hotkey=hotkey,
                    pagination_stats=None  # Could add pagination stats if available
                )
            except Exception as e:
                bt.logging.debug(f"Error displaying S3 validation table: {e}")

            if not s3_validation_result.is_valid and s3_validation_result.validation_issues:
                bt.logging.debug(f"{hotkey}: S3 validation issues: {', '.join(s3_validation_result.validation_issues[:3])}")

        except Exception as e:
            bt.logging.error(f"{hotkey}: Error in S3 validation: {str(e)}")
            s3_validation_result = S3ValidationResult(
                is_valid=False,
                validation_percentage=0.0,
                total_active_jobs=0,
                expected_jobs_count=0,
                recent_jobs_analyzed=0,
                recent_files_count=0,
                total_size_bytes=0,
                has_duplicates=False,
                duplicate_percentage=0.0,
                entities_validated=0,
                entities_passed_scraper=0,
                scraper_success_rate=0.0,
                entities_checked_for_job_match=0,
                entities_matched_job=0,
                job_match_rate=0.0,
                validation_issues=[f"Validation error: {str(e)}"],
                reason=f"S3 validation failed: {str(e)}",
                sample_validation_results=[],
                sample_job_mismatches=[]
            )

        # Update S3 validation storage
        if s3_validation_result:
            self.s3_storage.update_validation_info(hotkey, s3_validation_result.total_active_jobs, current_block)

        return s3_validation_result

    async def run_next_eval_batch(self) -> int:
        """Asynchronously runs the next batch of miner evaluations and returns the number of seconds to wait until the next batch.

        Args:
            block (int): The block at which we started this evaluation.
        """

        # Grab a snapshot of the metagraph
        metagraph = None
        with self.lock:
            metagraph = copy.deepcopy(self.metagraph)

        # Check if the next miner is due an update.
        next_uid = self.miner_iterator.peek()
        hotkey = metagraph.hotkeys[next_uid]
        last_evaluated = self.storage.read_miner_last_updated(hotkey)
        now = dt.datetime.utcnow()
        due_update = (
            last_evaluated is None
            or (now - last_evaluated) >= constants.MIN_EVALUATION_PERIOD
        )

        # If the next miner is not due an update, then all subsequent miners are also not due an update.
        # So we wait until this miner is due an update.
        if not due_update:
            return (
                last_evaluated + constants.MIN_EVALUATION_PERIOD - now
            ).total_seconds()

        t_start = time.perf_counter()
        # Run in batches of 5 (reduced from 15 to lower memory usage).
        miners_to_eval = 5

        # Otherwise, execute the next batch of evaluations.
        # Use a set in case the network has fewer than 5 miners.
        uids_to_eval = {next(self.miner_iterator) for _ in range(miners_to_eval)}

        bt.logging.info(
            f"Running validation on the following batch of uids: {uids_to_eval}."
        )
        threads = [
            threading.Thread(target=self.eval_miner_sync, args=(uid,))
            for uid in uids_to_eval
        ]
        for thread in threads:
            thread.start()

        bt.logging.trace(f"Waiting for {len(threads)} miner evals to finish.")
        end = datetime.datetime.now() + datetime.timedelta(seconds=300)
        for t in threads:
            # Compute the timeout, so that all threads are waited for a total of 5 minutes.
            timeout = max(0, (end - datetime.datetime.now()).total_seconds())
            t.join(timeout=timeout)

        duration = time.perf_counter() - t_start
        metrics.MINER_EVALUATOR_EVAL_BATCH_DURATION.labels(hotkey=self.wallet.hotkey.ss58_address).observe(duration)

        bt.logging.trace(f"Finished waiting for {len(threads)} miner eval.")

        # Run the next evaluation batch immediately.
        return 0

    def save_state(self):
        """Saves the state of the validator to a file."""
        bt.logging.trace("Saving evaluator state.")

        if not os.path.exists(self.config.neuron.full_path):
            os.makedirs(self.config.neuron.full_path)

        # Save the state of the validator to file.
        self.scorer.save_state(
            os.path.join(self.config.neuron.full_path, MinerEvaluator.SCORER_FILENAME)
        )

    def load_state(self):
        """Loads the state of the validator from a file."""
        bt.logging.info("Loading evaluator state.")

        with self.lock:
            # Load the state of the validator from file.
            filepath = os.path.join(
                self.config.neuron.full_path, MinerEvaluator.SCORER_FILENAME
            )
            if not os.path.exists(filepath):
                bt.logging.warning("No scorer state file found. Starting from scratch.")
                return

            try:
                self.scorer.load_state(filepath)
                bt.logging.success(f"Loaded scorer state from: {filepath}.")
            except Exception as e:
                bt.logging.warning(
                    f"Failed to load scorer state. Reason: {e}. Starting from scratch."
                )

            # Resize the scorer in case the loaded state is old and missing newly added neurons.
            self.scorer.resize(len(self.metagraph.hotkeys))

    async def _update_and_get_miner_index(
        self, hotkey: str, uid: int, miner_axon: bt.AxonInfo
    ) -> Optional[ScorableMinerIndex]:
        """Updates the index for the specified miner, and returns the latest known index or None if the miner hasn't yet provided an index."""

        bt.logging.info(f"UID:{uid} - HOTKEY:{hotkey}: Getting MinerIndex from miner.")

        try:
            responses: List[GetMinerIndex] = None
            async with bt.dendrite(wallet=self.wallet) as dendrite:
                responses = await dendrite.forward(
                    axons=[miner_axon],
                    synapse=GetMinerIndex(version=constants.PROTOCOL_VERSION),
                    timeout=120,
                )

            response = vali_utils.get_single_successful_response(
                responses, GetMinerIndex
            )
            if not response:
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: Miner failed to respond with an index. Using last known index if present."
                )
                # Miner failed to update the index. Use the latest index, if present.
                return self.storage.read_miner_index(hotkey)

            # Validate the index.
            miner_index = None
            try:
                miner_index = vali_utils.get_miner_index_from_response(response)
            except ValueError as e:
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: Miner returned an invalid index. Reason: {e}. Using last known index if present."
                )
                # Miner returned an invalid index. Use the latest index, if present.
                return self.storage.read_miner_index(hotkey)

            assert miner_index is not None, "Miner index should not be None."

            # Miner replied with a valid index. Store it and return it.
            miner_credibility = self.scorer.get_miner_credibility(uid)
            bt.logging.success(
                f"UID:{uid} - HOTKEY:{hotkey}: Got new compressed miner index of {CompressedMinerIndex.size_bytes(miner_index)} bytes "
                f"across {CompressedMinerIndex.bucket_count(miner_index)} buckets."
            )
            self.storage.upsert_compressed_miner_index(
                miner_index, hotkey, miner_credibility
            )

            return self.storage.read_miner_index(hotkey)
        except Exception:
            bt.logging.error(
                f"UID:{uid} - HOTKEY:{hotkey}: Failed to update and get miner index.\n{traceback.format_exc()}"
            )
            return None

    def _on_metagraph_updated(self, metagraph: bt.metagraph, netuid: int):
        """Handles an update to a metagraph."""
        bt.logging.info(
            f"Evaluator processing an update to metagraph on subnet {netuid}."
        )

        with self.lock:
            bt.logging.info(
                "Evaluator: Metagraph updated, re-syncing hotkeys, and moving averages."
            )
            # Zero out all hotkeys that have been replaced.
            old_hotkeys = self.metagraph.hotkeys
            for uid, hotkey in enumerate(old_hotkeys):
                if hotkey != metagraph.hotkeys[uid] or (
                    not utils.is_miner(uid, metagraph, self.vpermit_rao_limit)
                    and not utils.is_validator(uid, metagraph, self.vpermit_rao_limit)
                ):
                    bt.logging.info(
                        f"Hotkey {hotkey} w/ UID {uid} has been unregistered or does not qualify to mine/validate."
                    )
                    self.scorer.reset(uid)  # hotkey has been replaced
                    try:
                        self.storage.delete_miner(hotkey)
                    except Exception:
                        bt.logging.error(
                            f"{hotkey} Failed to delete miner index.",
                            traceback.format_exc(),
                        )
            # Update the iterator. It will keep its current position if possible.
            self.miner_iterator.set_miner_uids(
                #utils.get_miner_uids(self.metagraph, self.vpermit_rao_limit) # uses cached/stale self.metagraph --> iterator may miss new miners and keep removed ones.
                utils.get_miner_uids(metagraph, self.vpermit_rao_limit) # use fresh metagraph --> iterator gets latest eligible UIDs immediately
            )

            # Check to see if the metagraph has changed size.
            # If so, we need to add new hotkeys and moving averages.
            if len(self.metagraph.hotkeys) < len(metagraph.hotkeys):
                self.scorer.resize(len(metagraph.hotkeys))

            self.metagraph = copy.deepcopy(metagraph)

    def exit(self):
        self.should_exit = True
