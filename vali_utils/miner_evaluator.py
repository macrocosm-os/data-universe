import os
import gc
import copy
import json
import math
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

from typing import Dict, List, Optional, Tuple
from vali_utils.validator_s3_access import ValidatorS3Access
from vali_utils.s3_utils import validate_s3_miner_data, get_s3_validation_summary, S3ValidationResult, normalize_url_for_dedup
from vali_utils.s3_logging_utils import log_s3_validation_table

import httpx

from common.api_client import (
    DataUniverseApiClient,
    ListMinerJobsForValidationRequest,
    OnDemandJob,
    OnDemandJobSubmission,
    OnDemandMinerUpload,
)
from rewards.miner_scorer import MinerScorer
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
        # OD validator — set by validator.py after construction.
        # Used for inline OD evaluation during eval_miner().
        self.on_demand_validator: Optional[OnDemandValidator] = None
        # Track last OD eval time per miner to query only new jobs each cycle.
        # Not persisted — on restart defaults to a 2h lookback window.
        self._last_od_eval_at: Dict[str, dt.datetime] = {}
        # Cache API client construction params (derived once, reused every eval).
        self._api_base_url = self.config.s3_auth_url
        self._api_verify_ssl = "localhost" not in self._api_base_url

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.lock = threading.RLock()
        self.is_setup = False

    def _on_demand_client(self) -> DataUniverseApiClient:
        return DataUniverseApiClient(
            base_url=self._api_base_url,
            verify_ssl=self._api_verify_ssl,
            keypair=self.wallet.hotkey,
            timeout=60,
        )

    def get_scorer(self) -> MinerScorer:
        """Returns the scorer used by the evaluator."""
        return self.scorer

    def eval_miner_sync(self, uid: int) -> None:
        """Synchronous version of eval_miner."""
        asyncio.run(self.eval_miner(uid))

    # Maximum OD jobs to validate per miner per eval cycle.
    # Each validation downloads ~1MB + 1 scraper API call, so keep this bounded.
    OD_MAX_JOBS_TO_VALIDATE = 3
    # Number of entities to schema-check per downloaded submission.
    OD_SCHEMA_SAMPLE_SIZE = 5
    # Upper bound on scraper (Apify) checks per submission. Phase 3 scrapes
    # ~10% of the returned volume, but each check is a paid API call, so cap it.
    # 8 checks over a 5-entity-real submission already gives strong coverage;
    # over a 600-entity submission it forces the cheater to make ≥8 of the
    # sampled entities real, which compounds with the schema/job-match checks.
    OD_MAX_SCRAPER_CHECKS = 8
    # Hard cap on a single OD submission download (mirrors the API client's
    # FIFTEEN_MB_BYTES). The OD payload is miner-controlled S3 content; without a
    # cap a multi-GB upload OOM-kills the validator. (SECURITY_FINDINGS C1)
    OD_MAX_DOWNLOAD_BYTES = 15 * 1_000_000
    # Minimum fraction of a submission's URIs that must be unique. The unique
    # ratio across live submissions is sharply bimodal: honest miners cluster at
    # ~100% unique, padded submissions at a low fixed ratio — almost nothing in
    # between. 0.5 sits in that empty gap, so it fails padding without touching
    # honest miners (even those with some duplicate-URL posts). The volume reward
    # is always computed on the UNIQUE count regardless. (Appendix C / H-1)
    OD_MIN_UNIQUE_RATIO = 0.5

    async def _evaluate_od(self, uid: int, hotkey: str) -> None:
        """Evaluate a miner's on-demand submissions by querying the API directly.

        Calls the per-miner jobs endpoint to get this miner's recent OD
        submissions with fresh presigned URLs, then validates a random
        sample and applies per-job rewards/penalties.
        """
        if self.on_demand_validator is None:
            return

        # Determine time window: since last eval, or 2h on first run
        now = dt.datetime.now(dt.timezone.utc)
        expired_since = self._last_od_eval_at.get(
            hotkey, now - dt.timedelta(hours=2)
        )

        try:
            async with self._on_demand_client() as client:
                resp = await client.validator_list_miner_jobs(
                    ListMinerJobsForValidationRequest(
                        miner_hotkey=hotkey,
                        expired_since=expired_since,
                        expired_until=now,
                        limit=500,
                    )
                )
        except Exception as e:
            bt.logging.warning(f"UID:{uid} - HOTKEY:{hotkey}: OD API fetch failed: {e}")
            return

        self._last_od_eval_at[hotkey] = now

        jobs = resp.jobs
        if not jobs:
            return

        # Single-pass partition into empty vs non-empty submissions
        empty, non_empty = [], []
        for j in jobs:
            if (j.submission.s3_content_length or 0) > 0:
                non_empty.append(j)
            else:
                empty.append(j)

        for j in empty:
            self.scorer.apply_ondemand_penalty(uid=uid, mult_factor=1.0)

        if not non_empty:
            if empty:
                bt.logging.info(
                    f"UID:{uid} - HOTKEY:{hotkey}: OD — {len(empty)} empty submissions penalized, "
                    f"0 non-empty"
                )
            return

        # Sample up to OD_MAX_JOBS_TO_VALIDATE for deep validation
        to_validate = random.sample(
            non_empty, min(self.OD_MAX_JOBS_TO_VALIDATE, len(non_empty))
        )
        not_sampled_count = len(non_empty) - len(to_validate)

        # Validate sampled jobs concurrently
        validation_results: List[Tuple[Optional[bool], int]] = await asyncio.gather(*[
            self._validate_od_submission(
                uid, hotkey, j.job, j.submission, j.submission.job_id
            )
            for j in to_validate
        ])

        # Apply per-job rewards/penalties based on validation results
        validated_pass = 0
        validated_fail = 0
        validated_skipped = 0

        for j, (passed, entity_count) in zip(to_validate, validation_results):
            if passed is None:
                # Validator-side download failure (5xx/timeout) — neither reward nor
                # penalize; the miner is not at fault for our infrastructure. (companion to #805)
                validated_skipped += 1
                bt.logging.warning(
                    f"UID:{uid} - OD: job {j.submission.job_id} skipped — validator-side "
                    f"download failure, no reward/penalty"
                )
                continue

            speed_mult, vol_mult = (
                self.on_demand_validator.calculate_ondemand_reward_multipliers(
                    job_created_at=j.job.created_at,
                    submission_timestamp=j.submission.submitted_at,
                    returned_count=entity_count,
                    requested_limit=j.job.limit,
                )
            )

            if passed:
                self.scorer.apply_ondemand_reward(uid, speed_mult, vol_mult)
                validated_pass += 1
            else:
                self.scorer.apply_ondemand_penalty(uid, mult_factor=1.0)
                validated_fail += 1

        # Batch credibility bump for non-sampled but participating submissions
        if not_sampled_count > 0:
            self.scorer.apply_ondemand_credibility_bump(uid, count=not_sampled_count)

        bt.logging.info(
            f"UID:{uid} - HOTKEY:{hotkey}: OD summary — "
            f"{len(non_empty)} non-empty (validated: {validated_pass} pass, {validated_fail} fail, "
            f"{not_sampled_count} credibility-bumped), {len(empty)} empty penalized"
        )

    async def _validate_od_submission(
        self,
        uid: int,
        hotkey: str,
        job: OnDemandJob,
        submission: OnDemandJobSubmission,
        job_id: str,
    ) -> Tuple[Optional[bool], int]:
        """Download and validate a single OD submission.

        Returns (passed, entity_count) — entity_count is the number of
        entities the miner actually returned (used for volume_multiplier).
        passed=None signals a validator-side download failure (5xx/timeout):
        the caller leaves credibility unchanged (neither reward nor penalty). (#805)
        """
        try:
            # Reject up front if the API-reported size already exceeds the cap,
            # so we never even start a multi-GB download. (SECURITY_FINDINGS C1)
            if (submission.s3_content_length or 0) > self.OD_MAX_DOWNLOAD_BYTES:
                bt.logging.warning(
                    f"UID:{uid} - OD validate: submission {job_id} too large "
                    f"({submission.s3_content_length} bytes > {self.OD_MAX_DOWNLOAD_BYTES}) — failing"
                )
                return False, 0

            async with httpx.AsyncClient(timeout=30.0) as http:
                async with http.stream(
                    "GET", submission.s3_presigned_url, follow_redirects=True
                ) as dl_resp:
                    if dl_resp.status_code != 200:
                        bt.logging.warning(
                            f"UID:{uid} - OD validate: download failed ({dl_resp.status_code}) "
                            f"for job {job_id}"
                        )
                        if dl_resp.status_code >= 500:
                            # Validator/AWS server-side error — not the miner's fault.
                            return None, 0
                        return False, 0

                    # Stream with a hard byte cap — the payload is miner-controlled
                    # S3 content; an unbounded read would OOM the validator.
                    buf = bytearray()
                    async for chunk in dl_resp.aiter_bytes():
                        buf.extend(chunk)
                        if len(buf) > self.OD_MAX_DOWNLOAD_BYTES:
                            bt.logging.warning(
                                f"UID:{uid} - OD validate: submission {job_id} exceeded "
                                f"{self.OD_MAX_DOWNLOAD_BYTES} bytes mid-download — failing"
                            )
                            return False, 0

                miner_upload = OnDemandMinerUpload.model_validate(json.loads(bytes(buf)))

            entities = miner_upload.data_entities

            if not entities:
                ctx = OnDemandValidator.build_validation_context(job)
                data_exists = await self.on_demand_validator.check_data_exists(ctx)
                if data_exists:
                    bt.logging.info(
                        f"UID:{uid} - OD validate: empty submission but data exists "
                        f"for job {job_id}"
                    )
                    return False, 0
                else:
                    bt.logging.info(
                        f"UID:{uid} - OD validate: empty submission, data doesn't exist "
                        f"for job {job_id} — acceptable"
                    )
                    return True, 0

            entity_count = len(entities)

            # Uniqueness gate FIRST, on the full returned set — before any volume
            # cap — so the padding signal is measured on everything the miner sent.
            # Capping to job.limit first would let a cheater hide padding outside
            # the truncation window (e.g. 100 real ordered ahead of 500 dupes).
            # Fabricators pad a few real posts up to the cap with duplicates
            # (live: 600 returned / 100 unique). (SECURITY_FINDINGS Appendix C / H-1)
            seen_uris = set()
            unique_entities = []
            for e in entities:
                uri = getattr(e, "uri", None) or ""
                key = normalize_url_for_dedup(uri) if uri else self.on_demand_validator._get_post_id(e)
                if key in seen_uris:
                    continue
                seen_uris.add(key)
                unique_entities.append(e)

            if entity_count > 0 and (len(unique_entities) / entity_count) < self.OD_MIN_UNIQUE_RATIO:
                bt.logging.warning(
                    f"UID:{uid} - OD validate: PADDING FAILED for job {job_id} — "
                    f"{len(unique_entities)} unique / {entity_count} returned "
                    f"(< {self.OD_MIN_UNIQUE_RATIO:.0%} unique)"
                )
                return False, len(unique_entities)

            # Now cap the de-duplicated set to the job limit. Volume is paid on
            # this unique, capped count — never the claimed count.
            entities = unique_entities
            if job.limit and len(entities) > job.limit:
                entities = entities[:job.limit]
            unique_count = len(entities)

            # Phase 1: Schema validation on a sample
            sample_size = min(self.OD_SCHEMA_SAMPLE_SIZE, len(entities))
            schema_sample = random.sample(entities, sample_size)

            ctx = OnDemandValidator.build_validation_context(job)
            if not self.on_demand_validator._validate_miner_data_format(
                ctx, schema_sample, uid
            ):
                bt.logging.warning(
                    f"UID:{uid} - OD validate: SCHEMA FAILED for job {job_id} "
                    f"(wrong XContent format)"
                )
                return False, unique_count

            # Phase 2: Job match — check request fields on a sample
            for entity in schema_sample:
                post_id = self.on_demand_validator._get_post_id(entity)
                if not self.on_demand_validator._validate_request_fields(ctx, entity, uid):
                    bt.logging.warning(
                        f"UID:{uid} - OD validate: JOB MATCH FAILED for job {job_id}, "
                        f"post {post_id} (wrong username/keyword/date)"
                    )
                    return False, unique_count

            # Phase 3: Scraper validation on a volume-scaled sample drawn from the
            # FULL unique/capped set (not the 5-entity schema sample). Checking one
            # entity let a fabricator hide fakes behind a single real post; checking
            # ~10% of the volume (min 3, capped at OD_MAX_SCRAPER_CHECKS for Apify
            # cost) makes the per-fake escape probability compound: to keep a
            # meaningful escape chance a cheater must keep most sampled entities
            # real. Any failure fails the whole submission.
            scraper_k = max(3, math.ceil(0.10 * len(entities)))
            scraper_k = min(scraper_k, self.OD_MAX_SCRAPER_CHECKS, len(entities))
            to_scrape = random.sample(entities, scraper_k)
            for entity in to_scrape:
                post_id = self.on_demand_validator._get_post_id(entity)
                is_valid = await self.on_demand_validator._validate_entity(
                    ctx, entity, post_id, uid
                )
                if not is_valid:
                    bt.logging.warning(
                        f"UID:{uid} - OD validate: SCRAPER FAILED for job {job_id}, "
                        f"post {post_id}"
                    )
                    return False, unique_count

            bt.logging.info(
                f"UID:{uid} - OD validate: PASSED job {job_id} "
                f"({unique_count} unique / {entity_count} returned, "
                f"schema OK, job match OK, scraper OK on {len(to_scrape)})"
            )
            return True, unique_count

        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
            # Validator-side network failure downloading the submission — neutral, not miner's fault (#805)
            bt.logging.warning(
                f"UID:{uid} - OD validate: network error for job {job_id}: {e}"
            )
            return None, 0
        except Exception as e:
            bt.logging.warning(
                f"UID:{uid} - OD validate: error for job {job_id}: {e}"
            )
            return False, 0

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
                    self._last_od_eval_at.pop(hotkey, None)
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
