import copy
import datetime
import traceback
import asyncio
import threading
import os
import random
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
    HuggingFaceMetadata,
)
from common.protocol import GetDataEntityBucket, GetMinerIndex, GetHuggingFaceMetadata, DecodeURLRequest
from rewards.data_value_calculator import DataValueCalculator
from scraping.provider import ScraperProvider
from scraping.scraper import ScraperId, ValidationResult, HFValidationResult, S3ValidationResult
from storage.validator.sqlite_memory_validator_storage import (
    SqliteMemoryValidatorStorage,
)

from storage.validator.hf_validator_storage import HFValidationStorage
from storage.validator.s3_validator_storage import S3ValidationStorage

from vali_utils.miner_iterator import MinerIterator
from vali_utils import utils as vali_utils

from typing import List, Optional, Tuple, Dict, Any
from vali_utils.validator_s3_access import ValidatorS3Access
import pandas as pd
import io
from vali_utils.hf_utils import (
    get_latest_commit_files,
    get_validation_data,
    decode_dataframe,
    validate_hf_content,
    compare_latest_commits_parquet_files
)


from rewards.miner_scorer import MinerScorer
from scraping.provider import ScraperProvider


class MinerEvaluator:
    """MinerEvaluator is responsible for evaluating miners and updating their scores."""

    SCORER_FILENAME = "scorer.pickle"

    # Mapping of scrapers to use based on the data source to validate.
    PREFERRED_SCRAPERS = {
        DataSource.X: ScraperId.X_APIDOJO,
        DataSource.REDDIT: ScraperId.REDDIT_CUSTOM,
        DataSource.YOUTUBE: ScraperId.YOUTUBE_APIFY_TRANSCRIPT
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
            utils.get_miner_uids(self.metagraph, self.uid, self.vpermit_rao_limit)
        )
        self.scraper_provider = ScraperProvider()
        self.storage = SqliteMemoryValidatorStorage()
        self.hf_storage = HFValidationStorage(self.config.hf_results_path)
        self.s3_storage = S3ValidationStorage(self.config.s3_results_path)
        self.s3_reader = s3_reader
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

    async def eval_miner(self, uid: int) -> None:
        """Evaluates a miner and updates their score.

        Specifically:
            1. Gets the latest index from the miner
            2. Chooses a random data entity bucket to query
            3. Performs basic validation on the data entity bucket (right labels, matching size, etc.)
            4. Samples data from the data entity bucket and verifies the data is correct
            5. Passes the validation result to the scorer to update the miner's score.
        """

        axon_info = None
        hotkey = None
        with self.lock:
            axon_info = self.metagraph.axons[uid]
            hotkey = self.metagraph.hotkeys[uid]

        bt.logging.info(f"{hotkey}: Evaluating miner.")

        # Query the miner for the latest index.
        index = await self._update_and_get_miner_index(hotkey, uid, axon_info)
        if not index:
            # The miner hasn't provided an index yet, so we can't validate them. Count as a failed validation.
            bt.logging.info(
                f"{hotkey}: Failed to get an index for miner. Counting as a failed validation."
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
                ],
            )
            return

        ##########
        # Query HuggingFace metadata and perform enhanced HF validation.
        current_block = int(self.metagraph.block)
        validation_info = self.hf_storage.get_validation_info(hotkey)
        hf_validation_result = None
        if validation_info is None or (current_block - validation_info['block']) > 5100:  # ~17 hrs
            hf_validation_result = await self._perform_hf_validation(hotkey, uid, axon_info, current_block)
        
        # Perform S3 validation (only if enabled by date)
        s3_validation_info = self.s3_storage.get_validation_info(hotkey)
        current_time = dt.datetime.now(tz=dt.timezone.utc)
        if current_time >= constants.S3_VALIDATION_ENABLED_DATE:
            if s3_validation_info is None or (current_block - s3_validation_info['block']) > 5100:  # ~17 hrs
                await self._perform_s3_validation(hotkey, uid, current_block)
        ##########

        # From that index, find a data entity bucket to sample and get it from the miner.
        chosen_data_entity_bucket: DataEntityBucket = (
            vali_utils.choose_data_entity_bucket_to_query(index)
        )
        bt.logging.info(
            f"{hotkey} Querying miner for Bucket ID: {chosen_data_entity_bucket.id}."
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
                f"{hotkey}: Miner returned an invalid/failed response for Bucket ID: {chosen_data_entity_bucket.id}."
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
                ],
            )
            return

        # Perform basic validation on the entities.
        bt.logging.info(
            f"{hotkey}: Performing basic validation on Bucket ID: {chosen_data_entity_bucket.id} containing "
            + f"{chosen_data_entity_bucket.size_bytes} bytes across {len(data_entity_bucket.data_entities)} entities."
        )

        data_entities: List[DataEntity] = data_entity_bucket.data_entities
        (valid, reason) = vali_utils.are_entities_valid(
            data_entities, chosen_data_entity_bucket
        )
        if not valid:
            bt.logging.info(
                f"{hotkey}: Failed basic entity validation on Bucket ID: {chosen_data_entity_bucket.id} with reason: {reason}"
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
                ],
            )
            return

        # Perform uniqueness validation on the entity contents.
        # If we didn't, the miner could just return the same data over and over again.
        unique = vali_utils.are_entities_unique(data_entities)
        if not unique:
            bt.logging.info(
                f"{hotkey}: Failed enitity uniqueness checks on Bucket ID: {chosen_data_entity_bucket.id}."
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
                ],
            )
            return

        # Basic validation and uniqueness passed. Now sample some entities for data correctness.
        entities_to_validate: List[DataEntity] = vali_utils.choose_entities_to_verify(
            data_entities
        )

        entity_uris = [entity.uri for entity in entities_to_validate]

        bt.logging.info(
            f"{hotkey}: Basic validation on Bucket ID: {chosen_data_entity_bucket.id} passed. Validating uris: {entity_uris}."
        )

        scraper = self.scraper_provider.get(
            MinerEvaluator.PREFERRED_SCRAPERS[chosen_data_entity_bucket.id.source]
        )
        validation_results = await scraper.validate(entities_to_validate)

        bt.logging.success(
            f"{hotkey}: Data validation on selected entities finished with results: {validation_results}"
        )

        self.scorer.on_miner_evaluated(uid, index, validation_results)

        if hf_validation_result:
            if hf_validation_result.is_valid == True:
                bt.logging.info(f"{hotkey}: Miner {uid} passed HF validation. HF Validation Percentage: {hf_validation_result.validation_percentage}")
            else:
                bt.logging.info(f"{hotkey}: Miner {uid} did not pass HF validation, no bonus awarded. Reason: {hf_validation_result.reason}")

            self.scorer.update_hf_boost_and_cred(uid, hf_validation_result.validation_percentage)


    async def _perform_hf_validation(
            self, hotkey: str, uid: int, axon_info: bt.AxonInfo, current_block: int
    ) -> Optional[HFValidationResult]:
        """
        Performs HuggingFace validation for a miner using metadata from the miner.
        Enhanced logic:
          - If the two latest commits for the repo are identical, return an invalid result.
          - If the latest commit is greater than 19 hours old, return an invalid result.
          - Otherwise, proceed with URL decoding and content validation.

        Returns:
            An HFValidationResult with validation details or None if no metadata is provided.
        """
        hf_validation_result = None
        hf_metadatas = await self._query_huggingface_metadata(hotkey, uid, axon_info)
        if hf_metadatas:
            for hf_metadata in hf_metadatas:
                bt.logging.info(f"{hotkey}: Trying to validate {hf_metadata.repo_name}")

                # Get parquet files and commit date from the latest commit.
                new_parquet_files, commit_date = get_latest_commit_files(hf_metadata.repo_name)
                if not new_parquet_files:
                    bt.logging.warning(f"No new parquet files found for {hf_metadata.repo_name}")
                    continue

                # Temporarily remove parquet check until SN stabilizes

                # Check if the two latest commits are identical.
                #same_commits, _, _ = compare_latest_commits_parquet_files(hf_metadata.repo_name)
                # if same_commits:
                #     bt.logging.info(
                #         f"{hotkey}: Latest commits for {hf_metadata.repo_name} are identical. Marking HF validation as False."
                #     )
                #     hf_validation_result = HFValidationResult(
                #         is_valid=False,
                #         reason="Latest two commits are identical",
                #         validation_percentage=0.0,
                #     )
                #     self.hf_storage.update_validation_info(hotkey, str(hf_metadata.repo_name), current_block)
                #     continue

                # Check if the latest commit is greater than 19 hours old.
                if isinstance(commit_date, str):
                    try:
                        bt.logging.info(f"{hotkey}: Commit date is in string format, converting to datetime.datetime.")
                        commit_date = dt.datetime.fromisoformat(commit_date)
                    except Exception as e:
                        bt.logging.error(f"{hotkey}: Failed to parse commit date: {commit_date}. Error: {str(e)}") 
                     
                if commit_date and (dt.datetime.now(dt.timezone.utc) - commit_date) > dt.timedelta(hours=19):
                    bt.logging.info(
                        f"{hotkey}: Latest commit for {hf_metadata.repo_name} is greater than 19 hours old. Marking HF validation as False."
                    )
                    hf_validation_result = HFValidationResult(
                        is_valid=False,
                        reason="Latest commit is too old (>19 hours)",
                        validation_percentage=0.0,
                    )
                    self.hf_storage.update_validation_info(hotkey, str(hf_metadata.repo_name), current_block)
                    continue

                # Get encoded URLs and a DataFrame from the parquet files.
                encoded_urls, encoded_df = get_validation_data(hf_metadata.repo_name, new_parquet_files)
                if encoded_urls:
                    # Retrieve decoded URLs from the miner.
                    success, decoded_urls = await self._get_decoded_urls(hotkey, uid, axon_info, encoded_urls)
                    if success:
                        try:
                            if len(decoded_urls) == 0:
                                bt.logging.error(f"{hotkey}: Got empty decoded_urls list")
                                hf_validation_result = HFValidationResult(
                                    is_valid=False,
                                    reason=f"{hotkey}: Got empty decoded_urls list",
                                    validation_percentage=0.0,
                                )
                            else:
                                decoded_df = encoded_df.copy()
                                del encoded_df  # free up memory
                                # Ensure the DataFrame row count matches the decoded URLs count.
                                decoded_df = decoded_df.head(len(decoded_urls))
                                decoded_df['url'] = decoded_urls
                                hf_validation_result = await validate_hf_content(decoded_df, hf_metadata.source)
                                bt.logging.info(
                                    f"{hotkey}: HuggingFace validation result for {hf_metadata.repo_name}: {hf_validation_result}"
                                )
                        except ValueError as e:
                            bt.logging.error(f"{hotkey}: ValueError in URL decoding: {str(e)}")
                            hf_validation_result = HFValidationResult(
                                is_valid=False,
                                reason=f"{hotkey}: ValueError in URL decoding: {str(e)}",
                                validation_percentage=0.0,
                            )
                        except Exception as e:
                            bt.logging.error(f"{hotkey}: Unexpected error in URL decoding: {str(e)}")
                            hf_validation_result = HFValidationResult(
                                is_valid=False,
                                reason=f"{hotkey}: Unexpected error in URL decoding: {str(e)}",
                                validation_percentage=0.0,
                            )
                # Update the HF validation storage with the current block for this repo.
                self.hf_storage.update_validation_info(hotkey, str(hf_metadata.repo_name), current_block)
        else:
            self.hf_storage.update_validation_info(hotkey, "no_dataset_provided", current_block)
        return hf_validation_result

    async def _perform_s3_validation(
            self, hotkey: str, uid: int, current_block: int
    ) -> None:
        """
        Performs enhanced S3 validation for a miner using S3 data access.
        Validates data freshness, file structure, and content format with in-depth parquet analysis.
        
        SECURITY NOTE: All logging in this method must be public-safe as validator logs
        are publicly visible. Never log URLs, credentials, or sensitive data.
        
        Returns:
            An S3ValidationResult with validation details or None if no S3 data is found.
        """
        try:
            bt.logging.info(f"{hotkey}: Starting enhanced S3 validation")
            
            # Step 1: Job Discovery
            all_job_ids = self.scorer.data_value_calculator.model.get_all_job_ids()
            bt.logging.info(f"{hotkey}: Found {len(all_job_ids)} job IDs from dynamic desirability model")
            
            # Step 2: Miner Job Completion Analysis
            miner_job_data = await self._analyze_miner_job_completion(hotkey, all_job_ids)
            
            if not miner_job_data['completed_jobs']:
                bt.logging.info(f"{hotkey}: No completed jobs found for S3 validation")
                self.scorer.on_s3_evaluated(uid, 
                            [], 
                            S3ValidationResult(is_valid=False,
                                                content_size_bytes_validated=0,
                                                reason=f"{hotkey}: No completed jobs found for S3 validation"))
                self.s3_storage.update_validation_info(hotkey, 0, current_block)
                return
            
            # Step 3: Random Job Selection
            selected_job_data = self._select_random_job_for_validation(miner_job_data)
            
            if not selected_job_data:
                bt.logging.info(f"{hotkey}: No job selected for deep validation")
                self.scorer.on_s3_evaluated(uid, 
                                            miner_job_data['completed_jobs'], 
                                            S3ValidationResult(is_valid=False,
                                                               content_size_bytes_validated=0,
                                                               reason=f"{hotkey}: No job selected for deep validation"))
                self.s3_storage.update_validation_info(hotkey, len(miner_job_data['completed_jobs']), current_block)
                return
            
            # Step 4: Perform basic S3 validation (prerequisite)
            basic_validation_result = await self._perform_basic_s3_file_validation(hotkey, selected_job_data)
            
            if not basic_validation_result.is_valid:
                bt.logging.info(f"{hotkey}: Basic S3 validation failed for job {selected_job_data['job_id']}")
                self.scorer.on_s3_evaluated(uid, miner_job_data['completed_jobs'], basic_validation_result)
                self.s3_storage.update_validation_info(hotkey, len(miner_job_data['completed_jobs']), current_block)
                return
            
            # Step 5: Perform content validation on selected job
            validation_results = await self._deep_validate_job_parquets(hotkey, selected_job_data)
            
            # Step 6: Update S3 scoring and credibility (equivalent of on_miner_evaluated for S3)
            self.scorer.on_s3_evaluated(uid, miner_job_data['completed_jobs'], validation_results)
            
            bt.logging.info(f"{hotkey}: Enhanced S3 validation completed - {len(validation_results)} entities validated")
            
        except Exception as e:
            bt.logging.error(f"{hotkey}: Error in S3 validation: {str(e)}")
            self.scorer.on_s3_evaluated(uid, 
                                        miner_job_data['completed_jobs'] if miner_job_data['completed_jobs'] else [], 
                                        S3ValidationResult(is_valid=False,
                                                            content_size_bytes_validated=0,
                                                            reason=f"{hotkey}: Error in S3 validation: {str(e)}"))
        
        # Step 7: Update S3 validation storage for enhanced validation
        self.s3_storage.update_validation_info(hotkey, len(miner_job_data['completed_jobs']), current_block)
        

    async def _perform_basic_s3_validation(
            self, hotkey: str, uid: int, current_block: int
    ) -> S3ValidationResult:
        """
        Performs basic S3 validation (original logic) as fallback.
        """
        try:
            bt.logging.info(f"{hotkey}: Starting basic S3 validation")
            
            # Check if miner has any jobs in S3
            jobs = await self._get_miner_s3_jobs(hotkey)
            if not jobs:
                bt.logging.info(f"{hotkey}: No S3 jobs found")
                s3_validation_result = S3ValidationResult(
                    is_valid=False,
                    content_size_bytes_validated=0,
                    reason="No S3 data found"
                )
                self.s3_storage.update_validation_info(hotkey, 0, current_block)
                return s3_validation_result
            
            total_files = 0
            valid_files = 0
            total_bytes_validated = 0
            valid_bytes = 0
            
            # Validate files in each job and track actual file sizes
            for job_id in jobs[:5]:  # Limit to 5 jobs to avoid timeout
                try:
                    files = await self._get_job_files(hotkey, job_id)
                    if files:
                        for file_info in files[:10]:  # Limit to 10 files per job
                            file_size = file_info.get('Size', 0)
                            total_files += 1
                            total_bytes_validated += file_size
                            
                            if await self._validate_s3_file(hotkey, file_info):
                                valid_files += 1
                                valid_bytes += file_size
                except Exception as e:
                    bt.logging.warning(f"{hotkey}: Error validating job {job_id}: Connection error")
                    continue
            
            # Calculate validation percentage - require 100% of files to be valid
            validation_percentage = (valid_files / total_files * 100) if total_files > 0 else 0.0
            is_valid = validation_percentage == 100.0  # 100% of files must be valid
            
            s3_validation_result = S3ValidationResult(
                is_valid=is_valid,
                content_size_bytes_validated=total_bytes_validated,
                reason=f"Basic validation: {valid_files}/{total_files} files across {len(jobs)} jobs ({validation_percentage:.1f}% valid, {total_bytes_validated} bytes)"
            )
            
            bt.logging.info(f"{hotkey}: Basic S3 validation completed - {validation_percentage:.1f}% valid ({valid_files}/{total_files} files, {len(jobs)} jobs)")
            
        except Exception as e:
            bt.logging.error(f"{hotkey}: Error in basic S3 validation: {str(e)}")
            s3_validation_result = S3ValidationResult(
                is_valid=False,
                content_size_bytes_validated=0,
                reason=f"Basic S3 validation error: {str(e)}"
            )
        
        return s3_validation_result

    async def _get_miner_s3_jobs(self, hotkey: str) -> List[str]:
        """Get list of job IDs for a miner from S3."""
        try:
            return await self.s3_reader.list_jobs(hotkey)
        except Exception as e:
            bt.logging.warning(f"Failed to get S3 jobs for {hotkey}: Connection error")
            return []

    async def _get_job_files(self, hotkey: str, job_id: str) -> List[dict]:
        """Get list of files for a specific job."""
        try:
            return await self.s3_reader.list_files(hotkey, job_id)
        except Exception as e:
            bt.logging.warning(f"Failed to get S3 files for {hotkey}: Connection error")
            return []

    async def _validate_s3_file(self, hotkey: str, file_info: dict) -> bool:
        """Validate a single S3 file."""
        try:
            # Basic validation checks
            if not file_info.get('Key'):
                return False
            
            # Check file extension is parquet
            if not file_info['Key'].endswith('.parquet'):
                return False
            
            # Check file size is reasonable (not empty, not too large)
            size = file_info.get('Size', 0)
            if size < 1024 or size > 100 * 1024 * 1024:  # 1KB to 100MB
                return False
            
            # Check last modified date (within 30 days)
            import datetime as dt
            if 'LastModified' in file_info:
                last_modified = file_info['LastModified']
                if isinstance(last_modified, str):
                    last_modified = dt.datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
                
                age = dt.datetime.now(dt.timezone.utc) - last_modified
                if age > dt.timedelta(days=30):
                    return False
            
            return True
            
        except Exception as e:
            bt.logging.warning(f"Error validating S3 file for {hotkey}: Validation error")
            return False

    # Phase 1: Enhanced S3 Validation Methods

    async def _analyze_miner_job_completion(self, hotkey: str, all_job_ids: List[str]) -> Dict:
        """
        Analyze miner's job completion and calculate parquet file sizes.
        
        Args:
            hotkey: The miner's hotkey
            all_job_ids: List of all job IDs from dynamic desirability model
            
        Returns:
            Dictionary with job completion analysis
        """
        try:
            completed_jobs = []
            total_files = 0
            total_size_bytes = 0
            
            # Check each job ID to see if miner has submitted data for it
            for job_id in all_job_ids:
                try:
                    # Get files for this job from S3
                    files = await self._get_job_files(hotkey, job_id)
                    
                    if files:
                        # Filter for parquet files only
                        parquet_files = [f for f in files if f.get('Key', '').endswith('.parquet')]
                        
                        if parquet_files:
                            job_parquet_size = sum(file_info.get('Size', 0) for file_info in parquet_files)
                            
                            completed_jobs.append({
                                'job_id': job_id,
                                'total_size_bytes': job_parquet_size,
                                'file_count': len(parquet_files),
                                'all_files': parquet_files
                            })
                            
                            total_files += len(parquet_files)
                            total_size_bytes += job_parquet_size
                            
                except Exception as e:
                    # Job folder doesn't exist or other error - skip this job
                    continue
            
            return {
                'completed_jobs': completed_jobs,
                'total_files': total_files,
                'total_size_mb': total_size_bytes / (1024 * 1024),  # Convert to MB
                'total_jobs_available': len(all_job_ids)
            }
            
        except Exception as e:
            bt.logging.error(f"Error analyzing miner job completion for {hotkey}: {str(e)}")
            return {
                'completed_jobs': [],
                'total_files': 0,
                'total_size_mb': 0.0,
                'total_jobs_available': len(all_job_ids)
            }

    def _select_random_job_for_validation(self, miner_job_data: Dict) -> Optional[Dict]:
        """
        Randomly select a job for deep validation using nanosecond-based seeding.
        """
        try:
            completed_jobs = miner_job_data.get('completed_jobs', [])
            
            if not completed_jobs:
                return None
            
            # Use nanosecond precision timestamp as seed
            seed = time.time_ns()
            rng = random.Random(seed)
            
            # Randomly select one job
            selected_job = rng.choice(completed_jobs)
            
            bt.logging.info(f"Selected job {selected_job['job_id']} for deep validation "
                          f"({selected_job['file_count']} files, {selected_job['total_size_bytes']/1024/1024:.1f}MB)")
            
            return selected_job
            
        except Exception as e:
            bt.logging.error(f"Error selecting random job for validation: {str(e)}")
            return None

    async def _perform_basic_s3_file_validation(self, hotkey: str, selected_job_data: Dict) -> bool:
        """
        Perform basic S3 file validation on the selected job.
        All files must pass basic validation (100% requirement).
        
        Returns:
            bool: True if all files pass basic validation, False otherwise
        """
        try:
            job_id = selected_job_data['job_id']
            all_files = selected_job_data.get('all_files', [])
            
            if not all_files:
                bt.logging.warning(f"{hotkey}: No files found for basic validation in job {job_id}")
                return False
            
            bt.logging.info(f"{hotkey}: Performing basic validation on {len(all_files)} files from job {job_id}")
            
            # Validate each file - 100% must pass
            for file_info in all_files:
                if not await self._validate_s3_file(hotkey, file_info):
                    bt.logging.warning(f"{hotkey}: Basic validation failed for file {file_info.get('Key', 'unknown')} in job {job_id}")
                    return False
            
            bt.logging.info(f"{hotkey}: Basic validation passed for all {len(all_files)} files in job {job_id}")
            return True
            
        except Exception as e:
            bt.logging.error(f"{hotkey}: Error in basic S3 file validation: {str(e)}")
            return False

    async def _deep_validate_job_parquets(self, hotkey: str, selected_job_data: Dict) -> List[S3ValidationResult]:
        """
        Perform deep validation on parquet files from selected job.
        Phase 2: Content validation with schema validation and keyword matching.
        
        Returns:
            List[S3ValidationResult]: One result per validated entity (like eval_miner approach)
        """
        try:
            job_id = selected_job_data['job_id']
            all_files = selected_job_data.get('all_files', [])
            
            if not all_files:
                bt.logging.warning(f"{hotkey}: No parquet files found for deep validation in job {job_id}")
                return []
            
            # Get job data to determine data source and keyword
            job_data = self.scorer.data_value_calculator.model.get_job_data_by_id(job_id)
            if not job_data:
                bt.logging.warning(f"{hotkey}: Job data not found for job_id: {job_id}")
                return []
            
            data_source = job_data.get('data_source')
            job_keyword = job_data.get('keyword')
            
            bt.logging.info(f"{hotkey}: Deep validating job {job_id} (source: {data_source.name}, keyword: {job_keyword})")
            
            # Use nanosecond precision timestamp as seed for file sampling
            seed = time.time_ns()
            rng = random.Random(seed)
            
            # Randomly sample up to 10 parquet files
            max_files_to_sample = min(10, len(all_files))
            sampled_files = rng.sample(all_files, max_files_to_sample)
            
            all_validation_results = []
            
            for file_info in sampled_files:
                try:
                    # Get validation results for this parquet file
                    file_validation_results = await self._validate_parquet_content(
                        hotkey, file_info, data_source, job_keyword, job_data
                    )
                    all_validation_results.extend(file_validation_results)
                        
                except Exception as e:
                    bt.logging.error(f"{hotkey}: Error validating parquet file {file_info.get('Key', 'unknown')}: {str(e)}")
                    # Add failed validation result for this file
                    all_validation_results.append(S3ValidationResult(
                        is_valid=False,
                        content_size_bytes_validated=0,
                        reason=f"Parquet validation error: {str(e)}"
                    ))
            
            bt.logging.info(f"{hotkey}: Deep validation completed for job {job_id} - {len(all_validation_results)} entities validated")
            return all_validation_results
            
        except Exception as e:
            bt.logging.error(f"{hotkey}: Error in deep validation for job {job_id}: {str(e)}")
            return [S3ValidationResult(
                is_valid=False,
                content_size_bytes_validated=0,
                reason=f"Deep validation error: {str(e)}"
            )]

    async def _validate_parquet_content(
        self, hotkey: str, file_info: Dict, data_source: DataSource, job_keyword: Optional[str], job_data: Dict
    ) -> List[S3ValidationResult]:
        """
        Validate content of a single parquet file by converting rows to DataEntities.
        
        Args:
            hotkey: Miner's hotkey
            file_info: S3 file information
            data_source: DataSource enum (REDDIT, X, YOUTUBE)
            job_keyword: Optional keyword to validate in content
            job_data: Complete job data dictionary containing all parameters
            
        Returns:
            List[S3ValidationResult]: One result per validated row
        """
        try:
            file_key = file_info.get('Key')
            bt.logging.info(f"{hotkey}: Validating parquet content in {file_key}")
            
            # Read parquet file from S3
            parquet_data = await self._read_parquet_from_s3(hotkey, file_key)
            if parquet_data is None or parquet_data.empty:
                return [S3ValidationResult(
                    is_valid=False,
                    content_size_bytes_validated=0,
                    reason=f"Could not read parquet file {file_key}"
                )]
            
            # Validate DataFrame has data before sampling
            if len(parquet_data) == 0:
                return [S3ValidationResult(
                    is_valid=False,
                    content_size_bytes_validated=0,
                    reason=f"Parquet file {file_key} contains no data rows"
                )]
            
            # Sample up to 10 rows from the parquet file
            seed = time.time_ns()
            rng = random.Random(seed)
            max_rows_to_sample = min(10, len(parquet_data))
            
            # Additional safety check before sampling
            if max_rows_to_sample <= 0:
                return [S3ValidationResult(
                    is_valid=False,
                    content_size_bytes_validated=0,
                    reason=f"No valid rows to sample from {file_key}"
                )]
            
            if max_rows_to_sample < len(parquet_data):
                sampled_rows = parquet_data.sample(n=max_rows_to_sample, random_state=seed)
            else:
                sampled_rows = parquet_data
            
            bt.logging.info(f"{hotkey}: Sampled {len(sampled_rows)} rows from {file_key} for validation")
            
            # Convert rows to DataEntities and validate
            validation_results = []
            
            for idx, row in sampled_rows.iterrows():
                try:
                    # Step 5c: Validate that row conforms to job parameters
                    job_param_validation_result = self._validate_job_parameters_conformance(
                        row, job_data, data_source
                    )
                    
                    if not job_param_validation_result['is_valid']:
                        validation_results.append(S3ValidationResult(
                            is_valid=False,
                            content_size_bytes_validated=0,
                            reason=f"Job parameter validation failed: {job_param_validation_result['reason']}"
                        ))
                        continue
                    
                    # Convert row to appropriate content model and then to DataEntity
                    data_entity = self._convert_row_to_data_entity(row, data_source)
                    
                    if data_entity is None:
                        validation_results.append(S3ValidationResult(
                            is_valid=False,
                            content_size_bytes_validated=0,
                            reason=f"Failed to convert row {idx} to DataEntity"
                        ))
                        continue
                    
                    # Step 5d: Validate using appropriate scraper (previously step 5c)
                    entity_validation_results = await self._validate_data_entity(data_entity, data_source)
                    
                    # Convert ValidationResult to S3ValidationResult and add keyword validation
                    for val_result in entity_validation_results:
                        # Step 5e: Perform keyword validation if required (previously step 5d)
                        keyword_valid = self._validate_keyword_in_content(data_entity, job_keyword, data_source)
                        
                        # Entity is valid only if both content validation AND keyword validation pass
                        final_is_valid = val_result.is_valid and keyword_valid
                        
                        reason = val_result.reason
                        if not keyword_valid:
                            reason += f" (Keyword '{job_keyword}' not found)" if reason else f"Keyword '{job_keyword}' not found"
                        
                        validation_results.append(S3ValidationResult(
                            is_valid=final_is_valid,
                            content_size_bytes_validated=val_result.content_size_bytes_validated,
                            reason=reason
                        ))
                
                except Exception as e:
                    bt.logging.error(f"{hotkey}: Error validating row {idx}: {str(e)}")
                    validation_results.append(S3ValidationResult(
                        is_valid=False,
                        content_size_bytes_validated=0,
                        reason=f"Row validation error: {str(e)}"
                    ))
            
            return validation_results
            
        except Exception as e:
            bt.logging.error(f"{hotkey}: Error in parquet content validation: {str(e)}")
            return [S3ValidationResult(
                is_valid=False,
                content_size_bytes_validated=0,
                reason=f"Parquet content validation error: {str(e)}"
            )]

    async def _read_parquet_from_s3(self, hotkey: str, file_key: str) -> Optional[pd.DataFrame]:
        """Read parquet file from S3 and return as DataFrame."""
        try:
            # Use the S3 reader to get file content
            file_content = await self.s3_reader.get_file_content(hotkey, file_key)
            if file_content:
                # Read parquet from bytes
                parquet_bytes = io.BytesIO(file_content)
                df = pd.read_parquet(parquet_bytes)
                return df
            return None
        except Exception as e:
            bt.logging.error(f"{hotkey}: Error reading parquet file {file_key}: {str(e)}")
            return None

    def _convert_row_to_data_entity(self, row: pd.Series, data_source: DataSource) -> Optional[DataEntity]:
        """Convert a pandas row to DataEntity based on data source using clean property approach."""
        try:
            # Get the appropriate content model class for this data source
            content_model_class = data_source.content_model_class
            if not content_model_class:
                bt.logging.error(f"No content model class found for data source: {data_source}")
                return None
            
            # Convert row to dict for field validation
            row_dict = row.to_dict()
            
            # Validate required fields before content model creation
            if not self._validate_required_fields(row_dict, data_source):
                bt.logging.error(f"Missing required fields for data source {data_source}: {list(row_dict.keys())}")
                return None
            
            # Create content model instance (this will now be safe from missing required fields)
            content_instance = content_model_class(**row_dict)
            
            # Convert to DataEntity using the content model's to_data_entity method
            return content_model_class.to_data_entity(content_instance)
                
        except Exception as e:
            bt.logging.error(f"Error converting row to DataEntity: {str(e)}")
            return None

    def _validate_required_fields(self, row_dict: dict, data_source: DataSource) -> bool:
        """Validate that all required fields are present and not null/empty for the given data source."""
        try:
            # Define required fields for each data source
            required_fields = {
                DataSource.REDDIT: ['id', 'url', 'username', 'community', 'body', 'created_at', 'data_type'],
                DataSource.X: ['username', 'text', 'url', 'timestamp', 'tweet_hashtags'],
                DataSource.YOUTUBE: ['video_id', 'title', 'channel_id', 'transcript']
            }
            
            # Get required fields for this data source
            source_required_fields = required_fields.get(data_source, [])
            if not source_required_fields:
                bt.logging.warning(f"No required fields defined for data source: {data_source}")
                return True  # Allow unknown data sources for now
            
            # Check each required field
            for field in source_required_fields:
                if field not in row_dict:
                    bt.logging.error(f"Missing required field '{field}' for {data_source}")
                    return False
                
                value = row_dict[field]
                
                # Check for None or empty values
                if value is None:
                    bt.logging.error(f"Required field '{field}' is None for {data_source}")
                    return False
                
                # Check for empty strings
                if isinstance(value, str) and value.strip() == '':
                    bt.logging.error(f"Required field '{field}' is empty string for {data_source}")
                    return False
                
                # Check for empty lists (specifically for tweet_hashtags and transcript)
                if isinstance(value, list) and len(value) == 0 and field in ['tweet_hashtags', 'transcript']:
                    bt.logging.error(f"Required field '{field}' is empty list for {data_source}")
                    return False
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error validating required fields: {str(e)}")
            return False

    async def _validate_data_entity(self, data_entity: DataEntity, data_source: DataSource) -> List[ValidationResult]:
        """Validate DataEntity using appropriate scraper."""
        try:
            # Get appropriate scraper for data source
            scraper = self.scraper_provider.get(self.PREFERRED_SCRAPERS[data_source])
            
            # Validate the entity (returns List[ValidationResult])
            validation_results = await scraper.validate([data_entity])
            
            return validation_results
            
        except Exception as e:
            bt.logging.error(f"Error validating DataEntity: {str(e)}")
            return [ValidationResult(
                is_valid=False,
                content_size_bytes_validated=data_entity.content_size_bytes if data_entity else 0,
                reason=f"Entity validation error: {str(e)}"
            )]

    def _validate_job_parameters_conformance(self, row: pd.Series, job_data: Dict, data_source: DataSource) -> Dict[str, Any]:
        """
        Validate that the row conforms to job parameters including label, keyword, and time range.
        
        Args:
            row: Pandas Series containing the row data
            job_data: Job data dictionary containing parameters
            data_source: DataSource enum (REDDIT, X, YOUTUBE)
            
        Returns:
            Dict with 'is_valid' boolean and 'reason' string
        """
        try:
            # Extract job parameters
            job_label = job_data.get('label')
            job_keyword = job_data.get('keyword')
            start_timebucket = job_data.get('start_timebucket')
            end_timebucket = job_data.get('end_timebucket')
            
            # Convert row to dict for easier field access
            row_dict = row.to_dict()
            
            # Validate label field based on data source
            if job_label and not self._validate_label_field(row_dict, job_label, data_source):
                return {
                    'is_valid': False,
                    'reason': f"Row label does not match job label '{job_label}' for {data_source.name}"
                }
            
            # Validate keyword field based on data source
            if job_keyword and not self._validate_keyword_field(row_dict, job_keyword, data_source):
                return {
                    'is_valid': False,
                    'reason': f"Row does not contain keyword '{job_keyword}' for {data_source.name}"
                }
            
            # Validate time range if specified
            if (start_timebucket is not None or end_timebucket is not None) and \
               not self._validate_time_range(row_dict, start_timebucket, end_timebucket, data_source):
                return {
                    'is_valid': False,
                    'reason': f"Row timestamp not within job time range [{start_timebucket}-{end_timebucket}]"
                }
            
            return {'is_valid': True, 'reason': 'Job parameter validation passed'}
            
        except Exception as e:
            bt.logging.error(f"Error in job parameter validation: {str(e)}")
            return {
                'is_valid': False,
                'reason': f"Job parameter validation error: {str(e)}"
            }

    def _validate_label_field(self, row_dict: dict, job_label: str, data_source: DataSource) -> bool:
        """Validate that the row's label field matches the job label."""
        try:
            if data_source == DataSource.REDDIT:
                # For Reddit, label is the community field (subreddit)
                community = row_dict.get('community', '')
                # Remove 'r/' prefix if present for comparison
                community_clean = community.replace('r/', '') if community.startswith('r/') else community
                job_label_clean = job_label.replace('r/', '') if job_label.startswith('r/') else job_label
                return community_clean.lower() == job_label_clean.lower()
                
            elif data_source == DataSource.X:
                # For X, label should be present in tweet_hashtags
                hashtags = row_dict.get('tweet_hashtags', [])
                if isinstance(hashtags, list):
                    # Check if job_label appears in any hashtag (case-insensitive)
                    hashtags_str = ' '.join(hashtags).lower()
                    return job_label.lower() in hashtags_str
                return False
                
            elif data_source == DataSource.YOUTUBE:
                # For YouTube, the label could be channel or video related
                # This would need to be implemented based on YouTube label structure
                bt.logging.warning(f"YouTube label validation not yet implemented for label: {job_label}")
                return True  # Allow for now until YouTube structure is clarified
                
            else:
                bt.logging.warning(f"Unknown data source for label validation: {data_source}")
                return True  # Allow unknown sources
                
        except Exception as e:
            bt.logging.error(f"Error validating label field: {str(e)}")
            return False

    def _validate_keyword_field(self, row_dict: dict, job_keyword: str, data_source: DataSource) -> bool:
        """Validate that the row contains the job keyword in the appropriate text field."""
        try:
            if data_source == DataSource.REDDIT:
                # For Reddit, keyword should be in the body field
                body = row_dict.get('body', '')
                return job_keyword.lower() in body.lower()
                
            elif data_source == DataSource.X:
                # For X, keyword should be in the text field
                text = row_dict.get('text', '')
                return job_keyword.lower() in text.lower()
                
            elif data_source == DataSource.YOUTUBE:
                # For YouTube, keyword should be in the title field (changed from transcript per user request)
                title = row_dict.get('title', '')
                return job_keyword.lower() in title.lower()
                
            else:
                bt.logging.warning(f"Unknown data source for keyword validation: {data_source}")
                return True  # Allow unknown sources
                
        except Exception as e:
            bt.logging.error(f"Error validating keyword field: {str(e)}")
            return False

    def _validate_time_range(self, row_dict: dict, start_timebucket: Optional[int], 
                           end_timebucket: Optional[int], data_source: DataSource) -> bool:
        """Validate that the row's timestamp falls within the job's time range."""
        try:
            # Get the datetime field based on data source
            if data_source == DataSource.REDDIT:
                datetime_field = 'created_at'
            elif data_source == DataSource.X:
                datetime_field = 'timestamp'
            elif data_source == DataSource.YOUTUBE:
                # YouTube might use different field names depending on implementation
                datetime_field = 'published_at'  # Common field name for YouTube
                if datetime_field not in row_dict:
                    # Try alternative field names
                    for alt_field in ['upload_date', 'created_at', 'timestamp']:
                        if alt_field in row_dict:
                            datetime_field = alt_field
                            break
            else:
                bt.logging.warning(f"Unknown data source for time validation: {data_source}")
                return True  # Allow unknown sources
            
            # Get the datetime value from the row
            datetime_value = row_dict.get(datetime_field)
            if not datetime_value:
                bt.logging.warning(f"Missing datetime field '{datetime_field}' in row for {data_source}")
                return False
            
            # Convert to datetime if it's a string
            if isinstance(datetime_value, str):
                try:
                    import pandas as pd
                    datetime_value = pd.to_datetime(datetime_value)
                except Exception:
                    bt.logging.error(f"Failed to parse datetime string: {datetime_value}")
                    return False
            
            # Convert datetime to timebucket using the same logic as TimeBucket.from_datetime
            from common import utils
            datetime_timestamp = datetime_value.timestamp()
            row_timebucket = utils.seconds_to_hours(datetime_timestamp)
            
            # Check if timebucket falls within the specified range
            if start_timebucket is not None and row_timebucket < start_timebucket:
                return False
            
            if end_timebucket is not None and row_timebucket > end_timebucket:
                return False
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error validating time range: {str(e)}")
            return False

    def _validate_keyword_in_content(self, data_entity: DataEntity, job_keyword: Optional[str], data_source: DataSource) -> bool:
        """Validate that keyword appears in the content text field using clean property approach."""
        if not job_keyword:
            return True  # No keyword requirement
        
        try:
            # Get the appropriate content model class and text field name
            content_model_class = data_source.content_model_class
            text_field_name = data_source.text_field_name
            
            if not content_model_class or not text_field_name:
                bt.logging.error(f"Missing content model or text field for data source: {data_source}")
                return False
            
            # Convert DataEntity back to content model to access fields
            content_instance = content_model_class.from_data_entity(data_entity)
            
            # Extract text content using standard field access
            text_content = getattr(content_instance, text_field_name, '')
            
            # Check if keyword appears in text (case-insensitive)
            return job_keyword.lower() in text_content.lower()
            
        except Exception as e:
            bt.logging.error(f"Error validating keyword in content: {str(e)}")
            return False

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

        # Run in batches of 15.
        miners_to_eval = 15

        # Otherwise, execute the next batch of evaluations.
        # Use a set in case the network has fewer than 15 miners.
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

        bt.logging.info(f"{hotkey}: Getting MinerIndex from miner.")

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
                    f"{hotkey}: Miner failed to respond with an index. Using last known index if present."
                )
                # Miner failed to update the index. Use the latest index, if present.
                return self.storage.read_miner_index(hotkey)

            # Validate the index.
            miner_index = None
            try:
                miner_index = vali_utils.get_miner_index_from_response(response)
            except ValueError as e:
                bt.logging.info(
                    f"{hotkey}: Miner returned an invalid index. Reason: {e}. Using last known index if present."
                )
                # Miner returned an invalid index. Use the latest index, if present.
                return self.storage.read_miner_index(hotkey)

            assert miner_index is not None, "Miner index should not be None."

            # Miner replied with a valid index. Store it and return it.
            miner_credibility = self.scorer.get_miner_credibility(uid)
            bt.logging.success(
                f"{hotkey}: Got new compressed miner index of {CompressedMinerIndex.size_bytes(miner_index)} bytes "
                + f"across {CompressedMinerIndex.bucket_count(miner_index)} buckets."
            )
            self.storage.upsert_compressed_miner_index(
                miner_index, hotkey, miner_credibility
            )

            return self.storage.read_miner_index(hotkey)
        except Exception:
            bt.logging.error(
                f"{hotkey} Failed to update and get miner index.",
                traceback.format_exc(),
            )
            return None

    async def _query_huggingface_metadata(
            self, hotkey: str, uid: int, miner_axon: bt.AxonInfo
    ) -> Optional[List[HuggingFaceMetadata]]:
        bt.logging.info(f"{hotkey}: Getting HuggingFace metadata from miner.")

        try:
            synapse = GetHuggingFaceMetadata(version=constants.PROTOCOL_VERSION)
            async with bt.dendrite(wallet=self.wallet) as dendrite:
                responses = await dendrite.forward(
                    axons=[miner_axon],
                    synapse=synapse,
                    timeout=120,
                )

            if not responses or len(responses) == 0 or not isinstance(responses[0], GetHuggingFaceMetadata):
                bt.logging.info(f"{hotkey}: Miner failed to respond with HuggingFace metadata.")
                return None

            response = responses[0]
            bt.logging.success(f"{hotkey}: Got HuggingFace metadata with {len(response.metadata)} entries")
            return response.metadata
        except Exception:
            bt.logging.error(
                f"{hotkey} Failed to query HuggingFace metadata.",
                traceback.format_exc(),
            )
            return None

    async def _get_decoded_urls(self, hotkey: str, uid: int, axon_info: bt.AxonInfo,
                                encoded_urls: List[str]) -> Tuple[bool, List[str]]:
        """
        Gets decoded URLs from miner for validation.
        Returns:
            Tuple[bool, List[str]]: (success, decoded_urls)
        """
        try:
            async with bt.dendrite(wallet=self.wallet) as dendrite:
                responses = await dendrite.forward(
                    axons=[axon_info],
                    synapse=DecodeURLRequest(
                        encoded_urls=encoded_urls[:10],
                        version=constants.PROTOCOL_VERSION
                    ),
                    timeout=30
                )

            if not responses or len(responses) == 0:
                bt.logging.info(f"{hotkey}: No response received for URL decode request")
                return False, []

            return True, responses[0].decoded_urls

        except Exception as e:
            bt.logging.error(f"{hotkey}: Error validating URLs: {str(e)}")
            return False, []

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
                utils.get_miner_uids(self.metagraph, self.uid, self.vpermit_rao_limit)
            )

            # Check to see if the metagraph has changed size.
            # If so, we need to add new hotkeys and moving averages.
            if len(self.metagraph.hotkeys) < len(metagraph.hotkeys):
                self.scorer.resize(len(metagraph.hotkeys))

            self.metagraph = copy.deepcopy(metagraph)



    def exit(self):
        self.should_exit = True

