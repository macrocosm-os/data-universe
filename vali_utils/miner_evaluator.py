import copy
import datetime
import traceback
import asyncio
import threading
import os
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
from scraping.scraper import ScraperId, ValidationResult, HFValidationResult
from storage.validator.sqlite_memory_validator_storage import (
    SqliteMemoryValidatorStorage,
)

from storage.validator.hf_validator_storage import HFValidationStorage

from vali_utils.miner_iterator import MinerIterator
from vali_utils import utils as vali_utils

from typing import List, Optional, Tuple
from vali_utils.validator_s3_access import ValidatorS3Access
from vali_utils.hf_utils import (
    get_latest_commit_files,
    get_validation_data,
    decode_dataframe,
    validate_hf_content,
    compare_latest_commits_parquet_files
)


from rewards.miner_scorer import MinerScorer


class MinerEvaluator:
    """MinerEvaluator is responsible for evaluating miners and updating their scores."""

    SCORER_FILENAME = "scorer.pickle"

    # Mapping of scrapers to use based on the data source to validate.
    PREFERRED_SCRAPERS = {
        DataSource.X: ScraperId.X_APIDOJO,
        DataSource.REDDIT: ScraperId.REDDIT_CUSTOM,
        DataSource.YOUTUBE: ScraperId.YOUTUBE_TRANSCRIPT
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

