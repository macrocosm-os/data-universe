import copy
import datetime
import sys
import time
import traceback
import asyncio
import threading
import os
from common import constants
from common.data_v2 import ScorableMinerIndex
from common.metagraph_syncer import MetagraphSyncer
import common.utils as utils
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
from vali_utils.miner_iterator import MinerIterator
from vali_utils import utils as vali_utils
from storage.validator.validator_storage import (
    ValidatorStorage,
)
from storage.validator.mysql_databox_storage import MysqlDataboxStorage

from typing import List, Optional

from rewards.miner_scorer import MinerScorer
from datadog import statsd


class MinerEvaluator:
    """MinerEvaluator is responsible for evaluating miners and updating their scores."""

    SCORER_FILENAME = "scorer.pickle"

    # Mapping of scrapers to use based on the data source to validate.
    PREFERRED_SCRAPERS = {
        DataSource.X: ScraperId.X_MICROWORLDS,
        DataSource.REDDIT: ScraperId.REDDIT_CUSTOM,
    }

    def __init__(self, config: bt.config, uid: int, metagraph_syncer: MetagraphSyncer):
        self.config = config
        self.uid = uid
        self.metagraph_syncer = metagraph_syncer
        self.metagraph = self.metagraph_syncer.get_metagraph(config.netuid)
        self.metagraph_syncer.register_listener(
            self._on_metagraph_updated, netuids=[config.netuid]
        )

        self.wallet = bt.wallet(config=self.config)

        # Set up initial scoring weights for validation
        self.scorer = MinerScorer(self.metagraph.n, DataValueCalculator())

        # Setup dependencies.
        self.miner_iterator = MinerIterator(
            utils.get_miner_uids(self.metagraph, self.uid)
        )
        self.scraper_provider = ScraperProvider()
        self.storage = SqliteMemoryValidatorStorage()
        self.databox_storage: MysqlDataboxStorage = MysqlDataboxStorage(
            host=os.getenv("MYSQL_DATABOX_HOST"),
            user=os.getenv("MYSQL_DATABOX_USER"),
            password=os.getenv("MYSQL_DATABOX_PW"),
            database=os.getenv("MYSQL_DATABOX_DB"),
        )

        self.last_seen_38_datetime = None

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.lock = threading.RLock()
        self.is_setup = False

    def get_scorer(self) -> MinerScorer:
        """Returns the scorer used by the evaluator."""
        return self.scorer

    @statsd.timed("eval_miner")
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

        # From that index, find a data entity bucket to sample and get it from the miner.
        chosen_data_entity_bucket: DataEntityBucket = (
            vali_utils.choose_data_entity_bucket_to_query(index)
        )
        bt.logging.info(
            f"{hotkey} Querying miner for Bucket ID: {chosen_data_entity_bucket.id}."
        )

        responses = None
        with statsd.timed("get_data_entity_bucket_time"):
            async with bt.dendrite(wallet=self.wallet) as dendrite:
                responses = await dendrite.forward(
                    axons=[axon_info],
                    synapse=GetDataEntityBucket(
                        data_entity_bucket_id=chosen_data_entity_bucket.id,
                        version=constants.PROTOCOL_VERSION,
                    ),
                    timeout=120,
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

        # Output to datadog only for buckets that have passed basic validation.
        if chosen_data_entity_bucket.id.source == DataSource.REDDIT:
            statsd.distribution(
                "bucket_reddit.entity_count", len(data_entity_bucket.data_entities)
            )
            statsd.distribution(
                "bucket_reddit.size", chosen_data_entity_bucket.size_bytes
            )
        elif chosen_data_entity_bucket.id.label is None:
            statsd.distribution(
                "bucket_twitter_none.entity_count",
                len(data_entity_bucket.data_entities),
            )
            statsd.distribution(
                "bucket_twitter_none.size", chosen_data_entity_bucket.size_bytes
            )
        else:
            statsd.distribution(
                "bucket_twitter.entity_count", len(data_entity_bucket.data_entities)
            )
            statsd.distribution(
                "bucket_twitter.size", chosen_data_entity_bucket.size_bytes
            )

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
        now = datetime.datetime.utcnow()
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

        # Do not count the first loop since we start in a random place.
        if 38 in uids_to_eval:
            if self.last_seen_38_datetime is not None:
                # Emit how many minutes its been since we've last evaluated 38 (current top miner).
                statsd.gauge(
                    "evaluation_frequency_seconds",
                    (
                        datetime.datetime.now() - self.last_seen_38_datetime
                    ).total_seconds(),
                )

            # Update our sentinel to now.
            self.last_seen_38_datetime = datetime.datetime.now()

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
            with statsd.timed("get_miner_index_time"):
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
            # Output to datadog only for valid indexes.
            index_size = CompressedMinerIndex.size_bytes(miner_index)
            index_bucket_count = CompressedMinerIndex.bucket_count(miner_index)
            statsd.distribution("index.bucket_count", index_bucket_count)
            statsd.distribution("index.size", index_size)
            miner_credibility = self.scorer.get_miner_credibility(uid)
            bt.logging.success(
                f"{hotkey}: Got new compressed miner index of {index_size} bytes "
                + f"across {index_bucket_count} buckets."
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
                    not utils.is_miner(uid, metagraph)
                    and not utils.is_validator(uid, metagraph)
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
                utils.get_miner_uids(self.metagraph, self.uid)
            )

            # Check to see if the metagraph has changed size.
            # If so, we need to add new hotkeys and moving averages.
            if len(self.metagraph.hotkeys) < len(metagraph.hotkeys):
                self.scorer.resize(len(metagraph.hotkeys))

            self.metagraph = copy.deepcopy(metagraph)

    def exit(self):
        self.should_exit = True

    def run_databox(self):
        """
        Initiates and manages the databox loop for the validator, which

        1. Periodically updates the mysql databox tables with current information.
        """

        # Sleep on startup to avoid wiping the tables on restart.
        time.sleep(datetime.timedelta(minutes=90).total_seconds())

        # This loop maintains the validator's databox table updates until intentionally stopped.
        while not self.should_exit:
            try:
                bt.logging.trace("Updating databox tables.")

                next_databox_update = datetime.datetime.utcnow() + datetime.timedelta(
                    minutes=45
                )

                # Get Databox Miners from SqliteMemory and write to mysql.
                self.databox_storage.insert_miners(self.storage.read_databox_miners())

                # Get Databox Age Sizes from SqliteMemory and write to mysql.
                self.databox_storage.insert_age_sizes(
                    self.storage.read_databox_age_sizes()
                )

                # Get Databox Label Sizes from SqliteMemory and write to mysql.
                self.databox_storage.insert_label_sizes(
                    self.storage.read_databox_label_sizes()
                )

                wait_time = max(
                    0,
                    (next_databox_update - datetime.datetime.utcnow()).total_seconds(),
                )

                bt.logging.trace(
                    f"Finished updating databox tables. Waiting {wait_time} seconds until next update."
                )

                if wait_time > 0:
                    time.sleep(wait_time)

            # TODO: Confirm but I think this needs to be in both threads in case one or the other is running.
            # If someone intentionally stops the validator, it'll safely terminate operations.
            except KeyboardInterrupt:
                bt.logging.success(
                    "Validator killed by keyboard interrupt while in databox run."
                )
                sys.exit()

            # In case of unforeseen errors, the validator will log the error and continue operations.
            except Exception as err:
                bt.logging.error("Error during databox run", str(err))
                bt.logging.debug(
                    traceback.print_exception(type(err), err, err.__traceback__)
                )
