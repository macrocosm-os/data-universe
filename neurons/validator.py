# The MIT License (MIT)
# Copyright © 2023 Data Universe

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.


import copy
import datetime
import sys
import traceback
import torch
import asyncio
import threading
import time
import os
import common.utils as utils
import bittensor as bt
from common.data import (
    DataEntityBucket,
    DataEntity,
    DataSource,
    MinerIndex,
    ScorableMinerIndex,
)
from common.protocol import GetDataEntityBucket, GetMinerIndex
from neurons.config import NeuronType
from rewards.data_value_calculator import DataValueCalculator
from scraping.provider import ScraperProvider
from scraping.scraper import ScraperId, ValidationResult
from storage.validator.mysql_validator_storage import MysqlValidatorStorage
from storage.validator.validator_storage import ValidatorStorage
from vali_utils.miner_iterator import MinerIterator
from vali_utils import utils as vali_utils

from typing import List, Optional, Type
from traceback import print_exception

from neurons.base_neuron import BaseNeuron
from rewards.miner_scorer import MinerScorer


class Validator(BaseNeuron):
    # The minimum amount of time that must pass before we re-evaluate a miner.
    MIN_EVALUATION_PERIOD = datetime.timedelta(minutes=30)

    SCORER_FILENAME = "scorer.pickle"

    # Mapping of scrapers to use based on the data source to validate.
    PREFERRED_SCRAPERS = {
        DataSource.X: ScraperId.X_FLASH,
        DataSource.REDDIT: ScraperId.REDDIT_CUSTOM,
    }

    def __init__(self, config=None):
        super().__init__(config=config)

        # Save a copy of the hotkeys to local memory.
        self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)

        # Dendrite lets us send messages to other nodes (axons) in the network.
        self.dendrite = bt.dendrite(wallet=self.wallet)

        # Set up initial scoring weights for validation
        self.scorer = MinerScorer(self.metagraph.n, DataValueCalculator())

        # Create asyncio event loop to manage async tasks.
        self.loop = asyncio.get_event_loop()

        # Setup dependencies.
        self.miner_iterator = MinerIterator(self.get_miner_uids(self.metagraph))
        self.scraper_provider = ScraperProvider()

        # Setup the database.
        if not self.config.neuron.database_password:
            raise ValueError("Database password not set.")

        # Setup storage in setup()
        self.storage: ValidatorStorage = None

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: threading.Thread = None
        self.lock = asyncio.Lock()
        self.is_setup = False

    def neuron_type(self) -> NeuronType:
        return NeuronType.VALIDATOR

    def serve_axon(self):
        """Serve axon to enable external connections."""

        bt.logging.info("serving ip to chain...")
        try:
            # TODO: Expose a query endpoint on this axon
            self.axon = bt.axon(wallet=self.wallet, config=self.config)

            self.subtensor.serve_axon(
                netuid=self.config.netuid,
                axon=self.axon,
            )
        except Exception as e:
            bt.logging.error(f"Failed to setup Axon: {e}")
            sys.exit(1)

    async def _update_and_get_miner_index(
        self, hotkey: str, miner_axon: bt.AxonInfo
    ) -> Optional[ScorableMinerIndex]:
        """Updates the index for the specified miner, and returns the latest known index or None if the miner hasn't yet provided an index."""

        bt.logging.trace(f"{hotkey}: Getting MinerIndex from miner.")

        responses: List[GetMinerIndex] = await self.dendrite.forward(
            axons=[miner_axon],
            synapse=GetMinerIndex(),
            timeout=300,
        )

        get_miner_index_response = vali_utils.get_single_successful_response(
            responses, GetMinerIndex
        )
        if not get_miner_index_response:
            bt.logging.trace(
                f"{hotkey}: Miner returned an invalid/failed response for the index."
            )
            # Miner failed to update the index. Use the latest index, if present.
            return self._get_miner_index(hotkey)

        # Validate the index.
        miner_index = None
        try:
            miner_index = vali_utils.get_miner_index_from_response(
                get_miner_index_response, hotkey
            )
        except ValueError as e:
            bt.logging.debug(f"{hotkey}: Miner returned an invalid index. Reason: {e}")
            # Miner returned an invalid index. Use the latest index, if present.
            return self._get_miner_index(hotkey)

        # Miner replied with a valid index. Store it and return it.
        bt.logging.trace(
            f"{hotkey}: Got new miner index. Size={len(miner_index.data_entity_buckets)}"
        )
        self.storage.upsert_miner_index(
            MinerIndex(
                hotkey=hotkey, data_entity_buckets=miner_index.data_entity_buckets
            )
        )
        return self._get_miner_index(hotkey)

    def _get_miner_index(self, hotkey: str) -> Optional[ScorableMinerIndex]:
        """Gets the index for the specified miner, and returns the latest known index or None if the miner hasn't yet provided an index."""
        valid_miners = self.scorer.get_credible_miners()
        return self.storage.read_miner_index(
            hotkey=hotkey, valid_miners=set(valid_miners)
        )

    # TODO: Pull this out into a separate MinerEvaluator to make this more testable.
    async def eval_miner(self, uid: int) -> None:
        """Evaluates a miner and updates their score.

        Specifically:
            1. Gets the latest index from the miner
            2. Chooses a random data entity bucket to query
            3. Performs basic validation on the data entity bucket (right labels, matching size, etc.)
            4. Samples data from the data entity bucket and verifies the data is correct
            5. Passes the validation result to the scorer to update the miner's score.
        """
        axon_info = self.metagraph.axons[uid]
        hotkey = self.metagraph.hotkeys[uid]

        bt.logging.trace(f"{hotkey}: Evaluating miner")

        # Query the miner for the latest index.
        index = await self._update_and_get_miner_index(hotkey, axon_info)
        if not index:
            # The miner hasn't provided an index yet, so we can't validate them. Set their score to 0 and move on.
            bt.logging.trace(
                f"{hotkey}: Failed to get an index for miner. Setting score to 0."
            )
            self.scorer.reset(uid)
            return

        # From that index, find a data entity bucket to sample and get it from the miner.
        chosen_data_entity_bucket: DataEntityBucket = (
            vali_utils.choose_data_entity_bucket_to_query(index)
        )
        bt.logging.trace(
            f"{hotkey} Querying miner for chunk {chosen_data_entity_bucket}"
        )

        responses = await self.dendrite.forward(
            axons=[axon_info],
            synapse=GetDataEntityBucket(
                data_entity_bucket_id=chosen_data_entity_bucket.id
            ),
            timeout=180,
        )

        data_entity_bucket = vali_utils.get_single_successful_response(
            responses, GetDataEntityBucket
        )
        # Treat a failed response the same way we treat a failed validation.
        # If we didn't, the miner could just not respond to queries for data entity buckets it doesn't have.
        if data_entity_bucket is None:
            bt.logging.trace(f"{hotkey}: Miner returned an invalid/failed response.")
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason="Response failed or is invalid",
                        content_size_bytes_validated=data_entity_bucket.content_size_bytes,  # Whole bucket fails.
                        # Since there is just one failed result though the normalization by size doesn't matter.
                    )
                ],
            )
            return

        # Perform basic validation on the entities.
        bt.logging.trace(
            f"{hotkey}: Performing basic validation on entities ({len(data_entity_bucket.data_entities)})"
        )

        data_entities: List[DataEntity] = data_entity_bucket.data_entities
        (valid, reason) = vali_utils.are_entities_valid(
            data_entities, chosen_data_entity_bucket
        )
        if not valid:
            bt.logging.trace(
                f"Miner {hotkey} failed basic entity validation with reason {reason}."
            )
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason=reason,
                        content_size_bytes_validated=data_entity_bucket.content_size_bytes,  # Whole bucket fails.
                        # Since there is just one failed result though the normalization by size doesn't matter.
                    )
                ],
            )
            return

        # Perform uniqueness validation on the entity contents.
        # If we didn't, the miner could just return the same data over and over again.
        unique = vali_utils.are_entities_unique(data_entities)
        if not unique:
            bt.logging.trace(f"Miner {hotkey} failed enitity uniqueness checks.")
            self.scorer.on_miner_evaluated(
                uid,
                index,
                [
                    ValidationResult(
                        is_valid=False,
                        reason="Duplicate entities found.",
                        content_size_bytes_validated=data_entity_bucket.content_size_bytes,  # Whole bucket fails.
                        # Since there is just one failed result though the normalization by size doesn't matter.
                    )
                ],
            )
            return

        # Basic validation and uniqueness passed. Now sample some entities for data correctness.
        entities_to_validate: List[DataEntity] = vali_utils.choose_entities_to_verify(
            data_entities
        )

        bt.logging.trace(
            f"{hotkey}: Basic validation passed. Validating {entities_to_validate}"
        )

        scraper = self.scraper_provider.get(
            Validator.PREFERRED_SCRAPERS[chosen_data_entity_bucket.id.source]
        )
        validation_results = await scraper.validate(entities_to_validate)

        bt.logging.trace(
            f"{hotkey}: Data validation finished with results: {validation_results}"
        )

        self.scorer.on_miner_evaluated(uid, index, validation_results)

    async def run_next_eval_batch(self) -> int:
        """Asynchronously runs the next batch of miner evaluations and returns the number of seconds to wait until the next batch.

        Args:
            block (int): The block at which we started this evaluation.
        """
        seconds_until_next_batch = 0

        # Run in batches of 10.
        # TODO: Maybe make this configurable and run evaluations based on expected throughput
        miners_to_eval = 10
        # Use a set in case the network has fewer than 10 miners.
        uids_to_check = {next(self.miner_iterator) for _ in range(miners_to_eval)}
        uids_to_eval = set()

        # If any of the miners have been seen recently then it means we have looped through our iterator and should wait.
        for uid in uids_to_check:
            hotkey = self.metagraph.hotkeys[uid]
            last_evaluated = self.storage.read_miner_last_updated(hotkey)
            now = datetime.datetime.utcnow()

            # If we have aleady evaluated this miner recently then do not evaluate it.
            if (
                last_evaluated
                and (now - last_evaluated) < Validator.MIN_EVALUATION_PERIOD
            ):
                # Set the number of seconds until we expect to be able to run the next evaluation batch.
                # If multiple miners in this batch had already been seen just use the last one for reference.
                seconds_until_next_batch = (
                    last_evaluated + Validator.MIN_EVALUATION_PERIOD - now
                ).total_seconds()
            else:
                uids_to_eval.add(uid)

        coroutines = [self.eval_miner(uid) for uid in uids_to_eval]
        done, pending = await asyncio.wait(coroutines, timeout=300)

        for future in pending:
            future.cancel()  # Cancel unfinished tasks.

        if pending:
            bt.logging.trace(
                f"Validator run next eval batch timed out on the following calls: {pending}"
            )

        # Run the next evaluation batch after waiting the appropriate amount of time.
        return seconds_until_next_batch

    def setup(self):
        """A one-time setup method that must be called before the Validator starts its main loop."""
        assert not self.is_setup, "Validator already setup."

        bt.logging.info("Setting up validator.")

        # Setup the DB.
        self.storage = MysqlValidatorStorage(
            host=self.config.neuron.database_host,
            user=self.config.neuron.database_user,
            password=self.config.neuron.database_password,
            database=self.config.neuron.database_name,
        )

        # Load any state from previous runs.
        self.load_state()

        # TODO: Configure this to expose access to data to neurons on certain subnets.
        # Serve axon to enable external connections.
        if not self.config.neuron.axon_off:
            self.serve_axon()
        else:
            bt.logging.warning("axon off, not serving ip to chain.")

        self.is_setup = True

    def run(self):
        """
        Initiates and manages the main loop for the validator, which

        1. Periodically updates the metagraph
        2. Periodically writes the latest scores to the chain
        3. Evaluates miners
        4. Saves state
        """
        assert self.is_setup, "Validator must be setup before running."

        # Check that validator is registered on the network.
        self.sync()

        bt.logging.info(
            f"Running validator {self.axon} on network: {self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}"
        )

        bt.logging.info(f"Validator starting at block: {self.block}")

        # This loop maintains the validator's operations until intentionally stopped.
        try:
            while not self.should_exit:
                bt.logging.debug(f"step({self.step}) block({self.block})")

                # Run multiple forwards concurrently.
                next_batch_delay_secs = self.loop.run_until_complete(
                    self.run_next_eval_batch()
                )

                # Maybe sync the metagraph and potentially set weights.
                self.sync()

                self.step += 1

                if next_batch_delay_secs > 0:
                    bt.logging.debug(
                        f"Waiting {next_batch_delay_secs} seconds until running next evaluation loop."
                    )
                    time.sleep(next_batch_delay_secs)

        # If someone intentionally stops the validator, it'll safely terminate operations.
        except KeyboardInterrupt:
            self.axon.stop()
            bt.logging.success("Validator killed by keyboard interrupt.")
            sys.exit()

        # In case of unforeseen errors, the validator will log the error and continue operations.
        except Exception as err:
            bt.logging.error("Error during validation", str(err))
            bt.logging.debug(print_exception(type(err), err, err.__traceback__))

    def run_in_background_thread(self):
        """
        Starts the validator's operations in a background thread upon entering the context.
        This method facilitates the use of the validator in a 'with' statement.
        """

        # Setup the Validator.
        self.setup()

        if not self.is_running:
            bt.logging.debug("Starting validator in background thread.")
            self.should_exit = False
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            self.is_running = True
            bt.logging.debug("Started")

    def stop_run_thread(self):
        """
        Stops the validator's operations that are running in the background thread.
        """
        if self.is_running:
            bt.logging.debug("Stopping validator in background thread.")
            self.should_exit = True
            self.thread.join(5)
            self.is_running = False
            bt.logging.debug("Stopped")

    def __enter__(self):
        self.run_in_background_thread()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Stops the validator's background operations upon exiting the context.
        This method facilitates the use of the validator in a 'with' statement.

        Args:
            exc_type: The type of the exception that caused the context to be exited.
                      None if the context was exited without an exception.
            exc_value: The instance of the exception that caused the context to be exited.
                       None if the context was exited without an exception.
            traceback: A traceback object encoding the stack trace.
                       None if the context was exited without an exception.
        """
        if self.is_running:
            bt.logging.debug("Stopping validator in background thread.")
            self.should_exit = True
            self.thread.join(5)
            self.is_running = False
            bt.logging.debug("Stopped")

    def get_miner_uids(self, metagraph: bt.metagraph) -> List[int]:
        """Gets the uids of all miners in the metagraph."""
        return sorted(
            [
                uid.item()
                for uid in metagraph.uids
                if utils.is_miner(uid.item(), metagraph) and uid.item() != self.uid
            ]
        )

    def set_weights(self):
        """
        Sets the validator weights to the metagraph hotkeys based on the scores it has received from the miners. The weights determine the trust and incentive level the validator assigns to miner nodes on the network.
        """

        scores = self.scorer.get_scores()

        # Check if scores contains any NaN values and log a warning if it does.
        if torch.isnan(scores).any():
            bt.logging.warning(
                f"Scores contain NaN values. This may be due to a lack of responses from miners, or a bug in your reward functions."
            )

        # Calculate the average reward for each uid across non-zero values.
        # Replace any NaN values with 0.
        raw_weights = torch.nn.functional.normalize(scores, p=1, dim=0)
        bt.logging.trace("raw_weights", raw_weights)
        bt.logging.trace("top10 values", raw_weights.sort()[0])
        bt.logging.trace("top10 uids", raw_weights.sort()[1])

        # Process the raw weights to final_weights via subtensor limitations.
        (
            processed_weight_uids,
            processed_weights,
        ) = bt.utils.weight_utils.process_weights_for_netuid(
            uids=self.metagraph.uids.to("cpu"),
            weights=raw_weights.to("cpu"),
            netuid=self.config.netuid,
            subtensor=self.subtensor,
            metagraph=self.metagraph,
        )
        bt.logging.trace("processed_weights", processed_weights)
        bt.logging.trace("processed_weight_uids", processed_weight_uids)

        # Set the weights on chain via our subtensor connection.
        self.subtensor.set_weights(
            wallet=self.wallet,
            netuid=self.config.netuid,
            uids=processed_weight_uids,
            weights=processed_weights,
            wait_for_finalization=False,
            version_key=self.spec_version,
        )

        bt.logging.info(f"Set weights: {processed_weights}")

    def resync_metagraph(self):
        """Resyncs the metagraph and updates the hotkeys and moving averages based on the new metagraph."""
        bt.logging.info("resync_metagraph()")

        # Copies state of metagraph before syncing.
        previous_metagraph = copy.deepcopy(self.metagraph)

        # Sync the metagraph.
        self.metagraph.sync(subtensor=self.subtensor)

        # Check if the metagraph axon info has changed.
        if previous_metagraph.axons == self.metagraph.axons:
            return

        bt.logging.info("Metagraph updated, re-syncing hotkeys, and moving averages")
        # Zero out all hotkeys that have been replaced.
        for uid, hotkey in enumerate(self.hotkeys):
            if hotkey != self.metagraph.hotkeys[uid]:
                self.scorer.reset(uid)  # hotkey has been replaced
                try:
                    self.storage.delete_miner_index(hotkey)
                except Exception:
                    bt.logging.error(
                        f"{hotkey} Failed to delete miner index.",
                        traceback.format_exc(),
                    )
        # Update the iterator. It will keep its current position if possible.
        self.miner_iterator.set_miner_uids(self.get_miner_uids(self.metagraph))

        # Check to see if the metagraph has changed size.
        # If so, we need to add new hotkeys and moving averages.
        if len(self.hotkeys) < len(self.metagraph.hotkeys):
            self.scorer.resize(self.metagraph.n)

        # Update the hotkeys.
        self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)

    def save_state(self):
        """Saves the state of the validator to a file."""
        bt.logging.trace("Saving validator state.")

        if not os.path.exists(self.config.neuron.full_path):
            os.makedirs(self.config.neuron.full_path)

        # Save the state of the validator to file.
        self.scorer.save_state(
            os.path.join(self.config.neuron.full_path, Validator.SCORER_FILENAME)
        )

    def load_state(self):
        """Loads the state of the validator from a file."""
        bt.logging.info("Loading validator state.")

        # Load the state of the validator from file.
        filepath = os.path.join(self.config.neuron.full_path, Validator.SCORER_FILENAME)
        if not os.path.exists(filepath):
            bt.logging.warning("No state file found. Starting from scratch.")
            return

        self.scorer.load_state(filepath)

        # Resize the scorer in case the loaded state is old and missing newly added neurons.
        self.scorer.resize(self.metagraph.n)


# The main function parses the configuration and runs the validator.
if __name__ == "__main__":
    with Validator() as validator:
        while True:
            bt.logging.trace("Validator running...", time.time())
            time.sleep(60)
