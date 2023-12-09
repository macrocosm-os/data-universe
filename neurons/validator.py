# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# TODO(developer): Set your name
# Copyright © 2023 <your name>

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
import random
import sys
import torch
import asyncio
import threading
import time
import os
import bittensor as bt
from common.data import DataChunkSummary, DataEntity, DateRange, ScorableMinerIndex
from common.protocol import GetDataChunk
import common.utils as utils
from rewards.reward_distribution import RewardDistribution
from scraping.scraper import ValidationResult
from validator.miner_index import MinerIndexManager
from validator.miner_iterator import MinerIterator

from typing import List, Tuple
from traceback import print_exception

from template.base.neuron import BaseNeuron
from validator.miner_scorer import MinerScorer


class Validator(BaseNeuron):
    # The minimum number of blocks that must pass before we re-evaluate a miner.
    MIN_EVALUATION_PERIOD = datetime.timedelta(minutes=20)

    SCORER_FILENAME = "scorer.pickle"

    def __init__(self, config=None):
        super().__init__(config=config)

        # TODO: This really shouldn't be doing work in the constructor. Move to a start() method.

        # Save a copy of the hotkeys to local memory.
        self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)

        # Dendrite lets us send messages to other nodes (axons) in the network.
        self.dendrite = bt.dendrite(wallet=self.wallet)
        bt.logging.info(f"Dendrite: {self.dendrite}")

        # Set up initial scoring weights for validation
        bt.logging.info("Building validation weights.")
        self.scorer = MinerScorer(self.metagraph.n, RewardDistribution())

        # TODO: Configure this to expose access to data to neurons on certain subnets.
        # Serve axon to enable external connections.
        if not self.config.neuron.axon_off:
            self.serve_axon()
        else:
            bt.logging.warning("axon off, not serving ip to chain.")

        # Create asyncio event loop to manage async tasks.
        self.loop = asyncio.get_event_loop()

        self.miner_uids = sorted(
            [utils.is_miner(uid, self.metagraph) for uid in self.metagraph.uids]
        )
        self.miner_iterator = MinerIterator(self.miner_uids)
        self.miner_index_manager = MinerIndexManager()

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: threading.Thread = None
        self.lock = asyncio.Lock()

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

    @classmethod
    def choose_chunk_to_query(cls, index: ScorableMinerIndex) -> DataChunkSummary:
        """Chooses a random chunk to query from a MinerIndex.

        The random selection is done based on choosing a random byte in the total index to query, and then selecting that chunk
        """
        total_size = sum(chunk.size_bytes for chunk in index.chunks)
        chosen_byte = random.uniform(0, total_size)
        iterated_bytes = 0
        for chunk in index.chunks:
            if iterated_bytes + chunk.size_bytes >= chosen_byte:
                return chunk
            iterated_bytes += chunk.size_bytes
        assert False, "Failed to choose a chunk to query... which should never happen"

    @classmethod
    def choose_entities_to_verify(cls, entities: List[DataEntity]) -> List[DataEntity]:
        """Given a list of DataEntities from a DataChunk, chooses a random set of entities to verify."""

        # For now, we just sample 1 entity, based on size.
        # In future, consider sampling every N bytes.
        chosen_entities = []
        total_size = sum(entity.content_size_bytes for entity in entities)
        chosen_byte = random.uniform(0, total_size)
        iterated_bytes = 0
        for entity in entities:
            if iterated_bytes + entity.content_size_bytes >= chosen_byte:
                chosen_entities.append(entity)
                break
            iterated_bytes += entity.content_size_bytes
        return chosen_entities

    @classmethod
    def are_entities_valid(
        cls, entities: List[DataEntity], chunk: DataChunkSummary
    ) -> Tuple[bool, str]:
        """Performs basic validation on all entities in a chunk.

        Returns a tuple of (is_valid, reason) where is_valid is True if the entities are valid,
        and reason is a string describing why they are not valid.
        """

        # 1. Check the entity size, labels, source, and timestamp.
        actual_size = 0
        claimed_size = 0
        expected_datetime_range: DateRange = chunk.time_bucket.get_date_range()
        for entity in entities:
            actual_size += len(entity.content or b"")
            claimed_size += entity.content_size_bytes
            if entity.source != chunk.source:
                return (
                    False,
                    f"Entity source {entity.source} does not match chunk source {chunk.source}",
                )
            if entity.label != chunk.label:
                return (
                    False,
                    f"Entity label {entity.label} does not match chunk label {chunk.label}",
                )
            if not expected_datetime_range.contains(entity.datetime):
                return (
                    False,
                    f"Entity datetime {entity.datetime} is not in the expected range {expected_datetime_range}",
                )

        if actual_size < claimed_size or actual_size < chunk.size_bytes:
            return (
                False,
                f"Size not as expected. Actual={actual_size}. Claimed={claimed_size}. Expected={chunk.size_bytes}",
            )

        return (True, "")

    async def eval_miner(self, uid: int) -> None:
        """Evaluates a miner and updates their score.

        Specifically:
            1. Gets the latest index from the miner
            2. Chooses a random chunk to query
            3. Performs basic validation on the chunk (right labels, matching size, etc.)
            4. Samples data from the chunk and verifies the data is correct
            5. Passes the validation result to the scorer to update the miner's score.
        """

        axon_info = self.metagraph.axons[uid]
        # Query the miner for the latest index.
        index = self.miner_index_manager.update_index(uid, self.dendrite)
        if not index:
            # The miner hasn't provided an index yet, so we can't validate them. Set their score to 0 and move on.
            bt.logging.trace(
                f"Failed to get an index for miner {uid}. Setting score to 0."
            )
            self.scorer.reset_score(uid)
            return

        # From that index, find a chunk to sample and get it from the miner.
        chosen_chunk: DataChunkSummary = Validator.choose_chunk_to_query(index)
        bt.logging.trace(f"Querying miner {uid} for chunk {chosen_chunk}")
        response = await self.dendrite.forward(
            axons=[axon_info],
            synapse=GetDataChunk(data_chunk_summary=chosen_chunk),
            timeout=60,
        )

        # Treat a failed response the same way we treat a failed validation.
        # If we didn't, the miner could just not respond when queries for chunks it doesn't have.
        if (
            response is None
            or not isinstance(response, GetDataChunk)
            or not response.success
        ):
            bt.logging.trace(f"Miner {uid} returned an invalid/failed response.")
            self.scorer.on_miner_evaluated(
                uid, index, [ValidationResult(is_valid=False)]
            )
            return

        # Perform basic validation on the entities.
        data_entities: List[DataEntity] = response.data_entities
        (valid, reason) = Validator.are_entities_valid(data_entities, chosen_chunk)
        if not valid:
            bt.logging.trace(
                f"Miner {uid} failed basic entity validation with reason {reason}."
            )
            self.scorer.on_miner_evaluated(
                uid, index, [ValidationResult(is_valid=False)]
            )
            return

        # Basic validation passed. Now sample some entities for data correctness.
        entities_to_verify: List[DataEntity] = Validator.choose_entities_to_verify(
            data_entities
        )

        # TODO: Submit the verifications to the validation pool and await results.
        validation_results: List[ValidationResult] = []

        self.scorer.on_miner_evaluated(uid, index, validation_results)

    # TODO: Pull this out into a separate MinerEvaluator to make this more testable.
    async def run_next_eval_batch(self) -> int:
        """Asynchronously runs the next batch of miner evaluations and returns the number of seconds to wait until the next batch.

        Args:
            block (int): The block at which we started this evaluation.
        """
        next_uid = self.miner_iterator.peek()
        last_evaluated = self.miner_index_manager.get_datetime_index_last_updated(
            next_uid
        )
        now = datetime.datetime.utcnow()
        if last_evaluated and (now - last_evaluated) < Validator.MIN_EVALUATION_PERIOD:
            # Return the number of seconds until we expect to be able to run the next evaluation batch.
            return (
                last_evaluated + Validator.MIN_EVALUATION_PERIOD - now
            ).total_seconds()

        uids_to_eval = [
            next(self.miner_iterator)
            # Run in batches of 10.
            # TODO: Maybe make this configurable and run evaluations based on expected throughput
            for _ in range(10)
        ]
        coroutines = [self.eval_miner(uid) for uid in uids_to_eval]
        await asyncio.gather(*coroutines)

        # Run the next evaluation batch immediately.
        return 0

    def run(self):
        """
        Initiates and manages the main loop for the miner on the Bittensor network. The main loop handles graceful shutdown on keyboard interrupts and logs unforeseen errors.

        This function performs the following primary tasks:
        1. Check for registration on the Bittensor network.
        2. Continuously forwards queries to the miners on the network, rewarding their responses and updating the scores accordingly.
        3. Periodically resynchronizes with the chain; updating the metagraph with the latest network state and setting weights.

        The essence of the validator's operations is in the forward function, which is called every step. The forward function is responsible for querying the network and scoring the responses.

        Note:
            - The function leverages the global configurations set during the initialization of the miner.
            - The miner's axon serves as its interface to the Bittensor network, handling incoming and outgoing requests.

        Raises:
            KeyboardInterrupt: If the miner is stopped by a manual interruption.
            Exception: For unforeseen errors during the miner's operation, which are logged for diagnosis.
        """

        self.load_state()

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

                # Sync metagraph and potentially set weights.
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
        # TODO: In the past, this call has hung on me. We may want to do something special here to handle that.
        self.metagraph.sync(subtensor=self.subtensor)

        # Check if the metagraph axon info has changed.
        if previous_metagraph.axons == self.metagraph.axons:
            return

        bt.logging.info(
            "Metagraph updated, re-syncing hotkeys, dendrite pool and moving averages"
        )
        # Zero out all hotkeys that have been replaced.
        for uid, hotkey in enumerate(self.hotkeys):
            if hotkey != self.metagraph.hotkeys[uid]:
                self.scorer.reset_score(uid)  # hotkey has been replaced

        # Check to see if the metagraph has changed size.
        # If so, we need to add new hotkeys and moving averages.
        if len(self.hotkeys) < len(self.metagraph.hotkeys):
            self.scorer.resize(self.metagraph.n)

        # Update the hotkeys.
        self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)

        # TODO Delete the removed hotkeys from the ValidatorStorage

    def save_state(self):
        """Saves the state of the validator to a file."""
        bt.logging.info("Saving validator state.")

        # Save the state of the validator to file.
        torch.save(
            {
                "step": self.step,
                "hotkeys": self.hotkeys,
            },
            self.config.neuron.full_path + "/state.pt",
        )
        utils.serialize_to_file(
            self.scorer,
            os.path.join(self.config.neuron.full_path, Validator.SCORER_FILENAME),
        )

    def load_state(self):
        """Loads the state of the validator from a file."""
        bt.logging.info("Loading validator state.")

        # Load the state of the validator from file.
        state = torch.load(os.path.join(self.config.neuron.full_path, "/state.pt"))
        self.step = state["step"]
        self.hotkeys = state["hotkeys"]
        self.scorer = utils.deserialize_from_file(
            os.path.join(self.config.neuron.full_path, Validator.SCORER_FILENAME)
        )


# The main function parses the configuration and runs the validator.
if __name__ == "__main__":
    with Validator() as validator:
        while True:
            bt.logging.trace("Validator running...", time.time())
            time.sleep(5)
