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
import sys
import torch
import numpy as np
import asyncio
import threading
import time
import os
import wandb
import subprocess
from common.metagraph_syncer import MetagraphSyncer
from neurons.config import NeuronType, check_config, create_config
from dynamic_desirability.desirability_retrieval import sync_run_retrieval
from neurons import __spec_version__ as spec_version
from vali_utils.validator_s3_access import ValidatorS3Access
from rewards.data_value_calculator import DataValueCalculator
from rich.table import Table
from rich.console import Console
import warnings
import requests
from dotenv import load_dotenv

import bittensor as bt
from typing import Tuple
import json
from common.organic_protocol import OrganicRequest
from common.data import DataSource, DataLabel, DataEntity
from common import constants
from common.protocol import OnDemandRequest
from common import utils
from scraping.scraper import ScrapeConfig, ValidationResult
from common.date_range import DateRange
from scraping.provider import ScraperProvider
from scraping.x.enhanced_apidojo_scraper import EnhancedApiDojoTwitterScraper
from vali_utils.miner_evaluator import MinerEvaluator
from vali_utils.load_balancer.validator_registry import ValidatorRegistry
import random

load_dotenv()
# Temporary solution to getting rid of annoying bittensor trace logs
original_trace = bt.logging.trace


def filtered_trace(message, *args, **kwargs):
    if "Unexpected header key encountered" not in message:
        original_trace(message, *args, **kwargs)


bt.logging.trace = filtered_trace 

# Filter out the specific deprecation warning from datetime.utcnow()
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message="datetime.datetime.utcnow() is deprecated"
)
# import datetime after the warning filter
import datetime as dt

bt.logging.set_trace(True)  # TODO remove it in future


class Validator:
    def __init__(
        self,
        metagraph_syncer: MetagraphSyncer,
        evaluator: MinerEvaluator,
        uid: int,
        config=None,
        subtensor: bt.subtensor = None,
    ):
        """

        Args:
            metagraph_syncer (MetagraphSyncer): The syncer must already be initialized, with the initial metagraphs synced.
            evaluator (MinerEvaluator): The evaluator to evaluate miners.
            uid (int): The uid of the validator.
            config (_type_, optional): _description_. Defaults to None.
            subtensor (bt.subtensor, optional): _description_. Defaults to None.
        """
        self.metagraph_syncer = metagraph_syncer
        self.evaluator = evaluator
        self.uid = uid

        self.config = copy.deepcopy(config or create_config(NeuronType.VALIDATOR))
        check_config(self.config)

        bt.logging.info(self.config)

        # The wallet holds the cryptographic key pairs for the miner.
        self.wallet = bt.wallet(config=self.config)
        bt.logging.info(f"Wallet: {self.wallet}.")

        # The subtensor is our connection to the Bittensor blockchain.
        self.subtensor = subtensor or bt.subtensor(config=self.config)
        bt.logging.info(f"Subtensor: {self.subtensor}.")

        # The metagraph holds the state of the network, letting us know about other validators and miners.
        self.metagraph = self.metagraph_syncer.get_metagraph(self.config.netuid)
        self.metagraph_syncer.register_listener(
            self._on_metagraph_updated, netuids=[self.config.netuid]
        )
        bt.logging.info(f"Metagraph: {self.metagraph}.")
        
        self.validator_registry = ValidatorRegistry(metagraph=self.metagraph, organic_whitelist=self.config.organic_whitelist)

        # Create asyncio event loop to manage async tasks.
        self.loop = asyncio.get_event_loop()
        self.axon = None
        self.api = None
        self.step = 0
        self.wandb_run_start = None
        self.wandb_run = None

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: threading.Thread = None
        self.lock = threading.RLock()
        self.last_eval_time = dt.datetime.utcnow()
        self.last_weights_set_time = dt.datetime.utcnow()
        self.is_setup = False

        # Add counter for evaluation cycles since startup
        self.evaluation_cycles_since_startup = 0

    def setup(self):
        """A one-time setup method that must be called before the Validator starts its main loop."""
        assert not self.is_setup, "Validator already setup."

        if not self.config.wandb.off:
            self.new_wandb_run()
        else:
            bt.logging.warning("Not logging to wandb.")

        bt.logging.info("Setting up validator.")

        # Load any state from previous runs.
        self.load_state()

        # Getting latest dynamic lookup
        self.get_updated_lookup()

        # TODO: Configure this to expose access to data to neurons on certain subnets.
        # Serve axon to enable external connections.
        if not self.config.neuron.axon_off:
            self.serve_axon()
        else:
            bt.logging.warning("Axon off, not serving ip to chain.")

        self.is_setup = True

    def get_updated_lookup(self):
        try:
            bt.logging.info("Retrieving the latest dynamic lookup...")
            model = sync_run_retrieval(self.config)
            bt.logging.info("Model retrieved, updating value calculator...")
            self.evaluator.scorer.value_calculator = DataValueCalculator(model=model)
            bt.logging.info(f"Desirable data list: {model}")
            bt.logging.info(f"Evaluator: {self.evaluator.scorer.value_calculator}")
            bt.logging.info(f"Updated dynamic lookup at {dt.datetime.utcnow()}")
        except Exception as e:
            bt.logging.error(f"Error in get_updated_lookup: {str(e)}")
            bt.logging.exception("Exception details:")

    def get_version_tag(self):
        """Fetches version tag"""
        try:
            subprocess.run(['git', 'fetch', '--tags'], check=True)
            version_tag = subprocess.check_output(['git', 'describe', '--tags', '--abbrev=0']).strip().decode('utf-8')
            return version_tag
        
        except subprocess.CalledProcessError as e:
            print(f"Couldn't fetch latest version tag: {e}")
            return "error"
        
    def get_scraper_providers(self):
        """Fetches a validator's scraper providers to display in WandB logs."""
        scrapers = self.evaluator.PREFERRED_SCRAPERS
        return scrapers

    def new_wandb_run(self):
        """Creates a new wandb run to save information to."""
        # Create a unique run id for this run.
        now = dt.datetime.now()
        self.wandb_run_start = now
        run_id = now.strftime("%Y-%m-%d_%H-%M-%S")
        name = "validator-" + str(self.uid) + "-" + run_id
        version_tag = self.get_version_tag()
        scraper_providers = self.get_scraper_providers()

        self.wandb_run = wandb.init(
            name=name,
            project="data-universe-validators",
            entity="macrocosmos",
            config={
                "uid": self.uid,
                "hotkey": self.wallet.hotkey.ss58_address,
                "run_name": run_id,
                "type": "validator",
                "version": version_tag,
                "scrapers": scraper_providers
            },
            allow_val_change=True,
            anonymous="allow",
        )

        bt.logging.debug(f"Started a new wandb run: {name}")

    def run(self):
        """
        Initiates and manages the main loop for the validator, which

        1. Evaluates miners
        2. Periodically writes the latest scores to the chain
        3. Saves state
        """
        assert self.is_setup, "Validator must be setup before running."

        # Check that validator is registered on the network.
        utils.assert_registered(self.wallet, self.metagraph)

        bt.logging.info(
            f"Running validator {self.axon} on network: {self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}."
        )

        bt.logging.info(f"Validator starting at block: {self.block}.")

        # This loop maintains the validator's operations until intentionally stopped.
        while not self.should_exit:
            try:
                bt.logging.debug(
                    f"Validator running on step({self.step}) block({self.block})."
                )

                # Ensure validator hotkey is still registered on the network.
                utils.assert_registered(self.wallet, self.metagraph)

                # Evaluate the next batch of miners.
                next_batch_delay_secs = self.loop.run_until_complete(
                    self.evaluator.run_next_eval_batch()
                )
                self._on_eval_batch_complete()

                # Set the next batch start time.
                next_batch_start_time = dt.datetime.utcnow() + dt.timedelta(
                    seconds=next_batch_delay_secs
                )

                # Maybe set weights.
                if self.should_set_weights():
                    self.set_weights()

                # Always save state.
                self.save_state()

                # Update to the latest desirability list after each evaluation.
                self.get_updated_lookup()

                self.step += 1

                # Now that we've finished a full evaluation loop, compute how long we should
                # wait until the next evaluation loop.
                wait_time = max(
                    0,
                    (next_batch_start_time - dt.datetime.utcnow()).total_seconds(),
                )
                if wait_time > 0:
                    bt.logging.info(
                        f"Finished full evaluation loop early. Waiting {wait_time} seconds until running next evaluation loop."
                    )
                    time.sleep(wait_time)

                # Check if we should start a new wandb run.
                if not self.config.wandb.off:
                    if (dt.datetime.now() - self.wandb_run_start) >= dt.timedelta(
                        days=1
                    ):
                        bt.logging.info(
                            "Current wandb run is more than 1 day old. Starting a new run."
                        )
                        self.wandb_run.finish()
                        self.new_wandb_run()

            # If someone intentionally stops the validator, it'll safely terminate operations.
            except KeyboardInterrupt:
                self.axon.stop()
                bt.logging.success("Validator killed by keyboard interrupt.")
                sys.exit()

            # In case of unforeseen errors, the validator will log the error and continue operations.
            except Exception as err:
                bt.logging.error("Error during validation", str(err))

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
            bt.logging.debug("Started.")

    def stop_run_thread(self):
        """
        Stops the validator's operations that are running in the background thread.
        """
        if self.is_running:
            bt.logging.debug("Stopping validator in background thread.")
            self.should_exit = True
            self.thread.join(5)
            self.is_running = False
            bt.logging.debug("Stopped.")

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
            if self.wandb_run:
                self.wandb_run.finish()
            bt.logging.debug("Stopped.")

    def serve_axon(self):
        """Serve axon to enable external connections."""

        try:
            self.axon = bt.axon(wallet=self.wallet, config=self.config)
            # Import and attach organic synapse
            self.axon.attach(
                forward_fn=self.process_organic_query,
                blacklist_fn=self.organic_blacklist,
                priority_fn=self.organic_priority
            )

            self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor).start()
            if self.config.neuron.api_on:
                try:
                    bt.logging.info("Starting Validator API...")
                    from vali_utils.api.server import ValidatorAPI
                    self.api = ValidatorAPI(self, port=self.config.neuron.api_port)
                    self.api.start()
                    # Start monitoring to auto-restart if it fails
                    self._start_api_monitoring()

                except ValueError as e:
                    bt.logging.error(f"Failed to start API: {str(e)}")
                    bt.logging.info("Validator will continue running without API.")
                    self.config.neuron.api_on = False

            bt.logging.info(
                f"Serving validator axon {self.axon} on network: {self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}."
            )
        except Exception as e:
            bt.logging.error(f"Failed to setup Axon: {e}.")
            sys.exit(1)

    def _on_metagraph_updated(self, metagraph: bt.metagraph, netuid: int):
        """Processes an update to the metagraph"""
        with self.lock:
            assert netuid == self.config.netuid
            self.metagraph = copy.deepcopy(metagraph)

    def _on_eval_batch_complete(self):
        with self.lock:
            self.evaluation_cycles_since_startup += 1
            self.last_eval_time = dt.datetime.utcnow()

    def is_healthy(self) -> bool:
        """Returns true if the validator is healthy and is evaluating Miners."""
        with self.lock:
            return dt.datetime.utcnow() - self.last_eval_time < dt.timedelta(minutes=35)

    def should_set_weights(self) -> bool:
        # Check if enough epoch blocks have elapsed since the last epoch.
        if self.config.neuron.disable_set_weights:
            return False

        with self.lock:
            # After a restart, we want to wait two evaluation cycles
            # Check if we've completed at least two evaluation cycles since startup
            if not self.evaluation_cycles_since_startup:
                self.evaluation_cycles_since_startup = 0
                bt.logging.info("Initializing evaluation cycles counter for delayed weight setting")

            #  if we've completed fewer than the allotted number of evaluation cycles, don't set weights
            if self.evaluation_cycles_since_startup < constants.EVALUATION_ON_STARTUP:

                bt.logging.info(
                    f"Skipping weight setting - completed {self.evaluation_cycles_since_startup}/15 evaluation cycles since startup")
                return False

            # Normal 20-minute interval check for subsequent weight settings
            return dt.datetime.utcnow() - self.last_weights_set_time > dt.timedelta(minutes=20)

    def _start_api_monitoring(self):
        """Start a lightweight monitor to auto-restart API if it becomes unreachable"""

        master_key = os.getenv('MASTER_KEY')

        def monitor_api():
            consecutive_failures = 0
            max_failures = 3  # Restart after 3 consecutive failures

            while not self.should_exit:
                if not hasattr(self, 'api') or not self.api:
                    time.sleep(60 * 20)
                    continue

                try:
                    # Try a simple local request to check if API is responding
                    response = requests.get(
                        f"http://localhost:{self.config.neuron.api_port}/api/v1/monitoring/system-status",
                        headers={"X-API-Key": master_key},
                        timeout=10
                    )

                    if response.status_code == 200:
                        # API is working, reset failure counter
                        consecutive_failures = 0
                    else:
                        # HTTP error
                        consecutive_failures += 1
                        bt.logging.warning(f"API health check returned status {response.status_code}")
                except requests.RequestException:
                    # Connection error (most likely API is down)
                    consecutive_failures += 1
                    bt.logging.warning(f"API server not responding ({consecutive_failures}/{max_failures})")

                # If too many consecutive failures, restart API
                if consecutive_failures >= max_failures:
                    bt.logging.warning(f"API server unresponsive for {consecutive_failures} checks, restarting...")

                    try:
                        # Stop API if it's running
                        if hasattr(self.api, 'stop'):
                            self.api.stop()

                        # Wait a moment
                        time.sleep(2)

                        # Start API again
                        if hasattr(self.api, 'start'):
                            self.api.start()

                        bt.logging.info("API server restarted")
                        consecutive_failures = 0
                    except Exception as e:
                        bt.logging.error(f"Error restarting API server: {str(e)}")

                # Check every 30 minutes
                time.sleep(30 * 60)

        # Start monitoring in background thread
        thread = threading.Thread(target=monitor_api, daemon=True)
        thread.start()
        bt.logging.info("API monitoring started")

    def set_weights(self):
        """
        Sets the validator weights to the metagraph hotkeys based on the scores it has received from the miners. The weights determine the trust and incentive level the validator assigns to miner nodes on the network.
        """
        bt.logging.info("Attempting to set weights.")

        scorer = self.evaluator.get_scorer()
        scores = scorer.get_scores()
        credibilities = scorer.get_credibilities()

        # Check if scores contains any NaN values and log a warning if it does.
        if torch.isnan(scores).any():
            bt.logging.warning(
                f"Scores contain NaN values. This may be due to a lack of responses from miners, or a bug in your reward functions."
            )

        # Calculate the average reward for each uid across non-zero values.
        # Replace any NaN values with 0.
        raw_weights = torch.nn.functional.normalize(scores, p=1, dim=0)

        # Process the raw weights to final_weights via subtensor limitations.
        (
            processed_weight_uids,
            processed_weights,
        ) = bt.utils.weight_utils.process_weights_for_netuid(
            uids=torch.tensor(self.metagraph.uids, dtype=torch.int64).to("cpu"),
            weights=raw_weights.detach().cpu().numpy().astype(np.float32),
            netuid=self.config.netuid,
            subtensor=self.subtensor,
            metagraph=self.metagraph,
        )

        table = Table(title="All Weights")
        table.add_column("uid", justify="right", style="cyan", no_wrap=True)
        table.add_column("weight", style="magenta")
        table.add_column("score", style="magenta")
        table.add_column("credibility", style="magenta")
        uids_and_weights = list(
            zip(processed_weight_uids.tolist(), processed_weights.tolist())
        )
        # Sort by weights descending.
        sorted_uids_and_weights = sorted(
            uids_and_weights, key=lambda x: x[1], reverse=True
        )
        for uid, weight in sorted_uids_and_weights:
            table.add_row(
                str(uid),
                str(round(weight, 4)),
                str(int(scores[uid].item())),
                str(round(credibilities[uid].item(), 4)),
            )
        console = Console()
        console.print(table)

        # Set the weights on chain via our subtensor connection.
        self.subtensor.set_weights(
            wallet=self.wallet,
            netuid=self.config.netuid,
            uids=processed_weight_uids,
            weights=processed_weights,
            wait_for_finalization=False,
            version_key=spec_version,
        )

        with self.lock:
            self.last_weights_set_time = dt.datetime.utcnow()

        bt.logging.success("Finished setting weights.")

    @property
    def block(self):
        return utils.ttl_get_block(self)

    def save_state(self):
        """Saves the state of the validator to a file."""
        bt.logging.trace("Saving validator state.")

        self.evaluator.save_state()

    def load_state(self):
        """Loads the state of the validator from a file."""
        bt.logging.info("Loading validator state.")

        self.evaluator.load_state()

    async def process_organic_query(self, synapse: OrganicRequest) -> OrganicRequest:
        """
        Process organic queries through the validator axon.
        This is the main handler that will be attached to the validator's axon.

        Args:
            synapse: The OrganicRequest synapse containing query parameters

        Returns:
            The modified synapse with response data
        """
        bt.logging.info(f"Processing organic query for source: {synapse.source}")

        try:
            # Constants for miner selection and validation
            NUM_MINERS_TO_QUERY = 5
            VALIDATION_PROBABILITY = 0.05
            MIN_PENALTY = 0.01
            MAX_PENALTY = 0.05

            # Get all miner UIDs and sort by incentive
            miner_uids = utils.get_miner_uids(self.metagraph, self.uid, 10000)
            miner_scores = [(uid, float(self.metagraph.I[uid])) for uid in miner_uids]
            miner_scores.sort(key=lambda x: x[1], reverse=True)

            # Take top 60% of miners (but at least 5 if available)
            top_count = max(5, int(len(miner_scores) * 0.6))
            top_miners = miner_scores[:top_count]

            if len(top_miners) < 2:
                synapse.status = "error"
                synapse.meta = {"error": "Not enough miners available to service request"}
                synapse.data = []
                return synapse

            # Select a diverse set of miners
            selected_miners = []
            selected_coldkeys = set()

            # First, try to get a diverse set from top miners
            while len(selected_miners) < NUM_MINERS_TO_QUERY and top_miners:
                idx = random.randint(0, len(top_miners) - 1)
                uid, _ = top_miners.pop(idx)
                coldkey = self.metagraph.coldkeys[uid]

                # Only add if we haven't selected too many miners from this coldkey
                if coldkey not in selected_coldkeys or len(selected_coldkeys) < 2:
                    selected_miners.append(uid)
                    selected_coldkeys.add(coldkey)

            # If we couldn't get enough diverse miners, just take what we have
            if len(selected_miners) < 1:
                bt.logging.warning(f"Could only select {len(selected_miners)} diverse miners")
                # Get any miners to make up the numbers
                for uid in miner_uids:
                    if uid not in selected_miners:
                        selected_miners.append(uid)
                        if len(selected_miners) >= NUM_MINERS_TO_QUERY:
                            break

            bt.logging.info(f"Selected {len(selected_miners)} miners for on-demand query")

            # Create OnDemandRequest synapse to forward to miners
            on_demand_synapse = OnDemandRequest(
                source=DataSource[synapse.source.upper()],
                usernames=synapse.usernames,
                keywords=synapse.keywords,
                start_date=synapse.start_date,
                end_date=synapse.end_date,
                limit=synapse.limit,
                version=constants.PROTOCOL_VERSION
            )

            # Query selected miners
            bt.logging.info(f"Querying miners: {selected_miners}")
            miner_responses = {}
            miner_data_counts = {}

            async with bt.dendrite(wallet=self.wallet) as dendrite:
                axons = [self.metagraph.axons[uid] for uid in selected_miners]
                responses = await dendrite.forward(
                    axons=axons,
                    synapse=on_demand_synapse,
                    timeout=30
                )

                # Process responses
                for i, response in enumerate(responses):
                    if i < len(selected_miners):
                        uid = selected_miners[i]
                        hotkey = self.metagraph.hotkeys[uid]
                        
                        # Check if response exists and has a valid status
                        if response is not None and hasattr(response, 'status'):
                            data = getattr(response, 'data', [])
                            data_count = len(data) if data else 0
                            
                            miner_responses[uid] = response
                            miner_data_counts[uid] = data_count
                            
                            bt.logging.info(f"Miner {uid} ({hotkey}) returned {data_count} items")
                        else:
                            bt.logging.warning(f"Miner {uid} ({hotkey}) failed to respond properly")

            # Step 1: Check if we have a consensus on "no data"
            no_data_consensus = all(count == 0 for count in miner_data_counts.values())
            if no_data_consensus:
                bt.logging.info("All miners returned no data - performing verification check")

                # Perform a quick check to verify if data should exist
                try:
                    # For X data, use exactly the same approach as miners
                    if on_demand_synapse.source == DataSource.X:
                        # Initialize the enhanced scraper directly as miners do
                        scraper = EnhancedApiDojoTwitterScraper()
                    elif on_demand_synapse.source == DataSource.REDDIT:
                        # For other sources, use the standard provider
                        scraper_id = self.evaluator.PREFERRED_SCRAPERS.get(on_demand_synapse.source)
                        if not scraper_id:
                            bt.logging.warning(f"No preferred scraper for source {on_demand_synapse.source}")
                            # Return empty result with consensus
                            synapse.status = "success"
                            synapse.data = []
                            synapse.meta = {
                                "miners_queried": len(selected_miners),
                                "consensus": "no_data"
                            }
                            return synapse
                        scraper = ScraperProvider().get(scraper_id)
                    else:
                        bt.logging.warning(f"No preferred scraper for source {on_demand_synapse.source}")
                        synapse.status = "success"
                        synapse.data = []
                        synapse.meta = {
                            "miners_queried": len(selected_miners),
                            "consensus": "no_data"
                        }
                        return synapse

                    if not scraper:
                        bt.logging.warning(f"Could not initialize scraper for {on_demand_synapse.source}")
                        synapse.status = "success"
                        synapse.data = []
                        synapse.meta = {
                            "miners_queried": len(selected_miners),
                            "consensus": "no_data"
                        }
                        return synapse

                    # Create scrape config with limited scope (only check for a few items)
                    # For X data, combine keywords and usernames with appropriate label formatting
                    labels = []
                    if on_demand_synapse.keywords:
                        labels.extend([DataLabel(value=k) for k in on_demand_synapse.keywords])
                    if on_demand_synapse.usernames:
                        # Ensure usernames have @ prefix
                        labels.extend([DataLabel(value=f"@{u.strip('@')}" if not u.startswith('@') else u) for u in
                                       on_demand_synapse.usernames])

                    start_date = utils.parse_iso_date(on_demand_synapse.start_date) if on_demand_synapse.start_date else dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
                    end_date = utils.parse_iso_date(on_demand_synapse.end_date) if on_demand_synapse.end_date else dt.datetime.now(dt.timezone.utc)

                    # Create scrape config matching the miner's configuration
                    verify_config = ScrapeConfig(
                        entity_limit=synapse.limit,  
                        date_range=DateRange(
                            start=start_date,
                            end=end_date
                        ),
                        labels=labels,
                    )

                    # For X source, replicate exactly what miners do in handle_on_demand
                    if on_demand_synapse.source == DataSource.X:
                        await scraper.scrape(verify_config)

                        # Get enhanced content
                        enhanced_content = scraper.get_enhanced_content()

                        # IMPORTANT: Convert EnhancedXContent to DataEntity to maintain protocol compatibility
                        # while keeping the rich data in serialized form
                        verification_data = []
                        for content in enhanced_content:
                            # Convert to DataEntity but store full rich content in serialized form
                            api_response = content.to_api_response()
                            data_entity = DataEntity(
                                uri=content.url,
                                datetime=content.timestamp,
                                source=DataSource.X,
                                label=DataLabel(
                                    value=content.tweet_hashtags[0].lower()) if content.tweet_hashtags else None,
                                # Store the full enhanced content as serialized JSON in the content field
                                content=json.dumps(api_response).encode('utf-8'),
                                content_size_bytes=len(json.dumps(api_response))
                            )
                            verification_data.append(data_entity)
                    else:
                        # For other sources, use standard scrape
                        verification_data = await scraper.scrape(verify_config)

                    # If we found data but miners returned none, they should be penalized
                    if verification_data:
                        bt.logging.warning(
                            f"Miners returned no data, but validator found {len(verification_data)} items")

                        # Apply penalties to non-responsive miners
                        non_responsive_uids = [uid for uid in selected_miners if uid not in miner_responses]
                        if non_responsive_uids:
                            bt.logging.info(f"Applying penalties to {len(non_responsive_uids)} non-responsive miners")
                            for uid in non_responsive_uids:
                                bt.logging.info(f"Applying a 5% credibility penalty to miner {uid} for not responding.")
                                
                                # Update the miner's score with the validation results
                                self.evaluator.scorer.apply_ondemand_penalty(uid) 

                        # Process the verification data to match exactly what miners would return
                        processed_data = []
                        for item in verification_data:
                            if on_demand_synapse.source == DataSource.X:
                                # For X data, miners store the API response as JSON in the content field
                                # We need to decode it and parse it as JSON to match what miners return
                                try:
                                    json_content = item.content.decode('utf-8') if isinstance(item.content,
                                                                                              bytes) else item.content
                                    parsed_content = json.loads(json_content)
                                    processed_data.append(parsed_content)
                                except Exception as e:
                                    bt.logging.error(f"Error parsing X content: {str(e)}")
                                    # Fallback if parsing fails
                                    processed_data.append({
                                        'uri': item.uri,
                                        'datetime': item.datetime.isoformat(),
                                        'source': DataSource(item.source).name,
                                        'label': item.label.value if item.label else None,
                                        'content': str(item.content)[:1000]  # Truncate for safety
                                    })
                            else:
                                # For other sources, use standard format
                                item_dict = {
                                    'uri': item.uri,
                                    'datetime': item.datetime.isoformat(),
                                    'source': DataSource(item.source).name,
                                    'label': item.label.value if item.label else None,
                                    'content': item.content.decode('utf-8') if isinstance(item.content, bytes) else str(
                                        item.content)
                                }
                                processed_data.append(item_dict)

                        synapse.status = "warning"
                        synapse.data = processed_data[:synapse.limit]
                        synapse.meta = {
                            "miners_queried": len(selected_miners),
                            "miners_responded": len(miner_responses),
                            "verification_message": "Miners returned no data, but data was found. Results are from validator's direct check.",
                            "items_returned": len(processed_data)
                        }
                        return synapse
                except Exception as e:
                    bt.logging.error(f"Error during verification check: {str(e)}")

                # If verification failed or no data was found, return original empty result
                synapse.status = "success"
                synapse.data = []
                synapse.meta = {
                    "miners_queried": len(selected_miners),
                    "consensus": "no_data"
                }
                return synapse

            # Step 2: Check consistency of responses from miners that returned data
            miners_with_data = [uid for uid, count in miner_data_counts.items() if count > 0]

            if not miners_with_data:
                synapse.status = "error"
                synapse.meta = {"error": "No miners returned valid data"}
                synapse.data = []
                return synapse

            # Get the median data count as a reference point
            data_counts = [count for count in miner_data_counts.values() if count > 0]
            median_count = sorted(data_counts)[len(data_counts) // 2]

            # Define consistency threshold (within 30% of median is considered consistent)
            consistency_threshold = 0.3

            consistent_miners = {}
            inconsistent_miners = {}

            for uid in miners_with_data:
                count = miner_data_counts[uid]
                # Check if count is within threshold of median
                if median_count > 0 and abs(count - median_count) / median_count <= consistency_threshold:
                    consistent_miners[uid] = miner_responses[uid]
                else:
                    inconsistent_miners[uid] = miner_responses[uid]

            bt.logging.info(
                f"Found {len(consistent_miners)} consistent miners and {len(inconsistent_miners)} inconsistent miners")

            # Step 3: Should we validate? (random chance or if consistency is poor)
            should_validate = random.random() < VALIDATION_PROBABILITY or len(consistent_miners) < len(
                miners_with_data) // 2

            # Step 4: Validation and penalties
            validated_miners = {}

            if should_validate:
                bt.logging.info("Performing validation on returned data")
                # We'll only validate a subset of data to minimize API usage
                for uid, response in list(consistent_miners.items()) + list(inconsistent_miners.items()):
                    try:
                        # Only validate up to 2 items per miner to reduce API cost
                        data_to_validate = response.data[:2] if hasattr(response, 'data') and response.data else []

                        if not data_to_validate:
                            continue

                        # Use scraper to validate data
                        scraper_id = self.evaluator.PREFERRED_SCRAPERS.get(on_demand_synapse.source)
                        if not scraper_id:
                            bt.logging.warning(f"No preferred scraper for source {on_demand_synapse.source}")
                            continue

                        scraper = ScraperProvider().get(scraper_id)
                        if not scraper:
                            bt.logging.warning(f"Could not initialize scraper {scraper_id}")
                            continue

                        # Validate the data
                        validation_results = await scraper.validate(data_to_validate)

                        # Calculate validation success rate
                        valid_count = sum(1 for r in validation_results if r.is_valid)
                        validation_rate = valid_count / len(validation_results) if validation_results else 0

                        validated_miners[uid] = validation_rate

                        bt.logging.info(f"Miner {uid} validation rate: {validation_rate:.2f}")

                        # Apply penalty to miners with poor validation
                        if validation_rate < 0.5:  # Less than 50% valid
                            # This is a soft penalty - calculated based on how bad the data is
                            # Scale from MIN_PENALTY to MAX_PENALTY based on validation_rate
                            penalty = MIN_PENALTY + (0.5 - validation_rate) * 2 * (MAX_PENALTY - MIN_PENALTY)

                            # Ensure penalty doesn't make credibility negative
                            current_cred = self.evaluator.scorer.get_miner_credibility(uid)
                            safe_penalty = min(current_cred * 0.2, penalty)  # Never reduce by more than 20%

                            bt.logging.info(f"Applying penalty of {safe_penalty:.4f} to miner {uid}")

                            # Create a validation result for the scorer
                            validation_result = ValidationResult(
                                is_valid=False,
                                reason="Failed on-demand data validation",
                                content_size_bytes_validated=sum(e.content_size_bytes for e in data_to_validate)
                            )

                            # Update the miner's score with this result
                            index = self.evaluator.storage.read_miner_index(self.metagraph.hotkeys[uid])
                            self.evaluator.scorer.on_miner_evaluated(uid, index, [validation_result])

                            # Remove this miner from consistent miners if it was there
                            if uid in consistent_miners:
                                del consistent_miners[uid]
                                inconsistent_miners[uid] = response

                    except Exception as e:
                        bt.logging.error(f"Error validating data from miner {uid}: {str(e)}")

            # Step 5: Select best data to return to user
            best_data = []
            best_meta = {}

            # First preference: data from validated miners with high validation rates
            if validated_miners:
                # Find the miner with the highest validation rate
                best_uid = max(validated_miners.items(), key=lambda x: x[1])[0]
                best_miner_response = miner_responses[best_uid]
                best_data = best_miner_response.data if hasattr(best_miner_response, 'data') else []
                best_meta = {
                    "source": "validated",
                    "miner_uid": best_uid,
                    "miner_hotkey": self.metagraph.hotkeys[best_uid],
                    "validation_rate": validated_miners[best_uid]
                }

            # Second preference: data from consistent miners (if no validation was done or no miners passed validation)
            elif consistent_miners:
                # Pick the miner with the most data (but within reason)
                best_uid = max(consistent_miners.keys(), key=lambda uid: miner_data_counts[uid])
                best_miner_response = consistent_miners[best_uid]
                best_data = best_miner_response.data if hasattr(best_miner_response, 'data') else []
                best_meta = {
                    "source": "consistent",
                    "miner_uid": best_uid,
                    "miner_hotkey": self.metagraph.hotkeys[best_uid]
                }

            # Last resort: take data from any miner that returned something
            elif miners_with_data:
                # Take the miner with median data count
                median_uid = sorted(miners_with_data, key=lambda uid: miner_data_counts[uid])[
                    len(miners_with_data) // 2]
                best_miner_response = miner_responses[median_uid]
                best_data = best_miner_response.data if hasattr(best_miner_response, 'data') else []
                best_meta = {
                    "source": "inconsistent",
                    "miner_uid": median_uid,
                    "miner_hotkey": self.metagraph.hotkeys[median_uid]
                }

            # Process the data for return
            processed_data = []
            for item in best_data:
                # For X content from miners, item.content already contains the serialized API response
                # which needs to be parsed as JSON
                if on_demand_synapse.source == DataSource.X:
                    try:
                        # Extract the content as string and parse it as JSON
                        if hasattr(item, 'content') and item.content:
                            content_str = item.content.decode('utf-8') if isinstance(item.content,
                                                                                     bytes) else item.content
                            try:
                                item_dict = json.loads(content_str)
                                processed_data.append(item_dict)
                                continue
                            except json.JSONDecodeError:
                                # If JSON parsing fails, fall through to the standard processing
                                pass
                    except Exception as e:
                        bt.logging.error(f"Error processing X content: {str(e)}")

                # Standard processing for non-X content or if JSON parsing failed
                item_dict = {
                    'uri': item.uri if hasattr(item, 'uri') else None,
                    'datetime': item.datetime.isoformat() if hasattr(item, 'datetime') else None,
                    'source': DataSource(item.source).name if hasattr(item, 'source') else None,
                    'label': item.label.value if hasattr(item, 'label') and item.label else None,
                    'content': item.content.decode('utf-8') if hasattr(item, 'content') and isinstance(item.content,
                                                                                                       bytes) else
                    item.content if hasattr(item, 'content') else None
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

            # Add summary info to meta
            best_meta.update({
                "miners_queried": len(selected_miners),
                "miners_responded": len(miner_responses),
                "consistent_miners": len(consistent_miners),
                "inconsistent_miners": len(inconsistent_miners),
                "validated_miners": len(validated_miners) if should_validate else 0,
                "items_returned": len(unique_data)
            })

            synapse.status = "success"
            synapse.data = unique_data[:synapse.limit]
            synapse.meta = best_meta
            return synapse

        except Exception as e:
            bt.logging.error(f"Error in organic query: {str(e)}")
            synapse.status = "error"
            synapse.meta = {"error": str(e)}
            synapse.data = []
            return synapse

    async def organic_blacklist(self, synapse: OrganicRequest) -> Tuple[bool, str]:
        """
        Simplified blacklist function that only checks whitelist membership
        """
        # Only allow hotkeys in the whitelist
        if hasattr(self.config, 'organic_whitelist') and self.config.organic_whitelist:
            if synapse.dendrite.hotkey in self.config.organic_whitelist:
                return False, "Request accepted from whitelisted hotkey"
            else:
                return True, f"Sender {synapse.dendrite.hotkey} not in whitelist"

        # If no whitelist is defined, reject all requests
        return True, "No whitelist configured"

    def organic_priority(self, synapse: OrganicRequest) -> float:
        caller_uid = self.metagraph.hotkeys.index(synapse.dendrite.hotkey)
        priority = float(self.metagraph.S[caller_uid])
        bt.logging.trace(
            f"Prioritizing {synapse.dendrite.hotkey} with value: {priority}.",
        )
        return priority


def main():
    """Main constructs the validator with its dependencies."""

    config = create_config(NeuronType.VALIDATOR)
    check_config(config=config)

    bt.logging(config=config, logging_dir=config.full_path)

    subtensor = bt.subtensor(config=config)
    metagraph = subtensor.metagraph(netuid=config.netuid)
    wallet = bt.wallet(config=config)

    # Get the wallet's UID, if registered.
    utils.assert_registered(wallet, metagraph)
    uid = utils.get_uid(wallet, metagraph)

    # Create the metagraph syncer and perform the initial sync.
    metagraph_syncer = MetagraphSyncer(subtensor, config={config.netuid: 20 * 60})
    # Perform an initial sync of all tracked metagraphs.
    metagraph_syncer.do_initial_sync()
    metagraph_syncer.start()

    s3_reader = ValidatorS3Access(wallet=wallet, s3_auth_url=config.s3_auth_url)
    evaluator = MinerEvaluator(
        config=config, uid=uid, metagraph_syncer=metagraph_syncer, s3_reader=s3_reader
    )

    with Validator(
        metagraph_syncer=metagraph_syncer,
        evaluator=evaluator,
        uid=uid,
        config=config,
        subtensor=subtensor,
    ) as validator:
        while True:
            if not validator.is_healthy():
                bt.logging.error("Validator is unhealthy. Restarting.")
                # Sys.exit() may not shutdown the process because it'll wait for other threads
                # to complete. Use os._exit() instead.
                os._exit(1)
            bt.logging.trace("Validator running...", time.time())
            time.sleep(60)


# The main function parses the configuration and runs the validator.
if __name__ == "__main__":
    main()
