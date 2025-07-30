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
import macrocosmos as mc
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
from vali_utils.organic_query_processor import OrganicQueryProcessor
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
        self.organic_processor = None

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

        # macrocosmos logging
        self.mcl_client = None
        self.mc_logger = None
        self.mc_run = None
        self.mc_logger_start = None

        # Add counter for evaluation cycles since startup
        self.evaluation_cycles_since_startup = 0

    def setup(self):
        """A one-time setup method that must be called before the Validator starts its main loop."""
        assert not self.is_setup, "Validator already setup."

        if not self.config.mclogger.off:
            self.loop.run_until_complete(self.new_mc_logger_run())
        else:
            bt.logging.warning("Not logging to Macrocosmos logger.")

        if not self.config.wandb.off:
            try:
                self.new_wandb_run()
            except Exception as e:
                bt.logging.error(f"Failed to initialize wandb: {str(e)}")
                bt.logging.warning("Continuing without wandb logging.")
                self.config.wandb.off = True

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

        self.organic_processor = OrganicQueryProcessor(
            wallet=self.wallet,
            metagraph=self.metagraph,
            evaluator=self.evaluator
        )
        
        self.is_setup = True

    def get_updated_lookup(self):
        try:
            bt.logging.info("Retrieving the latest dynamic lookup...")
            model = sync_run_retrieval(self.config)
            bt.logging.info("Model retrieved, updating value calculator...")
            self.evaluator.scorer.value_calculator = DataValueCalculator(model=model)
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

    async def new_mc_logger_run(self):
        """Creates a new Macrocosmos logger run to save information to."""
        try:            
            # Create a unique run id for this run
            now = dt.datetime.now()
            self.mc_logger_start = now
            run_id = now.strftime("%Y-%m-%d_%H-%M-%S")
            run_name = f"validator-{self.uid}-{run_id}"
            
            version_tag = self.get_version_tag()
            scraper_providers = self.get_scraper_providers()
            
            # Initialize the client
            self.mcl_client = mc.AsyncLoggerClient(app_name="sn13_mainnet_validator")
            self.mc_logger = self.mcl_client.logger
            
            # Start a new run
            self.mc_run = await self.mc_logger.init(
                project="data-universe-validators",
                tags=[f"validator", f"uid-{self.uid}", f"version-{version_tag}"],
                notes=f"Validator {self.uid} on netuid {self.config.netuid}",
                config={
                    "uid": self.uid,
                    "hotkey": self.wallet.hotkey.ss58_address,
                    "netuid": self.config.netuid,
                    "version": version_tag,
                    "scrapers": scraper_providers,
                    "chain_endpoint": self.config.subtensor.chain_endpoint,
                },
                name=run_name,
                description=f"Data Universe Validator {self.uid} - {run_id}",
            )
            
            bt.logging.success(f"Macrocosmos logger initialized successfully with run ID: {self.mc_run.id}")
            
        except Exception as e:
            bt.logging.error(f"Failed to initialize Macrocosmos logger: {str(e)}")
            self.config.mclogger.off = True  

    async def log_mc_metrics(self):
        """Log metrics to Macrocosmos logger"""
        try:
            if not self.mc_logger:
                return
                
            scorer = self.evaluator.get_scorer()
            scores = scorer.get_scores()
            credibilities = scorer.get_credibilities()
            
            # Prepare metrics
            metrics = {
                "step": self.step,
                "block": self.block,
                "uid": self.uid,
                "evaluation_cycles": self.evaluation_cycles_since_startup,
                "timestamp": dt.datetime.utcnow().isoformat(),
            }
            
            # Add individual miner scores
            top_miners = torch.topk(scores, len(scores))
            for i, (score, uid_idx) in enumerate(zip(top_miners.values, top_miners.indices)):
                metrics[f"top_miner_{i+1}_uid"] = int(uid_idx.item())
                metrics[f"top_miner_{i+1}_score"] = float(score.item())
                metrics[f"top_miner_{i+1}_credibility"] = float(credibilities[uid_idx].item())
            
            await self.mc_logger.log(metrics)
            
        except Exception as e:
            bt.logging.error(f"Error logging metrics to Macrocosmos logger: {str(e)}")

    async def finish_mc_logger_run(self):
        """Finish the current Macrocosmos logger run"""
        try:
            if self.mc_logger and self.mc_run:
                await self.mc_logger.finish()
                bt.logging.info("Macrocosmos logger run finished")
        except Exception as e:
            bt.logging.error(f"Error finishing Macrocosmos logger run: {str(e)}")
        finally:
            self.mc_logger = None
            self.mc_run = None
            self.mcl_client = None

    def new_wandb_run(self):
        """Creates a new wandb run to save information to."""
        # Create a unique run id for this run.
        now = dt.datetime.now()
        self.wandb_run_start = now
        run_id = now.strftime("%Y-%m-%d_%H-%M-%S")
        name = "testnet-validator-" + str(self.uid) + "-" + run_id
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

                # Log to Macrocosmos logger
                if not self.config.mclogger.off and self.mc_logger:
                    self.loop.run_until_complete(self.log_mc_metrics())

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
                        hours=12
                    ):
                        bt.logging.info(
                            "Current wandb run is more than 12 hours old. Starting a new run."
                        )
                        self.wandb_run.finish()
                        self.new_wandb_run()

                # Check if we should start a new mc logger run.
                if not self.config.mclogger.off and self.mc_logger_start:
                    if (dt.datetime.now() - self.mc_logger_start) >= dt.timedelta(
                        days=1
                    ):
                        bt.logging.info(
                            "Current mc logger run is more than 1 day old. Starting a new run."
                        )
                        self.loop.run_until_complete(self.finish_mc_logger_run())
                        self.loop.run_until_complete(self.new_mc_logger_run())


            # If someone intentionally stops the validator, it'll safely terminate operations.
            except KeyboardInterrupt:
                self.axon.stop()
                
                # Cleanup loggers
                if self.wandb_run:
                    self.wandb_run.finish()
                if self.mc_logger:
                    self.loop.run_until_complete(self.finish_mc_logger_run())

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

            # Cleanup loggers
            if self.wandb_run:
                self.wandb_run.finish()
            if self.mc_logger:
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # If loop is running, schedule the coroutine
                        asyncio.create_task(self.finish_mc_logger_run())
                    else:
                        # If loop is not running, run it
                        loop.run_until_complete(self.finish_mc_logger_run())
                except Exception as e:
                    bt.logging.error(f"Error cleaning up Macrocosmos logger: {str(e)}")
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
        Delegates to OrganicQueryProcessor for actual processing.
        """
        if not self.organic_processor:
            synapse.status = "error"
            synapse.meta = {"error": "Organic query processor not initialized"}
            synapse.data = []
            return synapse
        
        return await self.organic_processor.process_organic_query(synapse)

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
