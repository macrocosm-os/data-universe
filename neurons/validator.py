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

from collections import defaultdict
import copy
import json
import random
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
from data_universe_api import (
    ListJobsWithSubmissionsForValidationRequest,
    DataUniverseApiClient,
    OnDemandMinerUpload,
)
from vali_utils.validator_s3_access import ValidatorS3Access
from rewards.data_value_calculator import DataValueCalculator
from rich.table import Table
from rich.console import Console
import warnings
import requests
from dotenv import load_dotenv
import bittensor as bt
from typing import Dict
from common.organic_protocol import OrganicRequest
from common import constants
from common import utils
from vali_utils.miner_evaluator import MinerEvaluator
from vali_utils.on_demand.organic_query_processor import OrganicQueryProcessor

from vali_utils import metrics

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
    message="datetime.datetime.utcnow() is deprecated",
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

        # Add counter for evaluation cycles since startup
        self.evaluation_cycles_since_startup = 0
        self.processed_job_ids_cache = utils.LRUSet(capacity=10_000)

        self.on_demand_thread: threading.Thread = None

    def _create_validation_context_from_job(self, job):
        """Convert OnDemandJob to OrganicRequest for validation purposes"""
        from common.organic_protocol import OrganicRequest

        # Extract usernames based on job platform
        usernames = []
        if hasattr(job.job, 'usernames') and job.job.usernames:
            usernames = job.job.usernames
        elif hasattr(job.job, 'channels') and job.job.channels:
            usernames = job.job.channels

        # Extract keywords based on job platform
        keywords = []
        if hasattr(job.job, 'keywords') and job.job.keywords:
            keywords = job.job.keywords
        if hasattr(job.job, 'subreddit') and job.job.subreddit:
            keywords.insert(0, job.job.subreddit)

        # Extract URL if present (for X or YouTube platforms)
        url = None
        if hasattr(job.job, 'url') and job.job.url:
            url = job.job.url

        return OrganicRequest(
            source=job.job.platform,
            usernames=usernames,
            keywords=keywords,
            url=url,
            keyword_mode=job.keyword_mode,
            start_date=job.start_date.isoformat() if job.start_date else None,
            end_date=job.end_date.isoformat() if job.end_date else None,
            limit=job.limit,
            data=[]
        )

    def setup(self):
        """A one-time setup method that must be called before the Validator starts its main loop."""
        assert not self.is_setup, "Validator already setup."

        if not self.config.wandb.off:
            try:
                self.new_wandb_run()
            except Exception as e:
                bt.logging.exception("W&B init failed; will retry later.")
                # Do NOT flip wandb.off here; just remember there is no active run.
                self.wandb_run = None
                self.wandb_run_start = None

        metrics.VALIDATOR_INFO.info(
            {
                "hotkey": self.wallet.hotkey.ss58_address,
                "uid": str(self.uid),
                "netuid": str(self.config.netuid),
                "version": self.get_version_tag(),
            }
        )

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
            wallet=self.wallet, metagraph=self.metagraph, evaluator=self.evaluator
        )

        self.data_universe_api_base_url = (
            "https://data-universe-api-branch-main.api.macrocosmos.ai"
            if "test" in self.config.subtensor.network
            else "https://data-universe-api.api.macrocosmos.ai"
        )
        self.verify_ssl = "localhost" not in self.data_universe_api_base_url
        bt.logging.info(
            f"Using Data Universe API URL: {self.data_universe_api_base_url}, {self.verify_ssl=}"
        )

        self.is_setup = True

    def _on_demand_client(self) -> DataUniverseApiClient:
        return DataUniverseApiClient(
            base_url=self.data_universe_api_base_url,
            verify_ssl=self.verify_ssl,
            keypair=self.wallet.hotkey,
            timeout=60,
        )

    def get_updated_lookup(self):
        try:
            t_start = time.perf_counter()
            bt.logging.info("Retrieving the latest dynamic lookup...")
            model = sync_run_retrieval(self.config)
            bt.logging.info("Model retrieved, updating value calculator...")
            self.evaluator.scorer.value_calculator = DataValueCalculator(model=model)
            bt.logging.info(f"Evaluator: {self.evaluator.scorer.value_calculator}")
            bt.logging.info(f"Updated dynamic lookup at {dt.datetime.utcnow()}")

            duration = time.perf_counter() - t_start

            metrics.DYNAMIC_DESIRABILITY_RETRIEVAL_PROCESS_DURATION.labels(
                hotkey=self.wallet.hotkey.ss58_address
            ).set(duration)
            metrics.DYNAMIC_DESIRABILITY_RETRIEVAL_LAST_SUCCESSFUL_TS.labels(
                hotkey=self.wallet.hotkey.ss58_address
            ).set(int(time.time()))
        except Exception as e:
            bt.logging.error(f"Error in get_updated_lookup: {str(e)}")
            bt.logging.exception("Exception details:")

    def get_version_tag(self):
        """Fetches version tag"""
        try:
            subprocess.run(["git", "fetch", "--tags"], check=True)
            version_tag = (
                subprocess.check_output(["git", "describe", "--tags", "--abbrev=0"])
                .strip()
                .decode("utf-8")
            )
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
        run_id = now.strftime("%Y-%m-%d_%H-%M-%S")
        name = "validator-" + str(self.uid) + "-" + run_id
        version_tag = self.get_version_tag()
        scraper_providers = self.get_scraper_providers()

        # Allow multiple runs in one process and only set start time after success
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
                "scrapers": scraper_providers,
            },
            allow_val_change=True,
            anonymous="allow",
            reinit=True,
            resume="never",  # force a brand-new run ID on each rotation
            settings=wandb.Settings(start_method="thread"),
        )

        # Start time after successful init so rotation scheduling is correct
        self.wandb_run_start = now

        bt.logging.debug(f"Started a new wandb run: {name}")

    async def loop_poll_on_demand_jobs_with_submissions(self):
        use_cache = True    # change to false for dev testing

        while not self.should_exit:
            bt.logging.info("Pulling on demand jobs with submissions")

            try:
                async with self._on_demand_client() as client:
                    jobs_with_submissions_downloaded_response = (
                        await client.validator_list_and_download_submission_json(
                            req=ListJobsWithSubmissionsForValidationRequest(
                                expired_since=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=45),
                                expired_until=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2),
                                limit=10, 
                            ),
                            job_ids_to_skip_downloading=set(self.processed_job_ids_cache.data.keys())
                        )
                    )
            except:
                bt.logging.exception("Failed to pull on demand jobs with submissions")
                await asyncio.sleep(20.0)
                continue
            
            try:
                # co locate each job id and miner hotkey
                job_list_with_submissions_resp, downloads = (
                    jobs_with_submissions_downloaded_response
                )

                job_data_per_job_id_and_miner_hotkey : Dict[str, Dict[str, Dict]]= {} # d[job id][miner hotkey]{download data}

                for job_with_submissions in job_list_with_submissions_resp.jobs_with_submissions:
                    job_data_per_job_id_and_miner_hotkey[job_with_submissions.job.id] = {} # job id -> hotkey

                for download in downloads:
                    job_id = download["job_id"]
                    miner_hotkey = download["miner_hotkey"]

                    job_data_per_job_id_and_miner_hotkey[job_id][miner_hotkey] = download

                bt.logging.info(
                    f"Pulled in: {len(job_data_per_job_id_and_miner_hotkey)} jobs with {sum([len(v) for v in job_data_per_job_id_and_miner_hotkey.values()])} total submissions"
                )

                bt.logging.debug(f"job_data_per_job_id_and_miner_hotkey:\n\n {job_data_per_job_id_and_miner_hotkey}")

                # validate
                for (
                    job_with_submission
                ) in job_list_with_submissions_resp.jobs_with_submissions:
                    job = job_with_submission.job

                    if use_cache and job.id in self.processed_job_ids_cache:
                        continue

                    submissions = job_with_submission.submissions
                    submissions_with_valid_downloads = [
                        sub for sub in submissions 
                        if sub.miner_hotkey in job_data_per_job_id_and_miner_hotkey[job.id] 
                        and job_data_per_job_id_and_miner_hotkey[job.id][sub.miner_hotkey]['error'] is None
                    ]

                    if len(submissions_with_valid_downloads) > 5: # amount of miners to validate per job id
                        random.shuffle(submissions_with_valid_downloads)
                        submissions_with_valid_downloads = submissions_with_valid_downloads[:5]

                    # validate miner data
                    validation_context = self._create_validation_context_from_job(job)

                    # Prepare miner responses in the format expected by validation methods
                    miner_responses = {}
                    miner_data_counts = {}
                    miner_submission_metadata = {}

                    for sub in submissions_with_valid_downloads:
                        miner_hotkey = sub.miner_hotkey
                        try:
                            # Convert hotkey to UID
                            uid = self.metagraph.hotkeys.index(miner_hotkey)

                            # Get uploaded data
                            miner_uploaded_raw_json = job_data_per_job_id_and_miner_hotkey[job.id][miner_hotkey]['data']
                            miner_upload = OnDemandMinerUpload.model_validate(miner_uploaded_raw_json)

                            # Store in format expected by validation methods
                            miner_responses[uid] = miner_upload.data_entities
                            miner_data_counts[uid] = len(miner_upload.data_entities)

                            # Track submission metadata for reward calculation
                            miner_submission_metadata[uid] = {
                                'submitted_at': sub.submitted_at,
                                'row_count': len(miner_upload.data_entities)
                            }

                        except ValueError:
                            bt.logging.warning(f"Miner hotkey {miner_hotkey} not found in metagraph")
                            continue

                    # Step 1: Format validation (reuse from OrganicQueryProcessor)
                    for uid, data in miner_responses.items():
                        if data and not self.organic_processor._validate_miner_data_format(validation_context, data, uid):
                            bt.logging.info(f"Miner {uid} failed format validation")
                            miner_responses[uid] = []  # Treat as empty
                            miner_data_counts[uid] = 0

                    # Step 2: Perform detailed validation on sample posts (reuse from OrganicQueryProcessor)
                    validation_results = {}
                    for uid, posts in miner_responses.items():
                        if not posts:
                            continue

                        # Sample posts for validation (similar to _perform_validation)
                        num_to_validate = min(2, len(posts))
                        selected_posts = random.sample(posts, num_to_validate)

                        for post in selected_posts:
                            post_id = self.organic_processor._get_post_id(post)
                            miner_post_key = f"{uid}:{post_id}" 
                            is_valid = await self.organic_processor._validate_entity(validation_context, post, post_id, uid)
                            validation_results[miner_post_key] = is_valid

                    # Step 3: Apply validation penalties and get successful miners
                    miner_scores, failed_miners, successful_miners = self.organic_processor._apply_validation_penalties(miner_responses, validation_results)

                    # Step 4: Volume consensus validation (reuse from OrganicQueryProcessor)
                    consensus_count = self.organic_processor._calculate_volume_consensus(miner_data_counts)
                    if consensus_count:
                        penalized_miners = self.organic_processor._apply_consensus_volume_penalties(
                            miner_data_counts, job.limit, consensus_count
                        )
                        bt.logging.info(f"Applied volume penalties to {len(penalized_miners)} miners")
                        # Remove consensus-penalized miners from successful list
                        successful_miners = [uid for uid in successful_miners if uid not in penalized_miners]

                    # Step 5: Apply rewards to successful miners
                    for uid in successful_miners:
                        if uid not in miner_submission_metadata:
                            bt.logging.warning(f"Missing metadata for successful miner {uid}")
                            continue

                        metadata = miner_submission_metadata[uid]

                        # Calculate reward multipliers
                        speed_mult, volume_mult = self.organic_processor.calculate_ondemand_reward_multipliers(
                            job_created_at=job.created_at,
                            submission_timestamp=metadata['submitted_at'],
                            returned_count=metadata['row_count'],
                            requested_limit=job.limit,
                            consensus_count=consensus_count
                        )

                        # Apply reward (scaled by existing credibility internally)
                        self.organic_processor.evaluator.scorer.apply_ondemand_reward(
                            uid=uid,
                            speed_multiplier=speed_mult,
                            volume_multiplier=volume_mult
                        )

                    bt.logging.info(
                        f"Job {job.id} validation complete: "
                        f"{len(successful_miners)} miners rewarded, "
                        f"{len(failed_miners)} miners failed validation"
                    )

                if use_cache:
                    job_ids_processed_in_this_loop_not_already_in_cache = set([ job_id for job_id in set(job_data_per_job_id_and_miner_hotkey.keys()) if job_id not in self.processed_job_ids_cache])
                    bt.logging.info(f"Adding processed on demand job ids to cache: {job_ids_processed_in_this_loop_not_already_in_cache}")
                    for job_id in job_ids_processed_in_this_loop_not_already_in_cache:
                        if job_id not in self.processed_job_ids_cache:
                            self.processed_job_ids_cache.add(job_id)
            except:
                bt.logging.exception("Error while validating on demand jobs and submissions")

            await asyncio.sleep(20.0)

    def run_on_demand(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        poll_task = loop.create_task(self.loop_poll_on_demand_jobs_with_submissions())

        try:
            loop.run_forever()
        finally:
            loop.close()
            loop = None

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

        while not self.should_exit:
            try:
                t_start = time.perf_counter()

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

                # Maybe set weights.
                if self.should_set_weights():
                    self.set_weights()

                # Always save state.
                self.save_state()

                # Update to the latest desirability list after each evaluation.
                self.get_updated_lookup()

                self.step += 1

                metrics.MAIN_LOOP_ITERATIONS_TOTAL.labels(
                    hotkey=self.wallet.hotkey.ss58_address
                ).inc()
                metrics.MAIN_LOOP_LAST_SUCCESS_TS.labels(
                    hotkey=self.wallet.hotkey.ss58_address
                ).set(int(time.time()))
                metrics.MAIN_LOOP_DURATION.labels(
                    hotkey=self.wallet.hotkey.ss58_address
                ).set(time.perf_counter() - t_start)

                wait_time = max(0.0, float(next_batch_delay_secs))
                if wait_time > 0:
                    bt.logging.info(
                        f"Finished full evaluation loop early. Waiting {wait_time} seconds until running next evaluation loop."
                    )
                    time.sleep(wait_time)

                if not self.config.wandb.off and self.wandb_run is None:
                    try:
                        self.new_wandb_run()
                        bt.logging.info("W&B: started new run successfully")
                    except Exception as e:
                        bt.logging.error(f"W&B init retry failed: {e}")

                # Rotation with retry (only when we actually have a start time)
                if (
                    (not self.config.wandb.off)
                    and (self.wandb_run_start is not None)
                    and (
                        (dt.datetime.now() - self.wandb_run_start)
                        >= dt.timedelta(hours=3)
                    )
                ):

                    try:
                        self.new_wandb_run()
                        bt.logging.info("W&B: rotated run successfully")
                    except Exception as e:
                        bt.logging.error(
                            f"W&B rotation failed; keeping current run active: {e}"
                        )

            except KeyboardInterrupt:
                self.axon.stop()
                if self.wandb_run:
                    self.wandb_run.finish()
            except Exception as err:
                metrics.MAIN_LOOP_ERRORS_TOTAL.labels(
                    hotkey=self.wallet.hotkey.ss58_address
                ).inc()
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

            self.on_demand_thread = threading.Thread(target=self.run_on_demand, daemon=True)
            self.on_demand_thread.start()
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
            self.on_demand_thread.join(5)
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
            self.on_demand_thread.join(5)
            self.is_running = False

            # Cleanup loggers
            if self.wandb_run:
                self.wandb_run.finish()
            bt.logging.debug("Stopped.")

    def serve_axon(self):
        """Serve axon to enable external connections."""

        try:
            self.axon = bt.axon(wallet=self.wallet, config=self.config)

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

        # Validator Health Checks
        hotkey = self.wallet.hotkey.ss58_address

        # Resolve current UID from hotkey (survives dereg/re-reg)
        try:
            uid = metagraph.hotkeys.index(hotkey)
            registered = 1
        except ValueError:
            uid = None
            registered = 0

        metrics.BITTENSOR_VALIDATOR_REGISTERED.labels(hotkey=hotkey).set(registered)

        # Set vtrust + block diff since last update only when registered; otherwise zero them
        if registered:
            try:
                vtrust = float(metagraph.validator_trust[uid])
            except Exception:
                vtrust = 0.0

            try:
                head_block = int(getattr(metagraph, "block", 0))
                last_update = int(metagraph.last_update[uid])
                block_diff = (
                    max(0, head_block - last_update)
                    if head_block and last_update
                    else 0
                )
            except Exception:
                block_diff = 0
        else:
            vtrust = 0.0
            block_diff = 0

        metrics.BITTENSOR_VALIDATOR_VTRUST.labels(hotkey=hotkey).set(vtrust)
        metrics.BITTENSOR_VALIDATOR_BLOCK_DIFFERENCE.labels(hotkey=hotkey).set(
            block_diff
        )
        metrics.METAGRAPH_LAST_UPDATE_TS.labels(hotkey=hotkey).set(int(time.time()))

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
                bt.logging.info(
                    "Initializing evaluation cycles counter for delayed weight setting"
                )

            #  if we've completed fewer than the allotted number of evaluation cycles, don't set weights
            if self.evaluation_cycles_since_startup < constants.EVALUATION_ON_STARTUP:

                bt.logging.info(
                    f"Skipping weight setting - completed {self.evaluation_cycles_since_startup}/15 evaluation cycles since startup"
                )
                return False

            # Normal 20-minute interval check for subsequent weight settings
            return dt.datetime.utcnow() - self.last_weights_set_time > dt.timedelta(
                minutes=20
            )

    def _start_api_monitoring(self):
        """Start a lightweight monitor to auto-restart API if it becomes unreachable"""

        master_key = os.getenv("MASTER_KEY")

        def monitor_api():
            consecutive_failures = 0
            max_failures = 3  # Restart after 3 consecutive failures

            while not self.should_exit:
                if not hasattr(self, "api") or not self.api:
                    time.sleep(60 * 20)
                    continue

                try:
                    # Try a simple local request to check if API is responding
                    response = requests.get(
                        f"http://localhost:{self.config.neuron.api_port}/api/v1/monitoring/system-status",
                        headers={"X-API-Key": master_key},
                        timeout=10,
                    )

                    if response.status_code == 200:
                        # API is working, reset failure counter
                        consecutive_failures = 0
                    else:
                        # HTTP error
                        consecutive_failures += 1
                        bt.logging.warning(
                            f"API health check returned status {response.status_code}"
                        )
                except requests.RequestException:
                    # Connection error (most likely API is down)
                    consecutive_failures += 1
                    bt.logging.warning(
                        f"API server not responding ({consecutive_failures}/{max_failures})"
                    )

                # If too many consecutive failures, restart API
                if consecutive_failures >= max_failures:
                    bt.logging.warning(
                        f"API server unresponsive for {consecutive_failures} checks, restarting..."
                    )

                    try:
                        # Stop API if it's running
                        if hasattr(self.api, "stop"):
                            self.api.stop()

                        # Wait a moment
                        time.sleep(2)

                        # Start API again
                        if hasattr(self.api, "start"):
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

        # Apply burn mechanism - redirect percentage to subnet owner
        raw_weights = utils.apply_burn_to_weights(
            raw_weights=raw_weights,
            metagraph=self.metagraph,
            subtensor=self.subtensor,
            netuid=self.config.netuid,
            burn_percentage=constants.EMISSION_CONTROL_PERCENTAGE
        )

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
        t0 = time.perf_counter()
        success, message = self.subtensor.set_weights(
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

        metric_status = "ok" if success else "fail"
        metrics.SET_WEIGHTS_SUBTENSOR_DURATION.labels(
            hotkey=self.wallet.hotkey.ss58_address, status=metric_status
        ).observe(time.perf_counter() - t0)
        metrics.SET_WEIGHTS_LAST_TS.labels(
            hotkey=self.wallet.hotkey.ss58_address, status=metric_status
        ).set(int(time.time()))

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
