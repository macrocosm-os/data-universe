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
import sys
import threading
import time
import traceback
import typing
import bittensor as bt
import datetime as dt
from common import constants, utils
from common.data import CompressedMinerIndex, TimeBucket
from common.protocol import (
    GetDataEntityBucket,
    GetMinerIndex,
    GetContentsByBuckets,
    GetHuggingFaceMetadata,
    DecodeURLRequest,
    REQUEST_LIMIT_BY_TYPE_PER_PERIOD
)

from scraping.config.config_reader import ConfigReader
from scraping.coordinator import ScraperCoordinator
from scraping.provider import ScraperProvider
from storage.miner.sqlite_miner_storage import SqliteMinerStorage
from neurons.config import NeuronType, check_config, create_config
from huggingface_utils.huggingface_uploader import DualUploader
from huggingface_utils.encoding_system import EncodingKeyManager, decode_url
from dynamic_desirability.desirability_retrieval import sync_run_retrieval

from common.data import DataLabel, DataSource, DataEntity
from common.protocol import OnDemandRequest
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, ScraperId

from scraping.x.enhanced_apidojo_scraper import EnhancedApiDojoTwitterScraper
import json

# Enable logging to the miner TODO move it to some different location
bt.logging.set_info(True)
bt.logging.set_debug(True)

class Miner:
    """The Glorious Miner."""

    def __init__(self, config=None):
        self.config = copy.deepcopy(config or create_config(NeuronType.MINER))
        check_config(self.config)

        bt.logging(config=self.config, logging_dir=self.config.full_path)
        bt.logging.info(self.config)
        self.use_hf_uploader = self.config.huggingface
        self.use_gravity_retrieval = self.config.gravity

        if self.config.offline:
            bt.logging.success(
                "Running in offline mode. Skipping bittensor object setup and axon creation."
            )
            self.uid = 0  # Offline mode so assume it's == 0

        else:
            # The wallet holds the cryptographic key pairs for the miner.
            self.wallet = bt.wallet(config=self.config)
            bt.logging.info(f"Wallet: {self.wallet}.")

            # The subtensor is our connection to the Bittensor blockchain.
            self.subtensor = bt.subtensor(config=self.config)
            bt.logging.info(f"Subtensor: {self.subtensor}.")

            # The metagraph holds the state of the network, letting us know about other validators and miners.
            self.metagraph = self.subtensor.metagraph(self.config.netuid)
            bt.logging.info(f"Metagraph: {self.metagraph}.")

            # Each miner gets a unique identity (UID) in the network for differentiation.
            # TODO: Stop doing meaningful work in the constructor to make neurons more testable.
            if self.wallet.hotkey.ss58_address in self.metagraph.hotkeys:
                self.uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
                bt.logging.info(
                    f"Running neuron on subnet: {self.config.netuid} with uid {self.uid} using network: {self.subtensor.chain_endpoint}."
                )
            else:
                self.uid = 0
                bt.logging.warning(
                    f"Hotkey {self.wallet.hotkey.ss58_address} not found in metagraph. Assuming this is a test."
                )

            self.last_sync_timestamp = dt.datetime.min
            self.step = 0

            # The axon handles request processing, allowing validators to send this miner requests.
            self.axon = bt.axon(wallet=self.wallet, port=self.config.axon.port)

            # Attach determiners which functions are called when servicing a request.
            bt.logging.info("Attaching forward function to miner axon.")
            self.axon.attach(
                forward_fn=self.get_index,
                blacklist_fn=self.get_index_blacklist,
                priority_fn=self.get_index_priority,
            ).attach(
                forward_fn=self.get_data_entity_bucket,
                blacklist_fn=self.get_data_entity_bucket_blacklist,
                priority_fn=self.get_data_entity_bucket_priority,
            ).attach(
                forward_fn=self.get_contents_by_buckets,
                blacklist_fn=self.get_contents_by_buckets_blacklist,
                priority_fn=self.get_contents_by_buckets_priority,
            ).attach(
                forward_fn=self.get_huggingface_metadata,
                blacklist_fn=self.get_huggingface_metadata_blacklist,
                priority_fn=self.get_huggingface_metadata_priority,
            ).attach(
                forward_fn=self.decode_urls,
                blacklist_fn=self.decode_urls_blacklist,
                priority_fn=self.decode_urls_priority
            ).attach(
                forward_fn=self.handle_on_demand,
                blacklist_fn=self.handle_on_demand_blacklist,
                priority_fn=self.handle_on_demand_priority
            )

            bt.logging.success(f"Axon created: {self.axon}.")

        # Instantiate runners.
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: threading.Thread = None
        self.compressed_index_refresh_thread: threading.Thread = None
        self.hugging_face_thread: threading.Thread = None
        self.lock = threading.RLock()
        self.vpermit_rao_limit = self.config.vpermit_rao_limit

        # Instantiate encoding keys
        self.encoding_key_manager = EncodingKeyManager(key_path=self.config.encoding_key_json_file)
        self.private_encoding_key_manager = EncodingKeyManager(key_path=self.config.private_encoding_key_json_file)

        bt.logging.info("Initialized EncodingKeyManager for URL encoding/decoding.")

        if self.use_hf_uploader:
            self.hf_uploader = DualUploader(
                db_path=self.config.neuron.database_name,
                encoding_key_manager=self.encoding_key_manager,
                private_encoding_key_manager=self.private_encoding_key_manager,
                wallet=self.wallet,
                subtensor=self.subtensor,
                state_file=self.config.miner_upload_state_file,
                s3_auth_url=self.config.s3_auth_url,

            )

        # Instantiate storage.
        self.storage = SqliteMinerStorage(
            self.config.neuron.database_name,
            self.config.neuron.max_database_size_gb_hint,
        )

        bt.logging.success(
            f"Successfully connected to miner storage: {self.config.neuron.database_name}."
        )

        # Configure the ScraperCoordinator
        bt.logging.info(
            f"Loading scraping config from {self.config.neuron.scraping_config_file}."
        )
        scraping_config = ConfigReader.load_config(
            self.config.neuron.scraping_config_file
        )
        bt.logging.success(f"Loaded scraping config: {scraping_config}.")

        self.scraping_coordinator = ScraperCoordinator(
            scraper_provider=ScraperProvider(),
            miner_storage=self.storage,
            config=scraping_config,
        )

        # Configure per hotkey per request limits.
        self.request_lock = threading.RLock()
        self.last_cleared_request_limits = dt.datetime.now()
        self.requests_by_type_by_hotkey = defaultdict(lambda: defaultdict(lambda: 0))

    def refresh_index(self):
        """
        Refreshes the cached compressed miner index periodically off the hot path of GetMinerIndex requests.
        """
        while not self.should_exit:
            try:
                # Refresh the index if it hasn't been refreshed in the configured time period.
                self.storage.refresh_compressed_index(
                    time_delta=constants.MINER_CACHE_FRESHNESS
                )
                bt.logging.trace("Refresh index thread finished refreshing the index.")
                # Wait freshness period + 1 minute to try refreshing again.
                # Wait the additional minute to ensure that the next refresh sees a 'stale' index.
                time.sleep(
                    (
                        constants.MINER_CACHE_FRESHNESS + dt.timedelta(minutes=1)
                    ).total_seconds()
                )
            # In case of unforeseen errors, the refresh thread will log the error and continue operations.
            except Exception:
                bt.logging.error(traceback.format_exc())
                # Sleep 5 minutes to avoid constant refresh attempts if they are consistently erroring.
                time.sleep(60 * 5)

    def get_updated_lookup(self):
        if not self.use_gravity_retrieval:
            bt.logging.info("Gravity lookup retrieval is not enabled.")
            return

        last_update = None
        while not self.should_exit:
            try:
                current_datetime = dt.datetime.utcnow()

                bt.logging.info(f"Checking for update. Last update: {last_update}, Current time: {current_datetime}")

                # Check if it's a new day and we haven't updated yet
                if last_update is None or current_datetime.date() > last_update.date():
                    bt.logging.info("Retrieving the latest dynamic lookup...")
                    sync_run_retrieval(self.config)
                    bt.logging.info(f"New desirable data list has been written to total.json")
                    last_update = current_datetime
                    bt.logging.info(f"Updated dynamic lookup at {last_update}")
                else:
                    bt.logging.info("No update needed at this time.")

                # Sleep for 5 minutes before checking again
                bt.logging.info("Sleeping for 5 minutes...")
                time.sleep(300)

            except Exception as e:
                bt.logging.error(f"Error in get_updated_lookup: {str(e)}")
                bt.logging.exception("Exception details:")
                time.sleep(300)  # Wait 5 minutes before trying again

    def upload_hugging_face(self):
        if not self.use_hf_uploader:
            bt.logging.info("HuggingFace Uploader is not enabled.")
            return

        time_sleep_val = dt.timedelta(minutes=30).total_seconds()
        time.sleep(time_sleep_val)

        while not self.should_exit:
            try:
                unique_id = self.hf_uploader.unique_id  # Assuming this exists in the DualUploader
                if self.storage.should_upload_hf_data(unique_id):
                    bt.logging.info("Trying to upload the data into HuggingFace and S3.")
                    hf_metadata_list = self.hf_uploader.upload_sql_to_huggingface()
                    if hf_metadata_list:
                        self.storage.store_hf_dataset_info(hf_metadata_list)
            except Exception:
                bt.logging.error(traceback.format_exc())

            time_sleep_val = dt.timedelta(minutes=90).total_seconds()
            time.sleep(time_sleep_val)
    def run(self):
        """
        Initiates and manages the main loop for the miner.
        """

        if self.config.offline:
            bt.logging.success("Running in offline mode. Skipping axon serving.")
        else:
            # Check that miner is registered on the network.
            self.sync()

            # Serve passes the axon information to the network + netuid we are hosting on.
            # This will auto-update if the axon port of external ip have changed.
            bt.logging.info(
                f"Serving miner axon {self.axon} on network: {self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}."
            )
            self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)

            # Start  starts the miner's axon, making it active on the network.
            self.axon.start()

            self.last_sync_timestamp = dt.datetime.now()
            bt.logging.success(f"Miner starting at {self.last_sync_timestamp}.")

        self.scraping_coordinator.run_in_background_thread()

        while not self.should_exit:
            # This loop maintains the miner's operations until intentionally stopped.
            try:
                # In offline mode we just idle while the scraping_coordinator runs.
                if self.config.offline:
                    while not self.should_exit:
                        time.sleep(12)
                else:
                    # Epoch length defaults to 100 blocks at 12 seconds each for 20 minutes.
                    while dt.datetime.now() - self.last_sync_timestamp < (
                        dt.timedelta(seconds=12 * self.config.neuron.epoch_length)
                    ):
                        # Wait before checking again.
                        time.sleep(12)

                        # Check if we should exit.
                        if self.should_exit:
                            break

                    # Sync metagraph.
                    self.sync()

                    self._log_status(self.step)

                    self.last_sync_timestamp = dt.datetime.now()
                    self.step += 1

            # If someone intentionally stops the miner, it'll safely terminate operations.
            except KeyboardInterrupt:
                if not self.config.offline:
                    self.axon.stop()
                self.scraping_coordinator.stop()
                bt.logging.success("Miner killed by keyboard interrupt.")
                sys.exit()

            # In case of unforeseen errors, the miner will log the error and continue operations.
            except Exception as e:
                bt.logging.error(traceback.format_exc())

    def run_in_background_thread(self):
        """
        Starts the miner's operations in a separate background thread.
        This is useful for non-blocking operations.
        """
        if not self.is_running:
            bt.logging.debug("Starting miner in background thread.")
            self.should_exit = False
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            self.compressed_index_refresh_thread = threading.Thread(
                target=self.refresh_index, daemon=True
            )
            self.compressed_index_refresh_thread.start()
            self.hugging_face_thread = threading.Thread(
                target=self.upload_hugging_face, daemon=True
            )
            self.hugging_face_thread.start()
            self.lookup_thread = threading.Thread(
                target=self.get_updated_lookup, daemon=True
            )
            self.lookup_thread.start()
            self.is_running = True
            bt.logging.debug("Started")

    def stop_run_thread(self):
        """
        Stops the miner's operations that are running in the background thread.
        """
        if self.is_running:
            bt.logging.debug("Stopping miner in background thread.")
            self.should_exit = True
            self.thread.join(5)
            self.compressed_index_refresh_thread.join(5)
            self.is_running = False
            bt.logging.debug("Stopped")

    def __enter__(self):
        """
        Starts the miner's operations in a background thread upon entering the context.
        This method facilitates the use of the miner in a 'with' statement.
        """
        self.run_in_background_thread()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Stops the miner's background operations upon exiting the context.
        This method facilitates the use of the miner in a 'with' statement.

        Args:
            exc_type: The type of the exception that caused the context to be exited.
                      None if the context was exited without an exception.
            exc_value: The instance of the exception that caused the context to be exited.
                       None if the context was exited without an exception.
            traceback: A traceback object encoding the stack trace.
                       None if the context was exited without an exception.
        """
        self.stop_run_thread()

    def resync_metagraph(self):
        """Resyncs the metagraph and updates the hotkeys and moving averages based on the new metagraph."""
        bt.logging.info("Attempting to resync the metagraph.")

        # Sync the metagraph.
        new_metagraph = self.subtensor.metagraph(netuid=self.config.netuid)
        with self.lock:
            self.metagraph = new_metagraph

        bt.logging.success("Successfuly resynced the metagraph.")

    def _log_status(self, step: int):
        """Logs a summary of the miner status in the subnet."""
        relative_incentive = self.metagraph.I[self.uid].item() / max(self.metagraph.I)
        incentive_and_hk = zip(self.metagraph.I, self.metagraph.hotkeys)
        incentive_and_hk = sorted(incentive_and_hk, key=lambda x: x[0], reverse=True)
        position = -1
        for i, (_, hk) in enumerate(incentive_and_hk):
            if hk == self.wallet.hotkey.ss58_address:
                position = i
                break
        log = (
            f"Step:{step} | "
            f"Block:{self.metagraph.block.item()} | "
            f"Stake:{self.metagraph.S[self.uid]} | "
            f"Incentive:{self.metagraph.I[self.uid]} | "
            f"Relative Incentive:{relative_incentive} | "
            f"Position:{position} | "
            f"Emission:{self.metagraph.E[self.uid]}"
        )
        bt.logging.info(log)

    async def get_index(self, synapse: GetMinerIndex) -> GetMinerIndex:
        """Runs after the GetMinerIndex synapse has been deserialized (i.e. after synapse.data is available)."""
        bt.logging.info(
            f"Got to a GetMinerIndex request from {synapse.dendrite.hotkey}."
        )

        # Only synapse.version 4 is supported at this time.
        if synapse.version < 4:
            bt.logging.error(f"Unsupported protocol version: {synapse.version}.")
            return synapse

        # Return the appropriate amount of max buckets based on protocol of the requesting validator.
        compressed_index = self.storage.get_compressed_index(
            bucket_count_limit=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4
        )
        synapse.compressed_index_serialized = compressed_index.model_dump_json()
        bt.logging.success(
            f"Returning compressed miner index of {CompressedMinerIndex.size_bytes(compressed_index)} bytes "
            + f"across {CompressedMinerIndex.bucket_count(compressed_index)} buckets to {synapse.dendrite.hotkey}."
        )

        synapse.version = constants.PROTOCOL_VERSION

        return synapse

    async def get_index_blacklist(
        self, synapse: GetMinerIndex
    ) -> typing.Tuple[bool, str]:
        return self.default_blacklist(synapse)

    async def get_index_priority(self, synapse: GetMinerIndex) -> float:
        return self.default_priority(synapse)

    async def get_data_entity_bucket(
        self, synapse: GetDataEntityBucket
    ) -> GetDataEntityBucket:
        """Runs after the GetDataEntityBucket synapse has been deserialized (i.e. after synapse.data is available)."""
        bt.logging.info(
            f"Got to a GetDataEntityBucket request from {synapse.dendrite.hotkey} for Bucket ID: {str(synapse.data_entity_bucket_id)}."
        )

        # List all the data entities that this miner has for the requested DataEntityBucket.
        synapse.data_entities = self.storage.list_data_entities_in_data_entity_bucket(
            synapse.data_entity_bucket_id
        )
        synapse.version = constants.PROTOCOL_VERSION

        bt.logging.success(
            f"Returning Bucket ID: {str(synapse.data_entity_bucket_id)} with {len(synapse.data_entities)} entities to {synapse.dendrite.hotkey}."
        )

        return synapse

    async def get_huggingface_metadata(self, synapse: GetHuggingFaceMetadata) -> GetHuggingFaceMetadata:
        bt.logging.info(f"Got a GetHuggingFaceMetadata request from {synapse.dendrite.hotkey}.")

        # Query the HuggingFace metadata from the database
        synapse.metadata = self.storage.get_hf_metadata(unique_id=self.hf_uploader.unique_id)

        if not synapse.metadata:
            bt.logging.info(f"No HuggingFace metadata available. Returning empty list to {synapse.dendrite.hotkey}.")
        else:
            bt.logging.success(
                f"Returning {len(synapse.metadata)} HuggingFace metadata entries to {synapse.dendrite.hotkey}.")

        return synapse

    async def decode_urls(self, synapse: DecodeURLRequest) -> DecodeURLRequest:
        """Handle URL decoding requests using miner's keys."""
        bt.logging.info(f"Processing URL decode request from {synapse.dendrite.hotkey}")

        # Try decoding with both private and public keys
        decoded_urls = []
        for url in synapse.encoded_urls:
            # Try private key first, then public key if no result
            result = decode_url(url, self.private_encoding_key_manager.get_fernet())
            if not result:
                result = decode_url(url, self.encoding_key_manager.get_fernet())
            decoded_urls.append(result if result else "")

        synapse.decoded_urls = decoded_urls
        return synapse

    async def decode_urls_blacklist(self, synapse: DecodeURLRequest) -> typing.Tuple[bool, str]:
        """The blacklist function for decode URL requests."""
        return self.default_blacklist(synapse)

    async def decode_urls_priority(self, synapse: DecodeURLRequest) -> float:
        """The priority function for decode URL requests."""
        return self.default_priority(synapse)

    async def get_huggingface_metadata_blacklist(self, synapse: GetHuggingFaceMetadata) -> typing.Tuple[bool, str]:
        return self.default_blacklist(synapse)

    async def get_huggingface_metadata_priority(self, synapse: GetHuggingFaceMetadata) -> float:
        return self.default_priority(synapse)

    async def get_data_entity_bucket_blacklist(
        self, synapse: GetDataEntityBucket
    ) -> typing.Tuple[bool, str]:
        return self.default_blacklist(synapse)

    async def get_data_entity_bucket_priority(
        self, synapse: GetDataEntityBucket
    ) -> float:
        return self.default_priority(synapse)

    async def handle_on_demand(self, synapse: OnDemandRequest) -> OnDemandRequest:
        """
        Handle on-demand data requests from validators.
        Uses enhanced scraper for X data while maintaining protocol compatibility.
        """
        bt.logging.info(f"Got on-demand request from {synapse.dendrite.hotkey}")

        try:
            # Get appropriate scraper from provider
            scraper_id = None
            if synapse.source == DataSource.X:
                scraper_id = ScraperId.X_APIDOJO
                # For X, combine keywords and usernames with appropriate label formatting
                labels = []
                if synapse.keywords:
                    labels.extend([DataLabel(value=k) for k in synapse.keywords])
                if synapse.usernames:
                    # Ensure usernames have @ prefix
                    labels.extend([DataLabel(value=f"@{u.strip('@')}" if not u.startswith('@') else u) for u in
                                synapse.usernames])

            elif synapse.source == DataSource.REDDIT:
                scraper_id = ScraperId.REDDIT_CUSTOM
                # For Reddit, ensure subreddit has r/ prefix
                if synapse.keywords and len(synapse.keywords) > 0:
                    subreddit = synapse.keywords[0]
                    if not subreddit.startswith('r/'):
                        subreddit = f"r/{subreddit}"
                    labels = [DataLabel(value=subreddit)]
                else:
                    labels = []

            if not scraper_id:
                bt.logging.error(f"No scraper ID for source {synapse.source}")
                synapse.data = []
                return synapse

            # Create date range with utility function
            start_dt = (utils.parse_iso_date(synapse.start_date)
                        if synapse.start_date else dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1))
            end_dt = (utils.parse_iso_date(synapse.end_date)
                    if synapse.end_date else dt.datetime.now(dt.timezone.utc))

            # Fallback to default dates if parsing failed
            if start_dt is None:
                start_dt = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
            if end_dt is None:
                end_dt = dt.datetime.now(dt.timezone.utc)

            # Log the labels being used
            bt.logging.info(f"Searching with labels: {[l.value for l in labels]}")
            bt.logging.info(f"Date range: {start_dt} to {end_dt}")

            config = ScrapeConfig(
                entity_limit=synapse.limit,
                date_range=DateRange(
                    start=start_dt,
                    end=end_dt
                ),
                labels=labels,
            )

            # For X source, use the enhanced scraper directly
            if synapse.source == DataSource.X:
                # Initialize the enhanced scraper directly instead of using the provider

                enhanced_scraper = EnhancedApiDojoTwitterScraper()
                await enhanced_scraper.scrape(config)

                # Get enhanced content
                enhanced_content = enhanced_scraper.get_enhanced_content()

                # Convert EnhancedXContent to DataEntity to maintain protocol compatibility
                enhanced_data_entities = []
                for content in enhanced_content:
                    # Convert to DataEntity but store full rich content in serialized form
                    api_response = content.to_api_response()
                    data_entity = DataEntity(
                        uri=content.url,
                        datetime=content.timestamp,
                        source=DataSource.X,
                        label=DataLabel(value=content.tweet_hashtags[0].lower()) if content.tweet_hashtags else None,
                        # Store the full enhanced content as serialized JSON in the content field
                        content=json.dumps(api_response).encode('utf-8'),
                        content_size_bytes=len(json.dumps(api_response))
                    )
                    enhanced_data_entities.append(data_entity)

                # Update response with enhanced data entities
                synapse.data = enhanced_data_entities[:synapse.limit] if synapse.limit else enhanced_data_entities
            else:
                # For Reddit, use the provider that's part of the coordinator
                from scraping.provider import ScraperProvider

                # Create a new scraper provider and get the appropriate scraper
                provider = ScraperProvider()
                scraper = provider.get(scraper_id)

                if not scraper:
                    bt.logging.error(f"No scraper available for ID {scraper_id}")
                    synapse.data = []
                    return synapse

                data = await scraper.scrape(config)
                synapse.data = data[:synapse.limit] if synapse.limit else data

            synapse.version = constants.PROTOCOL_VERSION

            bt.logging.success(
                f"Returning {len(synapse.data)} items to {synapse.dendrite.hotkey}"
            )

        except Exception as e:
            bt.logging.error(f"Error in on-demand request: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            synapse.data = []

        return synapse

    async def handle_on_demand_blacklist(
            self, synapse: OnDemandRequest
    ) -> typing.Tuple[bool, str]:
        """Blacklist function for on-demand requests"""
        return self.default_blacklist(synapse)

    async def handle_on_demand_priority(
            self, synapse: OnDemandRequest
    ) -> float:
        """Priority function for on-demand requests"""
        return self.default_priority(synapse)

    async def get_contents_by_buckets(
        self, synapse: GetContentsByBuckets
    ) -> GetContentsByBuckets:
        """Used to bulk expose raw contents for all entities within the requested buckets to validators for user queries."""
        bt.logging.info(
            f"Got to a GetContentsByBuckets request from {synapse.dendrite.hotkey} for Bucket IDs: {str(synapse.data_entity_bucket_ids)}."
        )

        # Check that the maximum number of buckets to be requested at once is respected.
        if len(synapse.data_entity_bucket_ids) > constants.BULK_BUCKETS_COUNT_LIMIT:
            bt.logging.warning(
                f"Rejecting GetContentsByBuckets request from {synapse.dendrite.hotkey} at {synapse.dendrite.ip} for requesting {len(synapse.data_entity_bucket_ids)} data entity buckets over limit of {constants.BULK_BUCKETS_COUNT_LIMIT}."
            )
            return synapse

        # Get a dict of all the contents by DataEntityBucketId for the requested Buckets.
        buckets_to_contents = self.storage.list_contents_in_data_entity_buckets(
            synapse.data_entity_bucket_ids
        )
        synapse.bucket_ids_to_contents = [
            (k, v) for k, v in buckets_to_contents.items()
        ]
        synapse.version = constants.PROTOCOL_VERSION

        bt.logging.success(
            f"Returning Bucket IDs: {str(synapse.data_entity_bucket_ids)} with {sum(len(contents) for (_,contents) in synapse.bucket_ids_to_contents)} entities to {synapse.dendrite.hotkey}."
        )

        return synapse

    async def get_contents_by_buckets_blacklist(
        self, synapse: GetContentsByBuckets
    ) -> typing.Tuple[bool, str]:
        return self.default_blacklist(synapse)

    async def get_contents_by_buckets_priority(
        self, synapse: GetContentsByBuckets
    ) -> float:
        return self.default_priority(synapse)

    def default_blacklist(self, synapse: bt.Synapse) -> typing.Tuple[bool, str]:
        """The default blacklist that only allows requests from validators."""
        hotkey = synapse.dendrite.hotkey
        ip = synapse.dendrite.ip
        synapse_type = type(synapse)

        if hotkey not in self.metagraph.hotkeys:
            # Ignore requests from unrecognized entities.
            return (
                True,
                f"Unrecognized hotkey {hotkey} at {ip}",
            )

        uid = self.metagraph.hotkeys.index(hotkey)
        
        if not utils.is_validator(uid, self.metagraph, self.vpermit_rao_limit):
            return (
                True,
                f"Hotkey {hotkey} at {ip} is not a validator",
            )

        with self.request_lock:
            # Check if we need to clear the request limit counters.
            if (
                dt.datetime.now() - self.last_cleared_request_limits
            ) >= constants.MIN_EVALUATION_PERIOD:
                bt.logging.trace(
                    f"Clearing request limit counters by hotkey after an eval period: {constants.MIN_EVALUATION_PERIOD}."
                )
                for request_type in self.requests_by_type_by_hotkey:
                    self.requests_by_type_by_hotkey[request_type] = defaultdict(
                        lambda: 0
                    )
                self.last_cleared_request_limits = dt.datetime.now()

            # Record request.
            self.requests_by_type_by_hotkey[synapse_type][hotkey] += 1

            # Safety check for unmapped request types although unrecognized request types are handled earlier.
            if synapse_type not in REQUEST_LIMIT_BY_TYPE_PER_PERIOD:
                return (
                    True,
                    f"Hotkey {hotkey} at {ip} sent request for unmapped type: {synapse_type.__name__}.",
                )

            # Allow 2x the limit per period to account for restarts.
            if (
                self.requests_by_type_by_hotkey[synapse_type][hotkey]
                > 2 * REQUEST_LIMIT_BY_TYPE_PER_PERIOD[synapse_type]
            ):
                return (
                    True,
                    f"Hotkey {hotkey} at {ip} over eval period request limit for {synapse_type.__name__}.",
                )

        return False, ""

    def default_priority(self, synapse: bt.Synapse) -> float:
        """The default priority that prioritizes by validator stake."""
        caller_uid = self.metagraph.hotkeys.index(synapse.dendrite.hotkey)
        priority = float(self.metagraph.S[caller_uid])
        bt.logging.trace(
            f"Prioritizing {synapse.dendrite.hotkey} with value: {priority}.",
        )
        return priority

    def get_config_for_test(self) -> bt.config:
        return self.config

    def sync(self):
        """
        Wrapper for synchronizing the state of the network for the given miner.
        """
        # Ensure miner hotkey is still registered on the network.
        self.check_registered()

        self.resync_metagraph()

    def check_registered(self):
        # --- Check for registration.
        if not self.subtensor.is_hotkey_registered(
            netuid=self.config.netuid,
            hotkey_ss58=self.wallet.hotkey.ss58_address,
        ):
            bt.logging.error(
                f"Wallet: {self.wallet} is not registered on netuid {self.config.netuid}."
                f" Please register the hotkey using `btcli subnets register` before trying again."
            )
            sys.exit(1)


# This is the main function, which runs the miner.
if __name__ == "__main__":
    with Miner() as miner:
        while True:
            time.sleep(60)
