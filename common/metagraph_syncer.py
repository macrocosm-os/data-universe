import asyncio
from concurrent.futures import ThreadPoolExecutor
import dataclasses
from dataclasses import field
from datetime import datetime
import functools
import bittensor as bt
from typing import Dict, List, Callable, Optional
import threading
import traceback

from common import utils


class MetagraphSyncer:
    @dataclasses.dataclass
    class _State:
        metagraph: Optional[bt.metagraph] = None
        last_synced_time: Optional[datetime] = None
        listeners: List = field(default_factory=list)

    def __init__(self, subtensor: bt.subtensor, config: Dict[int, int]):
        """Constructs a new MetagraphSyncer, that periodically refreshes metagraph defined in the config.

        Args:
            subtensor (bt.subtensor): The subtensor used to fetch the metagraphs.
            config (Dict[int, int]): A mapping of netuid to the cadence (in seconds) to sync the metagraph.
        """
        self.subtensor = subtensor
        self.config = config
        self.metagraph_map: Dict[int, MetagraphSyncer._State] = {
            netuid: MetagraphSyncer._State() for netuid in config.keys()
        }
        self.is_running = False
        self.done_initial_sync = False
        self.lock = threading.RLock()
        self.notify_listener_thread_pool = ThreadPoolExecutor(max_workers=1)

    def do_initial_sync(self):
        """Performs an initial sync of all metagraphs.

        Unlike regular syncs, this will not notify listeners of the updated metagraph.
        """
        bt.logging.debug("Metagraph syncer do_initial_sync called")

        for netuid in self.config.keys():
            fn = functools.partial(self.subtensor.metagraph, netuid)
            metagraph = utils.run_in_subprocess(fn, ttl=120)
            with self.lock:
                state = self.metagraph_map[netuid]
                state.metagraph = metagraph
                state.last_synced_time = datetime.now()

            bt.logging.debug(f"Successfully loaded metagraph for {netuid}")

        self.done_initial_sync = True

    def start(self):
        bt.logging.debug("Metagraph syncer start called")

        assert self.done_initial_sync, "Must call do_initial_sync before starting"

        self.is_running = True
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()

    async def _sync_metagraph_loop(self, netuid: int, cadence: int):
        while self.is_running:
            # On start, wait cadence before the first sync.
            await asyncio.sleep(cadence)

            try:
                # Intentionally block the shared thread so that we only
                # sync 1 metagraph at a time.
                metagraph = utils.run_in_subprocess(
                    functools.partial(self.subtensor.metagraph, netuid), ttl=120
                )
                state = None
                with self.lock:
                    # Store metagraph and sync time
                    state = self.metagraph_map[netuid]
                    state.metagraph = metagraph
                    state.last_synced_time = datetime.now()

                await self._notify_listeners(state, netuid)

                await asyncio.sleep(cadence)
            except (BaseException, Exception) as e:
                bt.logging.error(
                    f"Error when syncing metagraph for {netuid}: {e}. Retrying in 60 seconds."
                )
                await asyncio.sleep(60)

    async def _run_async(self):
        # For each netuid we should sync metagraphs for, spawn a Task to sync it.
        await asyncio.wait(
            [
                self._sync_metagraph_loop(netuid, cadence)
                for netuid, cadence in self.config.items()
            ],
            return_when=asyncio.ALL_COMPLETED,
        )

    def _run(self):
        asyncio.run(self._run_async())
        bt.logging.info("MetagraphSyncer _run complete.")

    def register_listener(
        self, listener: Callable[[bt.metagraph, int], None], netuids: List[int]
    ):
        """Registers a listener to be notified when a metagraph for any netuid in netuids is updated.

        The listener will be called from a different thread, so it must be thread-safe.
        """
        if not netuids:
            raise ValueError("Must provide at least 1 netuid")

        with self.lock:
            for netuid in netuids:
                if netuid not in self.metagraph_map:
                    raise ValueError(
                        f"Metagraph for {netuid} not being tracked in MetagraphSyncer."
                    )
                self.metagraph_map[netuid].listeners.append(listener)

    def get_metagraph(self, netuid: int) -> bt.metagraph:
        """Returns the last synced version of the metagraph for netuid."""
        with self.lock:
            if netuid not in self.metagraph_map:
                raise ValueError(
                    f"Metagraph for {netuid} not known to MetagraphSyncer."
                )
            metagraph = self.metagraph_map[netuid].metagraph
            if not metagraph:
                raise ValueError(f"Metagraph for {netuid} has not been synced yet.")
            return metagraph

    async def _notify_listeners(self, state: _State, netuid: int):
        """Notifies listeners of a new metagraph for netuid."""

        for listener in state.listeners:
            try:
                await self.notify_listener_thread_pool.submit(
                    listener(state.metagraph, netuid)
                )
            except Exception:
                bt.logging.error(
                    f"Exception caught notifying {netuid} listener of metagraph update.\n{traceback.format_exc()}"
                )
