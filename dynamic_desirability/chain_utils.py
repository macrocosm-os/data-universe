from datetime import datetime, timedelta
import functools
import multiprocessing
import json
from typing import Any, Optional, Dict
import bittensor as bt
import argparse

"""
Based off of taoverse run_in_subprocess and ChainModelMetadataStore
"""

def add_args(parser: argparse.ArgumentParser, is_upload: bool):
    parser.add_argument('--wallet', type=str, required=True, help='Name of the wallet')
    parser.add_argument('--hotkey', type=str, required=True, help='Name of the hotkey')
    parser.add_argument('--network', type=str, required=True, help='Name of the network')
    parser.add_argument('--netuid', type=int, required=True, help='UID of the subnet')
    
    if is_upload:
        parser.add_argument('--file_path', type=str, required=True, help='Path to the JSON file containing preferences')


def _wrapped_func(func: functools.partial, queue: multiprocessing.Queue):
    try:
        result = func()
        queue.put(result)
    except (Exception, BaseException) as e:
        # Catch exceptions here to add them to the queue.
        queue.put(e)

def run_in_subprocess(func: functools.partial, ttl: int, mode="fork") -> Any:
    """Runs the provided function on a subprocess with 'ttl' seconds to complete.

    Args:
        func (functools.partial): Function to be run.
        ttl (int): How long to try for in seconds.
        mode (str): Mode by which the multiprocessing context is obtained. Default to fork for pickling.

    Returns:
        Any: The value returned by 'func'
    """
    ctx = multiprocessing.get_context(mode)
    queue = ctx.Queue()
    process = ctx.Process(target=_wrapped_func, args=[func, queue])

    process.start()

    process.join(timeout=ttl)

    if process.is_alive():
        process.terminate()
        process.join()
        raise TimeoutError(f"Failed to {func.func.__name__} after {ttl} seconds")

    # Raises an error if the queue is empty. This is fine. It means our subprocess timed out.
    result = queue.get(block=False)

    # If we put an exception on the queue then raise instead of returning.
    if isinstance(result, Exception):
        raise result
    if isinstance(result, BaseException):
        raise Exception(f"BaseException raised in subprocess: {str(result)}")

    return result


class ChainPreferenceStore():
    """Chain based implementation for storing and retrieving validator preferences."""

    def __init__(
        self,
        subtensor: bt.subtensor,
        netuid: int,
        # Wallet is only needed to write to the chain, not to read.
        wallet: Optional[bt.wallet] = None,
    ):
        self.subtensor = subtensor
        self.wallet = wallet
        self.netuid = netuid

    async def store_preferences(
        self,
        data: str,
        wait_for_inclusion: bool = True,
        wait_for_finalization: bool = True,
    ):
        """Stores preferences on this subnet for a specific wallet."""
        if self.wallet is None:
            raise ValueError("No wallet available to write to the chain.")
        if not data:
            raise ValueError("No data provided to store on the chain.")

        # Wrap calls to the subtensor in a subprocess with a timeout to handle potential hangs.
        partial = functools.partial(
            bt.extrinsics.serving.publish_metadata,
            self.subtensor,
            self.wallet,
            self.netuid,
            f"Raw{len(data)}",
            data.encode(), 
            wait_for_inclusion,
            wait_for_finalization,
        )
        bt.logging.info("Writing to chain...")
        run_in_subprocess(partial, 60)

    async def retrieve_preferences(self, hotkey: str) -> str:
        """Retrieves latest github commit hash on this subnet for specific hotkey"""

        # Wrap calls to the subtensor in a subprocess with a timeout to handle potential hangs.
        partial = functools.partial(
            bt.extrinsics.serving.get_metadata, self.subtensor, self.netuid, hotkey
        )

        metadata = run_in_subprocess(partial, 60)

        if not metadata:
            return None

        commitment = metadata["info"]["fields"][0]
        hex_data = commitment[list(commitment.keys())[0]][2:]

        chain_str = bytes.fromhex(hex_data).decode()

        return chain_str