import asyncio
import functools
from typing import Dict, Optional, Any
import bittensor as bt
import multiprocessing
import argparse

def add_args(parser: argparse.ArgumentParser, is_upload: bool):
    """Add arguments to the parser"""
    parser.add_argument('--wallet', type=str, required=True, help='Name of the wallet')
    parser.add_argument('--hotkey', type=str, required=True, help='Name of the hotkey')
    parser.add_argument('--network', type=str, required=True, help='Name of the subtensor network', default='finney')
    parser.add_argument('--netuid', type=int, required=True, help='UID of the subnet', default=13)
    
    if is_upload:
        parser.add_argument('--file_path', type=str, required=True, help='Path to the JSON file containing preferences')

def _sync_retrieve_metadata(netuid: int, hotkey: str, network: str = "finney"):
    """Standalone function that can be pickled"""
    try:
        # Create a fresh subtensor instance for each call
        fresh_subtensor = bt.subtensor(network=network)
        
        metadata = bt.core.extrinsics.serving.get_metadata(
            fresh_subtensor, 
            netuid, 
            hotkey
        )
        
        if not metadata:
            return None
            
        commitment = metadata["info"]["fields"][0]
        hex_data = commitment[list(commitment.keys())[0]][2:]
        return bytes.fromhex(hex_data).decode()
    except Exception as e:
        bt.logging.error(f"Error retrieving metadata for {hotkey}: {str(e)}")
        return None


def _wrapped_func(func: functools.partial, queue: multiprocessing.Queue):
    try:
        result = func()
        queue.put(result)
    except Exception as e:
        queue.put(None)  # Return None instead of raising on error


def run_in_subprocess(func: functools.partial, ttl: int = 10) -> Any:
    """Runs with shorter timeout and better error handling"""
    ctx = multiprocessing.get_context('fork')
    queue = ctx.Queue()
    process = ctx.Process(target=_wrapped_func, args=[func, queue])

    process.start()
    process.join(timeout=ttl)

    if process.is_alive():
        process.terminate()
        process.join()
        return None  # Return None on timeout instead of raising

    try:
        result = queue.get(block=False)
        return result
    except Exception:
        return None


class ChainPreferenceStore:
    def __init__(
        self,
        subtensor: bt.subtensor,
        netuid: int,
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

        def sync_store():
            return bt.core.extrinsics.serving.publish_metadata(
                self.subtensor,
                self.wallet,
                self.netuid,
                f"Raw{len(data)}",
                data.encode(),
                wait_for_inclusion,
                wait_for_finalization,
            )

        partial = functools.partial(sync_store)
        bt.logging.info("Writing to chain...")
        return run_in_subprocess(partial, 60)

    async def retrieve_preferences(self, hotkey: str) -> Optional[str]:
        """Single retrieval with shorter timeout"""
        partial = functools.partial(_sync_retrieve_metadata, self.netuid, hotkey)
        return run_in_subprocess(partial, ttl=10)  # Shorter timeout per validator

    async def batch_retrieve_preferences(self, hotkeys: list[str], chunk_size: int = 5) -> Dict[str, Optional[str]]:
        """Retrieve preferences for multiple validators in chunks"""
        results = {}
        
        # Process in chunks to avoid overwhelming the system
        for i in range(0, len(hotkeys), chunk_size):
            chunk = hotkeys[i:i + chunk_size]
            chunk_tasks = []
            
            # Create tasks for each hotkey in the chunk
            for hotkey in chunk:
                task = asyncio.create_task(self.retrieve_preferences(hotkey))
                chunk_tasks.append((hotkey, task))
            
            # Wait for all tasks in chunk to complete
            for hotkey, task in chunk_tasks:
                try:
                    result = await task
                    results[hotkey] = result
                except Exception as e:
                    bt.logging.error(f"Error processing {hotkey}: {str(e)}")
                    results[hotkey] = None
            
            # Small delay between chunks
            await asyncio.sleep(0.1)
        
        return results


if __name__ == "__main__":
    # Example usage
    parser = argparse.ArgumentParser()
    add_args(parser, is_upload=False)
    args = parser.parse_args()

    subtensor = bt.subtensor(network=args.network)
    wallet = bt.wallet(name=args.wallet, hotkey=args.hotkey)
    
    store = ChainPreferenceStore(subtensor, args.netuid, wallet)
    
    async def test():
        result = await store.retrieve_preferences(args.hotkey)
        print(f"Retrieved preferences: {result}")
    
    asyncio.run(test())