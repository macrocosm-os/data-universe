"""General utility functions."""

import bittensor as bt

_KB = 1024
_MB = 1024 * _KB


def mb_to_bytes(mb: int) -> int:
    """Returns the total number of bytes."""
    return mb * _MB


def seconds_to_hours(seconds: int) -> int:
    """Returns the total number of hours, rounded down."""
    return seconds // 3600


def is_miner(uid: int, metagraph: bt.metagraph) -> bool:
    """Checks if a UID on the subnet is a miner."""
    # Assume everyone who isn't a validator is a miner.
    # This explicilty disallows validator/miner hybrids.
    return metagraph.Tv[uid] == 0
