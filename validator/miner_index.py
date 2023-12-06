"""Stores the data index for all miners."""


import bittensor as bt
import datetime
from typing import Optional
from common.data import ScorableMinerIndex


class MinerIndexManager:
    def __init__(self):
        pass

    def update_index(
        self, uid: int, dendrite: bt.dendrite
    ) -> Optional[ScorableMinerIndex]:
        """Updates the index for the 'uid' miner, and returns the latest known index or None if the miner hasn't yet provided an index.

        Args:
            uid (int): The miner's unique identifier.
            dendrite (bt.dendrite): The dendrite (client) to use to query the miner
        """
        pass

    def get_datetime_index_last_updated(self, uid: int) -> Optional[datetime.datetime]:
        """Returns the datetime (UTC) when the 'uid' miner's index was last updated.

        Returns None if the miner's index has never been updated.

        Args:
            uid (int): The miner's unique identifier.
        """
        # TODO: Read this from the index.
        return None
