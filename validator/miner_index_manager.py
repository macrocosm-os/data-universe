"""Stores the data index for all miners."""


import bittensor as bt
import datetime
from typing import Optional
from common.data import ScorableMinerIndex
from common.protocol import GetDataChunkIndex
from storage.validator.validator_storage import ValidatorStorage

class MinerIndexManager:
    """Manager for handling the data index for all miners on the subnet."""
    
    def __init__(self, storage: ValidatorStorage):
        self.storage = storage



    def get_datetime_index_last_updated(self, uid: int) -> Optional[datetime.datetime]:
        """Returns the datetime (UTC) when the 'uid' miner's index was last updated.

        Returns None if the miner's index has never been updated.

        Args:
            uid (int): The miner's unique identifier.
        """
        # TODO: Read this from the index.
        return None
