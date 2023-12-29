from abc import ABC, abstractmethod
from common.data import CompressedMinerIndex, MinerIndex
from typing import Optional, Set
import datetime as dt

from common.data_v2 import ScorableMinerIndex


class ValidatorStorage(ABC):
    """An abstract class which defines the contract that all implementations of ValidatorStorage must fulfill."""

    @abstractmethod
    def upsert_miner_index(self, index: MinerIndex):
        """Stores the index for all of the data that a specific miner promises to provide."""
        raise NotImplemented

    @abstractmethod
    def upsert_compressed_miner_index(self, index: CompressedMinerIndex):
        """Stores the index for all of the data that a specific miner promises to provide."""
        raise NotImplemented

    @abstractmethod
    def read_miner_index(
        self, miner_hotkey: str, valid_miners: Set[str]
    ) -> Optional[ScorableMinerIndex]:
        """Gets a scored index for all of the data that a specific miner promises to provide."""
        raise NotImplemented

    @abstractmethod
    def delete_miner_index(self, miner_hotkey: str):
        """Removes the index for the specified miner."""
        raise NotImplemented

    @abstractmethod
    def read_miner_last_updated(self, miner_hotkey: str) -> Optional[dt.datetime]:
        """Gets when a specific miner was last updated."""
        raise NotImplemented
