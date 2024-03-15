from abc import ABC, abstractmethod
from common.data import CompressedMinerIndex
from typing import Optional
import datetime as dt

from common.data_v2 import ScorableMinerIndex


class ValidatorStorage(ABC):
    """An abstract class which defines the contract that all implementations of ValidatorStorage must fulfill."""

    @abstractmethod
    def upsert_compressed_miner_index(
        self, index: CompressedMinerIndex, hotkey: str, credibility: float = 0
    ):
        """Stores the index for all of the data that a specific miner promises to provide."""
        raise NotImplemented

    @abstractmethod
    def read_miner_index(self, miner_hotkey: str) -> Optional[ScorableMinerIndex]:
        """Gets a scored index for all of the data that a specific miner promises to provide."""
        raise NotImplemented

    @abstractmethod
    def delete_miner(self, miner_hotkey: str):
        """Removes the index and miner information for the specified miner."""
        raise NotImplemented

    @abstractmethod
    def read_miner_last_updated(self, miner_hotkey: str) -> Optional[dt.datetime]:
        """Gets when a specific miner was last updated."""
        raise NotImplemented
