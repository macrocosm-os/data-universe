import datetime as dt
from abc import ABC, abstractmethod
from typing import Optional

from common.data import CompressedMinerIndex
from common.data_v2 import ScorableMinerIndex


class ValidatorStorage(ABC):
    """An abstract class which defines the contract that all implementations of ValidatorStorage must fulfill."""

    @abstractmethod
    def upsert_compressed_miner_index(self, index: CompressedMinerIndex, hotkey: str, credibility: float = 0):
        """Stores the index for all of the data that a specific miner promises to provide."""
        raise NotImplementedError

    @abstractmethod
    def read_miner_index(self, miner_hotkey: str) -> ScorableMinerIndex | None:
        """Gets a scored index for all of the data that a specific miner promises to provide."""
        raise NotImplementedError

    @abstractmethod
    def delete_miner(self, miner_hotkey: str):
        """Removes the index and miner information for the specified miner."""
        raise NotImplementedError

    @abstractmethod
    def read_miner_last_updated(self, miner_hotkey: str) -> dt.datetime | None:
        """Gets when a specific miner was last updated."""
        raise NotImplementedError
