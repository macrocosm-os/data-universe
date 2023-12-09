from abc import ABC, abstractmethod
from common.data import MinerIndex, ScorableMinerIndex
from typing import Set

class ValidatorStorage(ABC):
    """An abstract class which defines the contract that all implementations of ValidatorStorage must fulfill."""

    @abstractmethod
    def upsert_miner_index(self, index: MinerIndex):
        """Stores the index for all of the data that a specific miner promises to provide."""
        raise NotImplemented

    @abstractmethod
    def read_miner_index(self, miner_id: int, valid_miners: Set[str]) -> ScorableMinerIndex:
        """Gets a scored index for all of the data that a specific miner promises to provide."""
        raise NotImplemented

    @abstractmethod
    def delete_miner_index(self, miner_id: int):
        """Removes the index for the specified miner."""
        raise NotImplemented
