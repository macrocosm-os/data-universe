from abc import ABC, abstractmethod
from common.data import CompressedMinerIndex
from typing import List, Optional
import datetime as dt

from common.data_v2 import (
    DataBoxAgeSize,
    DataBoxLabelSize,
    DataBoxMiner,
    ScorableMinerIndex,
)


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

    @abstractmethod
    def read_databox_miners(self) -> List[DataBoxMiner]:
        """Gets details about miners for use in databox dashboards."""
        raise NotImplemented

    @abstractmethod
    def read_databox_age_sizes(self) -> List[DataBoxAgeSize]:
        """Gets details about age sizes for use in databox dashboards."""
        raise NotImplemented

    @abstractmethod
    def read_databox_label_sizes(self) -> List[DataBoxLabelSize]:
        """Gets details about label sizes for use in databox dashboards."""
        raise NotImplemented
