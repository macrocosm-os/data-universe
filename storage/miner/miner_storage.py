from abc import ABC, abstractmethod
from common.data import DataEntity, DataChunkSummary
from typing import List

class MinerStorage(ABC):
    """An abstract class which defines the contract that all implementations of MinerStorage must fulfill."""

    @abstractmethod
    def store_data_entities(self, dataEntities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""
        raise NotImplemented
    
    @abstractmethod
    def list_data_entities_in_data_chunk(self, data_chunk_summary: DataChunkSummary) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataChunkSummary."""
        raise NotImplemented

    @abstractmethod
    def list_data_chunk_summaries(self) -> List[DataChunkSummary]:
        """Lists all DataChunkSummaries for all the DataEntities that this MinerStorage is currently serving."""
        raise NotImplemented
