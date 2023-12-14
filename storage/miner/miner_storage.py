from abc import ABC, abstractmethod
from common.data import DataEntity, DataEntityBucket, DataEntityBucketId
from typing import List

class MinerStorage(ABC):
    """An abstract class which defines the contract that all implementations of MinerStorage must fulfill."""

    @abstractmethod
    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""
        raise NotImplemented
    
    @abstractmethod
    def list_data_entities_in_data_entity_bucket(self, data_entity_bucket_id: DataEntityBucketId) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataEntityBucket."""
        raise NotImplemented

    @abstractmethod
    def list_data_entity_buckets(self) -> List[DataEntityBucket]:
        """Lists all DataEntityBuckets for all the DataEntities that this MinerStorage is currently serving."""
        raise NotImplemented
