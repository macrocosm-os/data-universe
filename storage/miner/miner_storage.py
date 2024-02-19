from abc import ABC, abstractmethod
from common.data import (
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucketId,
)
from typing import Dict, List


class MinerStorage(ABC):
    """An abstract class which defines the contract that all implementations of MinerStorage must fulfill."""

    @abstractmethod
    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""
        raise NotImplemented

    @abstractmethod
    def list_data_entities_in_data_entity_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataEntityBucket."""
        raise NotImplemented

    @abstractmethod
    def get_compressed_index(self) -> CompressedMinerIndex:
        """Gets the compressed MinedIndex, which is a summary of all of the DataEntities that this MinerStorage is currently serving."""
        raise NotImplemented

    @abstractmethod
    def list_obfuscated_data_entities_in_data_entity_buckets(
        self, data_entity_bucket_ids: List[DataEntityBucketId]
    ) -> Dict[DataEntityBucketId, List[DataEntity]]:
        """Lists data entities with obfuscated timestamps for each requested DataEntityBucketId.

        Args:
            data_entity_bucket_ids (List[DataEntityBucketId]): Which buckets to get entities for.

        Returns:
            Dict[DataEntityBucketId, List[DataEntity]]: Map of each bucket id to contained entities.
        """
        raise NotImplemented
