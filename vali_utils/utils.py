import bittensor as bt
import hashlib
import random
from typing import List, Optional, Tuple, Type
from common.data import (
    DataEntity,
    DataEntityBucket,
    DateRange,
    ScorableMinerIndex,
    TimeBucket,
)


def choose_data_entity_bucket_to_query(index: ScorableMinerIndex) -> DataEntityBucket:
    """Chooses a random DataEntityBucket to query from a MinerIndex.

    The random selection is done based on choosing a random byte in the total index to query, and then selecting
    that DataEntityBucket
    """
    total_size = sum(
        scorable_bucket.data_entity_bucket.size_bytes
        for scorable_bucket in index.scorable_data_entity_buckets
    )
    chosen_byte = random.uniform(0, total_size)
    iterated_bytes = 0
    for scorable_bucket in index.scorable_data_entity_buckets:
        if (
            iterated_bytes + scorable_bucket.data_entity_bucket.size_bytes
            >= chosen_byte
        ):
            return scorable_bucket.data_entity_bucket
        iterated_bytes += scorable_bucket.data_entity_bucket.size_bytes
    assert (
        False
    ), "Failed to choose a DataEntityBucket to query... which should never happen"


def choose_entities_to_verify(entities: List[DataEntity]) -> List[DataEntity]:
    """Given a list of DataEntities from a DataEntityBucket, chooses a random set of entities to verify."""

    # For now, we just sample 1 entity, based on size.
    # In future, consider sampling every N bytes.
    chosen_entities = []
    total_size = sum(entity.content_size_bytes for entity in entities)
    chosen_byte = random.uniform(0, total_size)
    iterated_bytes = 0
    for entity in entities:
        if iterated_bytes + entity.content_size_bytes >= chosen_byte:
            chosen_entities.append(entity)
            break
        iterated_bytes += entity.content_size_bytes
    return chosen_entities


def are_entities_valid(
    entities: List[DataEntity], data_entity_bucket: DataEntityBucket
) -> Tuple[bool, str]:
    """Performs basic validation on all entities in a DataEntityBucket.

    Returns a tuple of (is_valid, reason) where is_valid is True if the entities are valid,
    and reason is a string describing why they are not valid.
    """

    # Check the entity size, labels, source, and timestamp.
    actual_size = 0
    claimed_size = 0
    expected_datetime_range: DateRange = TimeBucket.to_date_range(
        data_entity_bucket.id.time_bucket
    )

    for entity in entities:
        actual_size += len(entity.content or b"")
        claimed_size += entity.content_size_bytes
        if entity.source != data_entity_bucket.id.source:
            return (
                False,
                f"Entity source {entity.source} does not match data_entity_bucket source {data_entity_bucket.id.source}",
            )
        if entity.label != data_entity_bucket.id.label:
            return (
                False,
                f"Entity label {entity.label} does not match data_entity_bucket label {data_entity_bucket.id.label}",
            )
        if not expected_datetime_range.contains(entity.datetime):
            return (
                False,
                f"Entity datetime {entity.datetime} is not in the expected range {expected_datetime_range}",
            )

    if actual_size < claimed_size or actual_size < data_entity_bucket.size_bytes:
        return (
            False,
            f"Size not as expected. Actual={actual_size}. Claimed={claimed_size}. Expected={data_entity_bucket.size_bytes}",
        )

    return (True, "")


def are_entities_unique(entities: List[DataEntity]) -> bool:
    """Checks that all entities in a DataEntityBucket are unique.

    This is currently done by comparing hashes of only the content as the entire scrape response is serialized into
    the content of each DataEntity.

    Returns a tuple of (is_unique, reason) where is_unique is True if the entities are unique,
    and reason is a string describing why they are not unique.
    """

    # Create a set to store the hash of each entity content.
    entity_content_hash_set = set()

    for entity in entities:
        entity_content_hash = hashlib.sha1(entity.content).hexdigest()
        # Check that this hash has not been seen before.
        if entity_content_hash in entity_content_hash_set:
            return False
        else:
            entity_content_hash_set.add(entity_content_hash)

    return True


def get_single_successul_response(
    responses: List[bt.Synapse], expected_class: Type
) -> Optional[bt.Synapse]:
    """Helper function to extract the single response from a list of responses, if the response is valid.

    return: (response, is_valid): The response if it's valid, else None.
    """
    if (
        responses
        and isinstance(responses, list)
        and len(responses) == 1
        and isinstance(responses[0], expected_class)
        and responses[0].is_success
    ):
        return responses[0]
    return None
