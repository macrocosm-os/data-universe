import bittensor as bt
import hashlib
import random
from typing import List, Optional, Tuple, Type, Union
import datetime as dt
from common import constants
from common.data import (
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucket,
    TimeBucket,
)
from common.data_v2 import ScorableMinerIndex
from common.date_range import DateRange
from common.protocol import GetMinerIndex
from scraping.x import utils as x_utils


def choose_data_entity_bucket_to_query(index: ScorableMinerIndex) -> DataEntityBucket:
    """Chooses a random DataEntityBucket to query from a MinerIndex.

    The random selection is done based on choosing a random scorable byte in the total index to query, and then
    selecting that DataEntityBucket.
    """
    total_size = sum(
        scorable_bucket.scorable_bytes
        for scorable_bucket in index.scorable_data_entity_buckets
    )
    chosen_byte = random.uniform(0, total_size)
    iterated_bytes = 0
    for scorable_bucket in index.scorable_data_entity_buckets:
        if iterated_bytes + scorable_bucket.scorable_bytes >= chosen_byte:
            return scorable_bucket.to_data_entity_bucket()
        iterated_bytes += scorable_bucket.scorable_bytes
    assert (
        False
    ), "Failed to choose a DataEntityBucket to query... which should never happen"


def choose_entities_to_verify(entities: List[DataEntity]) -> List[DataEntity]:
    """Given a list of DataEntities from a DataEntityBucket, chooses a random set of entities to verify."""

    # For now, we just sample 2 entities, based on size. Ensure we choose different entities.
    # In future, consider sampling every N bytes.
    chosen_entities = []
    total_size = sum(entity.content_size_bytes for entity in entities)

    # Ensure we don't try to choose more entities than exist to choose from.
    num_entities_to_choose = min(2, len(entities))
    for _ in range(num_entities_to_choose):
        chosen_byte = random.uniform(0, total_size)
        iterated_bytes = 0
        for entity in entities:
            if entity in chosen_entities:
                # Ensure we skip over already chosen entities if we see them again.
                continue

            if iterated_bytes + entity.content_size_bytes >= chosen_byte:
                chosen_entities.append(entity)
                # Adjust total_size to account for the entity we already selected.
                total_size -= entity.content_size_bytes
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

        tz_datetime = entity.datetime
        # If the data entity does not specify any timezone information then use UTC for validation checks.
        if tz_datetime.tzinfo is None:
            tz_datetime = tz_datetime.replace(tzinfo=dt.timezone.utc)

        if not expected_datetime_range.contains(tz_datetime):
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


def _normalize_uri(uri: str) -> str:
    """Normalizes a URI (independent of DataSource) for equality comparison."""
    # For now, we only normalize twitter URIs. Other DataSources don't require any normalization.
    # Note: This will leave the URI untouched if it is not a twitter URI.
    return x_utils.normalize_url(uri)


def are_entities_unique(entities: List[DataEntity]) -> bool:
    """Checks that all entities in a DataEntityBucket are unique.

    This is currently done by comparing hashes of only the content as the entire scrape response is serialized into
    the content of each DataEntity.

    Returns a tuple of (is_unique, reason) where is_unique is True if the entities are unique,
    and reason is a string describing why they are not unique.
    """

    # Create a set to store the hash of each entity content.
    entity_content_hash_set = set()
    uris = set()

    for entity in entities:
        entity_content_hash = hashlib.sha1(entity.content).hexdigest()
        normalized_uri = _normalize_uri(entity.uri)
        # Check that the hash and URI have not been seen before
        if entity_content_hash in entity_content_hash_set or normalized_uri in uris:
            return False
        else:
            entity_content_hash_set.add(entity_content_hash)
            uris.add(normalized_uri)

    return True


def get_single_successful_response(
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


def get_miner_index_from_response(response: GetMinerIndex) -> CompressedMinerIndex:
    """Gets a MinerIndex from a GetMinerIndex response."""
    assert response.is_success

    if not response.compressed_index_serialized:
        raise ValueError("GetMinerIndex response has no index.")

    return CompressedMinerIndex.model_validate_json(response.compressed_index_serialized)
