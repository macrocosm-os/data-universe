import random
from typing import Any, Callable, Iterable, Tuple
import time
import datetime as dt

from common.data import (
    CompressedMinerIndex,
    DataSource,
)
from common.data_v2 import ScorableDataEntityBucket, ScorableMinerIndex


def get_only_element_matching_filter(
    iterable: Iterable[Any], filter: Callable[[Any], bool]
) -> Any:
    """Returns the only element in the iterable that matches the filter, or raises an exception if there are zero or more than one elements."""
    results = [x for x in iterable if filter(x)]
    if len(results) != 1:
        raise Exception(
            f"Expected exactly one element matching filter, but found {len(results)}"
        )
    return results[0]


def wait_for_condition(condition: Callable[[], bool], timeout: float = 10.0):
    """Waits until the provided condition is true, or until the timeout is reached."""
    start_time = time.time()
    while not condition():
        if time.time() - start_time > timeout:
            raise Exception("Timed out waiting for condition to be true.")
        time.sleep(0.1)


def convert_compressed_index_to_scorable_miner_index(
    index: CompressedMinerIndex, last_updated: dt.datetime
) -> ScorableMinerIndex:
    """Converts a CompressedMinerIndex to a ScorableMinerIndex, assuming size_bytes are fully scorable."""

    return ScorableMinerIndex(
        scorable_data_entity_buckets=[
            ScorableDataEntityBucket(
                time_bucket_id=time_bucket_id,
                source=source,
                label=bucket.label,
                size_bytes=size_bytes,
                scorable_bytes=size_bytes,
            )
            for source in index.sources
            for bucket in index.sources[source]
            for time_bucket_id, size_bytes in zip(
                bucket.time_bucket_ids, bucket.sizes_bytes
            )
        ],
        last_updated=last_updated,
    )


def are_scorable_indexes_equal(
    index1: ScorableMinerIndex, index2: ScorableMinerIndex
) -> Tuple[bool, str]:
    """Compares two ScorableMinerIndex instances for equality."""

    # Compare the last_updated fields.
    if index1.last_updated != index2.last_updated:
        return (
            False,
            f"last_updated fields do not match. {index1.last_updated} != {index2.last_updated}",
        )

    def sort_key(bucket: ScorableDataEntityBucket):
        return (
            bucket.time_bucket_id,
            bucket.source,
            bucket.label if bucket.label else "NULL",
        )

    index1_sorted = sorted(index1.scorable_data_entity_buckets, key=sort_key)
    index2_sorted = sorted(index2.scorable_data_entity_buckets, key=sort_key)
    for bucket1, bucket2 in zip(index1_sorted, index2_sorted):
        if bucket1 != bucket2:
            return (
                False,
                f"Buckets do not match. {bucket1} != {bucket2}",
            )

    return True, None


def are_compressed_indexes_equal(
    index1: CompressedMinerIndex, index2: CompressedMinerIndex
) -> bool:
    """Compares two CompressedMinerIndex instances for equality."""

    # Iterate both indexes, in order of sources.
    for source1, source2 in zip(sorted(index1.sources), sorted(index2.sources)):
        if source1 != source2:
            print(f"Sources do not match. {source1} != {source2}")
            return False

        # For a given source, compare the buckets.
        buckets1 = sorted(
            index1.sources[source1], key=lambda b: b.label if b.label else "NULL"
        )
        buckets2 = sorted(
            index2.sources[source2], key=lambda b: b.label if b.label else "NULL"
        )
        if buckets1 != buckets2:
            print(f"Buckets do not match. {buckets1} != {buckets2}")
            return False

    return True


def create_scorable_index(num_buckets: int) -> ScorableMinerIndex:
    """Creates a CompressedMinerIndex with ~ the specified number of buckets."""
    assert num_buckets > 1000

    labels = [f"label{i}" for i in range(num_buckets // 2 // 500)]
    time_buckets = [i for i in range(1, (num_buckets // 2 // len(labels)) + 1)]

    # Split max buckets equaly between sources with reddit having 100 time buckets and x having 500.
    buckets = []
    for source in [DataSource.REDDIT.value, DataSource.X.value]:
        for time_bucket in time_buckets:
            for label in labels:
                size = random.randint(50, 1000)
                scorable_bytes = int(random.random() * size)
                buckets.append(
                    ScorableDataEntityBucket(
                        time_bucket_id=time_bucket,
                        source=source,
                        label=label,
                        size_bytes=size,
                        scorable_bytes=scorable_bytes,
                    )
                )
    return ScorableMinerIndex(
        scorable_data_entity_buckets=buckets, last_updated=dt.datetime.now()
    )
