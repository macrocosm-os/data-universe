import threading
from typing import Any, Set, Optional, List, Tuple
from numpy import dtype
import numpy
import pandas as pd
import datetime as dt
import random

from sympy import Si
from common.data import DataLabel, DataSource, MinerIndex, CompressedMinerIndex
from common.data_v2 import ScorableDataEntityBucket, ScorableMinerIndex

from neurons import miner

# # Create miners
# miners = 5
# labels = 200_000
# unique_buckets = 1_000_000
# buckets_per_miner = 500_000
# total_buckets = miners * buckets_per_miner
# last_updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

# # Distribute credibilities evenly from 1 to 100%
# miner_values = [
#     [
#         "hotkey" + str(i),  # hotkey
#         last_updated,  # lastUpdated
#         i % 100 + 1,  # credibility
#     ]
#     for i in range(1, miners + 1)
# ]

# # Create labels
# label_values = [["label" + str(i)] for i in range(1, labels + 1)]

# # Create buckets
# bucket_values = [
#     [
#         i,  # timeBucketId
#         i % 2,  # source
#         i % labels + 1,  # labelId (these auto increment and therefore will match)
#         1000,  # credAdjSize
#     ]
#     for i in range(1, unique_buckets + 1)
# ]

# index_values = [
#     [
#         i // buckets_per_miner
#         + 1,  # minerId (these auto increment and therefore will match)
#         i % unique_buckets
#         + 1,  # bucketId (these auto increment and therefore will match)
#         100,  # contentSizeBytes
#     ]
#     for i in range(0, total_buckets)
# ]

# # Create a DataFrame with the specified columns
# data = {
#     'minerId': [i for i in range(1, miners + 1)] * buckets_per_miner,
#     'timebucketId': [i % unique_buckets for i in range(1, total_buckets + 1)],
#     'source': [i % 2 for i in range(1, total_buckets + 1)],
#     'labelId': [i % len(label_values) for i in range(1, total_buckets + 1)],
#     'size': [random.randint(1, 1000) for i in range(1, total_buckets + 1)]
# }


# TODO: Logging.


class AutoIncrementDict:
    """A dictionary that automatically assigns ids to keys.

    Provides O(1) ability to insert a key and get its id, and to lookup the key for an id.

    Not thread safe.
    """

    def __init__(self):
        self.available_ids = set()
        self.items = []
        self.indexes = {}

    def get_or_insert(self, key: Any) -> int:
        if key not in self.indexes:
            if self.available_ids:
                key_id = self.available_ids.pop()
                self.items[key_id] = key
                self.indexes[key] = key_id
            else:
                self.items.append(key)
                self.indexes[key] = len(self.items) - 1

        return self.indexes[key]

    def get_by_id(self, id: int) -> Any:
        return self.items[id]

    def delete_key(self, key: Any):
        if key in self.indexes:
            key_id = self.indexes[key]
            self.items[key_id] = None
            del self.indexes[key]
            self.available_ids.add(key_id)


class InMemoryValidatorStorage:
    def __init__(self):
        self.next_miner_id = 0
        self.miner_info = pd.DataFrame(
            columns=["miner_id", "last_updated", "credibility"],
            index=pd.Index([], name="hotkey", dtype="string"),
        )
        self.miner_info = self.miner_info.astype(
            {
                "miner_id": "UInt32",
                "last_updated": "datetime64[ns]",
                "credibility": "float64",
            }
        )

        index = pd.DataFrame({"miner_id": [], "bucket_id": []})
        index = index.astype({"miner_id": "UInt32", "bucket_id": "UInt32"})
        self.miner_index = pd.DataFrame(
            columns=["content_size_bytes"],
            dtype="UInt32",
            index=pd.MultiIndex.from_frame(index),
        )

        self.next_bucket_id = 0
        index = pd.DataFrame(columns=["source", "time_bucket_id", "label_id"])
        index = index.astype(
            {"source": "UInt8", "time_bucket_id": "UInt64", "label_id": "UInt64"}
        )
        self.buckets = pd.DataFrame(
            columns=["bucket_id"], dtype="UInt32", index=pd.MultiIndex.from_frame(index)
        )

        # A mapping from bucket_id to (source, time_bucket_id, label_id), for efficient reverse lookup.
        self.buckets_by_id = {}

        self.labels = AutoIncrementDict()

        # For now, keep the update logic simple by using a global lock for all updates
        self.lock = threading.RLock()

    def _get_next_miner_id(self) -> int:
        """Returns the next miner id to use."""
        self.next_miner_id += 1
        return self.next_miner_id - 1

    def _get_next_bucket_id(self) -> int:
        """Returns the next bucket id to use."""
        self.next_bucket_id += 1
        return self.next_bucket_id - 1

    def _get_next_bucket_ids(self, n: int) -> List[int]:
        """Returns the next n bucket ids to use."""
        ids = range(self.next_bucket_id, self.next_bucket_id + n)
        self.next_bucket_id += n
        return ids

    def _get_miner_id(self, hotkey: str) -> Optional[int]:
        """Returns the miner id for the specified hotkey, or None if it doesn't exist."""
        return self.miner_info["miner_id"].get(hotkey, default=None)

    def _upsert_miner(
        self, hotkey: str, last_updated: dt.datetime, credibility: float
    ) -> int:
        """Inserts the miner if it doesn't exist, or updates it if it does."""
        with self.lock:
            # First, check if the miner exists.
            miner_id = self._get_miner_id(hotkey)
            if miner_id is not None:
                # Miner exists. Update it.
                self.miner_info.at[hotkey, "last_updated"] = last_updated
                self.miner_info.at[hotkey, "credibility"] = credibility
                return miner_id

            # Miner doesn't exist. Create it.
            miner_id = self._get_next_miner_id()
            new_row = pd.DataFrame(
                {
                    "miner_id": miner_id,
                    "last_updated": [pd.to_datetime(last_updated)],
                    "credibility": [credibility],
                },
                index=pd.Index([hotkey], name="hotkey", dtype="string"),
            )
            self.miner_info = pd.concat([self.miner_info, new_row], ignore_index=False)
            return miner_id

    def _upsert_label(self, label: Optional[str]) -> int:
        """Gets the label id for the specified label, or inserts it if it doesn't exist."""
        return self.labels.get_or_insert(label)

    def _upsert_bucket(self, source: int, time_bucket_id: int, label_id: int) -> int:
        """Gets the bucket id for the specified bucket, or inserts it if it doesn't exist."""
        # First, check if the label exists.
        bucket_id = self.buckets["bucket_id"].get(
            (source, time_bucket_id, label_id), default=None
        )
        if bucket_id is not None:
            return bucket_id

        # Bucket doesn't exist. Create it.
        bucket_id = self._get_next_bucket_id()
        index = pd.DataFrame(
            {
                "source": [source],
                "time_bucket_id": [time_bucket_id],
                "label_id": [label_id],
            }
        )
        index = index.astype(
            {"source": "UInt8", "time_bucket_id": "UInt64", "label_id": "UInt64"}
        )
        new_row = pd.DataFrame(
            {"bucket_id": [bucket_id]},
            dtype="UInt64",
            index=pd.MultiIndex.from_frame(index),
        )
        self.buckets = pd.concat(
            [self.buckets, new_row], ignore_index=False, copy=False
        )
        self.buckets_by_id[bucket_id] = (source, time_bucket_id, label_id)
        return bucket_id

    def _upsert_buckets(self, buckets: List[Tuple[int, int, int]]) -> List[int]:
        """Gets the bucket id for the specified bucket, or inserts it if it doesn't exist."""
        # First, check if the label exists.
        index = pd.MultiIndex.from_tuples(
            buckets, names=["source", "time_bucket_id", "label_id"]
        )

        new_buckets = index
        if self.buckets.size > 0:
            new_buckets = index.difference(self.buckets.index)

        bucket_ids = self._get_next_bucket_ids(new_buckets.size)

        new_rows = pd.DataFrame(
            data=bucket_ids,
            dtype="UInt32",
            columns=["bucket_id"],
            index=new_buckets,
        )

        # Mark down the bucket ids
        for new_bucket, id in zip(new_buckets, bucket_ids):
            # new_bucket is a tuple of (source, time_bucket_id, label_id)
            self.buckets_by_id[id] = new_bucket

        if self.buckets.size == 0:
            self.buckets = new_rows
        else:
            self.buckets = pd.concat(
                [self.buckets, new_rows], ignore_index=False, copy=False
            )

        # Return the list of bucket ids in the same order the buckets were provided.
        # .values returns an ndarray.
        return self.buckets.loc[index].bucket_id.values.tolist()

    def _normalize_data_label(self, label: Optional[DataLabel]) -> Optional[str]:
        return label.value if label else None

    def upsert_miner_index(self, index: MinerIndex, credibility: float):
        """Stores the index for all of the data that a specific miner promises to provide."""
        with self.lock:
            # First, get the miner_id for this miner.
            miner_id = self._upsert_miner(
                index.hotkey, dt.datetime.utcnow(), credibility
            )

            # Delete the current index.
            self._delete_miner_index(miner_id)

            # Insert the index.
            # First, create the list of bucket_ids and their sizes.
            bucket_id_set = set()
            bucket_ids = []
            sizes = []
            # TODO: Fix
            for bucket in index.data_entity_buckets:
                bucket_id = self._upsert_bucket(
                    source=int(bucket.id.source),
                    time_bucket_id=bucket.id.time_bucket.id,
                    label_id=self._upsert_label(
                        self._normalize_data_label(bucket.id.label)
                    ),
                )
                if bucket_id in bucket_id_set:
                    # Found a dupe. Skip
                    continue
                bucket_ids.append(bucket_id)
                bucket_id_set.add(bucket_id)
                sizes.append(bucket.size_bytes)
            rows = pd.DataFrame(
                {"content_size_bytes": sizes},
                dtype="UInt32",
                index=pd.MultiIndex.from_arrays(
                    [
                        pd.Series([miner_id] * len(bucket_ids), dtype="UInt32"),
                        pd.Series(bucket_ids, dtype="UInt32"),
                    ],
                    names=["miner_id", "bucket_id"],
                ),
            )
            self.miner_index = pd.concat([self.miner_index, rows], ignore_index=False)

    def upsert_compressed_miner_index(
        self, index: CompressedMinerIndex, hotkey: str, credibility: float
    ):
        """Stores the index for all of the data that a specific miner promises to provide."""
        with self.lock:
            # First, get the miner_id for this miner.
            miner_id = self._upsert_miner(hotkey, dt.datetime.utcnow(), credibility)

            # Delete the current index.
            self._delete_miner_index(miner_id)

            # Insert the index.
            # First, create the list of bucket_ids and their sizes.
            unique_buckets = set()
            buckets = []
            sizes = []
            for source, compressed_buckets in index.sources.items():
                for compressed_bucket in compressed_buckets:
                    for time_bucket_id, size_bytes in zip(
                        compressed_bucket.time_bucket_ids, compressed_bucket.sizes_bytes
                    ):
                        bucket = (
                            source,
                            time_bucket_id,
                            self._upsert_label(compressed_bucket.label),
                        )
                        if bucket in unique_buckets:
                            # Found a dupe. Skip
                            continue
                        buckets.append(bucket)
                        sizes.append(size_bytes)

            bucket_ids = self._upsert_buckets(buckets)

            rows = pd.DataFrame(
                data=sizes,
                columns=["content_size_bytes"],
                dtype="UInt32",
                index=pd.MultiIndex.from_arrays(
                    [
                        pd.Series([miner_id] * len(bucket_ids), dtype="UInt32"),
                        pd.Series(bucket_ids, dtype="UInt32"),
                    ],
                    names=["miner_id", "bucket_id"],
                ),
            )

            if self.miner_index.size == 0:
                self.miner_index = rows
            else:
                self.miner_index = pd.concat(
                    [self.miner_index, rows], ignore_index=False, copy=False
                )

    def read_miner_index(self, miner_hotkey: str) -> Optional[ScorableMinerIndex]:
        """Gets a scored index for all of the data that a specific miner promises to provide."""
        with self.lock:
            miner_id = self._get_miner_id(miner_hotkey)
            if miner_id is None:
                return None

            # Compute the bucket sizes.
            miner_credibility = self.miner_info.at[miner_hotkey, "credibility"]
            miner_buckets = self.miner_index.query("miner_id == @miner_id")

            # Join on credibility.
            all_miner_buckets = pd.merge(
                self.miner_index.copy().reset_index(),
                self.miner_info[["miner_id", "credibility"]].copy().reset_index(),
                on=("miner_id"),
                how="inner",
            ).set_index(["miner_id", "bucket_id"])

            all_miner_buckets["scaled_size_bytes"] = (
                all_miner_buckets["content_size_bytes"]
                * all_miner_buckets["credibility"]
            )

            all_miner_buckets = all_miner_buckets.groupby("bucket_id", sort=False).sum()

            join = pd.merge(
                miner_buckets,
                all_miner_buckets,
                on=("bucket_id"),
                how="left",
                suffixes=("_self", "_other"),
            ).reset_index()

            join["scorable_bytes"] = (
                # First compute the ratio of this miners bytes relative to all miners.
                (
                    join["content_size_bytes_self"]
                    * miner_credibility
                    / join["scaled_size_bytes"]
                )
                # Then multiply that ratio by the miner's bucket size.
                * join["content_size_bytes_self"]
            )

            buckets = [None] * join.shape[0]
            for i, tup in enumerate(
                join[
                    ["bucket_id", "content_size_bytes_self", "scorable_bytes"]
                ].to_numpy(copy=False)
            ):
                source, time_bucket_id, label_id = self.buckets_by_id[tup[0]]

                buckets[i] = ScorableDataEntityBucket(
                    source=DataSource(source),
                    time_bucket_id=time_bucket_id,
                    label=self.labels.get_by_id(label_id),
                    size_bytes=tup[1],
                    scorable_bytes=tup[2],
                )

            last_updated = self.miner_info.at[miner_hotkey, "last_updated"]
            return ScorableMinerIndex(
                scorable_data_entity_buckets=buckets, last_updated=last_updated
            )

    def _delete_miner_index(self, miner_id: int):
        # An empty data frame won't have a "miner_id" column, so check for that first.
        if self.miner_index.size > 0:
            try:
                self.miner_index.drop(miner_id, level=0, axis=0, inplace=True)
            except KeyError:
                # The miner didn't have an index.
                pass

    # TODO: Change to delete miner.
    def delete_miner_index(self, miner_hotkey: str):
        """Removes the index for the specified miner."""
        with self.lock:
            miner_id = self._get_miner_id(miner_hotkey)
            if miner_id is not None:
                self._delete_miner_index(miner_id)

    def read_miner_last_updated(self, miner_hotkey: str) -> Optional[dt.datetime]:
        """Gets when a specific miner was last updated."""
        with self.lock:
            miner_id = self._get_miner_id(miner_hotkey)
            if miner_id is None:
                return None
            return self.miner_info.at[miner_hotkey, "last_updated"]
