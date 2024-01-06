import contextlib
import datetime as dt
import bittensor as bt
import sqlite3
import threading
from typing import Any, Dict, Optional, Set, Tuple
from common.data import CompressedMinerIndex, DataLabel, MinerIndex
from common.data_v2 import ScorableMinerIndex
from storage.validator.validator_storage import ValidatorStorage


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


# Use a timezone aware adapter for timestamp columns.
def tz_aware_timestamp_adapter(val):
    datepart, timepart = val.split(b" ")
    year, month, day = map(int, datepart.split(b"-"))

    if b"+" in timepart:
        timepart, tz_offset = timepart.rsplit(b"+", 1)
        if tz_offset == b"00:00":
            tzinfo = dt.timezone.utc
        else:
            hours, minutes = map(int, tz_offset.split(b":", 1))
            tzinfo = dt.timezone(dt.timedelta(hours=hours, minutes=minutes))
    elif b"-" in timepart:
        timepart, tz_offset = timepart.rsplit(b"-", 1)
        if tz_offset == b"00:00":
            tzinfo = dt.timezone.utc
        else:
            hours, minutes = map(int, tz_offset.split(b":", 1))
            tzinfo = dt.timezone(dt.timedelta(hours=-hours, minutes=-minutes))
    else:
        tzinfo = None

    timepart_full = timepart.split(b".")
    hours, minutes, seconds = map(int, timepart_full[0].split(b":"))

    if len(timepart_full) == 2:
        microseconds = int("{:0<6.6}".format(timepart_full[1].decode()))
    else:
        microseconds = 0

    val = dt.datetime(year, month, day, hours, minutes, seconds, microseconds, tzinfo)

    return val


class SqliteMemoryValidatorStorage(ValidatorStorage):
    """Sqlite in-memory backed Validator Storage"""

    # Integer Primary Key = ROWID alias which is auto-increment when assigning NULL on insert.
    # TODO Confirm DECIMAL works over a float here.
    MINER_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS Miner (
                            minerId     INTEGER         PRIMARY KEY,
                            hotkey      VARCHAR(64)     NOT NULL,
                            lastUpdated TIMESTAMP(6)    NOT NULL,
                            credibility DECIMAL(5,2)    NOT NULL    DEFAULT 0.00,
                            UNIQUE(hotkey)
                            )"""

    # INDEX id_cred_idx (minerId, credibility)
    # INDEX on minerId hotkey as well?
    MINER_TABLE_HOTKEY_INDEX = """CREATE INDEX IF NOT EXISTS data_entity_bucket_index
                                ON DataEntity (timeBucketId, source, label)"""

    # Updated Primary table in which the DataEntityBuckets for all miners are stored.
    # TODO consider a index on BucketId/ContentSizeBytes for uniqueness
    MINER_INDEX_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS MinerIndex (
                                    minerId             INTEGER         NOT NULL,
                                    source              TINYINT         NOT NULL,
                                    labelId             INTEGER         NOT NULL,
                                    timeBucketId        INTEGER         NOT NULL,
                                    contentSizeBytes    INTEGER         NOT NULL,
                                    PRIMARY KEY(minerId, source, labelId, timeBucketId)
                                    ) WITHOUT ROWID"""

    MINER_INDEX_TABLE_INDEX = """CREATE INDEX IF NOT EXISTS bucket_size_index
                            ON MinerIndex (source, labelId, timeBucketId, contentSizeBytes)"""

    def __init__(self):
        sqlite3.register_converter("timestamp", tz_aware_timestamp_adapter)

        self.connection = self._create_connection()
        self.label_dict = AutoIncrementDict()

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            # Create the Miner table (if it does not already exist).
            cursor.execute(SqliteMemoryValidatorStorage.MINER_TABLE_CREATE)
            # Whichever indexes we need.

            # Create the Index table (if it does not already exist)/
            cursor.execute(SqliteMemoryValidatorStorage.MINER_INDEX_TABLE_CREATE)
            cursor.execute(SqliteMemoryValidatorStorage.MINER_INDEX_TABLE_INDEX)

            # Lock to avoid concurrency issues on clearing and inserting an index.
            self.upsert_miner_index_lock = threading.Lock()

    def _create_connection(self):
        # Create the database if it doesn't exist, defaulting to the local directory.
        # Use PARSE_DECLTYPES to convert accessed values into the appropriate type.
        connection = sqlite3.connect(
            "file::memory:?cache=shared",
            uri=True,
            detect_types=sqlite3.PARSE_DECLTYPES,
            timeout=60.0,
        )
        # Allow this connection to parse results from returned rows by column name.
        connection.row_factory = sqlite3.Row

        return connection

    def _upsert_miner(self, hotkey: str, now_str: str, credibility: float) -> int:
        miner_id = 0

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            cursor.execute(
                "INSERT OR IGNORE INTO Miner (hotkey, lastUpdated, credibility) VALUES (?, ?, ?)",
                [hotkey, now_str, credibility],
            )
            connection.commit()

            # Then we get the existing or newly created minerId
            cursor.execute("SELECT minerId FROM Miner WHERE hotkey = ?", [hotkey])
            miner_id = cursor.fetchone()[0]

        return miner_id

    def _label_value_parse(self, label: Optional[DataLabel]) -> str:
        """Parses the value to store in the database out of an Optional DataLabel."""
        return "NULL" if (label is None) else label.value

    def _label_value_parse_str(self, label: Optional[str]) -> str:
        """Same as _label_value_parse but with a string as input"""
        return "NULL" if (label is None) else label.casefold()

    def upsert_miner_index(self, index: MinerIndex, credibility: float = 0):
        """Stores the index for all of the data that a specific miner promises to provide."""
        raise NotImplemented
        # now_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

        # # Upsert this Validator's minerId for the specified hotkey.
        # miner_id = self._upsert_miner(index.hotkey, now_str, credibility)

        # # Ensure that all the label ids in the upcoming entity buckets are known for this Validator.
        # label_values = set()

        # for data_entity_bucket in index.data_entity_buckets:
        #     label_values.add(self._label_value_parse(data_entity_bucket.id.label))

        # self._insert_labels(label_values)

        # # Get all label ids for use in mapping.
        # label_value_to_id_dict = self._get_label_value_to_id_dict(label_values)

        # # Ensure that all the bucket ids in the upcoming entity buckets are known for this Validator.
        # bucket_values = set()

        # for data_entity_bucket in index.data_entity_buckets:
        #     bucket_values.add(
        #         (
        #             int(data_entity_bucket.id.source),
        #             label_value_to_id_dict[
        #                 self._label_value_parse(data_entity_bucket.id.label)
        #             ],
        #             data_entity_bucket.id.time_bucket.id,
        #         )
        #     )

        # self._insert_buckets(bucket_values)

        # # Get all bucket ids for use in mapping.
        # bucket_value_to_id_dict = self._get_bucket_value_to_id_dict(bucket_values)

        # # Parse every DataEntityBucket from the index into a list of values to insert.
        # values = []
        # for data_entity_bucket in index.data_entity_buckets:
        #     try:
        #         bucket_id = bucket_value_to_id_dict[
        #             (
        #                 int(data_entity_bucket.id.source),
        #                 label_value_to_id_dict[
        #                     self._label_value_parse(data_entity_bucket.id.label)
        #                 ],
        #                 data_entity_bucket.id.time_bucket.id,
        #             )
        #         ]

        #         values.append(
        #             [
        #                 miner_id,
        #                 bucket_id,
        #                 data_entity_bucket.size_bytes,
        #             ]
        #         )
        #     except:
        #         # In the case that we fail to get a label (due to unsupported characters) we drop just that one bucket.
        #         pass

        # with self.upsert_miner_index_lock:
        #     # Clear the previous keys for this miner.
        #     self.delete_miner_index(index.hotkey)

        #     with contextlib.closing(self._create_connection()) as connection:
        #         cursor = connection.cursor()
        #         # Insert the new keys. (Ignore into to defend against a miner giving us multiple duplicate rows.)
        #         # Batch in groups of 1m if necessary to avoid congestion issues.
        #         value_subsets = [
        #             values[x : x + 1000000] for x in range(0, len(values), 1000000)
        #         ]
        #         for value_subset in value_subsets:
        #             cursor.executemany(
        #                 """INSERT OR IGNORE INTO MinerIndex (minerId, bucketId, contentSizeBytes) VALUES (?, ?, ?)""",
        #                 value_subset,
        #             )
        #         self.connection.commit()

    def upsert_compressed_miner_index(
        self, index: CompressedMinerIndex, hotkey: str, credibility: float = 0
    ):
        """Stores the index for all of the data that a specific miner promises to provide."""
        now_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

        # Upsert this Validator's minerId for the specified hotkey.
        miner_id = self._upsert_miner(hotkey, now_str, credibility)

        print("Creating label Values")

        # Ensure that all the label ids in the upcoming entity buckets are known for this Validator.
        label_values = {
            self._label_value_parse_str(compressed_bucket.label)
            for _, compressed_buckets in index.sources.items()
            for compressed_bucket in compressed_buckets
        }

        print("Constructing values for miner index")

        # Parse every DataEntityBucket from the index into a list of values to insert.
        values = []
        for source, compressed_buckets in index.sources.items():
            for compressed_bucket in compressed_buckets:
                for time_bucket_id, size_bytes in zip(
                    compressed_bucket.time_bucket_ids, compressed_bucket.sizes_bytes
                ):
                    try:
                        values.append(
                            [
                                miner_id,
                                int(source),
                                self.label_dict.get_or_insert(
                                    self._label_value_parse_str(compressed_bucket.label)
                                ),
                                time_bucket_id,
                                size_bytes,
                            ]
                        )
                    except:
                        # In the case that we fail to get a label (due to unsupported characters) we drop just that one bucket.
                        pass

        with self.upsert_miner_index_lock:
            # Clear the previous keys for this miner.
            print("Deleting for miner index")
            self.delete_miner_index(hotkey)

            print(f"Inserting miner index of size {len(values)}")
            with contextlib.closing(self._create_connection()) as connection:
                cursor = connection.cursor()
                # Insert the new keys. (Ignore into to defend against a miner giving us multiple duplicate rows.)
                # Batch in groups of 1m if necessary to avoid congestion issues.
                value_subsets = [
                    values[x : x + 1000000] for x in range(0, len(values), 1000000)
                ]
                for value_subset in value_subsets:
                    cursor.executemany(
                        """INSERT OR IGNORE INTO MinerIndex (minerId, source, labelId, timeBucketId, contentSizeBytes) VALUES (?, ?, ?, ?, ?)""",
                        value_subset,
                    )
                self.connection.commit()

    def read_miner_index(
        self, miner_hotkey: str, valid_miners: Set[str]
    ) -> Optional[ScorableMinerIndex]:
        """Gets a scored index for all of the data that a specific miner promises to provide."""
        raise NotImplemented

    def delete_miner_index(self, miner_hotkey: str):
        """Removes the index for the specified miner."""

        bt.logging.trace(f"{miner_hotkey}: Deleting miner index")

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            cursor.execute("SELECT minerId FROM Miner WHERE hotkey = ?", [miner_hotkey])

            # Delete the rows for the specified miner.
            miner_id = cursor.fetchone()[0]
            if miner_id is not None:
                cursor.execute("DELETE FROM MinerIndex WHERE minerId = ?", [miner_id])
                self.connection.commit()

    def read_miner_last_updated(self, miner_hotkey: str) -> Optional[dt.datetime]:
        """Gets when a specific miner was last updated."""
        raise NotImplemented
