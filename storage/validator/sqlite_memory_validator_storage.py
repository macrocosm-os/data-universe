import contextlib
import datetime as dt
import bittensor as bt
import sqlite3
import threading
from typing import Any, Dict, Optional, Set, Tuple, List
from common.data import CompressedMinerIndex, DataLabel
from common.data_v2 import ScorableDataEntityBucket, ScorableMinerIndex
from storage.validator.validator_storage import ValidatorStorage


class AutoIncrementDict:
    """A dictionary that automatically assigns ids to keys.

    Provides O(1) ability to insert a key and get its id, and to lookup the key for an id.

    Thread safe.
    """

    def __init__(self):
        self.available_ids = set()
        self.items = []
        self.indexes = {}
        self.lock = threading.Lock()

    def get_or_insert(self, key: Any) -> int:
        with self.lock:
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
        with self.lock:
            return self.items[id]

    def delete_key(self, key: Any):
        with self.lock:
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
    MINER_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS Miner (
                            minerId     INTEGER         PRIMARY KEY,
                            hotkey      VARCHAR(64)     NOT NULL,
                            lastUpdated TIMESTAMP(6)    NOT NULL,
                            credibility FLOAT           NOT NULL    DEFAULT 0.00,
                            UNIQUE(hotkey)
                            )"""

    MINER_TABLE_CREDIBILTY_INDEX = """CREATE INDEX IF NOT EXISTS miner_credibility_index
                                      ON Miner (minerId, credibility)"""

    # Updated Primary table in which the DataEntityBuckets for all miners are stored.
    MINER_INDEX_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS MinerIndex (
                                    minerId             INTEGER         NOT NULL,
                                    source              TINYINT         NOT NULL,
                                    labelId             INTEGER         NOT NULL,
                                    timeBucketId        INTEGER         NOT NULL,
                                    contentSizeBytes    INTEGER         NOT NULL,
                                    PRIMARY KEY(minerId, source, labelId, timeBucketId)
                                    ) WITHOUT ROWID"""

    MINER_INDEX_TABLE_BUCKET_SIZE_INDEX = """CREATE INDEX IF NOT EXISTS bucket_size_index
                                             ON MinerIndex (source, labelId, timeBucketId, contentSizeBytes)"""

    def __init__(self):
        sqlite3.register_converter("timestamp", tz_aware_timestamp_adapter)

        self.continuous_connection_do_not_reuse = self._create_connection()
        self.label_dict = AutoIncrementDict()

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            # Create the Miner table (if it does not already exist).
            cursor.execute(SqliteMemoryValidatorStorage.MINER_TABLE_CREATE)
            cursor.execute(SqliteMemoryValidatorStorage.MINER_TABLE_CREDIBILTY_INDEX)

            # Create the Index table (if it does not already exist).
            cursor.execute(SqliteMemoryValidatorStorage.MINER_INDEX_TABLE_CREATE)
            cursor.execute(
                SqliteMemoryValidatorStorage.MINER_INDEX_TABLE_BUCKET_SIZE_INDEX
            )

            # Lock to avoid concurrency issues on interacting with the database.
            self.lock = threading.RLock()

    def _create_connection(self):
        # Create the database if it doesn't exist, defaulting to the local directory.
        # Use PARSE_DECLTYPES to convert accessed values into the appropriate type.
        connection = sqlite3.connect(
            "file::memory:?cache=shared",
            uri=True,
            detect_types=sqlite3.PARSE_DECLTYPES,
            timeout=120.0,
        )
        # Avoid using a row_factory that would allow parsing results by column name for performance.
        # connection.row_factory = sqlite3.Row
        connection.isolation_level = None
        return connection

    def _upsert_miner(self, hotkey: str, now_str: str, credibility: float) -> int:
        miner_id = 0

        with self.lock:
            with contextlib.closing(self._create_connection()) as connection:
                cursor = connection.cursor()

                cursor.execute(
                    "UPDATE OR IGNORE Miner SET lastUpdated=?, credibility=? WHERE hotkey=?",
                    [now_str, credibility, hotkey],
                )
                cursor.execute(
                    """INSERT OR IGNORE INTO Miner (hotkey, lastUpdated, credibility) VALUES (?, ?, ?)""",
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

    def upsert_compressed_miner_index(
        self, index: CompressedMinerIndex, hotkey: str, credibility: float
    ):
        """
        Stores the index for all of the data that a specific miner promises to provide,
        with strict validation to avoid silent undercounting.

        Changes vs. old implementation:
        - NO zip() truncation: assert equal lengths for time_bucket_ids and sizes_bytes.
        - NO silent drops: any per-row failure logs + raises
        - ATOMIC delete+insert within a single transaction - either all changes become visible, or none do.
        """
        # Informational trace (safe if bucket_count raises)
        try:
            claimed = CompressedMinerIndex.bucket_count(index)
            bt.logging.trace(f"{hotkey}: Upserting miner index with {claimed} buckets")
        except Exception:
            bt.logging.trace(f"{hotkey}: Upserting miner index")

        now_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

        # Ensure Miner row exists / updated; get minerId
        miner_id = self._upsert_miner(hotkey, now_str, credibility)

        # Build rows with strict validation
        values = []
        expected_rows = 0

        for source, compressed_buckets in index.sources.items():
            src_int = int(source)
            for cb in compressed_buckets:
                tids  = list(cb.time_bucket_ids or [])
                sizes = list(cb.sizes_bytes or [])

                n_ids   = len(tids)
                n_sizes = len(sizes)

                # Refuse to truncate: lengths must match exactly
                if n_ids != n_sizes:
                    bt.logging.error(
                        f"{hotkey}: length mismatch for label={cb.label}, source={src_int} - "
                        f"len(time_bucket_ids)={n_ids} != len(sizes_bytes)={n_sizes}"
                    )
                    raise ValueError("Compressed index arrays have different lengths")

                expected_rows += n_ids

                # Resolve label id once per compressed bucket
                try:
                    label_val = self._label_value_parse_str(cb.label)
                    label_id = self.label_dict.get_or_insert(label_val)
                except Exception as e:
                    bt.logging.error(
                        f"{hotkey}: failed to resolve label id for label={cb.label}, source={src_int}: {e}"
                    )
                    raise

                # Materialize rows
                for i in range(n_ids):
                    try:
                        time_bucket_id = int(tids[i])
                        size_bytes     = int(sizes[i])
                    except Exception as e:
                        bt.logging.error(
                            f"{hotkey}: type conversion error for source={src_int}, "
                            f"label={cb.label}, i={i}: {e}"
                        )
                        raise

                    if size_bytes < 0:
                        bt.logging.error(
                            f"{hotkey}: negative size_bytes detected (label={cb.label}, "
                            f"source={src_int}, timeBucketId={time_bucket_id}, size={size_bytes})"
                        )
                        raise ValueError("contentSizeBytes must be >= 0")

                    values.append([
                        miner_id,
                        src_int,
                        label_id,
                        time_bucket_id,
                        size_bytes,
                    ])

        # Helpful pre-insert log
        bt.logging.info(f"{hotkey}: prepared {len(values)} rows for insert; expected={expected_rows}")

        # Atomic delete + insert
        with self.lock:
            with contextlib.closing(self._create_connection()) as connection:
                cursor = connection.cursor()
                try:
                    cursor.execute("BEGIN IMMEDIATE")

                    # Remove previous rows for this miner within the same transaction
                    cursor.execute("DELETE FROM MinerIndex WHERE minerId = ?", [miner_id])

                    # Batch insert to avoid giant statements
                    if values:
                        for start in range(0, len(values), 1_000_000):
                            chunk = values[start:start + 1_000_000]
                            cursor.executemany(
                                """
                                INSERT OR IGNORE INTO MinerIndex
                                (minerId, source, labelId, timeBucketId, contentSizeBytes)
                                VALUES (?, ?, ?, ?, ?)
                                """,
                                chunk,
                            )

                    connection.commit()
                except Exception as e:
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    bt.logging.error(
                        f"{hotkey}: upsert_compressed_miner_index failed; rolled back. Reason: {e}"
                    )
                    raise

        bt.logging.success(f"{hotkey}: upserted miner index rows={len(values)} (expected={expected_rows})")

    def read_miner_index(
        self,
        miner_hotkey: str,
    ) -> Optional[ScorableMinerIndex]:
        """Gets a scored index for all of the data that a specific miner promises to provide."""
        with self.lock:
            with contextlib.closing(self._create_connection()) as connection:
                cursor = connection.cursor()

                # locate miner
                cursor.execute(
                    "SELECT minerId, lastUpdated FROM Miner WHERE hotkey = ?",
                    [miner_hotkey],
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                miner_id, last_updated = row

                # Get all the DataEntityBuckets for this miner joined to the total content size of like buckets, credibility-free.
                sql = """
                WITH
                  TempBuckets AS (
                      SELECT source, labelId, timeBucketId
                      FROM   MinerIndex
                      WHERE  minerId = :mine
                  ),
                  TotalContent AS (
                      SELECT source,
                             labelId,
                             timeBucketId,
                             SUM(contentSizeBytes) AS bucketTotalBytes
                      FROM   MinerIndex
                      JOIN   TempBuckets USING (source, labelId, timeBucketId)
                      GROUP  BY source, labelId, timeBucketId
                  )
                SELECT  mi.source,
                        mi.labelId,
                        mi.timeBucketId,
                        mi.contentSizeBytes,
                        (mi.contentSizeBytes * mi.contentSizeBytes * 1.0
                         / NULLIF(TotalContent.bucketTotalBytes, 0)) AS scorableBytes
                FROM    MinerIndex AS mi
                JOIN    TotalContent USING (source, labelId, timeBucketId)
                WHERE   mi.minerId = :mine;
                """
                cursor.execute(sql, {"mine": miner_id})

                # bucket-building loop
                scored_data_entity_buckets = []

                # For each row (representing a DataEntityBucket and Uniqueness)
                # turn it into a ScorableDataEntityBucket.
                for row in cursor:
                    label_value = self.label_dict.get_by_id(row[1])

                    # Add the bucket to the list of scored buckets on the overall index.
                    scored_data_entity_buckets.append(
                        ScorableDataEntityBucket(
                            time_bucket_id=int(row[2]),
                            source=int(row[0]),
                            label=label_value if label_value != "NULL" else None,
                            size_bytes=int(row[3] if row[3] else 0),
                            scorable_bytes=int(row[4] if row[4] else 0),
                        )
                    )

                scored_index = ScorableMinerIndex(
                    scorable_data_entity_buckets=scored_data_entity_buckets,
                    last_updated=last_updated,
                )

                return scored_index

    def _delete_miner_index(self, miner_hotkey: str):
        """Removes the index for the specified miner."""

        bt.logging.trace(f"{miner_hotkey}: Deleting miner index")

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            cursor.execute("SELECT minerId FROM Miner WHERE hotkey = ?", [miner_hotkey])

            # Delete the rows for the specified miner.
            result = cursor.fetchone()
            if result is not None:
                cursor.execute("DELETE FROM MinerIndex WHERE minerId = ?", [result[0]])
                connection.commit()

    def delete_miner(self, hotkey: str):
        """Removes the index and miner details for the specified miner."""
        with self.lock:
            self._delete_miner_index(hotkey)
            with contextlib.closing(self._create_connection()) as connection:
                cursor = connection.cursor()
                cursor.execute("DELETE FROM Miner WHERE hotkey = ?", [hotkey])

    def read_miner_last_updated(self, miner_hotkey: str) -> Optional[dt.datetime]:
        """Gets when a specific miner was last updated."""
        with self.lock:
            with contextlib.closing(self._create_connection()) as connection:
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT lastUpdated FROM Miner WHERE hotkey = ?", [miner_hotkey]
                )
                result = cursor.fetchone()
                if result is not None:
                    return result[0]
                else:
                    return None
