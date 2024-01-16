from collections import defaultdict
import threading
from common import constants, utils
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
)
from storage.miner.miner_storage import MinerStorage
from typing import Dict, List
import datetime as dt
import sqlite3
import contextlib
import bittensor as bt


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


class SqliteMinerStorage(MinerStorage):
    """Sqlite backed MinerStorage"""

    # TODO Consider CHECK expression to limit source to expected ENUM values.
    # Sqlite type converters handle the mapping from Python datetime to Timestamp.
    DATA_ENTITY_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS DataEntity (
                                uri                 TEXT            PRIMARY KEY,
                                datetime            TIMESTAMP(6)    NOT NULL,
                                timeBucketId        INTEGER         NOT NULL,
                                source              INTEGER         NOT NULL,
                                label               CHAR(32)                ,
                                content             BLOB            NOT NULL,
                                contentSizeBytes    INTEGER         NOT NULL
                                ) WITHOUT ROWID"""

    DELETE_OLD_INDEX = """DROP INDEX IF EXISTS data_entity_bucket_index"""

    DATA_ENTITY_TABLE_INDEX = """CREATE INDEX IF NOT EXISTS data_entity_bucket_index2
                                ON DataEntity (timeBucketId, source, label, contentSizeBytes)"""

    def __init__(
        self,
        database="SqliteMinerStorage.sqlite",
        max_database_size_gb_hint=250,
    ):
        sqlite3.register_converter("timestamp", tz_aware_timestamp_adapter)
        self.database = database

        # TODO Account for non-content columns when restricting total database size.
        self.database_max_content_size_bytes = utils.gb_to_bytes(
            max_database_size_gb_hint
        )

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            # Create the DataEntity table (if it does not already exist).
            cursor.execute(SqliteMinerStorage.DATA_ENTITY_TABLE_CREATE)

            # Delete the old index (if it exists).
            cursor.execute(SqliteMinerStorage.DELETE_OLD_INDEX)

            # Create the Index (if it does not already exist).
            cursor.execute(SqliteMinerStorage.DATA_ENTITY_TABLE_INDEX)

        # Lock to avoid concurrency issues on clearing space when full
        self.clearing_space_lock = threading.Lock()

    def _create_connection(self):
        # Create the database if it doesn't exist, defaulting to the local directory.
        # Use PARSE_DECLTYPES to convert accessed values into the appropriate type.
        connection = sqlite3.connect(
            self.database, detect_types=sqlite3.PARSE_DECLTYPES, timeout=60.0
        )
        # Allow this connection to parse results from returned rows by column name.
        connection.row_factory = sqlite3.Row

        return connection

    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""

        added_content_size = 0
        for data_entity in data_entities:
            added_content_size += data_entity.content_size_bytes

        # If the total size of the store is larger than our maximum configured stored content size then ecept.
        if added_content_size > self.database_max_content_size_bytes:
            raise ValueError(
                "Content size to store: "
                + str(added_content_size)
                + " exceeds configured max: "
                + str(self.database_max_content_size_bytes)
            )

        with contextlib.closing(self._create_connection()) as connection:
            # Ensure only one thread is clearing space when necessary.
            with self.clearing_space_lock:
                # If we would exceed our maximum configured stored content size then clear space.
                cursor = connection.cursor()
                cursor.execute("SELECT SUM(contentSizeBytes) FROM DataEntity")

                # If there are no rows we convert the None result to 0
                result = cursor.fetchone()
                current_content_size = result[0] if result[0] else 0

                if (
                    current_content_size + added_content_size
                    > self.database_max_content_size_bytes
                ):
                    content_bytes_to_clear = (
                        self.database_max_content_size_bytes // 10
                        if self.database_max_content_size_bytes // 10
                        > added_content_size
                        else added_content_size
                    )
                    self.clear_content_from_oldest(content_bytes_to_clear)

            # Parse every DataEntity into an list of value lists for inserting.
            values = []

            for data_entity in data_entities:
                label = (
                    "NULL" if (data_entity.label is None) else data_entity.label.value
                )
                time_bucket_id = TimeBucket.from_datetime(data_entity.datetime).id
                values.append(
                    [
                        data_entity.uri,
                        data_entity.datetime,
                        time_bucket_id,
                        data_entity.source,
                        label,
                        data_entity.content,
                        data_entity.content_size_bytes,
                    ]
                )

            # Insert overwriting duplicate keys (in case of updated content).
            cursor.executemany("REPLACE INTO DataEntity VALUES (?,?,?,?,?,?,?)", values)

            # Commit the insert.
            connection.commit()

    def list_data_entities_in_data_entity_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataEntityBucketId."""
        # Get rows that match the DataEntityBucketId.
        label = (
            "NULL"
            if (data_entity_bucket_id.label is None)
            else data_entity_bucket_id.label.value
        )

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """SELECT * FROM DataEntity 
                        WHERE timeBucketId = ? AND source = ? AND label = ?""",
                [
                    data_entity_bucket_id.time_bucket.id,
                    data_entity_bucket_id.source,
                    label,
                ],
            )

            # Convert the rows into DataEntity objects and return them up to the configured max chuck size.
            data_entities = []

            running_size = 0

            for row in cursor:
                if (
                    running_size + row["contentSizeBytes"]
                    >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                ):
                    # If we would go over the max DataEntityBucket size instead return early.
                    return data_entities
                else:
                    # Construct the new DataEntity.

                    label = (
                        DataLabel(value=row["label"])
                        if row["label"] != "NULL"
                        else None
                    )

                    data_entity = DataEntity(
                        uri=row["uri"],
                        datetime=row["datetime"],
                        label=label,
                        source=DataSource(row["source"]),
                        content=row["content"],
                        content_size_bytes=row["contentSizeBytes"],
                    )

                    data_entities.append(data_entity)
                    running_size += row["contentSizeBytes"]

            # If we reach the end of the cursor then return all of the data entities for this DataEntityBucket.
            bt.logging.trace(
                f"Returning {len(data_entities)} data entities for bucket {data_entity_bucket_id}"
            )
            return data_entities

    def get_compressed_index(
        self,
        bucket_count_limit=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX,
    ) -> CompressedMinerIndex:
        """Gets the compressed MinedIndex, which is a summary of all of the DataEntities that this MinerStorage is currently serving."""

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            oldest_time_bucket_id = TimeBucket.from_datetime(
                dt.datetime.now()
                - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
            ).id

            # Get sum of content_size_bytes for all rows grouped by DataEntityBucket.
            cursor.execute(
                """SELECT SUM(contentSizeBytes) AS bucketSize, timeBucketId, source, label FROM DataEntity
                        WHERE timeBucketId >= ?
                        GROUP BY timeBucketId, source, label
                        ORDER BY bucketSize DESC
                        LIMIT ?
                        """,
                [
                    oldest_time_bucket_id,
                    bucket_count_limit,
                ],
            )

            buckets_by_source_by_label = defaultdict(dict)

            for row in cursor:
                # Ensure the miner does not attempt to report more than the max DataEntityBucket size.
                size = (
                    constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                    if row["bucketSize"]
                    >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                    else row["bucketSize"]
                )

                label = row["label"] if row["label"] != "NULL" else None

                bucket = buckets_by_source_by_label[DataSource(row["source"])].get(
                    label, CompressedEntityBucket(label=label)
                )
                bucket.sizes_bytes.append(size)
                bucket.time_bucket_ids.append(row["timeBucketId"])
                buckets_by_source_by_label[DataSource(row["source"])][label] = bucket

            # Convert the buckets_by_source_by_label into a list of lists of CompressedEntityBucket and return
            return CompressedMinerIndex(
                sources={
                    source: list(labels_to_buckets.values())
                    for source, labels_to_buckets in buckets_by_source_by_label.items()
                }
            )

    def clear_content_from_oldest(self, content_bytes_to_clear: int):
        """Deletes entries starting from the oldest until we have cleared the specified amount of content."""

        bt.logging.debug(f"Database full. Clearing {content_bytes_to_clear} bytes.")

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            # TODO Investigate way to select last X bytes worth of entries in a single query.
            # Get the contentSizeBytes of each row by timestamp desc.
            cursor.execute(
                "SELECT contentSizeBytes, datetime FROM DataEntity ORDER BY datetime ASC"
            )

            running_bytes = 0
            earliest_datetime_to_clear = dt.datetime.min
            # Iterate over rows until we have found bytes to clear or we reach the end and fail.
            for row in cursor:
                running_bytes += row["contentSizeBytes"]
                earliest_datetime_to_clear = row["datetime"]
                # Once we have enough content to clear then we do so.
                if running_bytes >= content_bytes_to_clear:
                    cursor.execute(
                        "DELETE FROM DataEntity WHERE datetime <= ?",
                        [earliest_datetime_to_clear],
                    )
                    connection.commit()

    def list_data_entity_buckets(self) -> List[DataEntityBucket]:
        """Lists all DataEntityBuckets for all the DataEntities that this MinerStorage is currently serving."""

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            oldest_time_bucket_id = TimeBucket.from_datetime(
                dt.datetime.now()
                - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
            ).id
            # Get sum of content_size_bytes for all rows grouped by DataEntityBucket.
            cursor.execute(
                """SELECT SUM(contentSizeBytes) AS bucketSize, timeBucketId, source, label FROM DataEntity
                        WHERE timeBucketId >= ?
                        GROUP BY timeBucketId, source, label
                        ORDER BY bucketSize DESC
                        LIMIT ?
                        """,
                [
                    oldest_time_bucket_id,
                    constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX,
                ],
            )

            data_entity_buckets = []

            for row in cursor:
                # Ensure the miner does not attempt to report more than the max DataEntityBucket size.
                size = (
                    constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                    if row["bucketSize"]
                    >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                    else row["bucketSize"]
                )

                # Construct the new DataEntityBucket with all non null columns.
                data_entity_bucket_id = DataEntityBucketId(
                    time_bucket=TimeBucket(id=row["timeBucketId"]),
                    source=DataSource(row["source"]),
                )

                # Add the optional Label field if not null.
                if row["label"] != "NULL":
                    data_entity_bucket_id.label = DataLabel(value=row["label"])

                data_entity_bucket = DataEntityBucket(
                    id=data_entity_bucket_id, size_bytes=size
                )

                data_entity_buckets.append(data_entity_bucket)

            # If we reach the end of the cursor then return all of the data entity buckets.
            return data_entity_buckets
