import threading
from common import utils
from common.data import DataEntity, DataChunkSummary, DataLabel, DataSource, TimeBucket
from storage.miner.miner_storage import MinerStorage
from typing import List
import datetime as dt
import sqlite3

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

    DATA_ENTITY_TABLE_INDEX = """CREATE INDEX IF NOT EXISTS chunk_index
                                ON DataEntity (timeBucketId, source, label)"""

    def __init__(self, database="SqliteMinerStorage.sqlite", database_max_content_size_bytes=utils.mb_to_bytes(10000)):
        self.database = database
        # TODO Account for non-content columns when restricting total database size.
        self.database_max_content_size_bytes = database_max_content_size_bytes

        # Create the database if it doesn't exist, defaulting to the local directory.
        # Use PARSE_DECLTYPES to convert accessed values into the appropriate type.
        self.connection = sqlite3.connect(self.database, detect_types=sqlite3.PARSE_DECLTYPES)
        # Allow this connection to parse results from returned rows by column name.
        self.connection.row_factory = sqlite3.Row

        cursor = self.connection.cursor()

        # Create the DataEntity table (if it does not already exist).
        cursor.execute(SqliteMinerStorage.DATA_ENTITY_TABLE_CREATE)

        # Create the Index (if it does not already exist).
        cursor.execute(SqliteMinerStorage.DATA_ENTITY_TABLE_INDEX)

        # Lock to avoid concurrency issues on clearing space when full
        self.clearing_space_lock = threading.Lock()


    def __del__(self):
        self.connection.close()

    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""

        added_content_size = 0
        for data_entity in data_entities:
            added_content_size += data_entity.content_size_bytes

        # If the total size of the store is larger than our maximum configured stored content size then ecept.
        if added_content_size > self.database_max_content_size_bytes:
            raise ValueError("Content size to store: " + str(added_content_size) + " exceeds configured max: "
                            + str(self.database_max_content_size_bytes))

        # Ensure only one thread is clearing space when necessary.
        with self.clearing_space_lock:
            # If we would exceed our maximum configured stored content size then clear space.
            cursor = self.connection.cursor()
            cursor.execute("SELECT SUM(contentSizeBytes) FROM DataEntity")

            # If there are no rows we convert the None result to 0
            result = cursor.fetchone()
            current_content_size = result[0] if result[0] else 0

            if current_content_size + added_content_size > self.database_max_content_size_bytes:
                content_bytes_to_clear = (self.database_max_content_size_bytes // 10
                                        if self.database_max_content_size_bytes // 10 > added_content_size
                                        else added_content_size)
                self.clear_content_from_oldest(content_bytes_to_clear)

        # Parse every DataEntity into an list of value lists for inserting.
        values = []

        for data_entity in data_entities:
            label = "NULL" if (data_entity.label is None) else data_entity.label.value
            timeBucketId = TimeBucket.from_datetime(data_entity.datetime).id
            values.append([data_entity.uri, data_entity.datetime, timeBucketId, data_entity.source.value, label,
                           data_entity.content, data_entity.content_size_bytes])


        # Insert ignoring duplicate keys.
        cursor.executemany("INSERT OR IGNORE INTO DataEntity VALUES (?,?,?,?,?,?,?)", values)
        
        # Commit the insert.
        self.connection.commit()
    
    def list_data_entities_in_data_chunk(self, data_chunk_summary: DataChunkSummary) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataChunkSummary."""
        # Get rows that match the DataChunkSummary.
        label = "NULL" if (data_chunk_summary.label is None) else data_chunk_summary.label.value

        cursor = self.connection.cursor()
        cursor.execute("""SELECT * FROM DataEntity 
                       WHERE timeBucketId = ? AND source = ? AND label = ?""",
                       [data_chunk_summary.time_bucket.id, data_chunk_summary.source.value, label])

        # Convert the rows into DataEntity objects and return them up to the configured max chuck size.
        data_entities = []

        # TODO use a configured max chunk size size.
        max_chunk_size = utils.mb_to_bytes(128)
        running_size = 0

        for row in cursor:
            if running_size + row['contentSizeBytes'] >= max_chunk_size:
                # If we would go over the max chunk size instead return early.
                return data_entities
            else:
                # Construct the new DataEntity with all non null columns.
                data_entity = DataEntity(uri=row['uri'],
                                        datetime=row['datetime'],
                                        source=DataSource(row['source']),
                                        content=row['content'],
                                        content_size_bytes=row['contentSizeBytes'])

                # Add the optional Label field if not null.
                if row['label'] != "NULL":
                    data_chunk_summary.label = DataLabel(value=row['label'])

                data_entities.append(data_entity)
                running_size += row['contentSizeBytes']

        # If we reach the end of the cursor then return all of the data entities for this chunk.
        return data_entities


    def list_data_chunk_summaries(self) -> List[DataChunkSummary]:
        """Lists all DataChunkSummaries for all the DataEntities that this MinerStorage is currently serving."""

        cursor = self.connection.cursor()

        # TODO use a configured max age.
        oldest_time_bucket_id = TimeBucket.from_datetime(dt.datetime.now() - dt.timedelta(days=7)).id
        # TODO use a configured max data chunk summary count.
        max_data_chunk_summary_count = 3000000

        # Get sum of content_size_bytes for all rows grouped by chunk.
        cursor.execute("""SELECT SUM(contentSizeBytes) AS chunkSize, timeBucketId, source, label FROM DataEntity
                       WHERE timeBucketId >= ?
                       GROUP BY timeBucketId, source, label
                       LIMIT ?
                       """, [oldest_time_bucket_id, max_data_chunk_summary_count])

        data_chunk_summaries = []

        # TODO use a configured max chunk size size.
        max_chunk_size = utils.mb_to_bytes(128)

        for row in cursor:
            # Ensure the miner does not attempt to report more than the max chunk size.
            size = max_chunk_size if row['chunkSize'] >= max_chunk_size else row['chunkSize']

            # Construct the new DataChunkSummary with all non null columns.
            data_chunk_summary = DataChunkSummary(time_bucket=TimeBucket(id=row['timeBucketId']),
                                                    source=DataSource(row['source']),
                                                    size_bytes=size)

            # Add the optional Label field if not null.
            if row['label'] != "NULL":
                data_chunk_summary.label = DataLabel(value=row['label'])

            data_chunk_summaries.append(data_chunk_summary)

        # If we reach the end of the cursor then return all of the data chunk summaries.
        return data_chunk_summaries

    def clear_content_from_oldest(self, contentBytesToClear: int):
        """Deletes entries starting from the oldest until we have cleared the specified amount of content."""
        cursor = self.connection.cursor()

        # TODO Investigate way to select last X bytes worth of entries in a single query.
        # Get the contentSizeBytes of each row by timestamp desc.
        cursor.execute("SELECT contentSizeBytes, datetime FROM DataEntity ORDER BY datetime ASC")

        running_bytes = 0
        earliest_datetime_to_clear = dt.datetime.min
        # Iterate over rows until we have found bytes to clear or we reach the end and fail.
        for row in cursor:
            running_bytes += row['contentSizeBytes']
            earliest_datetime_to_clear = row['datetime']
            # Once we have enough content to clear then we do so.
            if running_bytes >= contentBytesToClear:
                cursor.execute("DELETE FROM DataEntity WHERE datetime <= ?", [earliest_datetime_to_clear])
