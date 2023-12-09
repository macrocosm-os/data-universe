import threading
from common import utils
from common.data import DataEntity, DataChunkSummary, DataLabel, DataSource, MinerIndex, ScorableDataChunkSummary, ScorableMinerIndex, TimeBucket
from storage.validator.validator_storage import ValidatorStorage
from typing import List, Set
import datetime as dt
import mysql.connector

class MysqlValidatorStorage(ValidatorStorage):
    """MySQL backed Validator Storage"""

    MINER_INDEX_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS MinerIndex (
                                hotkey              VARCHAR(64)     NOT NULL,
                                timeBucketId        INT             NOT NULL,
                                source              TINYINT         NOT NULL,
                                label               VARCHAR(32)             ,
                                contentSizeBytes    INT             NOT NULL,
                                lastUpdated         DATETIME        NOT NULL,
                                PRIMARY KEY(hotkey, timeBucketId, source, label)
                                )"""

    def __init__(self, host: str, user: str, password: str, database: str):
        # Get the connection to the user-created MySQL database.
        self.connection = mysql.connector.connect=(host, user, password, database)
    
        cursor = self.connection.cursor()

        # Create the MinerIndex table if it doesn't exist
        cursor.execute(MysqlValidatorStorage.MINER_INDEX_TABLE_CREATE)

        # Lock to avoid concurrency issues on clearing and inserting an index.
        self.upsert_miner_index_lock = threading.Lock()

    def __del__(self):
        self.connection.close()

    def upsert_miner_index(self, index: MinerIndex):
        """Stores the index for all of the data that a specific miner promises to provide."""
        # Parse every DataChunkSummary from the index into a list of values to insert.
        values = []
        now = dt.datetime.utcnow()
        for data_chunk_summary in index.chunks:
            label = "NULL" if (data_chunk_summary.label is None) else data_chunk_summary.label.value
            values.append([index.hotkey,
                           data_chunk_summary.time_bucket.id,
                           data_chunk_summary.source.value,
                           label,
                           data_chunk_summary.size_bytes,
                           now.strftime("%Y-%m-%d %H:%M:%S")])

        cursor = self.connection.cursor()

        with self.upsert_miner_index_lock:
            # Clear the previous keys for this miner.
            self.delete_miner_index(index.hotkey)

            # Insert the new keys.
            cursor.executemany("""INSERT INTO MinerIndex VALUES (?, ?, ?, ?, ?, ?)""", values)
            self.connection.commit()

    def read_miner_index(self, miner_hotkey: str, valid_miners: Set[str]) -> ScorableMinerIndex:
        """Gets a scored index for all of the data that a specific miner promises to provide."""
        # Get a cursor to the database with dictionary enabled for accessing columns by name.
        outer_cursor = self.connection.cursor(dictionary=True)

        last_updated = None

        # Get all the DataChunkSummaries for this miner.
        outer_cursor.execute("SELECT * FROM MinerIndex WHERE hotkey = ?", [miner_hotkey])

        scored_index = ScorableMinerIndex(hotkey=miner_hotkey, last_updated=last_updated)

        # Include the specified miner in the set of miners we check even if it is invalid.
        valid_miners.add(miner_hotkey)
        
        # For each row (representing a DataChunkSummary) turn it into a ScorableDataChunkSummary.
        for row in outer_cursor:
            # Set last_updated to the first value since they are all the same for a given miner.
            if last_updated == None:
                last_updated = outer_cursor['lastUpdated']

            # Get the relevant primary key fields for comparing to other miners.
            time_bucket_id = row['timeBucketId']
            source = row['source']
            label=row['label']
            # Get the total bytes for this chunk for this miner before adjusting for uniqueness.
            content_size_bytes=row['contentSizeBytes']

            # Find the sum of contentBytes for identical chunk across all other miners.
            inner_cursor = self.connection.cursor()
            inner_cursor.execute("""SELECT SUM(contentSizeBytes) FROM MinerIndex WHERE 
                                 timeBucketId = ? AND
                                 source = ? AND
                                 label = ? AND
                                 hotkey IN ?""",
                                 [time_bucket_id, source, label, tuple(valid_miners)])
            
            result = inner_cursor.fetchone()
            sum_content_size_bytes = result[0]

            # Score the bytes as the fraction of the total content bytes for that chunk across all valid miners.
            scored_chunk = ScorableDataChunkSummary(
                            time_bucket_id=time_bucket_id,
                            source=DataSource(source),
                            size_bytes=content_size_bytes,
                            scorable_bytes=content_size_bytes*content_size_bytes/sum_content_size_bytes)
            
            if label:
                scored_chunk.label = label
            
            # Add the chunk to the list of scored chunks on the overall index.
            scored_index.scorable_chunks.append(scored_chunk)

        return scored_index


    def delete_miner_index(self, miner_hotkey: str):
        """Removes the index for the specified miner."""
        # Delete the rows for the specified miner.
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM MinerIndex WHERE hotkey = ?", [miner_hotkey])
        self.connection.commit()
