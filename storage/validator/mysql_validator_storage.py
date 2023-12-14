import threading
from common import utils
from common.data import (
    DataEntityBucket,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    MinerIndex,
    ScorableDataEntityBucket,
    ScorableMinerIndex,
    TimeBucket,
)
from storage.validator.validator_storage import ValidatorStorage
from typing import Optional, Set
import datetime as dt
import mysql.connector
import bittensor as bt


class MysqlValidatorStorage(ValidatorStorage):
    """MySQL backed Validator Storage"""

    # Primary table in which the DataEntityBuckets for all miners are stored.
    MINER_INDEX_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS MinerIndex (
                                minerId             INT             NOT NULL,
                                timeBucketId        INT             NOT NULL,
                                source              TINYINT         NOT NULL,
                                labelId             INT             NOT NULL,
                                contentSizeBytes    INT             NOT NULL,
                                PRIMARY KEY(minerId, timeBucketId, source, labelId)
                                )"""

    # Mapping table from miner hotkey to minerId and lastUpdated for use in the primary table.
    MINER_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS Miner (
                            hotkey      VARCHAR(64) NOT NULL    PRIMARY KEY,
                            minerId     INT         NOT NULL    AUTO_INCREMENT UNIQUE,
                            lastUpdated DATETIME(6) NOT NULL
                            )"""

    # Mapping table from label string to labelId for use in the primary table.
    LABEL_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS Label (
                            labelValue  VARCHAR(32) NOT NULL    PRIMARY KEY,
                            labelId     INT         NOT NULL    AUTO_INCREMENT UNIQUE
                            )"""

    def __init__(self, host: str, user: str, password: str, database: str):
        # Get the connection to the user-created MySQL database.
        self.connection = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )

        cursor = self.connection.cursor()

        # Create the MinerIndex table if it doesn't exist
        cursor.execute(MysqlValidatorStorage.MINER_INDEX_TABLE_CREATE)

        # Create the Miner table if it doesn't exist
        cursor.execute(MysqlValidatorStorage.MINER_TABLE_CREATE)

        # Create the Label table if it doesn't exist
        cursor.execute(MysqlValidatorStorage.LABEL_TABLE_CREATE)

        # Lock to avoid concurrency issues on clearing and inserting an index.
        self.upsert_miner_index_lock = threading.Lock()

    def __del__(self):
        self.connection.close()

    def _upsert_miner(self, hotkey: str, now_str: str) -> int:
        """Stores an encountered miner hotkey returning this Validator's unique id for it"""

        minerId = 0

        # Buffer to ensure rowcount is correct.
        cursor = self.connection.cursor(buffered=True)
        # Check to see if the Miner already exists.
        cursor.execute("SELECT minerId FROM Miner WHERE hotkey = %s", [hotkey])

        if cursor.rowcount:
            # If it does we can use the already fetched id.
            minerId = cursor.fetchone()[0]
            # If it does we only update the lastUpdated. (ON DUPLICATE KEY UPDATE consumes an autoincrement value)
            cursor.execute(
                "UPDATE Miner SET lastUpdated = %s WHERE hotkey = %s", [now_str, hotkey]
            )
            self.connection.commit()
        else:
            # If it doesn't we insert it.
            cursor.execute(
                "INSERT IGNORE INTO Miner (hotkey, lastUpdated) VALUES (%s, %s)",
                [hotkey, now_str],
            )
            self.connection.commit()
            # Then we get the newly created minerId
            cursor.execute("SELECT minerId FROM Miner WHERE hotkey = %s", [hotkey])
            minerId = cursor.fetchone()[0]

        return minerId

    def _get_or_insert_label(self, label: str) -> int:
        """Gets an encountered label or stores a new one returning this Validator's unique id for it"""

        labelId = 0

        # Buffer to ensure rowcount is correct.
        cursor = self.connection.cursor(buffered=True)
        # Check to see if the Label already exists.
        cursor.execute("SELECT labelId FROM Label WHERE labelValue = %s", [label])

        if cursor.rowcount:
            # If it does we can use the already fetched id.
            labelId = cursor.fetchone()[0]
        else:
            # If it doesn't we insert it.
            cursor.execute("INSERT IGNORE INTO Label (labelValue) VALUES (%s)", [label])
            self.connection.commit()
            # Then we get the newly created labelId
            cursor.execute("SELECT labelId FROM Label WHERE labelValue = %s", [label])
            labelId = cursor.fetchone()[0]

        return labelId

    def upsert_miner_index(self, index: MinerIndex):
        """Stores the index for all of the data that a specific miner promises to provide."""

        now_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        cursor = self.connection.cursor()

        # Upsert this Validator's minerId for the specified hotkey.
        miner_id = self._upsert_miner(index.hotkey, now_str)

        # Parse every DataEntityBucket from the index into a list of values to insert.
        values = []
        for data_entity_bucket in index.data_entity_buckets:
            label = (
                "NULL"
                if (data_entity_bucket.id.label is None)
                else data_entity_bucket.id.label.value
            )

            # Get or insert this Validator's labelId for the specified label.
            label_id = self._get_or_insert_label(label)

            values.append(
                [
                    miner_id,
                    data_entity_bucket.id.time_bucket.id,
                    data_entity_bucket.id.source.value,
                    label_id,
                    data_entity_bucket.size_bytes,
                ]
            )

        with self.upsert_miner_index_lock:
            # Clear the previous keys for this miner.
            self.delete_miner_index(index.hotkey)

            # Insert the new keys.
            cursor.executemany(
                """INSERT INTO MinerIndex VALUES (%s, %s, %s, %s, %s)""", values
            )
            self.connection.commit()

    def read_miner_index(
        self, hotkey: str, valid_miners: Set[str]
    ) -> Optional[ScorableMinerIndex]:
        """Gets a scored index for all of the data that a specific miner promises to provide.

        Args:
            hotkey (str): The hotkey of the miner to read the index for.
            valid_miners (Set[str]): The set of miners that should be used for uniqueness scoring.
        """

        # Get a cursor to the database with dictionary enabled for accessing columns by name.
        cursor = self.connection.cursor(dictionary=True)

        last_updated = None

        # Include the specified miner in the set of miners we check even if it is invalid.
        valid_miners.add(hotkey)

        # Add enough %s for the IN query to match the valid_miners list.
        format_valid_miners = ",".join(["%s"] * len(valid_miners))

        # Get all the DataEntityBuckets for this miner joined to the total content size of like buckets.
        sql_string = """SELECT mi1.*, m1.hotkey, m1.lastUpdated, l.labelValue, agg.totalContentSize 
                       FROM MinerIndex mi1
                       JOIN Miner m1 ON mi1.minerId = m1.minerId
                       LEFT JOIN Label l on mi1.labelId = l.labelId
                       LEFT JOIN (
                            SELECT mi2.timeBucketId, mi2.source, mi2.labelId, SUM(mi2.contentSizeBytes) as totalContentSize
                            FROM MinerIndex mi2
                            JOIN Miner m2 ON mi2.minerId = m2.minerId
                            WHERE m2.hotkey IN ({0})
                            GROUP BY mi2.timeBucketId, mi2.source, mi2.labelId
                       ) agg ON mi1.timeBucketId = agg.timeBucketId
                            AND mi1.source = agg.source
                            AND mi1.labelId = agg.labelId 
                       WHERE m1.hotkey = %s""".format(
            format_valid_miners
        )

        # Build the args, ensuring our final tuple has at least 2 elements.
        args = []
        args.extend(list(valid_miners))
        args.append(hotkey)

        cursor.execute(sql_string, tuple(args))

        # Create to a list to hold each of the ScorableDataEntityBuckets we generate for this miner.
        scored_data_entity_buckets = []

        # For each row (representing a DataEntityBucket and Uniqueness) turn it into a ScorableDataEntityBucket.
        bt.logging.trace(f"{hotkey}: Reading miner index")
        for row in cursor:
            bt.logging.trace(f"Got row: {row}")

            # Set last_updated to the first value since they are all the same for a given miner.
            if last_updated == None:
                last_updated = row["lastUpdated"]

            # Get the relevant primary key fields for comparing to other miners.
            label = row["labelValue"]
            # Get the total bytes for this bucket for this miner before adjusting for uniqueness.
            content_size_bytes = row["contentSizeBytes"]
            # Get the total bytes for this bucket across all valid miners (+ this miner).
            total_content_size_bytes = row["totalContentSize"]

            bt.logging.trace(f"Source is: {row['source']}")
            s = DataSource(row["source"])
            bt.logging.trace(f"DataSource is: {s}")
            # Score the bytes as the fraction of the total content bytes for that bucket across all valid miners.
            data_entity_bucket_id = DataEntityBucketId(
                time_bucket=TimeBucket(id=row["timeBucketId"]),
                source=s,
            )
            bt.logging.trace(f"Created data_entity_bucket_id: {data_entity_bucket_id}")
            if label != "NULL":
                data_entity_bucket_id.label = DataLabel(value=label)

            data_entity_bucket = DataEntityBucket(
                id=data_entity_bucket_id, size_bytes=content_size_bytes
            )
            scored_data_entity_bucket = ScorableDataEntityBucket(
                data_entity_bucket=data_entity_bucket,
                scorable_bytes=content_size_bytes
                * content_size_bytes
                / total_content_size_bytes,
            )

            # Add the bucket to the list of scored buckets on the overall index.
            scored_data_entity_buckets.append(scored_data_entity_bucket)

        # If we do not get any rows back then do not return an empty ScorableMinerIndex.
        if last_updated == None:
            return None

        scored_index = ScorableMinerIndex(
            hotkey=hotkey,
            scorable_data_entity_buckets=scored_data_entity_buckets,
            last_updated=last_updated,
        )
        return scored_index

    def delete_miner_index(self, hotkey: str):
        """Removes the index for the specified miner.

        Args:
        miner (str): The hotkey of the miner to remove from the index.
        """

        # Get the minerId from the hotkey.
        cursor = self.connection.cursor(buffered=True)
        cursor.execute("SELECT minerId FROM Miner WHERE hotkey = %s", [hotkey])

        # Delete the rows for the specified miner.
        if cursor.rowcount:
            miner_id = cursor.fetchone()[0]
            cursor.execute("DELETE FROM MinerIndex WHERE minerId = %s", [miner_id])
            self.connection.commit()

    def read_miner_last_updated(self, hotkey: str) -> Optional[dt.datetime]:
        """Gets when a specific miner was last updated. Or none.

        Args:
            hotkey (str): The hotkey of the miner to check for last updated.
        """

        # Buffer to ensure rowcount is correct.
        cursor = self.connection.cursor(buffered=True)
        # Check to see if the Miner already exists.
        cursor.execute("SELECT lastUpdated FROM Miner WHERE hotkey = %s", [hotkey])

        if cursor.rowcount:
            # If it does we can use the already fetched id.
            return cursor.fetchone()[0]
        else:
            return None
