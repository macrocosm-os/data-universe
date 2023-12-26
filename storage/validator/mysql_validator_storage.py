from collections import defaultdict
import threading
from common import constants
from common.data import (
    CompressedMinerIndex,
    DataLabel,
    MinerIndex,
)
from common.data_v2 import ScorableDataEntityBucket, ScorableMinerIndex
from storage.validator.validator_storage import ValidatorStorage
from typing import Optional, Set, Dict
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
            host=host,
            user=user,
            password=password,
            database=database,
        )

        cursor = self.connection.cursor()

        # Create the MinerIndex table if it doesn't exist
        cursor.execute(MysqlValidatorStorage.MINER_INDEX_TABLE_CREATE)

        # Create the Miner table if it doesn't exist
        cursor.execute(MysqlValidatorStorage.MINER_TABLE_CREATE)

        # Create the Label table if it doesn't exist
        cursor.execute(MysqlValidatorStorage.LABEL_TABLE_CREATE)

        # Update the database to use the correct collation for accent sensitivity.
        # This can't escape the database name as it is part of the command, but it is against your own database.
        cursor.execute("ALTER DATABASE " + database + " COLLATE utf8mb4_0900_as_ci;")

        self.connection.commit()

        # Lock to avoid concurrency issues on clearing and inserting an index.
        self.upsert_miner_index_lock = threading.Lock()

    def __del__(self):
        self.connection.close()

    def _upsert_miner(self, hotkey: str, now_str: str) -> int:
        """Stores an encountered miner hotkey returning this Validator's unique id for it"""

        miner_id = 0

        # Buffer to ensure rowcount is correct.
        cursor = self.connection.cursor(buffered=True)
        # Check to see if the Miner already exists.
        cursor.execute("SELECT minerId FROM Miner WHERE hotkey = %s", [hotkey])

        if cursor.rowcount:
            # If it does we can use the already fetched id.
            miner_id = cursor.fetchone()[0]
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
            miner_id = cursor.fetchone()[0]

        return miner_id

    def _get_or_insert_label(self, label: str) -> int:
        """Gets an encountered label or stores a new one returning this Validator's unique id for it"""

        label_id = 0

        # Buffer to ensure rowcount is correct.
        cursor = self.connection.cursor(buffered=True)
        # Check to see if the Label already exists.
        cursor.execute("SELECT labelId FROM Label WHERE labelValue = %s", [label])

        if cursor.rowcount:
            # If it does we can use the already fetched id.
            label_id = cursor.fetchone()[0]
        else:
            # If it doesn't we insert it.
            cursor.execute("INSERT IGNORE INTO Label (labelValue) VALUES (%s)", [label])
            self.connection.commit()
            # Then we get the newly created labelId
            cursor.execute("SELECT labelId FROM Label WHERE labelValue = %s", [label])
            label_id = cursor.fetchone()[0]

        return label_id

    def _insert_labels(self, labels: Set[str]):
        """Stores labels creating new unique ids if they have not been encountered yet."""
        cursor = self.connection.cursor()

        vals = [[label] for label in labels]

        cursor.executemany("INSERT IGNORE INTO Label (labelValue) VALUES (%s)", vals)
        self.connection.commit()

    def _get_label_value_to_id_dict(self) -> Dict[str, int]:
        """Gets a dictionary map for all label values to this Validator's unique id for it."""

        label_value_to_id_dict = {}

        # Get a cursor to the database with dictionary enabled for accessing columns by name.
        cursor = self.connection.cursor(dictionary=True)

        cursor.execute("SELECT labelValue, labelId FROM Label")

        # Reach each row and add to the mapping dictionary
        for row in cursor:
            label_value_to_id_dict[row["labelValue"]] = row["labelId"]

        return label_value_to_id_dict

    def _label_value_parse(self, label: Optional[DataLabel]) -> str:
        """Parses the value to store in the database out of an Optional DataLabel."""
        return "NULL" if (label is None) else label.value

    def _label_value_parse_str(self, label: Optional[str]) -> str:
        """Same as _label_value_parse but with a string as input"""
        return "NULL" if (label is None) else label.casefold()

    # TODO: Deprecate
    def upsert_miner_index(self, index: MinerIndex):
        """Stores the index for all of the data that a specific miner promises to provide."""

        bt.logging.trace(
            f"{index.hotkey}: Upserting miner index with {len(index.data_entity_buckets)} buckets"
        )

        now_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        cursor = self.connection.cursor()

        # Upsert this Validator's minerId for the specified hotkey.
        miner_id = self._upsert_miner(index.hotkey, now_str)

        # Ensure that all the label ids in the upcoming entity buckets are known for this Validator.
        label_values = set()

        for data_entity_bucket in index.data_entity_buckets:
            label = self._label_value_parse(data_entity_bucket.id.label)
            label_values.add(label)

        self._insert_labels(label_values)

        # Get all label ids for use in mapping.
        label_value_to_id_dict = self._get_label_value_to_id_dict()

        # Parse every DataEntityBucket from the index into a list of values to insert.
        values = []
        for data_entity_bucket in index.data_entity_buckets:
            label = self._label_value_parse(data_entity_bucket.id.label)

            # Get or this Validator's labelId for the specified label.
            try:
                label_id = label_value_to_id_dict[label]

                values.append(
                    [
                        miner_id,
                        data_entity_bucket.id.time_bucket.id,
                        data_entity_bucket.id.source,
                        label_id,
                        data_entity_bucket.size_bytes,
                    ]
                )
            except:
                # In the case that we fail to get a label (due to unsupported characters) we drop just that one bucket.
                pass

        with self.upsert_miner_index_lock:
            # Clear the previous keys for this miner.
            self.delete_miner_index(index.hotkey)

            # Insert the new keys. (Ignore into to defend against a miner giving us multiple duplicate rows.)
            # Batch in groups of 1m if necessary to avoid congestion issues.
            value_subsets = [
                values[x : x + 1000000] for x in range(0, len(values), 1000000)
            ]
            for value_subset in value_subsets:
                cursor.executemany(
                    """INSERT IGNORE INTO MinerIndex VALUES (%s, %s, %s, %s, %s)""",
                    value_subset,
                )
            self.connection.commit()

    def upsert_compressed_miner_index(self, index: CompressedMinerIndex, hotkey: str):
        """Stores the index for all of the data that a specific miner promises to provide."""

        bt.logging.trace(
            f"{hotkey}: Upserting miner index with {CompressedMinerIndex.size(index)} buckets"
        )

        now_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        cursor = self.connection.cursor()

        # Upsert this Validator's minerId for the specified hotkey.
        miner_id = self._upsert_miner(hotkey, now_str)

        # Ensure that all the label ids in the upcoming entity buckets are known for this Validator.
        label_values = set()

        # Collect all buckets that we'll insert, by skipping over any with an invalid label
        buckets_by_source = defaultdict(list)
        for source, compressed_buckets in index.sources.items():
            for bucket in compressed_buckets:
                if bucket.label and len(bucket.label) > constants.MAX_LABEL_LENGTH:
                    bt.logging.trace(
                        f"Ignoring label {bucket.label} because it's too long"
                    )
                    continue
                label = self._label_value_parse_str(bucket.label)
                label_values.add(label)
                buckets_by_source[source].append(bucket)

        self._insert_labels(label_values)

        # Get all label ids for use in mapping.
        label_value_to_id_dict = self._get_label_value_to_id_dict()

        # Parse every DataEntityBucket from the index into a list of values to insert.
        values = []
        for source in buckets_by_source:
            for compressed_bucket in buckets_by_source[source]:
                label = self._label_value_parse_str(compressed_bucket.label)

                # Get or this Validator's labelId for the specified label.
                try:
                    label_id = label_value_to_id_dict[label]

                    for time_bucket_id, size_bytes in zip(
                        compressed_bucket.time_bucket_ids, compressed_bucket.sizes_bytes
                    ):
                        values.append(
                            [
                                miner_id,
                                time_bucket_id,
                                source,
                                label_id,
                                size_bytes,
                            ]
                        )
                except:
                    # In the case that we fail to get a label (due to unsupported characters) we drop just that one bucket.
                    pass

        with self.upsert_miner_index_lock:
            # Clear the previous keys for this miner.
            self.delete_miner_index(hotkey)

            # Insert the new keys. (Ignore into to defend against a miner giving us multiple duplicate rows.)
            # Batch in groups of 1m if necessary to avoid congestion issues.
            value_subsets = [
                values[x : x + 1000000] for x in range(0, len(values), 1000000)
            ]
            for value_subset in value_subsets:
                cursor.executemany(
                    """INSERT IGNORE INTO MinerIndex VALUES (%s, %s, %s, %s, %s)""",
                    value_subset,
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
        for row in cursor:
            # Set last_updated to the first value since they are all the same for a given miner.
            if last_updated == None:
                last_updated = row["lastUpdated"]

            # Get the relevant primary key fields for comparing to other miners.
            label = row["labelValue"] if row["labelValue"] != "NULL" else None
            # Get the total bytes for this bucket for this miner before adjusting for uniqueness.
            content_size_bytes = row["contentSizeBytes"]
            # Get the total bytes for this bucket across all valid miners (+ this miner).
            total_content_size_bytes = row["totalContentSize"]

            scored_data_entity_bucket = ScorableDataEntityBucket(
                time_bucket_id=int(row["timeBucketId"]),
                source=int(row["source"]),
                label=label,
                size_bytes=int(content_size_bytes),
                scorable_bytes=float(
                    content_size_bytes * content_size_bytes / total_content_size_bytes
                ),
            )

            # Add the bucket to the list of scored buckets on the overall index.
            scored_data_entity_buckets.append(scored_data_entity_bucket)

        # If we do not get any rows back then do not return an empty ScorableMinerIndex.
        if last_updated == None:
            return None

        scored_index = ScorableMinerIndex(
            scorable_data_entity_buckets=scored_data_entity_buckets,
            last_updated=last_updated,
        )
        return scored_index

    def delete_miner_index(self, hotkey: str):
        """Removes the index for the specified miner.

        Args:
        miner (str): The hotkey of the miner to remove from the index.
        """

        bt.logging.trace(f"{hotkey}: Deleting miner index")

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
