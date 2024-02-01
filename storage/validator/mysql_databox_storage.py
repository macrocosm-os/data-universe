from typing import List
import datetime as dt
import mysql.connector
from common.data_v2 import DataBoxMiner, DataBoxLabelSize, DataBoxAgeSize


class MysqlDataboxStorage:
    """MySQL backed Databox Storage"""

    # Table for information by miner hotkey
    # We store all hotkeys. ~250 max.
    MINER_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS Miner (
                            hotkey                      VARCHAR(64) NOT NULL    PRIMARY KEY,
                            credibility                 FLOAT       NOT NULL,
                            bucketCount                 INTEGER     NOT NULL,
                            contentSizeBytesReddit      BIGINT      NOT NULL,
                            contentSizeBytesTwitter     BIGINT      NOT NULL,
                            lastUpdated                 DATETIME(6) NOT NULL
                            )"""

    # Table for content size by label (labelValue 'NULL' is OK.)
    # We only store top 1k per source for databox limits
    LABEL_SIZE_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS LabelSize (
                                    source              TINYINT         NOT NULL,
                                    labelValue          VARCHAR(32)     NOT NULL,
                                    contentSizeBytes    BIGINT          NOT NULL,
                                    adjContentSizeBytes BIGINT          NOT NULL,
                                    PRIMARY KEY(source, labelValue)
                                )"""

    # Table for content size by age
    # We only store top 1k per source for databox limits
    AGE_SIZE_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS AgeSize (
                                    source              TINYINT         NOT NULL,
                                    timeBucketId        INTEGER         NOT NULL,
                                    contentSizeBytes    BIGINT          NOT NULL,
                                    adjContentSizeBytes BIGINT          NOT NULL,
                                    PRIMARY KEY(source, timeBucketId)
                                )"""

    def __init__(self, host: str, user: str, password: str, database: str):
        # Get the connection to the user-created MySQL database.
        self.connection = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )

        cursor = self.connection.cursor()
        # Create the Miner table if it doesn't exist
        cursor.execute(MysqlDataboxStorage.MINER_TABLE_CREATE)
        # Create the Label Size table if it doesn't exist
        cursor.execute(MysqlDataboxStorage.LABEL_SIZE_TABLE_CREATE)
        # Create the Age Size table if it doesn't exist
        cursor.execute(MysqlDataboxStorage.AGE_SIZE_TABLE_CREATE)

        # Update the database to use the correct collation for accent sensitivity.
        # This can't escape the database name as it is part of the command, but it is against your own database.
        cursor.execute("ALTER DATABASE " + database + " COLLATE utf8mb4_0900_as_ci;")

        self.connection.commit()

    def __del__(self):
        self.connection.close()

    def insert_miners(self, miners: List[DataBoxMiner]):
        cursor = self.connection.cursor()

        vals = [
            [
                miner.hotkey,
                miner.credibility,
                miner.bucket_count,
                miner.content_size_bytes_reddit,
                miner.content_size_bytes_twitter,
                miner.last_updated,
            ]
            for miner in miners
        ]

        cursor.execute("TRUNCATE Miner")
        self.connection.commit()

        cursor.executemany(
            """INSERT IGNORE INTO Miner 
               (hotkey, credibility, bucketCount, contentSizeBytesReddit, contentSizeBytesTwitter, lastUpdated)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            vals,
        )

        self.connection.commit()

    def insert_label_sizes(self, label_sizes: List[DataBoxLabelSize]):
        cursor = self.connection.cursor()

        vals = [
            [
                label_size.source,
                label_size.label_value,
                label_size.content_size_bytes,
                label_size.adj_content_size_bytes,
            ]
            for label_size in label_sizes
        ]

        cursor.execute("TRUNCATE LabelSize")
        self.connection.commit()

        cursor.executemany(
            """INSERT IGNORE INTO LabelSize 
               (source, labelValue, contentSizeBytes, adjContentSizeBytes)
               VALUES (%s, %s, %s, %s)""",
            vals,
        )

        self.connection.commit()

    def insert_age_sizes(self, age_sizes: List[DataBoxAgeSize]):
        cursor = self.connection.cursor()

        vals = [
            [
                age_size.source,
                age_size.time_bucket_id,
                age_size.content_size_bytes,
                age_size.adj_content_size_bytes,
            ]
            for age_size in age_sizes
        ]

        cursor.execute("TRUNCATE AgeSize")
        self.connection.commit()

        cursor.executemany(
            """INSERT IGNORE INTO AgeSize 
               (source, timeBucketId, contentSizeBytes, adjContentSizeBytes)
               VALUES (%s, %s, %s, %s)""",
            vals,
        )

        self.connection.commit()
