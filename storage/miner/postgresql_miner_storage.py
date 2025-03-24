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
    HuggingFaceMetadata,
)
from storage.miner.miner_storage import MinerStorage
from typing import Dict, List
import datetime as dt
import psycopg2
import contextlib
import bittensor as bt
import pandas as pd


# Use a timezone aware adapter for timestamp columns (not needed for psycopg2, handled natively).
class PostgresMinerStorage(MinerStorage):
    """PostgreSQL backed MinerStorage"""

    DATA_ENTITY_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS DataEntity (
                                uri                 TEXT,
                                datetime            TIMESTAMPTZ NOT NULL,
                                timeBucketId        INTEGER     NOT NULL,
                                source              INTEGER     NOT NULL,
                                label               CHAR(32),
                                content             BYTEA       NOT NULL,
                                contentSizeBytes    INTEGER     NOT NULL,
                                PRIMARY KEY (uri, datetime)
                                ) PARTITION BY RANGE (datetime)"""

    DELETE_OLD_INDEX = """DROP INDEX IF EXISTS data_entity_bucket_index"""

    DATA_ENTITY_TABLE_INDEX = """CREATE INDEX IF NOT EXISTS data_entity_bucket_index2
                                ON DataEntity (timeBucketId, source, label, contentSizeBytes)"""

    HF_METADATA_TABLE_CREATE = """CREATE TABLE IF NOT EXISTS HFMetaData (
                                uri                 TEXT        PRIMARY KEY,
                                source              INTEGER     NOT NULL,
                                updatedAt           TIMESTAMPTZ NOT NULL,
                                encodingKey         TEXT
                                )"""

    def __init__(
        self,
        dbname="subnet13",
        user="subnet13",
        password="subnet13",
        host="localhost",
        max_database_size_gb_hint=250,
    ):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

        self.database_max_content_size_bytes = utils.gb_to_bytes(max_database_size_gb_hint)

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            # Create the DataEntity table (if it does not already exist).
            cursor.execute(PostgresMinerStorage.DATA_ENTITY_TABLE_CREATE)

            # Delete the old index (if it exists).
            cursor.execute(PostgresMinerStorage.DELETE_OLD_INDEX)

            # Create the Index (if it does not already exist).
            cursor.execute(PostgresMinerStorage.DATA_ENTITY_TABLE_INDEX)

            # Create the HFMetaData table
            cursor.execute(PostgresMinerStorage.HF_METADATA_TABLE_CREATE)

            # Use Write Ahead Logging equivalent in PostgreSQL (set in config, not pragma)
            connection.commit()

        self._ensure_hf_metadata_schema()
        self.clearing_space_lock = threading.Lock()
        self.cached_index_refresh_lock = threading.Lock()
        self.cached_index_lock = threading.Lock()
        self.cached_index_4 = None
        self.cached_index_updated = dt.datetime.min

    def _create_connection(self):
        connection = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            connect_timeout=60
        )
        return connection

    def _ensure_hf_metadata_schema(self):
        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()

            # Check if the encodingKey column exists
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'hfmetadata'")
            columns = [col[0] for col in cursor.fetchall()]

            if 'encodingkey' not in columns:
                cursor.execute("ALTER TABLE HFMetaData ADD COLUMN encodingKey TEXT")
                bt.logging.info("Added encodingKey column to HFMetaData table")

            connection.commit()

    def store_data_entities(self, data_entities: List[DataEntity]):
        added_content_size = 0
        current_utc_time = dt.datetime.now(dt.timezone.utc)  # Thời điểm hiện tại theo UTC
        thirty_days_ago = current_utc_time - dt.timedelta(days=30)  # Giới hạn 30 ngày trư
        
        # Lọc các data_entities hợp lệ (không cũ hơn 30 ngày)
        valid_data_entities = []

        for data_entity in data_entities:
            # Đảm bảo datetime là UTC (nếu chưa có tzinfo, gán UTC)
            entity_datetime = data_entity.datetime
            if entity_datetime.tzinfo is None:
                entity_datetime = entity_datetime.replace(tzinfo=dt.timezone.utc)
        
            # Kiểm tra nếu datetime cũ hơn 30 ngày
            if entity_datetime < thirty_days_ago:
                bt.logging.debug(f"Skipping entity {data_entity.uri}: datetime {entity_datetime} is older than 30 days")
                continue
            #add valid content
            valid_data_entities.append(data_entity)

            added_content_size += data_entity.content_size_bytes

        if added_content_size > self.database_max_content_size_bytes:
            raise ValueError(
                f"Content size to store: {added_content_size} exceeds configured max: {self.database_max_content_size_bytes}"
            )
        # Nếu không có entity nào hợp lệ, thoát sớm
        if not valid_data_entities:
            bt.logging.info("No valid data entities to store (all older than 30 days)")
            return
    
        with contextlib.closing(self._create_connection()) as connection:
            with self.clearing_space_lock:
                cursor = connection.cursor()
                cursor.execute("SELECT SUM(contentSizeBytes) FROM DataEntity")
                result = cursor.fetchone()
                current_content_size = result[0] if result[0] else 0

                if current_content_size + added_content_size > self.database_max_content_size_bytes:
                    content_bytes_to_clear = (
                        self.database_max_content_size_bytes // 10
                        if self.database_max_content_size_bytes // 10 > added_content_size
                        else added_content_size
                    )
                    self.clear_content_from_oldest(content_bytes_to_clear)

            values = []
            for data_entity in valid_data_entities:
                label = None if data_entity.label is None else data_entity.label.value
                data_entity.datetime.astimezone(dt.timezone.utc)
                time_bucket_id = TimeBucket.from_datetime(data_entity.datetime).id
                values.append(
                    (
                        data_entity.uri,
                        data_entity.datetime,
                        time_bucket_id,
                        data_entity.source,
                        label,
                        data_entity.content,
                        data_entity.content_size_bytes,
                    )
                )

            cursor.executemany(
                """INSERT INTO DataEntity (uri, datetime, timeBucketId, source, label, content, contentSizeBytes)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (uri, datetime) DO UPDATE SET
                   timeBucketId = EXCLUDED.timeBucketId,
                   source = EXCLUDED.source,
                   label = EXCLUDED.label,
                   content = EXCLUDED.content,
                   contentSizeBytes = EXCLUDED.contentSizeBytes""",
                values
            )
            connection.commit()

    def store_hf_dataset_info(self, hf_metadatas: List[HuggingFaceMetadata]):
        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            values = []
            for hf_metadata in hf_metadatas:
                values.append(
                    (
                        hf_metadata.repo_name,
                        hf_metadata.source,
                        hf_metadata.updated_at,
                        getattr(hf_metadata, 'encoding_key', None)
                    )
                )

            cursor.executemany(
                """INSERT INTO HFMetaData (uri, source, updatedAt, encodingKey)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (uri) DO UPDATE SET
                   source = EXCLUDED.source,
                   updatedAt = EXCLUDED.updatedAt,
                   encodingKey = EXCLUDED.encodingKey""",
                values
            )
            connection.commit()

    def get_earliest_data_datetime(self, source):
        query = "SELECT MIN(datetime) AS earliest_date FROM DataEntity WHERE source = %s"
        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute(query, (source,))
            result = cursor.fetchone()
            return result[0] if result and result[0] else None

    def should_upload_hf_data(self, unique_id: str) -> bool:
        sql_query = """
            SELECT TO_TIMESTAMP(AVG(EXTRACT(EPOCH FROM UpdatedAt))) AS AvgUpdatedAt
            FROM (
                SELECT UpdatedAt
                FROM HFMetaData
                WHERE uri LIKE %s
                ORDER BY UpdatedAt DESC
                LIMIT 2
            ) AS subquery;
        """
        try:
            with contextlib.closing(self._create_connection()) as connection:
                cursor = connection.cursor()
                cursor.execute(sql_query, (f"%_{unique_id}",))
                result = cursor.fetchone()

                if result is None or result[0] is None:
                    return True

                average_datetime = result[0].replace(tzinfo=dt.timezone.utc)
                current_datetime = dt.datetime.now(dt.timezone.utc)
                time_difference = dt.timedelta(seconds=61200)
                threshold_datetime = current_datetime - time_difference

                return threshold_datetime > average_datetime
        except psycopg2.Error as e:
            bt.logging.error(f"An error occurred: {e}")
            return False

    def get_hf_metadata(self, unique_id: str) -> List[HuggingFaceMetadata]:
        sql_query = """
            SELECT uri, source, updatedAt,
                   COALESCE(encodingKey, '') AS encodingKey
            FROM HFMetaData
            WHERE uri LIKE %s
            ORDER BY updatedAt DESC
            LIMIT 2;
        """
        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute(sql_query, (f"%_{unique_id}",))
            hf_metadatas = []

            for row in cursor:
                hf_metadata = HuggingFaceMetadata(
                    repo_name=row[0],
                    source=row[1],
                    updated_at=row[2],
                    encoding_key=row[3] if row[3] != '' else None
                )
                hf_metadatas.append(hf_metadata)

        return hf_metadatas

    def list_data_entities_in_data_entity_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        label = None if data_entity_bucket_id.label is None else data_entity_bucket_id.label.value
        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """SELECT uri, datetime, source, label, convert_from(content, 'UTF8') AS content, contentSizeBytes 
                   FROM DataEntity 
                   WHERE timeBucketId = %s AND source = %s AND (label = %s OR (label IS NULL AND %s IS NULL))""",
                (
                    data_entity_bucket_id.time_bucket.id,
                    data_entity_bucket_id.source,
                    label,
                    label,
                ),
            )

            data_entities = []
            running_size = 0

            for row in cursor:
                if running_size >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES:
                    return data_entities
                else:
                    data_entity = DataEntity(
                        uri=row[0],
                        datetime=row[1],
                        source=DataSource(row[2]),
                        content=row[4],
                        content_size_bytes=row[5],
                        label=DataLabel(value=row[3]) if row[3] is not None else None
                    )
                    data_entities.append(data_entity)
                    running_size += row[5]

            bt.logging.trace(
                f"Returning {len(data_entities)} data entities for bucket {data_entity_bucket_id}"
            )
            return data_entities

    def refresh_compressed_index(self, time_delta: dt.timedelta):
        with self.cached_index_lock:
            if dt.datetime.now() - self.cached_index_updated <= time_delta:
                bt.logging.trace(f"Skipping updating cached index. It is already fresher than {time_delta}.")
                return
            else:
                bt.logging.info(f"Cached index out of {time_delta} freshness period. Refreshing cached index.")

        with self.cached_index_refresh_lock:
            with self.cached_index_lock:
                if dt.datetime.now() - self.cached_index_updated <= time_delta:
                    bt.logging.trace("After waiting on refresh lock the index was already refreshed.")
                    return

            with contextlib.closing(self._create_connection()) as connection:
                cursor = connection.cursor()
                oldest_time_bucket_id = TimeBucket.from_datetime(
                    dt.datetime.now() - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
                ).id

                cursor.execute(
                    """SELECT SUM(contentSizeBytes) AS bucketSize, timeBucketId, source, label 
                       FROM DataEntity
                       WHERE timeBucketId >= %s
                       GROUP BY timeBucketId, source, label
                       ORDER BY bucketSize DESC
                       LIMIT %s""",
                    (
                        oldest_time_bucket_id,
                        constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4,
                    ),
                )

                buckets_by_source_by_label = defaultdict(dict)
                for row in cursor:
                    size = (
                        constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                        if row[0] >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                        else row[0]
                    )
                    label = row[3] if row[3] is not None else None
                    bucket = buckets_by_source_by_label[DataSource(row[2])].get(
                        label, CompressedEntityBucket(label=label)
                    )
                    bucket.sizes_bytes.append(size)
                    bucket.time_bucket_ids.append(row[1])
                    buckets_by_source_by_label[DataSource(row[2])][label] = bucket

                bt.logging.trace("Creating protocol 4 cached index.")
                with self.cached_index_lock:
                    self.cached_index_4 = CompressedMinerIndex(
                        sources={
                            source: list(labels_to_buckets.values())
                            for source, labels_to_buckets in buckets_by_source_by_label.items()
                        }
                    )
                    self.cached_index_updated = dt.datetime.now()
                    bt.logging.success(
                        f"Created cached index of {CompressedMinerIndex.size_bytes(self.cached_index_4)} bytes "
                        + f"across {CompressedMinerIndex.bucket_count(self.cached_index_4)} buckets."
                    )

    def list_contents_in_data_entity_buckets(
        self, data_entity_bucket_ids: List[DataEntityBucketId]
    ) -> Dict[DataEntityBucketId, List[bytes]]:
        if (
            len(data_entity_bucket_ids) == 0
            or len(data_entity_bucket_ids) > constants.BULK_BUCKETS_COUNT_LIMIT
        ):
            return defaultdict(list)

        time_bucket_ids_and_labels = []
        for bucket_id in data_entity_bucket_ids:
            time_bucket_ids_and_labels.append(bucket_id.time_bucket.id)
            label = None if bucket_id.label is None else bucket_id.label.value
            time_bucket_ids_and_labels.append(label)

        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            query = (
                """SELECT timeBucketId, source, label, convert_from(content, 'UTF8') AS content, contentSizeBytes 
                   FROM DataEntity
                   WHERE """
                + " OR ".join(
                    ["(timeBucketId = %s AND (label = %s OR (label IS NULL AND %s IS NULL)))"]
                    * len(data_entity_bucket_ids)
                )
                + " LIMIT %s"
            )
            cursor.execute(
                query,
                time_bucket_ids_and_labels + [constants.BULK_CONTENTS_COUNT_LIMIT],
            )

            buckets_ids_to_contents = defaultdict(list)
            running_size = 0

            for row in cursor:
                if running_size < constants.BULK_CONTENTS_SIZE_LIMIT_BYTES:
                    data_entity_bucket_id = DataEntityBucketId(
                        time_bucket=TimeBucket(id=row[0]),
                        source=DataSource(row[1]),
                        label=DataLabel(value=row[2]) if row[2] is not None else None
                    )
                    buckets_ids_to_contents[data_entity_bucket_id].append(row[3])
                    running_size += row[4]
                else:
                    break

            return buckets_ids_to_contents

    def get_compressed_index(
        self,
        bucket_count_limit=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4,
    ) -> CompressedMinerIndex:
        self.refresh_compressed_index(
            time_delta=(constants.MINER_CACHE_FRESHNESS + dt.timedelta(minutes=10))
        )
        with self.cached_index_lock:
            return self.cached_index_4

    def clear_content_from_oldest(self, content_bytes_to_clear: int):
        bt.logging.debug(f"Database full. Clearing {content_bytes_to_clear} bytes.")
        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT contentSizeBytes, datetime FROM DataEntity ORDER BY datetime ASC"
            )

            running_bytes = 0
            earliest_datetime_to_clear = dt.datetime.min
            for row in cursor:
                running_bytes += row[0]
                earliest_datetime_to_clear = row[1]
                if running_bytes >= content_bytes_to_clear:
                    cursor.execute(
                        "DELETE FROM DataEntity WHERE datetime <= %s",
                        (earliest_datetime_to_clear,),
                    )
                    connection.commit()
                    break

    def list_data_entity_buckets(self) -> List[DataEntityBucket]:
        with contextlib.closing(self._create_connection()) as connection:
            cursor = connection.cursor()
            oldest_time_bucket_id = TimeBucket.from_datetime(
                dt.datetime.now() - dt.timedelta(constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
            ).id
            cursor.execute(
                """SELECT SUM(contentSizeBytes) AS bucketSize, timeBucketId, source, label 
                   FROM DataEntity
                   WHERE timeBucketId >= %s
                   GROUP BY timeBucketId, source, label
                   ORDER BY bucketSize DESC
                   LIMIT %s""",
                (
                    oldest_time_bucket_id,
                    constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX,
                ),
            )

            data_entity_buckets = []
            for row in cursor:
                size = (
                    constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                    if row[0] >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES
                    else row[0]
                )
                data_entity_bucket_id = DataEntityBucketId(
                    time_bucket=TimeBucket(id=row[1]),
                    source=DataSource(row[2]),
                    label=DataLabel(value=row[3]) if row[3] is not None else None
                )
                data_entity_bucket = DataEntityBucket(
                    id=data_entity_bucket_id, size_bytes=size
                )
                data_entity_buckets.append(data_entity_bucket)

            return data_entity_buckets