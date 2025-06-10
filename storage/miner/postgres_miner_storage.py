from collections import defaultdict

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from typing import List, Dict, Optional, Tuple
from common.data import DataEntity, DataEntityBucketId, CompressedMinerIndex, TimeBucket, DataLabel
import json
from datetime import datetime
import threading


class PostgresMinerStorage:
    def __init__(self, connection_string: str, max_db_size_gb_hint: int = 10):
        self.connection_string = connection_string
        self.lock = threading.RLock()
        self._initialize_db()

    def _initialize_db(self):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Create tables if they don't exist
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_entities (
                    id SERIAL PRIMARY KEY,
                    uri TEXT NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    source TEXT NOT NULL,
                    label TEXT,
                    content BYTEA,
                    content_size_bytes INTEGER,
                    bucket_id TEXT NOT NULL,
                    UNIQUE(uri, datetime)
                );

                CREATE INDEX IF NOT EXISTS idx_data_entities_bucket_id ON data_entities(bucket_id);
                CREATE INDEX IF NOT EXISTS idx_data_entities_source ON data_entities(source);
                CREATE INDEX IF NOT EXISTS idx_data_entities_datetime ON data_entities(datetime);

                CREATE TABLE IF NOT EXISTS hf_metadata (
                    id SERIAL PRIMARY KEY,
                    unique_id TEXT NOT NULL,
                    metadata JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_hf_metadata_unique_id ON hf_metadata(unique_id);
                """)
                conn.commit()

    def _get_connection(self):
        return psycopg2.connect(self.connection_string)

    def store_data_entities(self, data_entities: List[DataEntity]) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                INSERT INTO data_entities (uri, datetime, source, label, content, content_size_bytes, bucket_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (uri, datetime) DO NOTHING
                """
                data = [(de.uri, de.datetime, de.source, de.label.value if de.label else None,
                         de.content, de.content_size_bytes, de.bucket_id()) for de in data_entities]
                execute_batch(cursor, query, data)
                conn.commit()
                return cursor.rowcount

    def list_data_entities_in_data_entity_bucket(self, bucket_id: DataEntityBucketId) -> List[DataEntity]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT uri, datetime, source, label, content, content_size_bytes
                FROM data_entities
                WHERE bucket_id = %s
                """, str(bucket_id),)

                return [
                    DataEntity(
                        uri=row[0],
                        datetime=row[1],
                        source=row[2],
                        label=DataLabel(row[3]) if row[3] else None,
                        content=row[4],
                        content_size_bytes=row[5]
                    ) for row in cursor.fetchall()
                ]

    def list_contents_in_data_entity_buckets(self, bucket_ids: List[DataEntityBucketId]) -> Dict[
        DataEntityBucketId, List[bytes]]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT bucket_id, content
                FROM data_entities
                WHERE bucket_id = ANY(%s)
                """, (bucket_ids,))

                result = defaultdict(list)
                for row in cursor.fetchall():
                    result[row[0]].append(row[1])
                return result

    def refresh_compressed_index(self, time_delta: datetime.timedelta) -> CompressedMinerIndex:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT 
                    source,
                    label,
                    DATE_TRUNC('hour', datetime) AS time_bucket,
                    COUNT(*) AS count,
                    SUM(content_size_bytes) AS size_bytes
                FROM data_entities
                WHERE datetime >= NOW() - INTERVAL %s
                GROUP BY source, label, DATE_TRUNC('hour', datetime)
                """, (f"{time_delta.days} days {time_delta.seconds} seconds",))

                # Process results into CompressedMinerIndex format
                # ... (implementation similar to your SQLite version)

    def store_hf_dataset_info(self, hf_metadata_list: List[Dict]):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                INSERT INTO hf_metadata (unique_id, metadata)
                VALUES (%s, %s)
                """
                data = [(self.unique_id, json.dumps(metadata)) for metadata in hf_metadata_list]
                execute_batch(cursor, query, data)
                conn.commit()

    def get_hf_metadata(self, unique_id: str) -> List[Dict]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT metadata
                FROM hf_metadata
                WHERE unique_id = %s
                ORDER BY created_at DESC
                """, (unique_id,))
                return [row[0] for row in cursor.fetchall()]

    def should_upload_hf_data(self, unique_id: str) -> bool:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT COUNT(*)
                FROM hf_metadata
                WHERE unique_id = %s AND created_at >= NOW() - INTERVAL '1 day'
                """, (unique_id,))
                return cursor.fetchone()[0] == 0