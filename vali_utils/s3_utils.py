"""
S3 validation utilities for enhanced miner data validation.
Provides comprehensive validation of S3-stored miner data using metadata analysis.
"""

import hashlib
import random
import re
import requests
import time
import xml.etree.ElementTree as ET
import urllib.parse
import pandas as pd
import json
import os
import duckdb
import datetime as dt
import math
import bittensor as bt
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass

from scraping.provider import ScraperProvider
from scraping.scraper import ScraperId, ValidationResult
from scraping.x.model import XContent
from scraping.reddit.model import RedditContent
from common.data import DataEntity, DataSource
from common.api_client import TaoSigner
from vali_utils.parquet_reader import read_random_row_group


@dataclass
class S3ValidationResult:
    """S3 validation result structure"""
    is_valid: bool
    validation_percentage: float

    # Job and file metrics
    total_active_jobs: int
    expected_jobs_count: int
    recent_jobs_analyzed: int
    recent_files_count: int
    total_size_bytes: int

    # Duplicate analysis
    has_duplicates: bool
    duplicate_percentage: float

    # Scraper validation metrics
    entities_validated: int
    entities_passed_scraper: int
    scraper_success_rate: float

    # Job content validation metrics
    entities_checked_for_job_match: int
    entities_matched_job: int
    job_match_rate: float

    # Issues and reasoning
    validation_issues: List[str]
    reason: str

    # Sample data for transparency
    sample_validation_results: List[str]
    sample_job_mismatches: List[str]

    # Competition-based scoring: effective_size = total_size_bytes × coverage²
    effective_size_bytes: float = 0.0
    job_coverage_rate: float = 0.0


# Mapping of scrapers to use based on the data source to validate
PREFERRED_SCRAPERS = {
    DataSource.X: ScraperId.X_APIDOJO,
    DataSource.REDDIT: ScraperId.REDDIT_MC,
}

class DuckDBSampledValidator:
    """
    DuckDB-based validator with random sampling for efficient validation.
    Uses batch processing to read multiple files in single queries.

    Key features:
    - Random sampling (default 10%) for efficient validation
    - Per-job duplicate checking (cross-job duplicates OK)
    - Batch DuckDB queries for speed
    - Competition scoring: effective_size = total_size × coverage²
    """

    # Validation thresholds
    MAX_DUPLICATE_RATE = 5.0    # 5% max duplicates within same job
    MAX_EMPTY_RATE = 10.0       # 10% max empty content
    # Missing URLs = instant fail (no rate threshold needed)
    MIN_JOB_MATCH_RATE = 95.0   # 95% min job content match rate
    MIN_SCRAPER_SUCCESS = 70.0  # 70% min scraper success rate

    # File size limits - prevent empty file exploit and oversized file OOM
    MIN_FILE_SIZE_BYTES = 15_000                   # 15KB - empty parquet header ≈ 8KB
    MAX_FILE_SIZE_BYTES = 512 * 1024 * 1024        # 512MB - single file cap

    # Scraper validation window — only files uploaded within this window are scraper-validated.
    # Older files rely on credibility from previous validation cycles.
    SCRAPER_MAX_AGE_HOURS = 6

    # Standard bytes per row for effective_size cap.
    # Real production data: X=77-515 B/row, Reddit=182-1682 B/row.
    STANDARD_BYTES_PER_ROW = 300

    # Filename pattern: data_YYYYMMDD_HHMMSS_{rowcount}_{hex16}.parquet
    _FILENAME_ROW_COUNT_RE = re.compile(r"^data_\d{8}_\d{6}_(\d+)_[a-f0-9]{16}\.parquet$")

    # No optional columns — files must match expected schema exactly
    OPTIONAL_COLUMNS: Set[str] = set()

    # Schema validation - expected columns per platform (from s3_uploader.py)
    EXPECTED_COLUMNS_X = {
        'datetime', 'label', 'username', 'text', 'tweet_hashtags', 'timestamp',
        'url', 'media', 'user_id', 'user_display_name', 'user_verified', 'tweet_id',
        'is_reply', 'is_quote', 'conversation_id', 'in_reply_to_user_id', 'language',
        'in_reply_to_username', 'quoted_tweet_id', 'like_count', 'retweet_count',
        'reply_count', 'quote_count', 'view_count', 'bookmark_count', 'user_blue_verified',
        'user_description', 'user_location', 'profile_image_url', 'cover_picture_url',
        'user_followers_count', 'user_following_count', 'scraped_at'
    }  # 33 columns

    EXPECTED_COLUMNS_REDDIT = {
        'datetime', 'label', 'id', 'username', 'communityName', 'body', 'title',
        'createdAt', 'dataType', 'parentId', 'url', 'media', 'is_nsfw', 'score',
        'upvote_ratio', 'num_comments', 'scrapedAt'
    }  # 17 columns

    def __init__(
        self,
        wallet,
        s3_auth_url: str,
        s3_reader=None,
        sample_percent: float = 10.0
    ):
        self.wallet = wallet
        self.s3_auth_url = s3_auth_url
        self.s3_reader = s3_reader
        self.sample_percent = sample_percent
        self.scraper_provider = ScraperProvider()
        self._signer = TaoSigner(keypair=wallet.hotkey)

    async def validate_miner_s3_data(
        self,
        miner_hotkey: str,
        expected_jobs: Dict
    ) -> S3ValidationResult:
        """
        Validate miner using DuckDB with random sampling.

        Returns S3ValidationResult with effective_size for competition scoring.
        """
        import time
        start_time = time.time()

        try:
            # Step 1: List ALL files (for size calculation - no reading needed)
            bt.logging.info(f"{miner_hotkey}: DuckDB validation - listing files...")

            if not self.s3_reader:
                return self._create_failed_result("S3 reader not available")

            all_files = await self.s3_reader.list_all_files_with_metadata(miner_hotkey)

            if not all_files:
                return self._create_failed_result("No files found")

            # Group files by job, filtering out empty/oversized files
            files_by_job = {}
            empty_files_skipped = 0
            oversized_files_skipped = 0
            for f in all_files:
                key = f.get('key', '')
                if '/job_id=' in key:
                    file_size = f.get('size', 0)
                    if file_size < self.MIN_FILE_SIZE_BYTES:
                        empty_files_skipped += 1
                        continue
                    if file_size > self.MAX_FILE_SIZE_BYTES:
                        oversized_files_skipped += 1
                        continue
                    # Skip files claiming 0 rows in filename (header-only files)
                    filename_rows = self._parse_row_count_from_filename(key)
                    if filename_rows is not None and filename_rows == 0:
                        empty_files_skipped += 1
                        continue
                    job_id = key.split('/job_id=')[1].split('/')[0]
                    existing = files_by_job.get(job_id)
                    if existing is None or f.get('last_modified', '') > existing[0].get('last_modified', ''):
                        files_by_job[job_id] = [f]

            # Filter to active jobs only
            active_job_ids = [jid for jid in files_by_job.keys() if jid in expected_jobs]

            if not active_job_ids:
                return self._create_failed_result("No active jobs found")

            # Calculate size and total rows from active jobs only
            # Per-job row clamping: if customer requested max_rows, excess rows don't count
            total_size_bytes = 0
            total_rows_from_filenames = 0
            for job_id in active_job_ids:
                job_config = expected_jobs.get(job_id, {})
                max_rows = job_config.get('max_rows') if isinstance(job_config, dict) else None
                job_rows = 0
                for f in files_by_job[job_id]:
                    total_size_bytes += f.get('size', 0)
                    fname_rows = self._parse_row_count_from_filename(f.get('key', ''))
                    if fname_rows is not None:
                        job_rows += fname_rows
                # Clamp to max_rows if set
                if max_rows and job_rows > max_rows:
                    total_rows_from_filenames += max_rows
                else:
                    total_rows_from_filenames += job_rows

            job_coverage_rate = (len(active_job_ids) / len(expected_jobs) * 100) if expected_jobs else 0

            if empty_files_skipped > 0:
                bt.logging.warning(
                    f"{miner_hotkey}: {empty_files_skipped} empty files skipped "
                    f"(< {self.MIN_FILE_SIZE_BYTES} bytes)"
                )
            if oversized_files_skipped > 0:
                bt.logging.warning(
                    f"{miner_hotkey}: {oversized_files_skipped} oversized files skipped "
                    f"(> {self.MAX_FILE_SIZE_BYTES/(1024*1024):.0f}MB)"
                )

            bt.logging.info(
                f"{miner_hotkey}: {len(all_files)} total files, "
                f"{len(active_job_ids)} active jobs, "
                f"{total_size_bytes/(1024*1024):.1f}MB, "
                f"{job_coverage_rate:.1f}% coverage"
            )

            # Step 2: Random sample files from active jobs
            active_files = []
            for job_id in active_job_ids:
                active_files.extend([(f, job_id) for f in files_by_job[job_id]])

            if not active_files:
                return self._create_failed_result("No files in active jobs")

            sample_count = max(10, int(len(active_files) * self.sample_percent / 100))
            sample_count = min(sample_count, len(active_files), 200)  # Cap at 200 files max

            sampled_files_with_job = random.sample(active_files, sample_count)
            sampled_files = [f for f, _ in sampled_files_with_job]

            bt.logging.info(f"{miner_hotkey}: Sampled {len(sampled_files)} files ({self.sample_percent}%)")

            # Step 3: Get presigned URLs for sample
            file_keys = [f['key'] for f in sampled_files]
            presigned_urls = await self._get_presigned_urls_batch(miner_hotkey, file_keys)

            if not presigned_urls:
                return self._create_failed_result("Failed to get presigned URLs")

            # Step 4: Lightweight DuckDB validation (sampled - memory safe)
            bt.logging.info(f"{miner_hotkey}: Running sampled DuckDB validation...")
            duckdb_result = await self._sampled_duckdb_validation(
                sampled_files, expected_jobs, presigned_urls, samples_per_file=100
            )

            # If schema validation failed, skip remaining checks (fail fast)
            if duckdb_result.get("reason"):
                bt.logging.warning(f"{miner_hotkey}: Schema validation failed - skipping remaining checks")
                job_match_result = {'total_checked': 0, 'total_matched': 0, 'match_rate': 0.0}
                scraper_result = {'entities_validated': 0, 'entities_passed': 0, 'success_rate': 0.0}
            else:
                # Step 5: Job content matching
                bt.logging.info(f"{miner_hotkey}: Checking job content matching...")
                job_match_result = await self._perform_job_content_matching(
                    sampled_files, expected_jobs, presigned_urls
                )

                # Step 6: Scraper validation — only on recently uploaded files
                cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=self.SCRAPER_MAX_AGE_HOURS)
                recent_files = []
                for f in sampled_files:
                    last_mod = f.get('last_modified', '')
                    if last_mod:
                        try:
                            file_dt = pd.to_datetime(last_mod, utc=True).to_pydatetime()
                            if file_dt >= cutoff:
                                recent_files.append(f)
                        except Exception:
                            pass

                if recent_files:
                    bt.logging.info(
                        f"{miner_hotkey}: Running scraper validation on {len(recent_files)} "
                        f"recent files (uploaded within {self.SCRAPER_MAX_AGE_HOURS}h)..."
                    )
                    scraper_result = await self._perform_scraper_validation(
                        miner_hotkey, recent_files, expected_jobs, presigned_urls, num_entities=10
                    )
                else:
                    bt.logging.info(
                        f"{miner_hotkey}: No recent files within {self.SCRAPER_MAX_AGE_HOURS}h — "
                        f"skipping scraper validation (credibility covers older data)"
                    )
                    scraper_result = {'entities_validated': 0, 'entities_passed': 0, 'success_rate': 100.0, 'sample_results': []}

            # Step 7: Validation decision
            issues = []
            duplicate_rate = duckdb_result["duplicate_rate_within_job"]
            empty_rate = duckdb_result["empty_rate"]
            job_match_rate = job_match_result['match_rate']
            scraper_success_rate = scraper_result['success_rate']
            compression_failures = duckdb_result.get("compression_failures", 0)
            row_count_mismatches = duckdb_result.get("row_count_mismatches", 0)

            # Check for schema validation failure
            if duckdb_result.get("reason"):
                issues.append(duckdb_result["reason"])

            if duplicate_rate > self.MAX_DUPLICATE_RATE:
                issues.append(f"High duplicates: {duplicate_rate:.1f}%")
            if empty_rate > self.MAX_EMPTY_RATE:
                issues.append(f"High empty content: {empty_rate:.1f}%")
            if job_match_rate < self.MIN_JOB_MATCH_RATE:
                issues.append(f"Low job match: {job_match_rate:.1f}%")
            if scraper_success_rate < self.MIN_SCRAPER_SUCCESS:
                issues.append(f"Low scraper success: {scraper_success_rate:.1f}%")

            # Compression exploit detection — always fail
            if compression_failures > 0:
                issues.append(f"Uncompressed files: {compression_failures}")

            # Row count mismatch — filename claims different count than parquet metadata
            if row_count_mismatches > 0:
                issues.append(f"Row count mismatch: {row_count_mismatches} files with filename!=metadata rows")

            is_valid = len(issues) == 0

            # Calculate effective_size for competition scoring
            # Row-based: total rows from filenames × standard bytes per row × coverage²
            # Filenames are validated against parquet metadata on sampled files (mismatch = fail)
            coverage_ratio = job_coverage_rate / 100.0
            if is_valid:
                if total_rows_from_filenames > 0:
                    row_based_size = total_rows_from_filenames * self.STANDARD_BYTES_PER_ROW
                    effective_size_bytes = row_based_size * (coverage_ratio ** 2)
                    bt.logging.info(
                        f"{miner_hotkey}: effective_size: "
                        f"{total_rows_from_filenames} rows × {self.STANDARD_BYTES_PER_ROW} B/row "
                        f"= {row_based_size/(1024*1024):.1f}MB × coverage²({coverage_ratio:.2f}) "
                        f"= {effective_size_bytes/(1024*1024):.1f}MB"
                    )
                else:
                    bt.logging.warning(
                        f"{miner_hotkey}: No row counts from filenames — effective_size set to 0"
                    )
                    effective_size_bytes = 0.0
            else:
                effective_size_bytes = 0.0

            # Calculate validation percentage (for backward compatibility)
            if is_valid:
                base_score = 100.0 - (duplicate_rate * 2) - (empty_rate * 0.5)
                size_bonus = min(20, math.log10(max(1, total_size_bytes / (10 * 1024 * 1024))) * 10)
                validation_percentage = max(0, base_score + size_bonus) * (coverage_ratio)
            else:
                validation_percentage = 0.0

            elapsed = time.time() - start_time

            # Log detailed results for miners to debug
            if is_valid:
                bt.logging.success(
                    f"{miner_hotkey}: S3 validation PASSED in {elapsed:.1f}s - "
                    f"effective_size={effective_size_bytes/(1024*1024):.1f}MB, "
                    f"dup={duplicate_rate:.1f}%, job_match={job_match_rate:.1f}%, scraper={scraper_success_rate:.1f}%"
                )
            else:
                bt.logging.warning(
                    f"{miner_hotkey}: S3 validation FAILED in {elapsed:.1f}s - "
                    f"Issues: {', '.join(issues)}"
                )
                bt.logging.info(
                    f"{miner_hotkey}: S3 validation details - "
                    f"dup={duplicate_rate:.1f}% (max {self.MAX_DUPLICATE_RATE}%), "
                    f"job_match={job_match_rate:.1f}% (min {self.MIN_JOB_MATCH_RATE}%), "
                    f"scraper={scraper_success_rate:.1f}% (min {self.MIN_SCRAPER_SUCCESS}%), "
                    f"compression_fails={compression_failures}, "
                    f"row_count_mismatches={row_count_mismatches}"
                )

            return S3ValidationResult(
                is_valid=is_valid,
                validation_percentage=validation_percentage,
                total_active_jobs=len(active_job_ids),
                expected_jobs_count=len(expected_jobs),
                recent_jobs_analyzed=len(active_job_ids),
                recent_files_count=len(sampled_files),
                total_size_bytes=total_size_bytes,
                has_duplicates=(duplicate_rate > self.MAX_DUPLICATE_RATE),
                duplicate_percentage=duplicate_rate,
                entities_validated=scraper_result['entities_validated'],
                entities_passed_scraper=scraper_result['entities_passed'],
                scraper_success_rate=scraper_success_rate,
                entities_checked_for_job_match=job_match_result['total_checked'],
                entities_matched_job=job_match_result['total_matched'],
                job_match_rate=job_match_rate,
                validation_issues=issues,
                reason=f"DuckDB validation: {'PASSED' if is_valid else 'FAILED'} - {', '.join(issues) if issues else 'All checks passed'}",

                sample_validation_results=scraper_result.get('sample_results', []),
                sample_job_mismatches=job_match_result.get('mismatch_samples', []),
                effective_size_bytes=effective_size_bytes,
                job_coverage_rate=job_coverage_rate
            )

        except Exception as e:
            bt.logging.error(f"{miner_hotkey}: DuckDB validation error: {str(e)}")
            return self._create_failed_result(f"Validation error: {str(e)}")

    async def _get_presigned_urls_batch(
        self,
        miner_hotkey: str,
        file_keys: List[str],
        batch_size: int = 500
    ) -> Dict[str, str]:
        """Get presigned download URLs in batches using Tao v2 auth."""

        all_urls = {}

        for i in range(0, len(file_keys), batch_size):
            batch = file_keys[i:i + batch_size]

            try:
                payload = {
                    "miner_hotkey": miner_hotkey,
                    "file_keys": batch,
                    "expiry_hours": 1
                }

                body = json.dumps(payload).encode()
                headers = self._signer.headers(body)
                headers["Content-Type"] = "application/json"

                response = requests.post(
                    f"{self.s3_auth_url}/get-file-presigned-urls",
                    data=body,
                    headers=headers,
                    timeout=120
                )

                if response.status_code == 200:
                    file_urls = response.json().get('file_urls', {})
                    for key, data in file_urls.items():
                        if isinstance(data, dict) and 'presigned_url' in data:
                            all_urls[key] = data['presigned_url']
                        elif isinstance(data, str):
                            all_urls[key] = data
                else:
                    bt.logging.warning(
                        f"get-file-presigned-urls failed: {response.status_code}"
                    )

            except Exception as e:
                bt.logging.warning(f"Presigned URL batch error: {e}")

        return all_urls

    # Max allowed rows per row group (must match uploader's row_group_size)
    MAX_ROW_GROUP_SIZE = 10_000

    def _check_file_metadata(self, presigned_url: str, conn) -> Optional[Dict]:
        """Read parquet metadata footer only (~1-10KB network read).
        Returns {total_rows, codecs, has_uncompressed, max_rg_rows} or None on error."""
        try:
            result = conn.execute(f"""
                SELECT SUM(row_group_num_rows), LIST(DISTINCT compression), MAX(row_group_num_rows)
                FROM parquet_metadata('{presigned_url}')
                WHERE column_id = 0
            """).fetchone()
            if result is None or result[0] is None:
                return None
            total_rows = int(result[0])
            codecs = set(result[1]) if result[1] else set()
            max_rg_rows = int(result[2])
            return {
                'total_rows': total_rows,
                'codecs': codecs,
                'has_uncompressed': 'UNCOMPRESSED' in codecs,
                'max_rg_rows': max_rg_rows
            }
        except Exception as e:
            bt.logging.debug(f"parquet_metadata error: {e}")
            return None

    def _parse_row_count_from_filename(self, key: str) -> Optional[int]:
        """Extract row count from new filename format: data_YYYYMMDD_HHMMSS_{count}_{hex16}.parquet"""
        filename = key.rsplit('/', 1)[-1]
        match = self._FILENAME_ROW_COUNT_RE.match(filename)
        return int(match.group(1)) if match else None

    async def _sampled_duckdb_validation(
        self,
        sampled_files: List[Dict],
        expected_jobs: Dict[str, Dict],
        presigned_urls: Dict[str, str],
        samples_per_file: int = 100
    ) -> Dict[str, Any]:
        """
        Lightweight validation using DuckDB metadata + DuckDB url column read + PyArrow row-group reads.

        Checks: schema, compression, row group size, empty content, missing URLs,
        per-job duplicates (blake2b hash of ALL urls via DuckDB columnar read).
        """
        total_rows = 0
        empty_count = 0
        schema_failures = 0
        files_checked = 0

        # Metadata-based checks
        compression_failures = 0
        row_count_mismatches = 0

        # Per-job dedup: separate hash set per job_id
        # DuckDB reads only the url column (all row groups, columnar) — ~500KB per file
        dedup_hashes_by_job: Dict[str, set] = {}
        dedup_total = 0
        dedup_duplicates = 0

        # Limit to 20 files max for checks
        files_to_check = random.sample(sampled_files, min(20, len(sampled_files)))

        for file_info in files_to_check:
            if schema_failures > 0:
                break

            file_size = file_info.get('size', 0)
            if file_size > self.MAX_FILE_SIZE_BYTES:
                continue

            presigned_url = presigned_urls.get(file_info['key'])
            if not presigned_url:
                continue

            file_key = file_info['key']
            if '/job_id=' in file_key:
                job_id = file_key.split('/job_id=')[1].split('/')[0]
                job_config = expected_jobs.get(job_id, {})
                params = job_config.get('params', {}) if isinstance(job_config, dict) else {}
                platform = params.get('platform', '').lower()
            else:
                job_id = 'unknown'
                platform = 'unknown'

            conn = None
            try:
                conn = duckdb.connect(':memory:')
                conn.execute("SET memory_limit='2GB';")
                conn.execute("SET threads=1;")

                # --- Metadata check (footer-only read, ~1-10KB) ---
                metadata = self._check_file_metadata(presigned_url, conn)
                if metadata:
                    file_rows = metadata['total_rows']

                    if metadata['has_uncompressed']:
                        compression_failures += 1
                        bt.logging.warning(
                            f"UNCOMPRESSED file detected: {file_key} "
                            f"({file_rows} rows, codecs={metadata['codecs']})"
                        )

                    if metadata['max_rg_rows'] > self.MAX_ROW_GROUP_SIZE:
                        schema_failures += 1
                        bt.logging.warning(
                            f"Row group too large: {metadata['max_rg_rows']} rows "
                            f"(max {self.MAX_ROW_GROUP_SIZE}) in {file_key}"
                        )
                        break

                    filename_rows = self._parse_row_count_from_filename(file_key)
                    if filename_rows is not None and file_rows != filename_rows:
                        row_count_mismatches += 1
                        bt.logging.warning(
                            f"Row count mismatch: filename claims {filename_rows}, "
                            f"metadata has {file_rows} in {file_key}"
                        )

                # --- Schema check (footer-only read) ---
                schema_result = conn.execute(
                    f"SELECT name FROM parquet_schema('{presigned_url}')"
                ).fetchall()
                excluded_names = {'schema', 'list', 'element', 'model_config'}
                all_column_names = [row[0].lower() for row in schema_result if row[0].lower() not in excluded_names]
                available_columns = set(all_column_names)

                optional_lower = {col.lower() for col in self.OPTIONAL_COLUMNS}
                if platform in ['x', 'twitter']:
                    expected_columns = {col.lower() for col in self.EXPECTED_COLUMNS_X}
                    max_columns = len(self.EXPECTED_COLUMNS_X) + len(self.OPTIONAL_COLUMNS)
                else:
                    expected_columns = {col.lower() for col in self.EXPECTED_COLUMNS_REDDIT}
                    max_columns = len(self.EXPECTED_COLUMNS_REDDIT) + len(self.OPTIONAL_COLUMNS)

                allowed_columns = expected_columns | optional_lower
                files_checked += 1

                if len(all_column_names) > max_columns:
                    bt.logging.warning(
                        f"Invalid schema: file has {len(all_column_names)} columns, expected max {max_columns}. "
                        f"Sample columns: {all_column_names[:10]}..."
                    )
                    schema_failures += 1
                    break

                unexpected_columns = available_columns - allowed_columns
                if unexpected_columns:
                    bt.logging.warning(
                        f"Invalid schema: unexpected columns {list(unexpected_columns)[:5]}"
                    )
                    schema_failures += 1
                    break

                # --- Per-job dedup: read ALL urls via DuckDB (columnar read, ~500KB per file) ---
                if job_id not in dedup_hashes_by_job:
                    dedup_hashes_by_job[job_id] = set()
                job_hashes = dedup_hashes_by_job[job_id]

                url_rows = conn.execute(
                    f"SELECT url FROM read_parquet('{presigned_url}')"
                ).fetchall()
                for (url_val,) in url_rows:
                    h = hashlib.blake2b(str(url_val).encode(), digest_size=8).digest()
                    dedup_total += 1
                    if h in job_hashes:
                        dedup_duplicates += 1
                    else:
                        job_hashes.add(h)

                # --- PyArrow row-group read for empty/missing content check ---
                if platform in ['x', 'twitter']:
                    read_cols = ['url', 'text']
                else:
                    read_cols = ['url', 'body', 'title']
                read_cols = [c for c in read_cols if c in available_columns]
                if 'url' not in read_cols:
                    read_cols.append('url')

                rg_df = read_random_row_group(
                    presigned_url, file_size, columns=read_cols
                )
                if rg_df is not None and len(rg_df) > 0:
                    rg_df.columns = [c.lower() for c in rg_df.columns]
                    total_rows += len(rg_df)

                    # Missing URL check — instant fail
                    url_missing = rg_df['url'].isna() | (rg_df['url'].astype(str).str.strip() == '')
                    missing_urls = int(url_missing.sum())
                    if missing_urls > 0:
                        bt.logging.warning(f"File has {missing_urls} rows with missing URL: {file_key}")
                        return {
                            "success": False,
                            "duplicate_rate_within_job": 100.0,
                            "empty_rate": 100.0,
                            "total_rows": 0,
                            "reason": f"File contains {missing_urls} rows with missing URL"
                        }

                    # Empty content check
                    if platform in ['x', 'twitter']:
                        empty_mask = rg_df.get('text', pd.Series(dtype=str)).fillna('').str.strip() == ''
                    else:
                        body_empty = rg_df.get('body', pd.Series(dtype=str)).fillna('').str.strip() == ''
                        title_empty = rg_df.get('title', pd.Series(dtype=str)).fillna('').str.strip() == ''
                        empty_mask = body_empty & title_empty
                    empty_count += int(empty_mask.sum())

            except Exception as e:
                bt.logging.debug(f"Sampled validation error for file: {e}")
                continue
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass

        # Zero tolerance for schema failures
        if schema_failures > 0:
            bt.logging.warning(
                f"Schema validation FAILED: {schema_failures}/{files_checked} files have invalid schema"
            )
            return {
                "success": False,
                "duplicate_rate_within_job": 100.0,
                "empty_rate": 100.0,
                "total_rows": 0,
                "reason": f"Invalid schema in {schema_failures} files"
            }

        # Per-job duplicate rate
        duplicate_rate = (dedup_duplicates / dedup_total * 100) if dedup_total > 0 else 0.0
        if duplicate_rate > self.MAX_DUPLICATE_RATE:
            bt.logging.warning(
                f"Per-job dedup: {duplicate_rate:.1f}% duplicates "
                f"({dedup_duplicates}/{dedup_total} urls across {len(dedup_hashes_by_job)} jobs)"
            )

        if total_rows == 0:
            return {
                "success": True,
                "duplicate_rate_within_job": duplicate_rate,
                "empty_rate": 0.0,
                "total_rows": 0,
                "compression_failures": compression_failures,
                "row_count_mismatches": row_count_mismatches,
            }

        empty_rate = (empty_count / total_rows * 100) if total_rows > 0 else 0

        bt.logging.info(
            f"Sampled validation: {total_rows} rows, "
            f"dup={duplicate_rate:.1f}% (per-job, {len(dedup_hashes_by_job)} jobs), "
            f"empty={empty_rate:.1f}%, compression_fails={compression_failures}"
        )

        return {
            "success": True,
            "duplicate_rate_within_job": duplicate_rate,
            "empty_rate": empty_rate,
            "total_rows": total_rows,
            "compression_failures": compression_failures,
            "row_count_mismatches": row_count_mismatches,
        }

    async def _perform_job_content_matching(
        self,
        sampled_files: List[Dict],
        expected_jobs: Dict[str, Dict],
        presigned_urls: Dict[str, str],
        samples_per_file: int = 10
    ) -> Dict[str, Any]:
        """Check if data matches job requirements (label/keyword/time)."""
        total_checked = 0
        total_matched = 0
        mismatch_samples = []

        files_by_job = {}
        for file_info in sampled_files:
            file_key = file_info['key']
            if '/job_id=' in file_key:
                job_id = file_key.split('/job_id=')[1].split('/')[0]
                if job_id not in files_by_job:
                    files_by_job[job_id] = []
                files_by_job[job_id].append(file_info)

        for job_id, files in files_by_job.items():
            job_config = expected_jobs.get(job_id)
            if not job_config:
                continue

            # Access params nested structure (Gravity schema)
            params = job_config.get('params', {}) if isinstance(job_config, dict) else {}
            platform = params.get('platform', '').lower()
            job_label = params.get('label')
            job_keyword = params.get('keyword')
            job_start_date = params.get('post_start_datetime')
            job_end_date = params.get('post_end_datetime')

            job_start_dt = self._parse_datetime(job_start_date)
            job_end_dt = self._parse_datetime(job_end_date)

            has_label = bool(job_label and str(job_label).strip())
            has_keyword = bool(job_keyword and str(job_keyword).strip())
            has_time = bool(job_start_dt or job_end_dt)

            sample_files = random.sample(files, min(2, len(files)))

            for file_info in sample_files:
                # Skip oversized files to prevent OOM
                file_size = file_info.get('size', 0)
                if file_size > self.MAX_FILE_SIZE_BYTES:
                    continue

                presigned_url = presigned_urls.get(file_info['key'])
                if not presigned_url:
                    continue

                conn = None
                try:
                    # Schema validation — only reads parquet footer
                    conn = duckdb.connect(':memory:')
                    schema_result = conn.execute(
                        f"SELECT name FROM parquet_schema('{presigned_url}')"
                    ).fetchall()
                    excluded_names = {'schema', 'list', 'element', 'model_config'}
                    all_column_names = [r[0].lower() for r in schema_result if r[0].lower() not in excluded_names]

                    if platform in ['x', 'twitter']:
                        max_columns = len(self.EXPECTED_COLUMNS_X) + len(self.OPTIONAL_COLUMNS)
                    else:
                        max_columns = len(self.EXPECTED_COLUMNS_REDDIT) + len(self.OPTIONAL_COLUMNS)

                    if len(all_column_names) > max_columns:
                        bt.logging.debug(f"Skipping file with {len(all_column_names)} columns (expected max {max_columns})")
                        continue

                    # Read 1 random row group via Range requests (~3MB vs full scan)
                    sample_df = read_random_row_group(
                        presigned_url, file_size,
                        columns=None, max_rows=samples_per_file
                    )

                    if sample_df is None or len(sample_df) == 0:
                        continue

                    for _, row in sample_df.iterrows():
                        total_checked += 1
                        matches = self._check_row_matches_job(
                            row, platform, job_label, job_keyword,
                            job_start_dt, job_end_dt, has_label, has_keyword, has_time
                        )
                        if matches:
                            total_matched += 1
                        elif len(mismatch_samples) < 5:
                            uri = row.get('url', 'unknown')
                            mismatch_samples.append(f"Job {job_id[:8]}: {uri}")

                    del sample_df

                except Exception as e:
                    continue
                finally:
                    if conn:
                        try:
                            conn.close()
                        except:
                            pass

        return {
            'total_checked': total_checked,
            'total_matched': total_matched,
            'match_rate': (total_matched / total_checked * 100) if total_checked > 0 else 0.0,
            'mismatch_samples': mismatch_samples
        }

    def _check_row_matches_job(
        self, row, platform, job_label, job_keyword,
        job_start_dt, job_end_dt, has_label, has_keyword, has_time
    ) -> bool:
        """Check if a single row matches job requirements."""
        label_matches = True
        keyword_matches = True
        time_matches = True

        if has_label:
            job_label_norm = str(job_label).lower().strip()
            if platform in ['x', 'twitter']:
                raw_hashtags = row.get('tweet_hashtags', [])
                hashtags_list = []
                if raw_hashtags is not None:
                    if hasattr(raw_hashtags, '__iter__') and not isinstance(raw_hashtags, str):
                        try:
                            hashtags_list = list(raw_hashtags)
                        except:
                            pass
                    elif not pd.isna(raw_hashtags):
                        hashtags_list = [raw_hashtags]
                hashtags_lower = [str(h).lower().strip() for h in hashtags_list if h]
                label_without_hash = job_label_norm.lstrip('#')
                label_matches = (
                    f"#{label_without_hash}" in hashtags_lower or
                    label_without_hash in hashtags_lower
                )
            elif platform == 'reddit':
                entity_label = str(row.get('communityName', '')).lower().strip()
                label_matches = (
                    entity_label == job_label_norm or
                    entity_label.removeprefix('r/') == job_label_norm.removeprefix('r/')
                )

        if has_keyword:
            job_kw_norm = str(job_keyword).lower().strip()
            if platform == 'reddit':
                body = str(row.get('body', '')).lower()
                title = str(row.get('title', '')).lower()
                keyword_matches = (job_kw_norm in body or job_kw_norm in title)
            else:
                text = str(row.get('text', '')).lower()
                keyword_matches = job_kw_norm in text

        if has_time:
            entity_dt = row.get('datetime')
            if entity_dt is not None and not pd.isna(entity_dt):
                try:
                    entity_dt = pd.to_datetime(entity_dt, utc=True)
                    if job_start_dt and entity_dt < job_start_dt:
                        time_matches = False
                    if job_end_dt and entity_dt > job_end_dt:
                        time_matches = False
                except:
                    time_matches = False
            else:
                time_matches = False

        return (
            (not has_label or label_matches) and
            (not has_keyword or keyword_matches) and
            (not has_time or time_matches)
        )

    async def _perform_scraper_validation(
        self,
        miner_hotkey: str,
        sampled_files: List[Dict],
        expected_jobs: Dict[str, Dict],
        presigned_urls: Dict[str, str],
        num_entities: int = 10
    ) -> Dict[str, Any]:
        """Validate random entities using real scrapers."""
        all_entities = []

        for file_info in sampled_files:
            if len(all_entities) >= num_entities * 2:
                break

            # Skip oversized files to prevent OOM
            file_size = file_info.get('size', 0)
            if file_size > self.MAX_FILE_SIZE_BYTES:
                continue

            file_key = file_info['key']
            presigned_url = presigned_urls.get(file_key)
            if not presigned_url:
                continue

            if '/job_id=' in file_key:
                job_id = file_key.split('/job_id=')[1].split('/')[0]
                job_config = expected_jobs.get(job_id, {})
                # Access params nested structure (Gravity schema)
                params = job_config.get('params', {}) if isinstance(job_config, dict) else {}
                platform = params.get('platform', 'unknown').lower()
            else:
                continue

            conn = None
            try:
                # Schema validation — only reads parquet footer
                conn = duckdb.connect(':memory:')
                schema_result = conn.execute(
                    f"SELECT name FROM parquet_schema('{presigned_url}')"
                ).fetchall()
                excluded_names = {'schema', 'list', 'element', 'model_config'}
                all_column_names = [r[0].lower() for r in schema_result if r[0].lower() not in excluded_names]

                if platform in ['x', 'twitter']:
                    max_columns = len(self.EXPECTED_COLUMNS_X) + len(self.OPTIONAL_COLUMNS)
                else:
                    max_columns = len(self.EXPECTED_COLUMNS_REDDIT) + len(self.OPTIONAL_COLUMNS)

                if len(all_column_names) > max_columns:
                    bt.logging.debug(f"Skipping file with {len(all_column_names)} columns (expected max {max_columns})")
                    continue

                # Read 1 random row group via Range requests (~3MB vs full scan)
                df = read_random_row_group(
                    presigned_url, file_size,
                    columns=None, max_rows=5
                )

                if df is None or len(df) == 0:
                    continue

                for _, row in df.iterrows():
                    entity = self._create_data_entity(row, platform)
                    if entity:
                        all_entities.append((entity, platform, job_id))

                del df
            except:
                continue
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass

        if not all_entities:
            return {'entities_validated': 0, 'entities_passed': 0, 'success_rate': 0, 'sample_results': []}

        entities_to_validate = random.sample(all_entities, min(num_entities, len(all_entities)))

        total_validated = 0
        total_passed = 0
        sample_results = []

        entities_by_platform = {}
        for entity, platform, job_id in entities_to_validate:
            if platform not in entities_by_platform:
                entities_by_platform[platform] = []
            entities_by_platform[platform].append((entity, job_id))

        for platform, entities_with_jobs in entities_by_platform.items():
            entities = [e[0] for e in entities_with_jobs]
            job_ids = [e[1] for e in entities_with_jobs]
            try:
                results = await self._validate_with_scraper(entities, platform)
                for i, result in enumerate(results):
                    job_id = job_ids[i] if i < len(job_ids) else 'unknown'
                    total_validated += 1
                    if result.is_valid:
                        total_passed += 1
                        sample_results.append(f"✅ {platform} ({job_id[:16]}): {result.reason}")
                    else:
                        sample_results.append(f"❌ {platform} ({job_id[:16]}): {result.reason}")
            except Exception as e:
                total_validated += len(entities)
                sample_results.append(f"❌ {platform}: Scraper error - {e}")

        success_rate = (total_passed / total_validated * 100) if total_validated > 0 else 0

        # Log detailed results for miners to debug
        bt.logging.success(
            f"{miner_hotkey}: S3 scraper validation finished: {total_passed}/{total_validated} passed ({success_rate:.1f}%)"
        )
        bt.logging.info(
            f"{miner_hotkey}: S3 scraper validation details: {sample_results}"
        )

        return {
            'entities_validated': total_validated,
            'entities_passed': total_passed,
            'success_rate': success_rate,
            'sample_results': sample_results
        }

    def _create_data_entity(self, row, platform: str) -> Optional[DataEntity]:
        """Create DataEntity from parquet row."""
        try:
            if platform in ['x', 'twitter']:
                return self._create_twitter_entity(row)
            elif platform == 'reddit':
                return self._create_reddit_entity(row)
            return None
        except:
            return None

    def _create_twitter_entity(self, row) -> Optional[DataEntity]:
        """Create Twitter DataEntity with ALL fields for scraper validation."""
        try:
            username = row.get('username', '')
            text = row.get('text', '')
            url = row.get('url', '')
            if not all([username, text, url]):
                return None

            datetime_val = row.get('datetime', row.get('timestamp', ''))
            if datetime_val and pd.notna(datetime_val):
                try:
                    timestamp = pd.to_datetime(datetime_val)
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
                except Exception:
                    timestamp = dt.datetime.now(dt.timezone.utc)
            else:
                timestamp = dt.datetime.now(dt.timezone.utc)

            # Handle tweet_hashtags array field
            raw_hashtags = row.get('tweet_hashtags', None)
            if raw_hashtags is None:
                tweet_hashtags = []
            elif hasattr(raw_hashtags, '__iter__') and not isinstance(raw_hashtags, str):
                try:
                    tweet_hashtags = list(raw_hashtags)
                except TypeError:
                    tweet_hashtags = []
            else:
                try:
                    tweet_hashtags = [] if pd.isna(raw_hashtags) else [raw_hashtags]
                except Exception:
                    tweet_hashtags = [raw_hashtags]

            # Handle media array field (same as tweet_hashtags)
            raw_media = row.get('media', None)
            if raw_media is None:
                media_value = None
            elif hasattr(raw_media, '__iter__') and not isinstance(raw_media, str):
                try:
                    media_value = list(raw_media)
                except TypeError:
                    media_value = None
            else:
                try:
                    media_value = None if pd.isna(raw_media) else [raw_media]
                except Exception:
                    media_value = [raw_media]

            # Create XContent with ALL uploaded fields (required for scraper validation)
            x_content = XContent(
                username=str(username),
                text=str(text),
                url=str(url),
                timestamp=timestamp,
                tweet_hashtags=tweet_hashtags,
                media=media_value,
                user_id=str(row.get('user_id', '')),
                user_display_name=str(row.get('user_display_name', '')) if pd.notna(row.get('user_display_name', None)) else None,
                user_verified=bool(row.get('user_verified', False)),
                tweet_id=str(row.get('tweet_id', '')),
                is_reply=bool(row.get('is_reply', False)),
                is_quote=bool(row.get('is_quote', False)),
                conversation_id=str(row.get('conversation_id', '')),
                in_reply_to_user_id=str(row.get('in_reply_to_user_id', '')),
                language=str(row.get('language', '')) if pd.notna(row.get('language', None)) else None,
                in_reply_to_username=str(row.get('in_reply_to_username', '')) if pd.notna(row.get('in_reply_to_username', None)) else None,
                quoted_tweet_id=str(row.get('quoted_tweet_id', '')) if pd.notna(row.get('quoted_tweet_id', None)) else None,
                like_count=int(row.get('like_count', 0)) if pd.notna(row.get('like_count', None)) else None,
                retweet_count=int(row.get('retweet_count', 0)) if pd.notna(row.get('retweet_count', None)) else None,
                reply_count=int(row.get('reply_count', 0)) if pd.notna(row.get('reply_count', None)) else None,
                quote_count=int(row.get('quote_count', 0)) if pd.notna(row.get('quote_count', None)) else None,
                view_count=int(row.get('view_count', 0)) if pd.notna(row.get('view_count', None)) else None,
                bookmark_count=int(row.get('bookmark_count', 0)) if pd.notna(row.get('bookmark_count', None)) else None,
                user_blue_verified=bool(row.get('user_blue_verified', False)) if pd.notna(row.get('user_blue_verified', None)) else None,
                user_description=str(row.get('user_description', '')) if pd.notna(row.get('user_description', None)) else None,
                user_location=str(row.get('user_location', '')) if pd.notna(row.get('user_location', None)) else None,
                profile_image_url=str(row.get('profile_image_url', '')) if pd.notna(row.get('profile_image_url', None)) else None,
                cover_picture_url=str(row.get('cover_picture_url', '')) if pd.notna(row.get('cover_picture_url', None)) else None,
                user_followers_count=int(row.get('user_followers_count', 0)) if pd.notna(row.get('user_followers_count', None)) else None,
                user_following_count=int(row.get('user_following_count', 0)) if pd.notna(row.get('user_following_count', None)) else None,
                scraped_at=pd.to_datetime(row.get('scraped_at')).to_pydatetime() if row.get('scraped_at') is not None and pd.notna(row.get('scraped_at')) else None,
            )
            return XContent.to_data_entity(x_content)
        except Exception:
            return None

    def _create_reddit_entity(self, row) -> Optional[DataEntity]:
        """Create Reddit DataEntity. Accept if title OR body is present."""
        try:
            username = row.get('username', '')
            url = row.get('url', '')
            reddit_id = row.get('id', '')

            body = row.get('body', row.get('text', ''))
            title = row.get('title', '')

            username_str = str(username).strip() if username is not None else ""
            url_str = str(url).strip() if url is not None else ""
            reddit_id_str = str(reddit_id).strip() if reddit_id is not None else ""

            # Preserve original body/title (don't strip - scraper preserves exact content)
            body_str = str(body) if body is not None and not pd.isna(body) else ""
            title_str = str(title) if title is not None and not pd.isna(title) else ""

            # Accept if either title or body has content (media posts have empty body)
            if not body_str.strip() and not title_str.strip():
                return None

            # Require basic identity fields
            if not username_str or not url_str or not reddit_id_str:
                return None

            # Use body if available, otherwise use title
            content_str = body_str if body_str else title_str

            datetime_val = row.get('datetime', row.get('createdAt', ''))
            if datetime_val is not None and pd.notna(datetime_val):
                try:
                    created_at = pd.to_datetime(datetime_val)
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=dt.timezone.utc)
                except Exception:
                    created_at = dt.datetime.now(dt.timezone.utc)
            else:
                created_at = dt.datetime.now(dt.timezone.utc)

            reddit_content = RedditContent(
                id=reddit_id_str,
                username=username_str,
                body=content_str,
                url=url_str,
                communityName=str(row.get('communityName', '') or ''),
                createdAt=created_at,
                dataType=str(row.get('dataType', 'post') or 'post'),
                parentId=str(row.get('parentId')) if row.get('parentId') and pd.notna(row.get('parentId')) else None,
                title=str(row.get('title', '')) if pd.notna(row.get('title')) else None,
                media=row.get('media', None) if pd.notna(row.get('media')) else None,
                is_nsfw=bool(row.get('is_nsfw', False)) if pd.notna(row.get('is_nsfw')) else None,
                score=int(row.get('score', 0)) if pd.notna(row.get('score')) else None,
                upvote_ratio=float(row.get('upvote_ratio', 0.0)) if pd.notna(row.get('upvote_ratio')) else None,
                num_comments=int(row.get('num_comments', 0)) if pd.notna(row.get('num_comments')) else None,
                scrapedAt=pd.to_datetime(row.get('scrapedAt')).to_pydatetime() if row.get('scrapedAt') is not None and pd.notna(row.get('scrapedAt')) else None,
            )
            return RedditContent.to_data_entity(reddit_content)
        except Exception:
            return None

    async def _validate_with_scraper(
        self, entities: List[DataEntity], platform: str
    ) -> List[ValidationResult]:
        """Validate entities using appropriate scraper."""
        try:
            if platform in ['x', 'twitter']:
                data_source = DataSource.X
            elif platform == 'reddit':
                data_source = DataSource.REDDIT
            else:
                return [ValidationResult(is_valid=False, reason=f"Unknown platform: {platform}", content_size_bytes_validated=0) for _ in entities]

            scraper = self.scraper_provider.get(PREFERRED_SCRAPERS[data_source])
            return await scraper.validate(entities)
        except Exception as e:
            return [ValidationResult(is_valid=False, reason=f"Scraper error: {e}", content_size_bytes_validated=0) for _ in entities]

    def _parse_datetime(self, dt_val) -> Optional[dt.datetime]:
        """Parse datetime from job config."""
        if dt_val is None:
            return None
        try:
            if isinstance(dt_val, dt.datetime):
                if dt_val.tzinfo is None:
                    return dt_val.replace(tzinfo=dt.timezone.utc)
                return dt_val
            return pd.to_datetime(dt_val, utc=True).to_pydatetime()
        except:
            return None

    def _create_failed_result(self, reason: str) -> S3ValidationResult:
        """Create a failed validation result."""
        return S3ValidationResult(
            is_valid=False,
            validation_percentage=0.0,
            total_active_jobs=0,
            expected_jobs_count=0,
            recent_jobs_analyzed=0,
            recent_files_count=0,
            total_size_bytes=0,
            has_duplicates=True,
            duplicate_percentage=100.0,
            entities_validated=0,
            entities_passed_scraper=0,
            scraper_success_rate=0.0,
            entities_checked_for_job_match=0,
            entities_matched_job=0,
            job_match_rate=0.0,
            validation_issues=[reason],
            reason=reason,
            sample_validation_results=[],
            sample_job_mismatches=[],
            effective_size_bytes=0.0,
            job_coverage_rate=0.0
        )

    def close(self):
        """Cleanup resources."""
        pass


def load_expected_jobs_from_gravity() -> Dict:
    """Load expected jobs from dynamic_desirability/total.json"""
    try:
        current_dir = os.getcwd()
        for _ in range(3):
            total_json_path = os.path.join(current_dir, "dynamic_desirability", "total.json")
            if os.path.exists(total_json_path):
                with open(total_json_path, 'r') as f:
                    jobs_list = json.load(f)
                    
                jobs_dict = {}
                for job in jobs_list:
                    if isinstance(job, dict) and 'id' in job:
                        jobs_dict[job['id']] = job
                
                return jobs_dict
            current_dir = os.path.dirname(current_dir)
        
        bt.logging.warning("dynamic_desirability/total.json not found")
        return {}
    except Exception as e:
        bt.logging.error(f"Error loading expected jobs: {e}")
        return {}


async def validate_s3_miner_data(
    wallet, s3_auth_url: str, miner_hotkey: str,
    config=None, s3_reader=None,
    sample_percent: float = 10.0
) -> S3ValidationResult:
    """
    S3 validation using DuckDB-based sampled validation with competition scoring.

    Args:
        wallet: Validator wallet for S3 authentication
        s3_auth_url: S3 authentication service URL
        miner_hotkey: Target miner's hotkey
        config: Configuration object with validation settings
        s3_reader: Optional ValidatorS3Access instance for efficient file listing
        sample_percent: Percentage of files to sample for DuckDB validation (default 10%)

    Returns:
        S3ValidationResult with validation metrics including effective_size_bytes for competition
    """

    # Load expected jobs
    expected_jobs = load_expected_jobs_from_gravity()

    validator = None
    try:
        validator = DuckDBSampledValidator(
            wallet=wallet,
            s3_auth_url=s3_auth_url,
            s3_reader=s3_reader,
            sample_percent=sample_percent
        )
        result = await validator.validate_miner_s3_data(
            miner_hotkey, expected_jobs
        )
        return result

    except Exception as e:
        bt.logging.error(f"DuckDB validation failed for {miner_hotkey}: {str(e)}")
        return S3ValidationResult(
            is_valid=False,
            validation_percentage=0.0,
            total_active_jobs=0,
            expected_jobs_count=0,
            recent_jobs_analyzed=0,
            recent_files_count=0,
            total_size_bytes=0,
            has_duplicates=False,
            duplicate_percentage=0.0,
            entities_validated=0,
            entities_passed_scraper=0,
            scraper_success_rate=0.0,
            entities_checked_for_job_match=0,
            entities_matched_job=0,
            job_match_rate=0.0,
            validation_issues=[f"Validation error: {str(e)}"],
            reason=f"Validation failed: {str(e)}",
            sample_validation_results=[],
            sample_job_mismatches=[]
        )

    finally:
        if validator:
            validator.close()


def get_s3_validation_summary(result: S3ValidationResult) -> str:
    """Generate a summary string for S3 validation result with detailed breakdown"""

    size_mb = result.total_size_bytes / (1024 * 1024)

    if result.is_valid:
        return (
            f"✅ S3 PASSED ({result.validation_percentage:.1f}%): "
            f"{result.total_active_jobs} jobs, {result.recent_files_count} files ({size_mb:.1f}MB) | "
            f"Dup: {result.duplicate_percentage:.1f}%, JobMatch: {result.job_match_rate:.1f}%, Scraper: {result.scraper_success_rate:.1f}%"
        )
    else:
        return (
            f"❌ S3 FAILED ({result.validation_percentage:.1f}%): "
            f"{result.total_active_jobs} jobs, {result.recent_files_count} files ({size_mb:.1f}MB) | "
            f"Dup: {result.duplicate_percentage:.1f}%, JobMatch: {result.job_match_rate:.1f}%, Scraper: {result.scraper_success_rate:.1f}% | "
            f"Issues: {', '.join(result.validation_issues[:3])}"
        )
