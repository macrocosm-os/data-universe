"""
S3 validation utilities for enhanced miner data validation.
Provides comprehensive validation of S3-stored miner data using metadata analysis.
"""

import random
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
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from scraping.provider import ScraperProvider
from scraping.scraper import ScraperId, ValidationResult
from scraping.x.model import XContent
from scraping.reddit.model import RedditContent
from common.data import DataEntity, DataSource
import re


@dataclass
class S3ValidationResult:
    """S3 validation result structure"""
    is_valid: bool
    validation_percentage: float
    job_count: int
    total_files: int
    total_size_bytes: int
    valid_jobs: int
    recent_files: int
    quality_metrics: Dict[str, float]
    issues: List[str]
    reason: str

    # Enhanced validation fields (optional, for backward compatibility)
    enhanced_validation: Optional['S3ValidationResultDetailed'] = None

    # Empty file detection
    empty_file_detected: bool = False

    # Competition-based scoring: effective_size = total_size_bytes × coverage²
    # This is used for proportional S3 boost distribution among miners
    effective_size_bytes: float = 0.0
    job_coverage_rate: float = 0.0  # Percentage of active jobs covered (0-100)


@dataclass
class S3ValidationResultDetailed:
    """Detailed S3 validation result with comprehensive metrics"""
    is_valid: bool
    validation_percentage: float

    # Job and file metrics
    total_active_jobs: int
    expected_jobs_count: int  # Total number of expected jobs from Gravity
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
    sample_duplicate_uris: List[str]
    sample_validation_results: List[str]
    sample_job_mismatches: List[str]

    # Empty file detection
    empty_file_detected: bool = False

    # Competition-based scoring
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
    MAX_MISSING_URI_RATE = 5.0  # 5% max missing URIs
    MIN_JOB_MATCH_RATE = 95.0   # 95% min job content match rate
    MIN_SCRAPER_SUCCESS = 70.0  # 70% min scraper success rate

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
        self.conn = duckdb.connect(':memory:')
        self._setup_duckdb()

    def _setup_duckdb(self):
        """Configure DuckDB for HTTP parquet reading"""
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        self.conn.execute("SET http_timeout=120000;")
        self.conn.execute("SET enable_progress_bar=false;")

    async def validate_miner_s3_data(
        self,
        miner_hotkey: str,
        expected_jobs: Dict
    ) -> S3ValidationResultDetailed:
        """
        Validate miner using DuckDB with random sampling.

        Returns S3ValidationResultDetailed with effective_size for competition scoring.
        """
        import time
        import requests
        start_time = time.time()

        try:
            # Step 1: List ALL files (for size calculation - no reading needed)
            bt.logging.info(f"{miner_hotkey}: DuckDB validation - listing files...")

            if not self.s3_reader:
                return self._create_failed_result("S3 reader not available")

            all_files = await self.s3_reader.list_all_files_with_metadata(miner_hotkey)

            if not all_files:
                return self._create_failed_result("No files found")

            # Group files by job
            files_by_job = {}
            for f in all_files:
                key = f.get('key', '')
                if '/job_id=' in key:
                    job_id = key.split('/job_id=')[1].split('/')[0]
                    if job_id not in files_by_job:
                        files_by_job[job_id] = []
                    files_by_job[job_id].append(f)

            # Filter to active jobs only
            active_job_ids = [jid for jid in files_by_job.keys() if jid in expected_jobs]

            if not active_job_ids:
                return self._create_failed_result("No active jobs found")

            # Calculate size from active jobs only
            total_size_bytes = 0
            for job_id in active_job_ids:
                for f in files_by_job[job_id]:
                    total_size_bytes += f.get('size', 0)

            job_coverage_rate = (len(active_job_ids) / len(expected_jobs) * 100) if expected_jobs else 0

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
            sample_count = min(sample_count, len(active_files))

            sampled_files_with_job = random.sample(active_files, sample_count)
            sampled_files = [f for f, _ in sampled_files_with_job]

            bt.logging.info(f"{miner_hotkey}: Sampled {len(sampled_files)} files ({self.sample_percent}%)")

            # Step 3: Get presigned URLs for sample
            file_keys = [f['key'] for f in sampled_files]
            presigned_urls = await self._get_presigned_urls_batch(miner_hotkey, file_keys)

            if not presigned_urls:
                return self._create_failed_result("Failed to get presigned URLs")

            # Group by job for DuckDB validation
            urls_by_job = {}
            for file_info, job_id in sampled_files_with_job:
                file_key = file_info['key']
                url = presigned_urls.get(file_key)
                if not url:
                    continue

                job_config = expected_jobs.get(job_id, {})
                platform = job_config.get('platform', 'unknown').lower()

                if job_id not in urls_by_job:
                    urls_by_job[job_id] = {'platform': platform, 'urls': []}
                urls_by_job[job_id]['urls'].append(url)

            # Step 4: DuckDB validation (duplicates within job, empty content)
            bt.logging.info(f"{miner_hotkey}: Running DuckDB validation...")
            duckdb_result = self._validate_sample_with_duckdb(urls_by_job)

            if not duckdb_result.get("success"):
                return self._create_failed_result(f"DuckDB error: {duckdb_result.get('error')}")

            # Step 5: Job content matching
            bt.logging.info(f"{miner_hotkey}: Checking job content matching...")
            job_match_result = await self._perform_job_content_matching(
                sampled_files, expected_jobs, presigned_urls
            )

            # Step 6: Scraper validation
            bt.logging.info(f"{miner_hotkey}: Running scraper validation...")
            scraper_result = await self._perform_scraper_validation(
                sampled_files, expected_jobs, presigned_urls, num_entities=10
            )

            # Step 7: Validation decision
            issues = []
            duplicate_rate = duckdb_result["duplicate_rate_within_job"]
            empty_rate = duckdb_result["empty_rate"]
            missing_uri_rate = duckdb_result["missing_url_rate"]
            job_match_rate = job_match_result['match_rate']
            scraper_success_rate = scraper_result['success_rate']

            if duplicate_rate > self.MAX_DUPLICATE_RATE:
                issues.append(f"High duplicates: {duplicate_rate:.1f}%")
            if empty_rate > self.MAX_EMPTY_RATE:
                issues.append(f"High empty content: {empty_rate:.1f}%")
            if missing_uri_rate > self.MAX_MISSING_URI_RATE:
                issues.append(f"High missing URIs: {missing_uri_rate:.1f}%")
            if job_match_rate < self.MIN_JOB_MATCH_RATE:
                issues.append(f"Low job match: {job_match_rate:.1f}%")
            if scraper_success_rate < self.MIN_SCRAPER_SUCCESS:
                issues.append(f"Low scraper success: {scraper_success_rate:.1f}%")

            is_valid = len(issues) == 0

            # Calculate effective_size for competition scoring
            coverage_ratio = job_coverage_rate / 100.0
            if is_valid:
                effective_size_bytes = total_size_bytes * (coverage_ratio ** 2)
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

            bt.logging.info(
                f"{miner_hotkey}: DuckDB validation complete in {elapsed:.1f}s - "
                f"{'PASSED' if is_valid else 'FAILED'}, "
                f"effective_size={effective_size_bytes/(1024*1024):.1f}MB"
            )

            return S3ValidationResultDetailed(
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
                sample_duplicate_uris=[],
                sample_validation_results=scraper_result.get('sample_results', [])[:5],
                sample_job_mismatches=job_match_result.get('mismatch_samples', [])[:5],
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
        """Get presigned URLs in batches"""
        import requests
        all_urls = {}

        for i in range(0, len(file_keys), batch_size):
            batch = file_keys[i:i + batch_size]

            try:
                hotkey = self.wallet.hotkey.ss58_address
                timestamp = int(time.time())
                commitment = f"s3:validator:files:{miner_hotkey}:{hotkey}:{timestamp}"
                signature = self.wallet.hotkey.sign(commitment.encode())

                payload = {
                    "hotkey": hotkey,
                    "timestamp": timestamp,
                    "signature": signature.hex(),
                    "miner_hotkey": miner_hotkey,
                    "file_keys": batch,
                    "expiry_hours": 1
                }

                response = requests.post(
                    f"{self.s3_auth_url}/get-file-presigned-urls",
                    json=payload,
                    timeout=120
                )

                if response.status_code == 200:
                    file_urls = response.json().get('file_urls', {})
                    for key, data in file_urls.items():
                        if isinstance(data, dict) and 'presigned_url' in data:
                            all_urls[key] = data['presigned_url']
                        elif isinstance(data, str):
                            all_urls[key] = data

            except Exception as e:
                bt.logging.warning(f"Presigned URL batch error: {e}")

        return all_urls

    def _validate_sample_with_duckdb(
        self,
        urls_by_job: Dict[str, Dict[str, List[str]]]
    ) -> Dict[str, Any]:
        """
        Run DuckDB validation on sampled files.
        Batch by platform for efficiency.
        """
        total_rows = 0
        total_duplicates = 0
        total_with_url = 0
        empty_content = 0
        missing_url = 0

        try:
            # Group by platform
            x_jobs = {}
            reddit_jobs = {}
            for job_id, job_data in urls_by_job.items():
                platform = job_data['platform']
                urls = job_data['urls']
                if not urls:
                    continue
                if platform in ['x', 'twitter']:
                    x_jobs[job_id] = urls
                elif platform == 'reddit':
                    reddit_jobs[job_id] = urls

            # Process X/Twitter
            if x_jobs:
                x_result = self._batch_validate_platform(x_jobs, 'x')
                total_rows += x_result['total_rows']
                total_duplicates += x_result['duplicates']
                total_with_url += x_result['with_url']
                empty_content += x_result['empty_content']
                missing_url += x_result['missing_url']

            # Process Reddit
            if reddit_jobs:
                reddit_result = self._batch_validate_platform(reddit_jobs, 'reddit')
                total_rows += reddit_result['total_rows']
                total_duplicates += reddit_result['duplicates']
                total_with_url += reddit_result['with_url']
                empty_content += reddit_result['empty_content']
                missing_url += reddit_result['missing_url']

            return {
                "success": True,
                "total_rows": total_rows,
                "duplicate_rate_within_job": (total_duplicates / total_rows * 100) if total_rows > 0 else 0,
                "empty_rate": (empty_content / total_rows * 100) if total_rows > 0 else 0,
                "missing_url_rate": (missing_url / total_rows * 100) if total_rows > 0 else 0,
            }

        except Exception as e:
            bt.logging.error(f"DuckDB validation error: {e}")
            return {"success": False, "error": str(e)}

    def _batch_validate_platform(
        self,
        jobs_urls: Dict[str, List[str]],
        platform: str
    ) -> Dict[str, int]:
        """Validate all jobs for a platform in a single DuckDB query."""
        result = {
            'total_rows': 0,
            'duplicates': 0,
            'with_url': 0,
            'empty_content': 0,
            'missing_url': 0
        }

        all_urls = []
        for job_id, urls in jobs_urls.items():
            all_urls.extend(urls)

        if not all_urls:
            return result

        url_list_str = ", ".join([f"'{url}'" for url in all_urls])

        # Platform-specific empty check
        if platform in ['x', 'twitter']:
            content_col = 'text'
            empty_check = f"COALESCE({content_col}, '') = '' AND (tweet_hashtags IS NULL OR LENGTH(CAST(tweet_hashtags AS VARCHAR)) <= 2)"
        else:
            content_col = 'body'
            empty_check = f"COALESCE({content_col}, '') = '' AND (media IS NULL OR LENGTH(CAST(media AS VARCHAR)) <= 2)"

        try:
            query = f"""
                WITH raw_data AS (
                    SELECT
                        url as entity_url,
                        filename,
                        CASE WHEN {empty_check} THEN 1 ELSE 0 END as is_empty,
                        CASE WHEN COALESCE(url, '') = '' THEN 1 ELSE 0 END as is_missing_url
                    FROM read_parquet([{url_list_str}], filename=true)
                ),
                tagged_data AS (
                    SELECT
                        entity_url,
                        regexp_extract(filename, '/job_id=([^/]+)/', 1) as job_id,
                        is_empty,
                        is_missing_url
                    FROM raw_data
                ),
                job_duplicates AS (
                    SELECT
                        job_id,
                        entity_url,
                        COUNT(*) as cnt
                    FROM tagged_data
                    WHERE entity_url IS NOT NULL AND entity_url != ''
                    GROUP BY job_id, entity_url
                )
                SELECT
                    (SELECT COUNT(*) FROM tagged_data) as total_rows,
                    (SELECT COUNT(DISTINCT entity_url) FROM tagged_data WHERE entity_url IS NOT NULL AND entity_url != '') as unique_urls,
                    (SELECT COALESCE(SUM(cnt - 1), 0) FROM job_duplicates WHERE cnt > 1) as duplicate_count,
                    (SELECT SUM(is_empty) FROM tagged_data) as empty_content,
                    (SELECT SUM(is_missing_url) FROM tagged_data) as missing_url
            """

            query_result = self.conn.execute(query).fetchone()

            result['total_rows'] = query_result[0] or 0
            result['with_url'] = query_result[1] or 0
            result['duplicates'] = query_result[2] or 0
            result['empty_content'] = query_result[3] or 0
            result['missing_url'] = query_result[4] or 0

        except Exception as e:
            bt.logging.warning(f"Batch query error for {platform}: {e}")

        return result

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

            platform = job_config.get('platform', '').lower()
            job_label = job_config.get('label')
            job_keyword = job_config.get('keyword')
            job_start_date = job_config.get('post_start_datetime')
            job_end_date = job_config.get('post_end_datetime')

            job_start_dt = self._parse_datetime(job_start_date)
            job_end_dt = self._parse_datetime(job_end_date)

            has_label = bool(job_label and str(job_label).strip())
            has_keyword = bool(job_keyword and str(job_keyword).strip())
            has_time = bool(job_start_dt or job_end_dt)

            sample_files = random.sample(files, min(2, len(files)))

            for file_info in sample_files:
                presigned_url = presigned_urls.get(file_info['key'])
                if not presigned_url:
                    continue

                try:
                    df = pd.read_parquet(presigned_url)
                    if len(df) == 0:
                        continue

                    sample_df = df.head(min(samples_per_file, len(df)))

                    for _, row in sample_df.iterrows():
                        total_checked += 1
                        matches = self._check_row_matches_job(
                            row, platform, job_label, job_keyword,
                            job_start_dt, job_end_dt, has_label, has_keyword, has_time
                        )
                        if matches:
                            total_matched += 1
                        elif len(mismatch_samples) < 5:
                            uri = row.get('url', row.get('uri', 'unknown'))
                            mismatch_samples.append(f"Job {job_id[:8]}: {uri}")

                except Exception as e:
                    continue

        return {
            'total_checked': total_checked,
            'total_matched': total_matched,
            'match_rate': (total_matched / total_checked * 100) if total_checked > 0 else 100.0,
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

            file_key = file_info['key']
            presigned_url = presigned_urls.get(file_key)
            if not presigned_url:
                continue

            if '/job_id=' in file_key:
                job_id = file_key.split('/job_id=')[1].split('/')[0]
                job_config = expected_jobs.get(job_id, {})
                platform = job_config.get('platform', 'unknown').lower()
            else:
                continue

            try:
                df = pd.read_parquet(presigned_url)
                if len(df) == 0:
                    continue

                for _, row in df.head(3).iterrows():
                    entity = self._create_data_entity(row, platform)
                    if entity:
                        all_entities.append((entity, platform))
            except:
                continue

        if not all_entities:
            return {'entities_validated': 0, 'entities_passed': 0, 'success_rate': 0, 'sample_results': []}

        entities_to_validate = random.sample(all_entities, min(num_entities, len(all_entities)))

        total_validated = 0
        total_passed = 0
        sample_results = []

        entities_by_platform = {}
        for entity, platform in entities_to_validate:
            if platform not in entities_by_platform:
                entities_by_platform[platform] = []
            entities_by_platform[platform].append(entity)

        for platform, entities in entities_by_platform.items():
            try:
                results = await self._validate_with_scraper(entities, platform)
                for result in results:
                    total_validated += 1
                    if result.is_valid:
                        total_passed += 1
                        sample_results.append(f"✅ {platform}: {result.reason}")
                    else:
                        sample_results.append(f"❌ {platform}: {result.reason}")
            except Exception as e:
                total_validated += len(entities)
                sample_results.append(f"❌ {platform}: Scraper error - {e}")

        return {
            'entities_validated': total_validated,
            'entities_passed': total_passed,
            'success_rate': (total_passed / total_validated * 100) if total_validated > 0 else 0,
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
        """Create Twitter DataEntity."""
        try:
            username = row.get('username', '')
            text = row.get('text', '')
            url = row.get('url', row.get('uri', ''))
            if not all([username, text, url]):
                return None

            datetime_val = row.get('datetime', row.get('timestamp', ''))
            if datetime_val and pd.notna(datetime_val):
                timestamp = pd.to_datetime(datetime_val)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
            else:
                timestamp = dt.datetime.now(dt.timezone.utc)

            raw_hashtags = row.get('tweet_hashtags', None)
            tweet_hashtags = []
            if raw_hashtags is not None:
                if hasattr(raw_hashtags, '__iter__') and not isinstance(raw_hashtags, str):
                    try:
                        tweet_hashtags = list(raw_hashtags)
                    except:
                        pass
                elif not pd.isna(raw_hashtags):
                    tweet_hashtags = [raw_hashtags]

            x_content = XContent(
                username=str(username),
                text=str(text),
                url=str(url),
                timestamp=timestamp,
                tweet_hashtags=tweet_hashtags,
            )
            return XContent.to_data_entity(x_content)
        except:
            return None

    def _create_reddit_entity(self, row) -> Optional[DataEntity]:
        """Create Reddit DataEntity."""
        try:
            username = row.get('username', '')
            body = row.get('body', row.get('text', ''))
            url = row.get('url', row.get('uri', ''))
            reddit_id = row.get('id', '')
            if not all([username, body, url, reddit_id]):
                return None

            datetime_val = row.get('datetime', row.get('createdAt', ''))
            if datetime_val and pd.notna(datetime_val):
                created_at = pd.to_datetime(datetime_val)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=dt.timezone.utc)
            else:
                created_at = dt.datetime.now(dt.timezone.utc)

            reddit_content = RedditContent(
                id=str(reddit_id),
                username=str(username),
                body=str(body),
                url=str(url),
                communityName=str(row.get('communityName', '')),
                createdAt=created_at,
                dataType=str(row.get('dataType', 'post')),
            )
            return RedditContent.to_data_entity(reddit_content)
        except:
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

    def _create_failed_result(self, reason: str) -> S3ValidationResultDetailed:
        """Create a failed validation result."""
        return S3ValidationResultDetailed(
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
            sample_duplicate_uris=[],
            sample_validation_results=[],
            sample_job_mismatches=[],
            effective_size_bytes=0.0,
            job_coverage_rate=0.0
        )

    def close(self):
        """Close DuckDB connection."""
        self.conn.close()


class S3Validator:
    """
    S3 validator that performs comprehensive validation including:
    - Recent data analysis (3 hour window)
    - Duplicate detection within validation batches
    - Real scraper validation using actual scrapers
    - Composite scoring with multiple factors
    """

    def __init__(self, s3_reader=None):
        self.scraper_provider = ScraperProvider()
        self.s3_reader = s3_reader  # Optional ValidatorS3Access instance

    async def validate_miner_s3_data(
        self,
        wallet,
        s3_auth_url: str,
        miner_hotkey: str,
        expected_jobs: Dict
    ) -> S3ValidationResultDetailed:
        """
        Perform S3 validation for a miner

        Args:
            wallet: Validator wallet for S3 authentication
            s3_auth_url: S3 authentication service URL
            miner_hotkey: Target miner's hotkey
            expected_jobs: Dictionary of expected job configurations

        Returns:
            S3ValidationResultDetailed with comprehensive validation metrics
        """
        bt.logging.info(f"Starting S3 validation for miner: {miner_hotkey}")

        try:
            # Step 1: Get ALL files with metadata (for size calculation)
            # This also extracts job IDs from file paths - no pagination bug!
            all_files = []
            all_job_ids = set()

            if self.s3_reader:
                # Use ValidatorS3Access to get ALL files
                all_files = await self.s3_reader.list_all_files_with_metadata(miner_hotkey)

                # Extract job IDs from file paths (data/hotkey=X/job_id=Y/file.parquet)
                for file_info in all_files:
                    file_key = file_info.get('key', '')
                    if '/job_id=' in file_key:
                        job_id_part = file_key.split('/job_id=')[1]
                        job_id = job_id_part.split('/')[0]
                        all_job_ids.add(job_id)

                bt.logging.info(
                    f"{miner_hotkey}: Extracted {len(all_job_ids)} unique job IDs from {len(all_files)} files"
                )
            else:
                # Fallback: use old method (has pagination bug but better than nothing)
                bt.logging.warning(f"{miner_hotkey}: ValidatorS3Access not available, using fallback")
                job_folders = await self._get_miner_job_folders(wallet, s3_auth_url, miner_hotkey)
                all_job_ids = {jf.get('job_id') for jf in job_folders if jf.get('job_id')}

            if not all_job_ids:
                return self._create_failed_result("Could not access miner S3 data")

            # Step 2: Filter active jobs
            active_job_ids = [job_id for job_id in all_job_ids if job_id in expected_jobs]

            if not active_job_ids:
                return self._create_failed_result("No active jobs found")

            bt.logging.info(
                f"Found {len(active_job_ids)} active jobs out of "
                f"{len(all_job_ids)} total for {miner_hotkey}"
            )

            # Step 2.5: EMPTY FILE SPOT-CHECK across ALL files
            # Sample random files from ALL jobs (not just active ones) to catch empty files
            empty_file_check = await self._spot_check_for_empty_files(
                wallet, s3_auth_url, miner_hotkey, all_files
            )
            if empty_file_check.get('empty_file_detected'):
                bt.logging.error(
                    f"{miner_hotkey}: Empty file found in spot-check: "
                    f"{empty_file_check.get('empty_file_key')}"
                )
                return self._create_failed_result(
                    f"Empty file detected (0 rows): {empty_file_check.get('empty_file_key')}",
                    empty_file_detected=True
                )

            # Step 3: Analyze recent data
            recent_data_analysis = await self._analyze_recent_data(
                wallet, s3_auth_url, miner_hotkey, active_job_ids, all_files
            )

            if not recent_data_analysis['has_recent_data']:
                return self._create_failed_result(
                    "No recent data found in last 3 hours"
                )

            # Log EMA decay statistics
            raw_size_mb = recent_data_analysis['raw_size_bytes'] / (1024 * 1024)
            weighted_size_mb = recent_data_analysis['total_size_bytes'] / (1024 * 1024)
            decay_factor = (weighted_size_mb / raw_size_mb * 100) if raw_size_mb > 0 else 0
            bt.logging.info(
                f"{miner_hotkey}: EMA decay applied - Raw: {raw_size_mb:.1f}MB, "
                f"Weighted: {weighted_size_mb:.1f}MB ({decay_factor:.1f}% effective)"
            )

            # Step 4: Check for duplicates
            duplicate_analysis = await self._check_for_duplicates(
                wallet, s3_auth_url, miner_hotkey, recent_data_analysis['recent_job_files']
            )

            # Check for empty files - immediate failure
            if duplicate_analysis.get('empty_file_detected'):
                return self._create_failed_result(
                    "Empty file detected (0 rows)",
                    empty_file_detected=True
                )

            # Step 5: Validate job content matching
            job_match_analysis = await self._check_job_content_match(
                wallet, s3_auth_url, miner_hotkey,
                recent_data_analysis['recent_job_files'], expected_jobs
            )

            # Check for empty files - immediate failure
            if job_match_analysis.get('empty_file_detected'):
                return self._create_failed_result(
                    "Empty file detected (0 rows)",
                    empty_file_detected=True
                )

            # Step 6: Perform scraper validation
            scraper_validation = await self._perform_scraper_validation(
                wallet, s3_auth_url, miner_hotkey,
                recent_data_analysis['recent_job_files'], expected_jobs
            )

            # Check for empty files - immediate failure
            if scraper_validation.get('empty_file_detected'):
                return self._create_failed_result(
                    "Empty file detected (0 rows)",
                    empty_file_detected=True
                )

            # Step 7: Calculate job completion rate for reward scaling
            num_expected_jobs = len(expected_jobs)
            num_active_jobs = len(active_job_ids)
            job_completion_rate = (num_active_jobs / num_expected_jobs * 100) if num_expected_jobs > 0 else 100.0

            # Step 8: Calculate final validation result with job completion multiplier
            return self._calculate_final_result(
                num_active_jobs,
                num_expected_jobs,
                recent_data_analysis,
                duplicate_analysis,
                job_match_analysis,
                scraper_validation,
                job_completion_rate
            )

        except Exception as e:
            bt.logging.error(f"S3 validation error for {miner_hotkey}: {str(e)}")
            return self._create_failed_result(f"Validation error: {str(e)}")

    async def _get_miner_job_folders(
        self, wallet, s3_auth_url: str, miner_hotkey: str
    ) -> List[Dict]:
        """Get job folders for the specified miner"""
        try:
            hotkey = wallet.hotkey.ss58_address
            timestamp = int(time.time())
            commitment = f"s3:validator:folders:{hotkey}:{timestamp}"
            signature = wallet.hotkey.sign(commitment.encode())

            payload = {
                "hotkey": hotkey,
                "timestamp": timestamp,
                "signature": signature.hex(),
                "miner_hotkey": miner_hotkey
            }

            response = requests.post(
                f"{s3_auth_url}/get-folder-presigned-urls",
                json=payload,
                timeout=180
            )

            if response.status_code != 200:
                return []

            folder_data = response.json()
            folder_urls = folder_data.get('folder_urls', {})

            return folder_urls.get(miner_hotkey, {}).get('job_folders', [])

        except Exception as e:
            bt.logging.error(f"Error getting job folders for {miner_hotkey}: {str(e)}")
            return []

    async def _analyze_recent_data(
        self, wallet, s3_auth_url: str, miner_hotkey: str, active_job_ids: List[str], all_files: List[Dict]
    ) -> Dict:
        """
        Analyze data with EMA decay scoring.
        - Activity filter: Job must have at least 1 file uploaded in last 3 hours
        - Scoring: All files scored with 3.5-day (84-hour) half-life exponential decay

        Args:
            active_job_ids: List of job IDs that are currently active
            all_files: Already-fetched list of ALL files with metadata (from ValidatorS3Access)
        """
        now = dt.datetime.now(dt.timezone.utc)
        activity_threshold = now - dt.timedelta(hours=3)  # 3 hour activity check

        # EMA decay: half-life = 3.5 days (84 hours)
        # Formula: weight = e^(-λ * age_hours), where λ = ln(2) / half_life
        # Rationale: Jobs typically last 7 days, so data is worth 50% at mid-job, 25% at expiry
        half_life_hours = 84.0  # 3.5 days
        decay_lambda = 0.693147 / half_life_hours  # ln(2) / 84

        # =====================================================================
        # PART 1: Calculate total size from ALL files (for fair rewards)
        # =====================================================================
        total_size_bytes = 0  # Raw total size across ALL files
        weighted_size_bytes = 0  # EMA weighted size across ALL files
        total_files_all_jobs = 0

        if all_files:
            # Use already-fetched files (no additional API calls needed!)
            bt.logging.info(f"{miner_hotkey}: Calculating size from {len(all_files)} pre-fetched files...")

            for file_info in all_files:
                try:
                    file_size = file_info.get('size', 0)
                    last_modified_str = file_info.get('last_modified', '')

                    # Parse file age
                    if last_modified_str and 'T' in last_modified_str:
                        if last_modified_str.endswith('Z'):
                            parsed_time = dt.datetime.fromisoformat(last_modified_str.replace('Z', '+00:00'))
                        else:
                            parsed_time = dt.datetime.fromisoformat(last_modified_str)

                        if parsed_time.tzinfo is None:
                            parsed_time = parsed_time.replace(tzinfo=dt.timezone.utc)

                        # Calculate age in hours
                        age_hours = (now - parsed_time).total_seconds() / 3600.0

                        # Apply exponential decay: weight = e^(-λ * age)
                        weight = math.exp(-decay_lambda * age_hours)

                        # Accumulate weighted size
                        weighted_size_bytes += file_size * weight
                    else:
                        # No timestamp - give minimal weight (assume old)
                        weighted_size_bytes += file_size * 0.01

                    # Always count raw size for statistics
                    total_size_bytes += file_size
                    total_files_all_jobs += 1

                except Exception as e:
                    bt.logging.debug(f"Error calculating file weight: {str(e)}")
                    total_size_bytes += file_info.get('size', 0)
                    continue

            bt.logging.info(
                f"{miner_hotkey}: Size calculation complete - "
                f"{total_files_all_jobs} total files, "
                f"Raw: {total_size_bytes/(1024*1024):.1f}MB, "
                f"Weighted: {weighted_size_bytes/(1024*1024):.1f}MB"
            )
        else:
            # Fallback: old method (only samples 5 jobs - inaccurate!)
            bt.logging.warning(
                f"{miner_hotkey}: ValidatorS3Access not available, using fallback (5-job sampling)"
            )

        # =====================================================================
        # PART 2: Sample 5 jobs for validation checks (game theory)
        # =====================================================================
        recent_job_files = {}
        recent_jobs_count = 0

        # Group files by job_id from the already-fetched files
        files_by_job = {}
        for file_info in all_files:
            file_key = file_info.get('key', '')
            if '/job_id=' in file_key:
                job_id_part = file_key.split('/job_id=')[1]
                job_id = job_id_part.split('/')[0]

                # Only include active jobs
                if job_id in active_job_ids:
                    if job_id not in files_by_job:
                        files_by_job[job_id] = []
                    files_by_job[job_id].append(file_info)

        # First, identify jobs with recent activity (uploaded files in last 3 hours)
        # This ensures we prioritize sampling from active jobs instead of relying on random luck
        jobs_with_recent_activity = []
        jobs_without_recent_activity = []

        for job_id, job_files_metadata in files_by_job.items():
            has_recent_activity = False
            for file_info in job_files_metadata:
                try:
                    last_modified_str = file_info.get('last_modified', '')
                    if self._is_file_recent(last_modified_str, activity_threshold):
                        has_recent_activity = True
                        break
                except Exception:
                    continue

            if has_recent_activity:
                jobs_with_recent_activity.append(job_id)
            else:
                jobs_without_recent_activity.append(job_id)

        # Prioritize sampling from jobs with recent activity
        # If we have enough jobs with recent activity, sample only from those
        # Otherwise, sample from all available jobs
        if len(jobs_with_recent_activity) >= 5:
            sample_job_ids = random.sample(jobs_with_recent_activity, 5)
            bt.logging.info(
                f"{miner_hotkey}: Sampling 5 jobs from {len(jobs_with_recent_activity)} jobs with recent activity "
                f"(out of {len(files_by_job)} total active jobs)"
            )
        elif len(jobs_with_recent_activity) > 0:
            # Take all jobs with recent activity, plus some random ones to reach 5
            sample_job_ids = jobs_with_recent_activity.copy()
            remaining_needed = min(5 - len(sample_job_ids), len(jobs_without_recent_activity))
            if remaining_needed > 0:
                sample_job_ids.extend(random.sample(jobs_without_recent_activity, remaining_needed))
            bt.logging.info(
                f"{miner_hotkey}: Sampling {len(sample_job_ids)} jobs: {len(jobs_with_recent_activity)} with recent activity + "
                f"{len(sample_job_ids) - len(jobs_with_recent_activity)} without recent activity"
            )
        else:
            # No jobs with recent activity, sample randomly from all
            available_jobs = list(files_by_job.keys())
            sample_job_ids = random.sample(available_jobs, min(5, len(available_jobs)))
            bt.logging.warning(
                f"{miner_hotkey}: No jobs with recent activity found! Sampling {len(sample_job_ids)} random jobs from "
                f"{len(available_jobs)} active jobs"
            )

        validation_files_count = 0
        for job_id in sample_job_ids:
            job_files_metadata = files_by_job[job_id]

            # Check if this job has recent activity
            has_recent_activity = job_id in jobs_with_recent_activity

            # Only include jobs with recent activity for validation
            if has_recent_activity:
                # Convert file metadata to format expected by validation functions
                job_files = [
                    {
                        'key': f.get('key'),
                        'size': f.get('size'),
                        'last_modified': f.get('last_modified')
                    }
                    for f in job_files_metadata
                ]

                recent_job_files[job_id] = {
                    'files': job_files,
                    'presigned_url': None  # Not needed anymore, we have all the data
                }
                validation_files_count += len(job_files)
                recent_jobs_count += 1

        # Check if we have recent data
        has_recent_data = validation_files_count > 0 or total_files_all_jobs > 0

        bt.logging.info(
            f"{miner_hotkey}: Validation sampling complete - "
            f"{recent_jobs_count} jobs with recent activity, {validation_files_count} files for validation"
        )

        return {
            'has_recent_data': has_recent_data,
            'recent_jobs_count': recent_jobs_count,
            'recent_files_count': validation_files_count,
            'total_size_bytes': int(weighted_size_bytes),  # Weighted size from ALL files
            'raw_size_bytes': total_size_bytes,  # Raw size from ALL files
            'recent_job_files': recent_job_files  # Sampled jobs for validation only
        }

    async def _spot_check_for_empty_files(
        self, wallet, s3_auth_url: str, miner_hotkey: str, all_files: List[Dict]
    ) -> Dict:
        """
        Sample random files from ALL jobs to detect empty files.

        This check runs BEFORE filtering to active jobs, so it catches empty files
        in inactive/obsolete jobs as well.

        Samples 10 random files from across ALL jobs (not just active ones).
        Files larger than 1GB are skipped to prevent memory issues.
        """
        if not all_files:
            return {'empty_file_detected': False}

        # Filter out files larger than 1GB to prevent memory issues
        max_file_size = 1 * 1024 * 1024 * 1024  # 1GB
        eligible_files = [f for f in all_files if f.get('size', 0) <= max_file_size]

        if not eligible_files:
            bt.logging.warning(f"{miner_hotkey}: No files under 1GB for empty file spot-check")
            return {'empty_file_detected': False}

        # Sample 10 random files from eligible files (regardless of job status)
        sample_size = min(10, len(eligible_files))
        sampled_files = random.sample(eligible_files, sample_size)

        file_keys = [f['key'] for f in sampled_files]

        bt.logging.info(
            f"{miner_hotkey}: Empty file spot-check: sampling {sample_size} files "
            f"from {len(eligible_files)} eligible files ({len(all_files)} total, excluding >1GB)"
        )

        # Get presigned URLs for sampled files
        file_urls = await self._get_file_presigned_urls(
            wallet, s3_auth_url, miner_hotkey, file_keys
        )

        if not file_urls:
            bt.logging.warning(f"{miner_hotkey}: Could not get presigned URLs for spot-check")
            return {'empty_file_detected': False}

        # Check each file for empty content
        files_checked = 0
        for file_key, file_info in file_urls.items():
            try:
                presigned_url = file_info.get('presigned_url')
                if not presigned_url:
                    continue

                df = pd.read_parquet(presigned_url)
                files_checked += 1

                # Check for empty files (0 rows)
                if len(df) == 0:
                    bt.logging.error(
                        f"{miner_hotkey}: Empty file detected in spot-check: {file_key}"
                    )
                    return {
                        'empty_file_detected': True,
                        'empty_file_key': file_key
                    }

                # Also check for files with only placeholder columns
                if len(df.columns) > 0:
                    placeholder_cols = [c for c in df.columns if 'placeholder' in c.lower() or 'empty' in c.lower()]
                    if len(placeholder_cols) == len(df.columns):
                        bt.logging.error(
                            f"{miner_hotkey}: Placeholder file detected: {file_key} "
                            f"(columns: {list(df.columns)[:5]})"
                        )
                        return {
                            'empty_file_detected': True,
                            'empty_file_key': file_key
                        }

            except Exception as e:
                bt.logging.debug(f"Error in spot-check for {file_key}: {str(e)}")
                continue

        bt.logging.info(
            f"{miner_hotkey}: Empty file spot-check PASSED - checked {files_checked} files, no empty files found"
        )

        return {'empty_file_detected': False}

    async def _check_for_duplicates(
        self, wallet, s3_auth_url: str, miner_hotkey: str, recent_job_files: Dict
    ) -> Dict:
        """Check for URI duplicates within the validation batch"""
        validation_batch_uris = set()
        duplicate_uris = []
        total_entities = 0
        duplicate_entities = 0

        for job_id, job_data in recent_job_files.items():
            files = job_data['files']

            # Sample files for duplicate checking
            sample_files = random.sample(
                files, min(3, len(files))  # files_per_job_sample = 3
            )
            file_keys = [f['key'] for f in sample_files]

            # Get presigned URLs for files
            file_urls = await self._get_file_presigned_urls(
                wallet, s3_auth_url, miner_hotkey, file_keys
            )
            if not file_urls:
                continue

            # Check URIs in files
            for file_key, file_info in file_urls.items():
                try:
                    presigned_url = file_info.get('presigned_url')
                    if not presigned_url:
                        continue

                    df = pd.read_parquet(presigned_url)

                    # Check for empty files (0 rows)
                    if len(df) == 0:
                        bt.logging.error(
                            f"{miner_hotkey}: Empty file detected (0 rows): {file_key}"
                        )
                        return {
                            'total_entities': 0,
                            'duplicate_entities': 0,
                            'duplicate_percentage': 0,
                            'sample_duplicates': [],
                            'empty_file_detected': True,
                            'empty_file_key': file_key
                        }

                    # Sample rows to check for duplicates
                    sample_size = min(20, len(df))  # rows_per_file_sample = 20
                    sample_df = df.head(sample_size)

                    for _, row in sample_df.iterrows():
                        uri_value = self._get_uri_value(row)
                        if uri_value:
                            total_entities += 1
                            if uri_value in validation_batch_uris:
                                duplicate_entities += 1
                                duplicate_uris.append(uri_value)
                            else:
                                validation_batch_uris.add(uri_value)

                except Exception as e:
                    bt.logging.debug(f"Error checking file for duplicates: {str(e)}")
                    continue

        duplicate_percentage = (
            (duplicate_entities / total_entities * 100)
            if total_entities > 0 else 0
        )

        return {
            'total_entities': total_entities,
            'duplicate_entities': duplicate_entities,
            'duplicate_percentage': duplicate_percentage,
            'sample_duplicates': duplicate_uris[:10]
        }

    def _parse_job_datetime(self, raw, miner_hotkey: str) -> Optional[dt.datetime]:
        """Parse job datetime from Gravity; treat null-ish values as no constraint."""
        if raw is None:
            return None
        if isinstance(raw, str) and raw.strip().lower() in ("", "null", "none"):
            return None
        try:
            return pd.to_datetime(raw, utc=True)
        except Exception as e:
            bt.logging.warning(f"{miner_hotkey}: Failed to parse job datetime '{raw}': {e}")
            return None

    async def _check_job_content_match(
        self, wallet, s3_auth_url: str, miner_hotkey: str,
        recent_job_files: Dict, expected_jobs: Dict
    ) -> Dict:
        """Validate that uploaded data matches job requirements (label/keyword AND time window via DataEntity.datetime)."""
        total_checked = 0
        total_matched = 0
        mismatch_samples = []
        checked_uris = []  # Track URIs being checked

        for job_id, job_data in recent_job_files.items():
            files = job_data['files']
            expected_job = expected_jobs.get(job_id, {})

            if not expected_job:
                bt.logging.debug(f"No expected job config found for job_id: {job_id}")
                continue

            params = expected_job.get('params', {})
            platform = params.get('platform', '').lower()
            job_label = params.get('label')
            job_keyword = params.get('keyword')
            job_start_date = params.get('post_start_datetime')
            job_end_date = params.get('post_end_datetime')

            # Parse job time window (UTC)
            job_start_dt = self._parse_job_datetime(job_start_date, miner_hotkey)
            job_end_dt = self._parse_job_datetime(job_end_date, miner_hotkey)
            has_time_requirement = bool(job_start_dt or job_end_dt)

            # Sample files to check (2 files per job)
            sample_files = random.sample(files, min(2, len(files)))
            file_keys = [f['key'] for f in sample_files]

            # Get presigned URLs for files
            file_urls = await self._get_file_presigned_urls(
                wallet, s3_auth_url, miner_hotkey, file_keys
            )
            if not file_urls:
                continue

            # Check entities in sampled files
            for file_key, file_info in file_urls.items():
                try:
                    presigned_url = file_info.get('presigned_url')
                    if not presigned_url:
                        continue

                    df = pd.read_parquet(presigned_url)

                    # Check for empty files (0 rows)
                    if len(df) == 0:
                        bt.logging.error(
                            f"{miner_hotkey}: Empty file detected (0 rows): {file_key}"
                        )
                        return {
                            'total_checked': 0,
                            'total_matched': 0,
                            'match_rate': 0,
                            'mismatch_samples': [],
                            'empty_file_detected': True,
                            'empty_file_key': file_key
                        }

                    # Sample rows to check (up to 10 per file)
                    sample_size = min(10, len(df))
                    sample_df = df.head(sample_size)

                    for _, row in sample_df.iterrows():
                        total_checked += 1
                        matches_job = False

                        label_matches = None
                        keyword_matches = None
                        time_matches = None
                        entity_dt_str = None

                        has_label_requirement = bool(job_label and job_label.strip())
                        has_keyword_requirement = bool(job_keyword and job_keyword.strip())

                        if has_label_requirement:
                            # Label-based job: check if entity label matches
                            entity_label = str(row.get('label', '')).lower().strip()
                            job_label_normalized = job_label.lower().strip()

                            # Handle different label formats
                            if platform in ['x', 'twitter']:
                                # For X: check if label is in tweet_hashtags array (handles multiple hashtags)
                                tweet_hashtags_raw = row.get('tweet_hashtags', None)

                                hashtags_list = []
                                if tweet_hashtags_raw is not None:
                                    # If it's an iterable (list / array / Series), but not a string
                                    if hasattr(tweet_hashtags_raw, '__iter__') and not isinstance(tweet_hashtags_raw, str):
                                        try:
                                            hashtags_list = list(tweet_hashtags_raw)
                                        except TypeError:
                                            hashtags_list = []
                                    else:
                                        # Scalar value: drop NaN/NA if possible
                                        try:
                                            if not pd.isna(tweet_hashtags_raw):
                                                hashtags_list = [tweet_hashtags_raw]
                                        except Exception:
                                            hashtags_list = [tweet_hashtags_raw]

                                # Normalize hashtags to lowercase
                                hashtags_lower = [
                                    str(h).lower().strip()
                                    for h in hashtags_list
                                    if h is not None and str(h).strip() != ''
                                ]

                                label_without_hash = job_label_normalized.lstrip('#')
                                label_with_hash = f"#{label_without_hash}"

                                # Check if job label is in the hashtags array
                                label_matches = (
                                    label_with_hash in hashtags_lower or
                                    label_without_hash in hashtags_lower or
                                    # Fallback: check main label field
                                    entity_label == label_with_hash or
                                    entity_label == label_without_hash
                                )

                            elif platform == 'reddit':
                                # For Reddit: check with and without r/ prefix
                                label_matches = (
                                    entity_label == job_label_normalized or
                                    entity_label == f"r/{job_label_normalized.removeprefix('r/')}" or
                                    entity_label.removeprefix('r/') == job_label_normalized.removeprefix('r/')
                                )
                            else:
                                label_matches = (entity_label == job_label_normalized)

                        if has_keyword_requirement:
                            # Keyword-based job: check if content contains keyword
                            job_keyword_normalized = job_keyword.lower().strip()

                            # Check in relevant content fields based on platform
                            if platform == 'reddit':
                                body = str(row.get('body', '')).lower()
                                title = str(row.get('title', '')).lower()
                                keyword_matches = (job_keyword_normalized in body or job_keyword_normalized in title)
                            else:  # X/Twitter
                                text = str(row.get('text', '')).lower()
                                keyword_matches = job_keyword_normalized in text

                        # Time window validation
                        if has_time_requirement:
                            entity_dt = row.get('datetime')
                            if entity_dt is not None and not pd.isna(entity_dt):
                                entity_dt = pd.to_datetime(entity_dt, utc=True)
                                entity_dt_str = entity_dt.isoformat()
                                time_matches = True
                                if job_start_dt and entity_dt < job_start_dt:
                                    time_matches = False
                                if job_end_dt and entity_dt > job_end_dt:
                                    time_matches = False
                            else:
                                time_matches = False
                        else:
                            time_matches = True

                        # Combine all requirements
                        matches_job = True
                        if has_label_requirement and not label_matches:
                            matches_job = False
                        if has_keyword_requirement and not keyword_matches:
                            matches_job = False
                        if has_time_requirement and not time_matches:
                            matches_job = False

                        uri = self._get_uri_value(row)
                        if uri and len(checked_uris) < 20:
                            checked_uris.append(uri)

                        if matches_job:
                            total_matched += 1
                        else:
                            # Record mismatch sample with time window info
                            if len(mismatch_samples) < 10:
                                msg = f"Job {job_id[:8]}: label={job_label or 'any'} keyword={job_keyword or 'any'}"
                                if has_time_requirement:
                                    msg += f" time=[{job_start_dt.isoformat() if job_start_dt else 'any'} to {job_end_dt.isoformat() if job_end_dt else 'any'}]"
                                msg += f" | matched: label={label_matches} keyword={keyword_matches} time={time_matches}"
                                if entity_dt_str:
                                    msg += f" entity_dt={entity_dt_str}"
                                msg += f" - {uri or 'unknown'}"
                                mismatch_samples.append(msg)

                except Exception as e:
                    bt.logging.debug(f"Error checking job content match: {str(e)}")
                    continue

        match_rate = (total_matched / total_checked * 100) if total_checked > 0 else 0

        # Log job match check details (same style as regular validation)
        bt.logging.info(
            f"{miner_hotkey}: S3 job match validation: Checked {total_checked} entities, "
            f"{total_matched} matched job requirements ({match_rate:.1f}%)"
        )
        if checked_uris:
            bt.logging.info(f"{miner_hotkey}: S3 job match: Sample URIs checked: {checked_uris[:5]}")
        if mismatch_samples:
            bt.logging.warning(f"{miner_hotkey}: S3 job match failures: {mismatch_samples[:3]}")

        return {
            'total_checked': total_checked,
            'total_matched': total_matched,
            'match_rate': match_rate,
            'mismatch_samples': mismatch_samples
        }

    async def _perform_scraper_validation(
        self, wallet, s3_auth_url: str, miner_hotkey: str,
        recent_job_files: Dict, expected_jobs: Dict
    ) -> Dict:
        """Validate random entities using real scrapers"""
        all_entities = []
        sample_results = []

        # Collect entities from recent job files
        for job_id, job_data in recent_job_files.items():
            files = job_data['files']
            expected_job = expected_jobs.get(job_id, {})
            platform = expected_job.get('params', {}).get('platform', 'unknown').lower()

            if files and len(all_entities) < 15:  # Get extra to ensure we have enough
                sample_file = random.choice(files)
                file_keys = [sample_file['key']]

                file_urls = await self._get_file_presigned_urls(
                    wallet, s3_auth_url, miner_hotkey, file_keys
                )

                if file_urls:
                    for file_key, file_info in file_urls.items():
                        try:
                            presigned_url = file_info.get('presigned_url')
                            if presigned_url:
                                df = pd.read_parquet(presigned_url)

                                # Check for empty files (0 rows)
                                if len(df) == 0:
                                    bt.logging.error(
                                        f"{miner_hotkey}: Empty file detected (0 rows): {file_key}"
                                    )
                                    return {
                                        'entities_validated': 0,
                                        'entities_passed': 0,
                                        'success_rate': 0,
                                        'sample_results': [],
                                        'empty_file_detected': True,
                                        'empty_file_key': file_key
                                    }

                                # Sample rows from this file
                                sample_size = min(3, len(df))
                                sample_df = df.head(sample_size)

                                for _, row in sample_df.iterrows():
                                    entity = self._create_data_entity_from_row(row, platform)
                                    if entity:
                                        all_entities.append((entity, platform, job_id))

                                    if len(all_entities) >= 15:
                                        break

                                if len(all_entities) >= 15:
                                    break

                        except Exception as e:
                            bt.logging.debug(f"Error reading file for validation: {str(e)}")
                            continue

            if len(all_entities) >= 15:
                break

        # Select entities for validation
        entities_to_validate = random.sample(
            all_entities, min(10, len(all_entities))  # sample_size = 10
        )

        # Log URIs being validated (same style as regular validation)
        entity_uris = [entity[0].uri for entity in entities_to_validate]
        bt.logging.info(
            f"{miner_hotkey}: S3 validation passed basic checks. Validating uris from S3: {entity_uris}"
        )

        # Group by platform for efficient validation
        entities_by_platform = {}
        for entity, platform, job_id in entities_to_validate:
            if platform not in entities_by_platform:
                entities_by_platform[platform] = []
            entities_by_platform[platform].append((entity, job_id))

        # Validate with real scrapers
        total_validated = 0
        total_passed = 0
        all_validation_results = []  # Collect all ValidationResult objects

        for platform, platform_entities in entities_by_platform.items():
            entities_only = [e[0] for e in platform_entities]

            try:
                validation_results = await self._validate_entities_with_scraper(
                    entities_only, platform
                )

                for i, result in enumerate(validation_results):
                    total_validated += 1
                    all_validation_results.append(result)  # Collect all results

                    job_id = (
                        platform_entities[i][1]
                        if i < len(platform_entities) else "unknown"
                    )

                    if result.is_valid:
                        total_passed += 1
                        sample_results.append(f"✅ {platform} ({job_id}): {result.reason}")
                    else:
                        sample_results.append(f"❌ {platform} ({job_id}): {result.reason}")

            except Exception as e:
                bt.logging.error(f"Error validating {platform} entities: {str(e)}")
                # Count as failed validation
                total_validated += len(entities_only)
                for i in range(len(entities_only)):
                    failed_result = ValidationResult(
                        is_valid=False,
                        reason=f"Scraper error: {str(e)}",
                        content_size_bytes_validated=0
                    )
                    all_validation_results.append(failed_result)
                    sample_results.append(f"❌ {platform}: Scraper error - {str(e)}")

        success_rate = (total_passed / total_validated * 100) if total_validated > 0 else 0

        # Log results in same style as regular validation
        bt.logging.success(
            f"{miner_hotkey}: S3 data validation on selected entities finished with results: {all_validation_results}"
        )
        bt.logging.info(
            f"{miner_hotkey}: S3 scraper validation summary: {total_passed}/{total_validated} passed ({success_rate:.1f}%)"
        )

        return {
            'entities_validated': total_validated,
            'entities_passed': total_passed,
            'success_rate': success_rate,
            'sample_results': sample_results
        }
    
    def _calculate_final_result(
        self, total_active_jobs: int, expected_jobs_count: int, recent_data_analysis: Dict,
        duplicate_analysis: Dict, job_match_analysis: Dict, scraper_validation: Dict,
        job_completion_rate: float
    ) -> S3ValidationResultDetailed:
        """Calculate final validation result based on all analyses"""

        # Allow up to 10% duplicates (accidents happen during uploads)
        duplicate_threshold = 10.0  # Allow 10% duplicates, penalize proportionally
        has_duplicates = (
            duplicate_analysis['duplicate_percentage'] > duplicate_threshold
        )
        duplicate_validation_passed = not has_duplicates

        # Determine if job content matching passed
        min_job_match_rate = 95.0  # Require 95% of data to match job requirements todo change it back to 100 later.
        job_match_validation_passed = (
            job_match_analysis['match_rate'] >= min_job_match_rate
        )

        # Determine if scraper validation passed
        min_scraper_success_rate = 80.0  # min_scraper_success_rate = 80.0
        scraper_validation_passed = (
            scraper_validation['success_rate'] >= min_scraper_success_rate
        )

        # Calculate size bonus (logarithmic scaling - bigger uploads = better scores with diminishing returns)
        size_bytes = recent_data_analysis['total_size_bytes']
        min_size_for_bonus = 10 * 1024 * 1024  # 10MB minimum for any bonus
        if size_bytes >= min_size_for_bonus:
            size_bonus = math.log10(size_bytes / min_size_for_bonus) * 20  # Log base 10 scaling
        else:
            size_bonus = 0

        # Final validation decision - must pass ALL checks
        is_valid = duplicate_validation_passed and job_match_validation_passed and scraper_validation_passed

        # Calculate duplicate score (proportional penalty)
        # 0% duplicates = 30 points, 10% duplicates = 0 points, linear scale
        duplicate_pct = duplicate_analysis['duplicate_percentage']
        duplicate_score = max(0, 30.0 * (1 - duplicate_pct / 10.0))

        # Calculate base validation percentage WITHOUT size bonus
        # Base: 30% duplicates (proportional) + 70% scraper = 100%
        base_validation_percentage = (
            duplicate_score +
            (scraper_validation['success_rate'] * 0.7)
        )

        # Only apply size bonus if validation passed all checks
        # Size bonus should reward volume AFTER quality is proven
        if is_valid:
            validation_percentage = base_validation_percentage + size_bonus
        else:
            # Failed validation = no bonus from size
            validation_percentage = 0.0

        # Apply job completion rate multiplier to reward miners with more complete job coverage
        # 100% jobs = 1.0x, 50% jobs = 0.5x, 10% jobs = 0.1x
        # This prevents "1 job gets 100M boost" exploit
        job_completion_multiplier = job_completion_rate / 100.0
        validation_percentage *= job_completion_multiplier

        bt.logging.info(
            f"Job completion rate: {job_completion_rate:.1f}% ({total_active_jobs} jobs), "
            f"applied {job_completion_multiplier:.2f}x multiplier to validation score"
        )

        # Collect validation issues
        issues = []
        if has_duplicates:
            issues.append(
                f"Too many duplicates: {duplicate_analysis['duplicate_percentage']:.1f}% (max 10% allowed)"
            )
        if not job_match_validation_passed:
            issues.append(
                f"Low job match rate: {job_match_analysis['match_rate']:.1f}% (need ≥{min_job_match_rate}%)"
            )
        if not scraper_validation_passed:
            issues.append(
                f"Low scraper success rate: {scraper_validation['success_rate']:.1f}%"
            )

        reason = (
            f"S3 validation: {'PASSED' if is_valid else 'FAILED'} - "
            f"Duplicates: {duplicate_analysis['duplicate_percentage']:.1f}%, "
            f"Job match: {job_match_analysis['match_rate']:.1f}%, "
            f"Scraper: {scraper_validation['success_rate']:.1f}%"
        )

        # Calculate effective_size for competition-based scoring
        # effective_size = total_size_bytes × coverage² (only if validation passed)
        coverage_ratio = job_completion_rate / 100.0  # 0-1
        if is_valid:
            effective_size_bytes = size_bytes * (coverage_ratio ** 2)
        else:
            effective_size_bytes = 0.0

        bt.logging.info(
            f"Competition scoring: size={size_bytes/(1024*1024):.1f}MB, "
            f"coverage={job_completion_rate:.1f}%, "
            f"effective_size={effective_size_bytes/(1024*1024):.1f}MB"
        )

        return S3ValidationResultDetailed(
            is_valid=is_valid,
            validation_percentage=validation_percentage,
            total_active_jobs=total_active_jobs,
            expected_jobs_count=expected_jobs_count,
            recent_jobs_analyzed=recent_data_analysis['recent_jobs_count'],
            recent_files_count=recent_data_analysis['recent_files_count'],
            total_size_bytes=recent_data_analysis['total_size_bytes'],
            has_duplicates=has_duplicates,
            duplicate_percentage=duplicate_analysis['duplicate_percentage'],
            entities_validated=scraper_validation['entities_validated'],
            entities_passed_scraper=scraper_validation['entities_passed'],
            scraper_success_rate=scraper_validation['success_rate'],
            entities_checked_for_job_match=job_match_analysis['total_checked'],
            entities_matched_job=job_match_analysis['total_matched'],
            job_match_rate=job_match_analysis['match_rate'],
            validation_issues=issues,
            reason=reason,
            sample_duplicate_uris=duplicate_analysis['sample_duplicates'][:5],
            sample_validation_results=scraper_validation['sample_results'][:5],
            sample_job_mismatches=job_match_analysis['mismatch_samples'][:5],
            effective_size_bytes=effective_size_bytes,
            job_coverage_rate=job_completion_rate
        )
    
    async def _validate_entities_with_scraper(
        self, entities: List[DataEntity], platform: str
    ) -> List[ValidationResult]:
        """Validate entities using appropriate scraper"""
        try:
            # Map platform to data source and get scraper using provider
            data_source = None
            if platform in ['x', 'twitter']:
                data_source = DataSource.X
            elif platform == 'reddit':
                data_source = DataSource.REDDIT
            else:
                return [ValidationResult(
                    is_valid=False,
                    reason=f"Unknown platform: {platform}",
                    content_size_bytes_validated=0
                ) for _ in entities]
            
            # Get scraper using provider (like in miner_evaluator)
            scraper = self.scraper_provider.get(PREFERRED_SCRAPERS[data_source])
            return await scraper.validate(entities)
            
        except Exception as e:
            return [ValidationResult(
                is_valid=False,
                reason=f"Scraper error: {str(e)}",
                content_size_bytes_validated=0
            ) for _ in entities]
    
    def _create_failed_result(self, reason: str, empty_file_detected: bool = False) -> S3ValidationResultDetailed:
        """Create a failed validation result"""
        return S3ValidationResultDetailed(
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
            sample_duplicate_uris=[],
            sample_validation_results=[],
            sample_job_mismatches=[],
            empty_file_detected=empty_file_detected,
            effective_size_bytes=0.0,
            job_coverage_rate=0.0
        )
    
    # Additional helper methods
    async def _get_job_files(self, presigned_url: str) -> List[Dict]:
        """Get parquet files from job using proven XML parsing"""
        try:
            response = requests.get(presigned_url, timeout=30)
            if response.status_code != 200:
                return []
            
            root = ET.fromstring(response.text)
            namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
            
            files = []
            for content in root.findall('.//s3:Contents', namespaces):
                try:
                    key_elem = content.find('s3:Key', namespaces)
                    size_elem = content.find('s3:Size', namespaces)
                    modified_elem = content.find('s3:LastModified', namespaces)
                    
                    if key_elem is not None and key_elem.text:
                        key = urllib.parse.unquote(key_elem.text)
                        size = int(size_elem.text) if size_elem is not None and size_elem.text else 0
                        last_modified = modified_elem.text if modified_elem is not None else ""
                        
                        if key.endswith('.parquet'):
                            files.append({
                                'key': key,
                                'size': size,
                                'last_modified': last_modified
                            })
                except Exception:
                    continue
            
            return files
            
        except Exception:
            return []
    
    def _is_file_recent(self, last_modified_str: str, threshold_time: dt.datetime) -> bool:
        """Check if file was modified after threshold time"""
        try:
            if last_modified_str and 'T' in last_modified_str:
                if last_modified_str.endswith('Z'):
                    parsed_time = dt.datetime.fromisoformat(last_modified_str.replace('Z', '+00:00'))
                else:
                    parsed_time = dt.datetime.fromisoformat(last_modified_str)
                
                if parsed_time.tzinfo is None:
                    parsed_time = parsed_time.replace(tzinfo=dt.timezone.utc)
                
                return parsed_time >= threshold_time
        except Exception:
            pass
        return False
    
    async def _get_file_presigned_urls(
        self, wallet, s3_auth_url: str, miner_hotkey: str, file_keys: List[str]
    ) -> Optional[Dict]:
        """Get presigned URLs for specific files"""
        try:
            hotkey = wallet.hotkey.ss58_address
            timestamp = int(time.time())
            commitment = f"s3:validator:files:{miner_hotkey}:{hotkey}:{timestamp}"
            signature = wallet.hotkey.sign(commitment.encode())
            
            payload = {
                "hotkey": hotkey,
                "timestamp": timestamp,
                "signature": signature.hex(),
                "miner_hotkey": miner_hotkey,
                "file_keys": file_keys,
                "expiry_hours": 1
            }
            
            response = requests.post(
                f"{s3_auth_url}/get-file-presigned-urls",
                json=payload,
                timeout=60
            )
            
            return response.json().get('file_urls', {}) if response.status_code == 200 else None
            
        except Exception:
            return None
    
    def _get_uri_value(self, row) -> str:
        """Extract URI/URL value from DataFrame row"""
        try:
            for uri_field in ['uri', 'url', 'link', 'source_url']:
                if uri_field in row.index and pd.notna(row[uri_field]):
                    uri_value = str(row[uri_field]).strip()
                    if uri_value and uri_value != '':
                        return uri_value
            return ""
        except Exception:
            return ""
    
    def _create_data_entity_from_row(self, row, platform: str) -> Optional[DataEntity]:
        """Create DataEntity from parquet row based on platform"""
        try:
            if platform in ['x', 'twitter']:
                return self._create_twitter_data_entity(row)
            elif platform == 'reddit':
                return self._create_reddit_data_entity(row)
            else:
                return None
        except Exception:
            return None
    
    def _create_twitter_data_entity(self, row) -> Optional[DataEntity]:
        """Create Twitter DataEntity from parquet row"""
        try:
            # Get required fields
            username = row.get('username', '')
            text = row.get('text', '')
            url = row.get('url', row.get('uri', ''))

            if not all([username, text, url]):
                return None

            # Get datetime from row or use current time
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

            # ---- SAFE NORMALIZATION FOR ARRAY-LIKE FIELDS ----
            # tweet_hashtags can be list/array/Series or scalar/NaN
            raw_hashtags = row.get('tweet_hashtags', None)
            if raw_hashtags is None:
                tweet_hashtags = []
            elif hasattr(raw_hashtags, '__iter__') and not isinstance(raw_hashtags, str):
                # Iterable (list, numpy array, Series, etc.)
                try:
                    tweet_hashtags = list(raw_hashtags)
                except TypeError:
                    tweet_hashtags = []
            else:
                # Scalar: drop NaN/NA if possible
                try:
                    tweet_hashtags = [] if pd.isna(raw_hashtags) else [raw_hashtags]
                except Exception:
                    tweet_hashtags = [raw_hashtags]

            # media can also be non-scalar; we only want to clear obvious NaN
            raw_media = row.get('media', None)
            if isinstance(raw_media, float):
                # Handle float("nan") specially
                media_value = None if math.isnan(raw_media) else raw_media
            else:
                media_value = raw_media
            # ---------------------------------------------------

            # Create XContent object with ALL uploaded fields
            x_content = XContent(
                username=str(username),
                text=str(text),
                url=str(url),
                timestamp=timestamp,
                tweet_hashtags=tweet_hashtags,
                media=media_value,
                user_id=str(row.get('user_id', '')),
                # user_display_name is OPTIONAL (None when NaN/missing)
                user_display_name=str(row.get('user_display_name', '')) if pd.notna(row.get('user_display_name', None)) else None,
                user_verified=bool(row.get('user_verified', False)),
                tweet_id=str(row.get('tweet_id', '')),
                is_reply=bool(row.get('is_reply', False)),
                is_quote=bool(row.get('is_quote', False)),
                conversation_id=str(row.get('conversation_id', '')),
                in_reply_to_user_id=str(row.get('in_reply_to_user_id', '')),
                # Add missing engagement metrics that miners upload
                language=str(row.get('language', '')) if pd.notna(row.get('language', None)) else None,
                in_reply_to_username=str(row.get('in_reply_to_username', '')) if pd.notna(row.get('in_reply_to_username', None)) else None,
                quoted_tweet_id=str(row.get('quoted_tweet_id', '')) if pd.notna(row.get('quoted_tweet_id', None)) else None,
                like_count=int(row.get('like_count', 0)) if pd.notna(row.get('like_count', None)) else None,
                retweet_count=int(row.get('retweet_count', 0)) if pd.notna(row.get('retweet_count', None)) else None,
                reply_count=int(row.get('reply_count', 0)) if pd.notna(row.get('reply_count', None)) else None,
                quote_count=int(row.get('quote_count', 0)) if pd.notna(row.get('quote_count', None)) else None,
                view_count=int(row.get('view_count', 0)) if pd.notna(row.get('view_count', None)) else None,
                bookmark_count=int(row.get('bookmark_count', 0)) if pd.notna(row.get('bookmark_count', None)) else None,
                # Add missing user profile data that miners upload
                user_blue_verified=bool(row.get('user_blue_verified', False)) if pd.notna(row.get('user_blue_verified', None)) else None,
                user_description=str(row.get('user_description', '')) if pd.notna(row.get('user_description', None)) else None,
                user_location=str(row.get('user_location', '')) if pd.notna(row.get('user_location', None)) else None,
                profile_image_url=str(row.get('profile_image_url', '')) if pd.notna(row.get('profile_image_url', None)) else None,
                cover_picture_url=str(row.get('cover_picture_url', '')) if pd.notna(row.get('cover_picture_url', None)) else None,
                user_followers_count=int(row.get('user_followers_count', 0)) if pd.notna(row.get('user_followers_count', None)) else None,
                user_following_count=int(row.get('user_following_count', 0)) if pd.notna(row.get('user_following_count', None)) else None
            )

            return XContent.to_data_entity(x_content)
        except Exception:
            return None


    def _create_reddit_data_entity(self, row) -> Optional[DataEntity]:
        """Create Reddit DataEntity from parquet row"""
        try:
            # Get required fields
            username = row.get('username', '')
            body = row.get('body', row.get('text', ''))
            url = row.get('url', row.get('uri', ''))
            reddit_id = row.get('id', '')
            
            if not all([username, body, url, reddit_id]):
                return None
            
            # Get datetime from row - use as-is without rounding
            # The parquet already contains obfuscated datetime, and to_data_entity expects unrounded datetime
            datetime_val = row.get('datetime', row.get('createdAt', ''))
            if datetime_val and pd.notna(datetime_val):
                try:
                    created_at = pd.to_datetime(datetime_val)
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=dt.timezone.utc)
                except Exception:
                    created_at = dt.datetime.now(dt.timezone.utc)
            else:
                created_at = dt.datetime.now(dt.timezone.utc)
            
            # Create RedditContent object with ALL uploaded fields
            reddit_content = RedditContent(
                id=str(reddit_id),
                username=str(username),
                body=str(body),
                url=str(url),
                communityName=str(row.get('communityName', '')),
                createdAt=created_at,
                dataType=str(row.get('dataType', 'post')),
                parentId=str(row.get('parentId')) if row.get('parentId') and pd.notna(row.get('parentId')) else None,
                # Add missing fields that miners upload
                title=str(row.get('title', '')) if pd.notna(row.get('title')) else None,
                media=row.get('media', None) if pd.notna(row.get('media')) else None,
                is_nsfw=bool(row.get('is_nsfw', False)) if pd.notna(row.get('is_nsfw')) else None,
                score=int(row.get('score', 0)) if pd.notna(row.get('score')) else None,
                upvote_ratio=float(row.get('upvote_ratio', 0.0)) if pd.notna(row.get('upvote_ratio')) else None,
                num_comments=int(row.get('num_comments', 0)) if pd.notna(row.get('num_comments')) else None
            )
            
            return RedditContent.to_data_entity(reddit_content)
        except Exception:
            return None
    
async def get_miner_s3_validation_data(wallet, s3_auth_url: str, miner_hotkey: str) -> Optional[List[Dict]]:
    """Get S3 file data for a specific miner"""
    try:
        # Get miner-specific presigned URL
        hotkey = wallet.hotkey.ss58_address
        timestamp = int(time.time())
        commitment = f"s3:validator:miner:{miner_hotkey}:{timestamp}"
        signature = wallet.hotkey.sign(commitment.encode())
        signature_hex = signature.hex()

        payload = {
            "hotkey": hotkey,
            "timestamp": timestamp,
            "signature": signature_hex,
            "miner_hotkey": miner_hotkey
        }

        response = requests.post(
            f"{s3_auth_url}/get-miner-specific-access",
            json=payload,
            timeout=60  # Increased timeout for S3 auth
        )

        if response.status_code != 200:
            bt.logging.warning(f"Failed to get S3 access for {miner_hotkey}: {response.status_code}")
            return None

        access_data = response.json()
        miner_url = access_data.get('miner_url', '')
        
        if not miner_url:
            bt.logging.warning(f"No miner URL provided for {miner_hotkey}")
            return None

        # Parse S3 file list
        xml_response = requests.get(miner_url, timeout=60)  # Increased timeout for S3 data fetch
        
        if xml_response.status_code != 200:
            bt.logging.warning(f"Failed to get S3 file list for {miner_hotkey}: {xml_response.status_code}")
            return None

        return parse_s3_file_list(xml_response.text)
        
    except Exception as e:
        bt.logging.error(f"Error getting S3 validation data for {miner_hotkey}: {str(e)}")
        return None


def parse_s3_file_list(xml_content: str) -> List[Dict]:
    """Parse S3 XML response to extract file metadata"""
    try:
        root = ET.fromstring(xml_content)
        namespaces = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        
        files_data = []
        for content in root.findall('.//s3:Contents', namespaces):
            key = content.find('s3:Key', namespaces).text
            size = content.find('s3:Size', namespaces).text
            last_modified = content.find('s3:LastModified', namespaces).text
            
            if key and key.endswith('.parquet'):
                decoded_key = urllib.parse.unquote(key)
                files_data.append({
                    'key': decoded_key,
                    'size': int(size) if size else 0,
                    'last_modified': last_modified,
                    'job_id': extract_job_id_from_path(decoded_key)
                })
        
        return files_data
        
    except Exception as e:
        bt.logging.error(f"Error parsing S3 file list: {str(e)}")
        return []


def extract_job_id_from_path(file_path: str) -> str:
    """Extract job_id from S3 file path"""
    if '/job_id=' in file_path:
        job_part = file_path.split('/job_id=')[1]
        return job_part.split('/')[0]
    return "unknown"


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
    use_enhanced_validation: bool = False, config=None, s3_reader=None,
    use_duckdb_validation: bool = True, sample_percent: float = 10.0
) -> S3ValidationResult:
    """
    Comprehensive S3 validation using metadata analysis and statistical methods.
    Validates file structure, job alignment, and data quality indicators.

    Args:
        wallet: Validator wallet for S3 authentication
        s3_auth_url: S3 authentication service URL
        miner_hotkey: Target miner's hotkey
        use_enhanced_validation: If True, performs enhanced validation with real scrapers (legacy)
        config: Configuration object with enhanced validation settings
        s3_reader: Optional ValidatorS3Access instance for efficient file listing
        use_duckdb_validation: If True (default), use new DuckDB-based validation with competition scoring
        sample_percent: Percentage of files to sample for DuckDB validation (default 10%)

    Returns:
        S3ValidationResult with validation metrics including effective_size_bytes for competition
    """

    # Load expected jobs
    expected_jobs = load_expected_jobs_from_gravity()

    # NEW: Use DuckDB-based validation (default)
    if use_duckdb_validation:
        try:
            validator = DuckDBSampledValidator(
                wallet=wallet,
                s3_auth_url=s3_auth_url,
                s3_reader=s3_reader,
                sample_percent=sample_percent
            )
            duckdb_result = await validator.validate_miner_s3_data(
                miner_hotkey, expected_jobs
            )
            validator.close()

            # Convert to S3ValidationResult format
            return S3ValidationResult(
                is_valid=duckdb_result.is_valid,
                validation_percentage=duckdb_result.validation_percentage,
                job_count=duckdb_result.total_active_jobs,
                total_files=duckdb_result.recent_files_count,
                total_size_bytes=duckdb_result.total_size_bytes,
                valid_jobs=duckdb_result.recent_jobs_analyzed,
                recent_files=duckdb_result.recent_files_count,
                quality_metrics={
                    'duplicate_percentage': duckdb_result.duplicate_percentage,
                    'job_match_rate': duckdb_result.job_match_rate,
                    'scraper_success_rate': duckdb_result.scraper_success_rate
                },
                issues=duckdb_result.validation_issues,
                reason=duckdb_result.reason,
                enhanced_validation=duckdb_result,
                empty_file_detected=False,
                effective_size_bytes=duckdb_result.effective_size_bytes,
                job_coverage_rate=duckdb_result.job_coverage_rate
            )

        except Exception as e:
            bt.logging.error(f"DuckDB validation failed for {miner_hotkey}: {str(e)}, falling back to enhanced validation")
            # Fall through to enhanced validation

    if use_enhanced_validation:
        # Use enhanced validation with real scrapers (legacy)
        try:
            validator = S3Validator(s3_reader=s3_reader)
            enhanced_result = await validator.validate_miner_s3_data(
                wallet, s3_auth_url, miner_hotkey, expected_jobs
            )
            
            # Convert enhanced result to standard S3ValidationResult format
            return S3ValidationResult(
                is_valid=enhanced_result.is_valid,
                validation_percentage=enhanced_result.validation_percentage,
                job_count=enhanced_result.total_active_jobs,
                total_files=enhanced_result.recent_files_count,
                total_size_bytes=enhanced_result.total_size_bytes,
                valid_jobs=enhanced_result.recent_jobs_analyzed,
                recent_files=enhanced_result.recent_files_count,
                quality_metrics={
                    'duplicate_percentage': enhanced_result.duplicate_percentage,
                    'job_match_rate': enhanced_result.job_match_rate,
                    'scraper_success_rate': enhanced_result.scraper_success_rate
                },
                issues=enhanced_result.validation_issues,
                reason=enhanced_result.reason,
                enhanced_validation=enhanced_result,
                empty_file_detected=enhanced_result.empty_file_detected,
                effective_size_bytes=enhanced_result.effective_size_bytes,
                job_coverage_rate=enhanced_result.job_coverage_rate
            )
            
        except Exception as e:
            bt.logging.error(f"S3 validation with real scrapers failed for {miner_hotkey}: {str(e)}")
            # Fall back to basic validation
            
    # Get miner file data (basic validation)
    files_data = await get_miner_s3_validation_data(wallet, s3_auth_url, miner_hotkey)
    if not files_data:
        return S3ValidationResult(
            is_valid=False,
            validation_percentage=0.0,
            job_count=0,
            total_files=0,
            total_size_bytes=0,
            valid_jobs=0,
            recent_files=0,
            quality_metrics={},
            issues=["No S3 data accessible"],
            reason="Could not access miner S3 data"
        )
    
    # Perform basic analysis
    return analyze_miner_s3_data(files_data, expected_jobs, miner_hotkey)


def analyze_miner_s3_data(files_data: List[Dict], expected_jobs: Dict, miner_hotkey: str) -> S3ValidationResult:
    """Analyze miner S3 data using DuckDB for statistical validation"""
    
    issues = []
    quality_metrics = {}
    
    # Basic statistics
    total_files = len(files_data)
    total_size_bytes = sum(f['size'] for f in files_data)
    
    if total_files == 0:
        return S3ValidationResult(
            is_valid=False,
            validation_percentage=0.0,
            job_count=0,
            total_files=0,
            total_size_bytes=0,
            valid_jobs=0,
            recent_files=0,
            quality_metrics={},
            issues=["No files found"],
            reason="No files available for validation"
        )
    
    # Use DuckDB for advanced analysis
    conn = duckdb.connect()
    
    try:
        # Load data into DuckDB
        df = pd.DataFrame(files_data)
        conn.register('miner_files', df)
        
        # 1. File quality validation
        size_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_files,
                SUM(CASE WHEN size >= 1024 AND size <= 1073741824 THEN 1 ELSE 0 END) as size_valid_files,
                MIN(size) as min_size,
                MAX(size) as max_size,
                ROUND(AVG(size), 0) as avg_size
            FROM miner_files
        """).fetchone()
        
        total, size_valid, min_size, max_size, avg_size = size_stats
        file_quality_score = (size_valid / total * 100) if total > 0 else 0
        quality_metrics['file_quality'] = file_quality_score
        
        # 2. Temporal validation
        temporal_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_files,
                SUM(CASE WHEN CAST(last_modified AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '30 days' THEN 1 ELSE 0 END) as recent_30d,
                SUM(CASE WHEN CAST(last_modified AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '7 days' THEN 1 ELSE 0 END) as recent_7d
            FROM miner_files
        """).fetchone()
        
        total, recent_30d, recent_7d = temporal_stats
        temporal_quality_score = (recent_30d / total * 100) if total > 0 else 0
        quality_metrics['temporal_quality'] = temporal_quality_score
        recent_files = recent_30d
        
        # 3. Job-based validation
        job_stats = conn.execute("""
            SELECT 
                job_id,
                COUNT(*) as file_count,
                SUM(size) as job_size_bytes
            FROM miner_files
            WHERE job_id != 'unknown'
            GROUP BY job_id
            ORDER BY file_count DESC
        """).fetchall()
        
        job_count = len(job_stats)
        unique_jobs = len(set(f['job_id'] for f in files_data if f['job_id'] != 'unknown'))
        avg_files_per_job = total_files / job_count if job_count > 0 else 0
        
        # Job presence validation - if we find any jobs, consider it valid
        valid_jobs = job_count  # Any job found is considered valid
        job_validity_score = 100.0 if job_count > 0 else 0.0
        quality_metrics['job_validity'] = job_validity_score
        
        # 4. Distribution analysis
        distribution_score = min(100.0, avg_files_per_job * 5) if avg_files_per_job > 0 else 0
        quality_metrics['distribution'] = distribution_score
        
        # 5. Calculate overall validation score
        scores = [
            ('file_quality', file_quality_score, 0.3),
            ('temporal_quality', temporal_quality_score, 0.2),
            ('job_validity', job_validity_score, 0.3),
            ('distribution', distribution_score, 0.2)
        ]
        
        weighted_sum = sum(score * weight for name, score, weight in scores)
        total_weight = sum(weight for name, score, weight in scores)
        validation_percentage = weighted_sum / total_weight if total_weight > 0 else 0
        
        # 6. Validation decision criteria - simplified
        is_valid = (
            validation_percentage >= 60.0 and
            job_count > 0 and  # Must have at least one job
            recent_files >= 10
        )
        
        # Collect issues
        if validation_percentage < 60.0:
            issues.append(f"Overall quality score: {validation_percentage:.1f}%")
        if job_count == 0:
            issues.append("No jobs found")
        if recent_files < 10:
            issues.append(f"Recent files: {recent_files}")

        reason = f"S3 validation: {validation_percentage:.1f}% quality, {job_count} jobs found, {recent_files} recent files"

        # Calculate job coverage and effective_size for competition scoring
        expected_jobs_count = len(expected_jobs) if expected_jobs else 1
        job_coverage_rate = min(100.0, (job_count / expected_jobs_count) * 100) if expected_jobs_count > 0 else 0.0
        coverage_ratio = job_coverage_rate / 100.0

        if is_valid:
            effective_size_bytes = total_size_bytes * (coverage_ratio ** 2)
        else:
            effective_size_bytes = 0.0

        bt.logging.info(
            f"Basic validation competition scoring: size={total_size_bytes/(1024*1024):.1f}MB, "
            f"coverage={job_coverage_rate:.1f}%, effective_size={effective_size_bytes/(1024*1024):.1f}MB"
        )

        return S3ValidationResult(
            is_valid=is_valid,
            validation_percentage=validation_percentage,
            job_count=job_count,
            total_files=total_files,
            total_size_bytes=total_size_bytes,
            valid_jobs=valid_jobs,
            recent_files=recent_files,
            quality_metrics=quality_metrics,
            issues=issues,
            reason=reason,
            effective_size_bytes=effective_size_bytes,
            job_coverage_rate=job_coverage_rate
        )
        
    except Exception as e:
        bt.logging.error(f"Error in S3 data analysis for {miner_hotkey}: {str(e)}")
        return S3ValidationResult(
            is_valid=False,
            validation_percentage=0.0,
            job_count=0,
            total_files=0,
            total_size_bytes=0,
            valid_jobs=0,
            recent_files=0,
            quality_metrics={},
            issues=[f"Analysis error: {str(e)}"],
            reason=f"S3 validation failed: {str(e)}"
        )
    finally:
        conn.close()


def get_s3_validation_summary(result: S3ValidationResult) -> str:
    """Generate a summary string for S3 validation result with detailed breakdown"""

    # Get quality metrics if available
    dup_rate = result.quality_metrics.get('duplicate_percentage', 0)
    job_match = result.quality_metrics.get('job_match_rate', 0)
    scraper_rate = result.quality_metrics.get('scraper_success_rate', 0)

    size_mb = result.total_size_bytes / (1024 * 1024)

    if result.is_valid:
        return (
            f"✅ S3 PASSED ({result.validation_percentage:.1f}%): "
            f"{result.job_count} jobs, {result.total_files} files ({size_mb:.1f}MB) | "
            f"Dup: {dup_rate:.1f}%, JobMatch: {job_match:.1f}%, Scraper: {scraper_rate:.1f}%"
        )
    else:
        # Show breakdown even on failure so miners know what to fix
        return (
            f"❌ S3 FAILED ({result.validation_percentage:.1f}%): "
            f"{result.job_count} jobs, {result.total_files} files ({size_mb:.1f}MB) | "
            f"Dup: {dup_rate:.1f}%, JobMatch: {job_match:.1f}%, Scraper: {scraper_rate:.1f}% | "
            f"Issues: {', '.join(result.issues[:3])}"
        )
