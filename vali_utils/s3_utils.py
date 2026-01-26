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
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from scraping.provider import ScraperProvider
from scraping.scraper import ScraperId, ValidationResult
from scraping.x.model import XContent
from scraping.reddit.model import RedditContent
from common.data import DataEntity, DataSource


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
                params = job_config.get('params', {}) if isinstance(job_config, dict) else {}
                platform = params.get('platform', 'unknown').lower()

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
                miner_hotkey, sampled_files, expected_jobs, presigned_urls, num_entities=10
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
                    f"scraper={scraper_success_rate:.1f}% (min {self.MIN_SCRAPER_SUCCESS}%)"
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
        """Get presigned URLs in batches"""

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
            # Twitter: empty if text is empty (simple check - hashtags/media are bonus)
            empty_check = "COALESCE(text, '') = ''"
        else:
            # Reddit: empty if body is empty (simple check)
            empty_check = "COALESCE(body, '') = ''"

        try:
            query = f"""
                WITH raw_data AS (
                    SELECT
                        url as entity_url,
                        filename,
                        CASE WHEN {empty_check} THEN 1 ELSE 0 END as is_empty,
                        CASE WHEN COALESCE(url, '') = '' THEN 1 ELSE 0 END as is_missing_url
                    FROM read_parquet([{url_list_str}], filename=true, union_by_name=true)
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

            try:
                df = pd.read_parquet(presigned_url)
                if len(df) == 0:
                    continue

                for _, row in df.head(3).iterrows():
                    entity = self._create_data_entity(row, platform)
                    if entity:
                        all_entities.append((entity, platform, job_id))
            except:
                continue

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
            url = row.get('url', row.get('uri', ''))
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

            # Handle media field (can be NaN)
            raw_media = row.get('media', None)
            if isinstance(raw_media, float):
                media_value = None if math.isnan(raw_media) else raw_media
            else:
                media_value = raw_media

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
                user_following_count=int(row.get('user_following_count', 0)) if pd.notna(row.get('user_following_count', None)) else None
            )
            return XContent.to_data_entity(x_content)
        except Exception:
            return None

    def _create_reddit_entity(self, row) -> Optional[DataEntity]:
        """Create Reddit DataEntity. Accept if title OR body is present."""
        try:
            username = row.get('username', '')
            url = row.get('url', row.get('uri', ''))
            reddit_id = row.get('id', '')

            body = row.get('body', row.get('text', ''))
            title = row.get('title', '')

            username_str = str(username).strip() if username is not None else ""
            url_str = str(url).strip() if url is not None else ""
            reddit_id_str = str(reddit_id).strip() if reddit_id is not None else ""

            body_str = str(body).strip() if body is not None else ""
            title_str = str(title).strip() if title is not None else ""

            # Accept if either title or body has content (media posts have empty body)
            if not body_str and not title_str:
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
                num_comments=int(row.get('num_comments', 0)) if pd.notna(row.get('num_comments')) else None
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
        bt.logging.error(f"DuckDB validation failed for {miner_hotkey}: {str(e)}")
        return S3ValidationResult(
            is_valid=False,
            validation_percentage=0.0,
            job_count=0,
            total_files=0,
            total_size_bytes=0,
            valid_jobs=0,
            recent_files=0,
            quality_metrics={},
            issues=[f"Validation error: {str(e)}"],
            reason=f"Validation failed: {str(e)}"
        )


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
