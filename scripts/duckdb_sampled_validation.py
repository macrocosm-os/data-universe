#!/usr/bin/env python3
"""
DuckDB Sampled Validation for S3 Competition

This script validates miners with random sampling and prepares data for competition:
1. Lists ALL files for each miner (to get total size/file count)
2. Randomly samples X% of files for DuckDB validation
3. Runs quality checks (duplicates within job, empty content, scraper validation)
4. Outputs competition-ready metrics for miner comparison

Key metrics for competition:
- total_size_bytes: Actual S3 storage size (from file listing, no reading needed)
- jobs_covered: Number of active jobs with data
- validation_passed: Quality gate (duplicates, empty, scraper)

Usage:
    python scripts/duckdb_sampled_validation.py \
        --num-miners 10 \
        --sample-percent 10 \
        --wallet-name "validator" \
        --wallet-hotkey "default"
"""

import argparse
import asyncio
import json
import math
import os
import sys
import time
import random
import requests
import pandas as pd
import datetime as dt
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bittensor as bt
from common.utils import get_miner_uids
from common.api_client import DataUniverseApiClient
from common.data import DataEntity, DataSource

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    print("ERROR: DuckDB required. Install with: pip install duckdb")
    sys.exit(1)

from vali_utils.validator_s3_access import ValidatorS3Access
from scraping.provider import ScraperProvider
from scraping.scraper import ScraperId, ValidationResult
from scraping.x.model import XContent
from scraping.reddit.model import RedditContent


# Scraper mapping (same as s3_utils.py)
PREFERRED_SCRAPERS = {
    DataSource.X: ScraperId.X_APIDOJO,
    DataSource.REDDIT: ScraperId.REDDIT_MC,
}


@dataclass
class MinerCompetitionResult:
    """Result for miner competition comparison"""
    miner_hotkey: str
    uid: int
    timestamp: str

    # Full metrics (from listing ALL files - no reading needed)
    total_files: int = 0
    total_size_bytes: int = 0
    jobs_in_s3: int = 0
    active_jobs_covered: int = 0
    job_coverage_rate: float = 0.0

    # Sample metrics
    sample_percent: float = 0.0
    files_sampled: int = 0
    sample_rows: int = 0

    # DuckDB validation metrics (quality gate)
    duplicate_rate_within_job: float = 0.0
    empty_content_rate: float = 0.0
    missing_uri_rate: float = 0.0

    # Job content matching metrics
    job_match_checked: int = 0
    job_match_passed: int = 0
    job_match_rate: float = 0.0

    # Scraper validation metrics
    entities_validated: int = 0
    entities_passed: int = 0
    scraper_success_rate: float = 0.0

    # Validation result
    validation_passed: bool = False
    issues: List[str] = field(default_factory=list)

    # Competition score: effective_size = total_size × coverage²
    effective_size_bytes: float = 0.0

    # Timing
    total_time_ms: float = 0.0


class DuckDBSampledValidator:
    """
    Validates miners using DuckDB with random sampling.
    Designed for competition comparison between miners.
    """

    # Validation thresholds
    MAX_DUPLICATE_RATE = 5.0    # 5% max duplicates within same job
    MAX_EMPTY_RATE = 10.0       # 10% max empty content
    MAX_MISSING_URI_RATE = 5.0  # 5% max missing URIs
    MIN_JOB_MATCH_RATE = 95.0   # 95% min job content match rate
    MIN_SCRAPER_SUCCESS = 80.0  # 80% min scraper success rate

    def __init__(
        self,
        wallet: bt.wallet,
        s3_auth_url: str,
        sample_percent: float = 10.0,
        debug: bool = False
    ):
        self.wallet = wallet
        self.s3_auth_url = s3_auth_url
        self.sample_percent = sample_percent
        self.debug = debug

        self.s3_reader = ValidatorS3Access(
            wallet=wallet,
            s3_auth_url=s3_auth_url,
            debug=debug
        )

        self.scraper_provider = ScraperProvider()

        self.conn = duckdb.connect(':memory:')
        self._setup_duckdb()

    def _setup_duckdb(self):
        """Configure DuckDB for HTTP parquet reading"""
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        self.conn.execute("SET http_timeout=120000;")
        self.conn.execute("SET enable_progress_bar=false;")
        self._log("DuckDB configured with httpfs")

    def _log(self, msg: str):
        if self.debug:
            print(f"[DEBUG] {msg}")

    async def get_presigned_urls_batch(
        self,
        miner_hotkey: str,
        file_keys: List[str],
        batch_size: int = 500
    ) -> Dict[str, str]:
        """Get presigned URLs in batches"""
        all_urls = {}
        total_batches = (len(file_keys) + batch_size - 1) // batch_size

        for i in range(0, len(file_keys), batch_size):
            batch = file_keys[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            self._log(f"  Batch {batch_num}/{total_batches}: {len(batch)} files")

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
                self._log(f"  Batch {batch_num} error: {e}")

        return all_urls

    def validate_sample_with_duckdb(
        self,
        urls_by_job: Dict[str, Dict[str, List[str]]]
    ) -> Dict[str, Any]:
        """
        Run DuckDB validation on sampled files.

        Args:
            urls_by_job: Dict[job_id -> {'platform': str, 'urls': List[str]}]

        Returns metrics for quality gate.
        Duplicate check is WITHIN SAME JOB only (cross-job duplicates OK).
        """
        total_rows = 0
        total_duplicates_within_job = 0
        total_with_url = 0
        empty_content = 0
        missing_url = 0

        try:
            for job_id, job_data in urls_by_job.items():
                platform = job_data['platform']
                urls = job_data['urls']

                if not urls:
                    continue

                url_list_str = ", ".join([f"'{url}'" for url in urls])

                # Content column varies by platform
                if platform in ['x', 'twitter']:
                    content_col = 'text'
                elif platform == 'reddit':
                    content_col = 'body'
                else:
                    content_col = 'text'

                self._log(f"Job {job_id[:8]}... ({platform}): {len(urls)} files")

                # Query 1: Count rows and duplicates WITHIN THIS JOB
                try:
                    dup_result = self.conn.execute(f"""
                        WITH all_data AS (
                            SELECT url as entity_url
                            FROM read_parquet([{url_list_str}])
                        ),
                        url_counts AS (
                            SELECT entity_url, COUNT(*) as cnt
                            FROM all_data
                            WHERE entity_url IS NOT NULL AND entity_url != ''
                            GROUP BY entity_url
                        )
                        SELECT
                            (SELECT COUNT(*) FROM all_data) as total_rows,
                            COUNT(*) as unique_urls,
                            SUM(CASE WHEN cnt > 1 THEN cnt - 1 ELSE 0 END) as duplicate_count
                        FROM url_counts
                    """).fetchone()

                    job_rows = dup_result[0] or 0
                    job_duplicates = dup_result[2] or 0

                    total_rows += job_rows
                    total_duplicates_within_job += job_duplicates
                    total_with_url += dup_result[1] or 0

                except Exception as e:
                    self._log(f"  Duplicate query error for job {job_id[:8]}: {e}")

                # Query 2: Content quality
                try:
                    quality_result = self.conn.execute(f"""
                        SELECT
                            SUM(CASE WHEN COALESCE({content_col}, '') = '' THEN 1 ELSE 0 END) as empty_content,
                            SUM(CASE WHEN COALESCE(url, '') = '' THEN 1 ELSE 0 END) as missing_url
                        FROM read_parquet([{url_list_str}])
                    """).fetchone()

                    empty_content += quality_result[0] or 0
                    missing_url += quality_result[1] or 0

                except Exception as e:
                    self._log(f"  Quality query error for job {job_id[:8]}: {e}")

            return {
                "success": True,
                "total_rows": total_rows,
                "duplicate_count_within_job": total_duplicates_within_job,
                "duplicate_rate_within_job": (total_duplicates_within_job / total_rows * 100) if total_rows > 0 else 0,
                "empty_content": empty_content,
                "empty_rate": (empty_content / total_rows * 100) if total_rows > 0 else 0,
                "missing_url": missing_url,
                "missing_url_rate": (missing_url / total_rows * 100) if total_rows > 0 else 0,
            }

        except Exception as e:
            self._log(f"DuckDB error: {e}")
            return {"success": False, "error": str(e)}

    async def perform_scraper_validation(
        self,
        miner_hotkey: str,
        sampled_files: List[Dict],
        expected_jobs: Dict[str, Dict],
        presigned_urls: Dict[str, str],
        num_entities: int = 10
    ) -> Dict[str, Any]:
        """
        Validate random entities using real scrapers (same as s3_utils.py).

        Args:
            sampled_files: List of sampled file metadata
            expected_jobs: Job configs with platform info
            presigned_urls: Mapping of file_key -> presigned_url
            num_entities: Number of entities to validate

        Returns:
            Dict with scraper validation results
        """
        all_entities = []

        # Collect entities from sampled files
        for file_info in sampled_files:
            if len(all_entities) >= num_entities * 2:  # Get extra to ensure enough
                break

            file_key = file_info['key']
            presigned_url = presigned_urls.get(file_key)

            if not presigned_url:
                continue

            # Extract job_id and platform
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

                # Sample rows from this file
                sample_size = min(3, len(df))
                sample_df = df.head(sample_size)

                for _, row in sample_df.iterrows():
                    entity = self._create_data_entity_from_row(row, platform)
                    if entity:
                        all_entities.append((entity, platform, job_id))

                    if len(all_entities) >= num_entities * 2:
                        break

            except Exception as e:
                self._log(f"Error reading file for scraper validation: {e}")
                continue

        if not all_entities:
            return {
                'entities_validated': 0,
                'entities_passed': 0,
                'success_rate': 0,
                'error': 'No entities found for validation'
            }

        # Select random entities for validation
        entities_to_validate = random.sample(
            all_entities, min(num_entities, len(all_entities))
        )

        # Log URIs being validated
        entity_uris = [entity[0].uri for entity in entities_to_validate]
        print(f"    Validating URIs: {entity_uris[:5]}...")

        # Group by platform for efficient validation
        entities_by_platform = {}
        for entity, platform, job_id in entities_to_validate:
            if platform not in entities_by_platform:
                entities_by_platform[platform] = []
            entities_by_platform[platform].append((entity, job_id))

        # Validate with real scrapers
        total_validated = 0
        total_passed = 0
        sample_results = []

        for platform, platform_entities in entities_by_platform.items():
            entities_only = [e[0] for e in platform_entities]

            try:
                validation_results = await self._validate_entities_with_scraper(
                    entities_only, platform
                )

                for i, result in enumerate(validation_results):
                    total_validated += 1
                    job_id = platform_entities[i][1] if i < len(platform_entities) else "unknown"

                    if result.is_valid:
                        total_passed += 1
                        sample_results.append(f"✅ {platform} ({job_id[:8]}): {result.reason}")
                    else:
                        sample_results.append(f"❌ {platform} ({job_id[:8]}): {result.reason}")

            except Exception as e:
                print(f"    Scraper error for {platform}: {e}")
                total_validated += len(entities_only)
                for _ in entities_only:
                    sample_results.append(f"❌ {platform}: Scraper error - {e}")

        success_rate = (total_passed / total_validated * 100) if total_validated > 0 else 0

        return {
            'entities_validated': total_validated,
            'entities_passed': total_passed,
            'success_rate': success_rate,
            'sample_results': sample_results
        }

    async def _validate_entities_with_scraper(
        self, entities: List[DataEntity], platform: str
    ) -> List[ValidationResult]:
        """Validate entities using appropriate scraper"""
        try:
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

            scraper = self.scraper_provider.get(PREFERRED_SCRAPERS[data_source])
            return await scraper.validate(entities)

        except Exception as e:
            return [ValidationResult(
                is_valid=False,
                reason=f"Scraper error: {str(e)}",
                content_size_bytes_validated=0
            ) for _ in entities]

    def _create_data_entity_from_row(self, row, platform: str) -> Optional[DataEntity]:
        """Create DataEntity from parquet row based on platform"""
        try:
            if platform in ['x', 'twitter']:
                return self._create_twitter_data_entity(row)
            elif platform == 'reddit':
                return self._create_reddit_data_entity(row)
            return None
        except Exception:
            return None

    def _create_twitter_data_entity(self, row) -> Optional[DataEntity]:
        """Create Twitter DataEntity from parquet row"""
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

            # Handle tweet_hashtags
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

            x_content = XContent(
                username=str(username),
                text=str(text),
                url=str(url),
                timestamp=timestamp,
                tweet_hashtags=tweet_hashtags,
            )

            return XContent.to_data_entity(x_content)
        except Exception:
            return None

    def _create_reddit_data_entity(self, row) -> Optional[DataEntity]:
        """Create Reddit DataEntity from parquet row"""
        try:
            username = row.get('username', '')
            body = row.get('body', row.get('text', ''))
            url = row.get('url', row.get('uri', ''))
            reddit_id = row.get('id', '')

            if not all([username, body, url, reddit_id]):
                return None

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
        except Exception:
            return None

    async def perform_job_content_matching(
        self,
        sampled_files: List[Dict],
        expected_jobs: Dict[str, Dict],
        presigned_urls: Dict[str, str],
        samples_per_file: int = 10
    ) -> Dict[str, Any]:
        """
        Check if data in each job folder matches the job requirements.

        Checks:
        - Label matching (hashtag for X, subreddit for Reddit)
        - Keyword matching (keyword in text/body)
        - Time window (entity datetime within job's start/end)

        Same logic as s3_utils.py _perform_job_content_matching
        """
        total_checked = 0
        total_matched = 0
        mismatch_samples = []

        # Group sampled files by job
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

            # Parse time window
            job_start_dt = self._parse_datetime(job_start_date)
            job_end_dt = self._parse_datetime(job_end_date)

            has_label_requirement = bool(job_label and str(job_label).strip())
            has_keyword_requirement = bool(job_keyword and str(job_keyword).strip())
            has_time_requirement = bool(job_start_dt or job_end_dt)

            # Sample files from this job
            sample_files = random.sample(files, min(2, len(files)))

            for file_info in sample_files:
                file_key = file_info['key']
                presigned_url = presigned_urls.get(file_key)
                if not presigned_url:
                    continue

                try:
                    df = pd.read_parquet(presigned_url)
                    if len(df) == 0:
                        continue

                    # Sample rows
                    sample_df = df.head(min(samples_per_file, len(df)))

                    for _, row in sample_df.iterrows():
                        total_checked += 1
                        label_matches = True
                        keyword_matches = True
                        time_matches = True

                        # Label matching
                        if has_label_requirement:
                            job_label_normalized = str(job_label).lower().strip()

                            if platform in ['x', 'twitter']:
                                # Check hashtags
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
                                label_without_hash = job_label_normalized.lstrip('#')
                                label_with_hash = f"#{label_without_hash}"

                                entity_label = str(row.get('label', '')).lower().strip()
                                label_matches = (
                                    label_with_hash in hashtags_lower or
                                    label_without_hash in hashtags_lower or
                                    entity_label == label_with_hash or
                                    entity_label == label_without_hash
                                )

                            elif platform == 'reddit':
                                entity_label = str(row.get('communityName', row.get('label', ''))).lower().strip()
                                label_matches = (
                                    entity_label == job_label_normalized or
                                    entity_label == f"r/{job_label_normalized.removeprefix('r/')}" or
                                    entity_label.removeprefix('r/') == job_label_normalized.removeprefix('r/')
                                )

                        # Keyword matching
                        if has_keyword_requirement:
                            job_keyword_normalized = str(job_keyword).lower().strip()

                            if platform == 'reddit':
                                body = str(row.get('body', '')).lower()
                                title = str(row.get('title', '')).lower()
                                keyword_matches = (job_keyword_normalized in body or job_keyword_normalized in title)
                            else:  # X/Twitter
                                text = str(row.get('text', '')).lower()
                                keyword_matches = job_keyword_normalized in text

                        # Time window matching
                        if has_time_requirement:
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

                        # Combine
                        matches_job = True
                        if has_label_requirement and not label_matches:
                            matches_job = False
                        if has_keyword_requirement and not keyword_matches:
                            matches_job = False
                        if has_time_requirement and not time_matches:
                            matches_job = False

                        if matches_job:
                            total_matched += 1
                        else:
                            if len(mismatch_samples) < 5:
                                uri = row.get('url', row.get('uri', 'unknown'))
                                mismatch_samples.append(
                                    f"Job {job_id[:8]}: label={label_matches} keyword={keyword_matches} time={time_matches} - {uri}"
                                )

                except Exception as e:
                    self._log(f"Error checking job match for {file_key}: {e}")
                    continue

        match_rate = (total_matched / total_checked * 100) if total_checked > 0 else 0

        return {
            'total_checked': total_checked,
            'total_matched': total_matched,
            'match_rate': match_rate,
            'mismatch_samples': mismatch_samples
        }

    def _parse_datetime(self, dt_val) -> Optional[dt.datetime]:
        """Parse datetime from job config"""
        if dt_val is None:
            return None
        try:
            if isinstance(dt_val, dt.datetime):
                if dt_val.tzinfo is None:
                    return dt_val.replace(tzinfo=dt.timezone.utc)
                return dt_val
            parsed = pd.to_datetime(dt_val, utc=True)
            return parsed.to_pydatetime()
        except:
            return None

    async def validate_miner(
        self,
        miner_hotkey: str,
        uid: int,
        expected_jobs: Dict[str, Dict]
    ) -> MinerCompetitionResult:
        """
        Validate a single miner with random sampling.
        Returns competition-ready metrics.
        """
        result = MinerCompetitionResult(
            miner_hotkey=miner_hotkey,
            uid=uid,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
            sample_percent=self.sample_percent
        )

        total_start = time.perf_counter()

        print(f"\n{'='*60}")
        print(f"MINER: UID {uid} | {miner_hotkey[:20]}...")
        print(f"{'='*60}")

        # Step 1: List ALL files (for size - no reading needed)
        print(f"\n[1] Listing ALL files...")
        try:
            all_files = await self.s3_reader.list_all_files_with_metadata(miner_hotkey)
        except Exception as e:
            result.issues.append(f"Error listing files: {e}")
            return result

        if not all_files:
            result.issues.append("No files found")
            print(f"  No files found!")
            return result

        result.total_files = len(all_files)

        # Group by job
        files_by_job = {}
        for f in all_files:
            key = f.get('key', '')
            if '/job_id=' in key:
                job_id = key.split('/job_id=')[1].split('/')[0]
                if job_id not in files_by_job:
                    files_by_job[job_id] = []
                files_by_job[job_id].append(f)

        result.jobs_in_s3 = len(files_by_job)
        active_job_ids = [jid for jid in files_by_job.keys() if jid in expected_jobs]
        result.active_jobs_covered = len(active_job_ids)
        result.job_coverage_rate = (result.active_jobs_covered / len(expected_jobs) * 100) if expected_jobs else 0

        # Calculate size from ACTIVE jobs only (not old/expired jobs)
        result.total_size_bytes = 0
        for job_id in active_job_ids:
            for f in files_by_job[job_id]:
                result.total_size_bytes += f.get('size', 0)

        print(f"  Total files (all): {result.total_files:,}")
        print(f"  Jobs in S3: {result.jobs_in_s3} (active: {result.active_jobs_covered})")
        print(f"  Active job size: {result.total_size_bytes / (1024*1024):.1f} MB")
        print(f"  Job coverage: {result.active_jobs_covered}/{len(expected_jobs)} ({result.job_coverage_rate:.1f}%)")

        # Step 2: Random sample files from active jobs
        print(f"\n[2] Random sampling {self.sample_percent}% of files...")

        active_files = []
        for job_id in active_job_ids:
            active_files.extend([(f, job_id) for f in files_by_job[job_id]])

        if not active_files:
            result.issues.append("No files in active jobs")
            print(f"  No files in active jobs!")
            return result

        # Random sample
        sample_count = max(10, int(len(active_files) * self.sample_percent / 100))
        sample_count = min(sample_count, len(active_files))

        sampled_files_with_job = random.sample(active_files, sample_count)
        sampled_files = [f for f, _ in sampled_files_with_job]
        result.files_sampled = len(sampled_files)

        print(f"  Active job files: {len(active_files):,}")
        print(f"  Sampled: {result.files_sampled:,} files")

        # Step 3: Get presigned URLs for sample
        print(f"\n[3] Getting presigned URLs...")
        file_keys = [f['key'] for f in sampled_files]
        presigned_urls = await self.get_presigned_urls_batch(miner_hotkey, file_keys)

        if not presigned_urls:
            result.issues.append("Failed to get presigned URLs")
            return result

        print(f"  URLs received: {len(presigned_urls)}")

        # Group by job for DuckDB validation (duplicates within same job)
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
        print(f"\n[4] DuckDB validation (per-job duplicate check)...")
        validation = self.validate_sample_with_duckdb(urls_by_job)

        if not validation.get("success"):
            result.issues.append(f"DuckDB error: {validation.get('error')}")
            return result

        result.sample_rows = validation["total_rows"]
        result.duplicate_rate_within_job = validation["duplicate_rate_within_job"]
        result.empty_content_rate = validation["empty_rate"]
        result.missing_uri_rate = validation["missing_url_rate"]

        print(f"  Sample rows: {result.sample_rows:,}")
        print(f"  Duplicate rate (within job): {result.duplicate_rate_within_job:.2f}%")
        print(f"  Empty content rate: {result.empty_content_rate:.2f}%")
        print(f"  Missing URI rate: {result.missing_uri_rate:.2f}%")

        # Step 5: Job content matching (data matches job requirements)
        print(f"\n[5] Job content matching (label/keyword/time)...")
        job_match_result = await self.perform_job_content_matching(
            sampled_files, expected_jobs, presigned_urls
        )

        result.job_match_checked = job_match_result['total_checked']
        result.job_match_passed = job_match_result['total_matched']
        result.job_match_rate = job_match_result['match_rate']

        print(f"  Entities checked: {result.job_match_checked}")
        print(f"  Entities matched job: {result.job_match_passed}")
        print(f"  Job match rate: {result.job_match_rate:.1f}%")

        if job_match_result['mismatch_samples']:
            print(f"  Mismatches:")
            for ms in job_match_result['mismatch_samples'][:3]:
                print(f"    {ms}")

        # Step 6: Scraper validation (real API calls)
        print(f"\n[6] Scraper validation (real API verification)...")
        scraper_result = await self.perform_scraper_validation(
            miner_hotkey, sampled_files, expected_jobs, presigned_urls, num_entities=10
        )

        result.entities_validated = scraper_result['entities_validated']
        result.entities_passed = scraper_result['entities_passed']
        result.scraper_success_rate = scraper_result['success_rate']

        print(f"  Entities validated: {result.entities_validated}")
        print(f"  Entities passed: {result.entities_passed}")
        print(f"  Scraper success rate: {result.scraper_success_rate:.1f}%")

        if 'sample_results' in scraper_result:
            for sr in scraper_result['sample_results'][:5]:
                print(f"    {sr}")

        # Step 7: Validation decision
        print(f"\n[7] Validation decision...")

        issues = []
        if result.duplicate_rate_within_job > self.MAX_DUPLICATE_RATE:
            issues.append(f"High duplicates within job: {result.duplicate_rate_within_job:.1f}%")
        if result.empty_content_rate > self.MAX_EMPTY_RATE:
            issues.append(f"High empty content: {result.empty_content_rate:.1f}%")
        if result.missing_uri_rate > self.MAX_MISSING_URI_RATE:
            issues.append(f"High missing URIs: {result.missing_uri_rate:.1f}%")
        if result.job_match_rate < self.MIN_JOB_MATCH_RATE:
            issues.append(f"Low job match rate: {result.job_match_rate:.1f}%")
        if result.scraper_success_rate < self.MIN_SCRAPER_SUCCESS:
            issues.append(f"Low scraper success: {result.scraper_success_rate:.1f}%")

        result.issues = issues
        result.validation_passed = len(issues) == 0

        # Calculate effective size for competition: size × coverage²
        # This penalizes miners who only cover few jobs
        coverage_ratio = result.job_coverage_rate / 100.0  # 0-1
        coverage_factor = coverage_ratio ** 2  # Squared penalty

        if result.validation_passed:
            result.effective_size_bytes = result.total_size_bytes * coverage_factor
        else:
            result.effective_size_bytes = 0  # Failed validation = no competition score

        result.total_time_ms = (time.perf_counter() - total_start) * 1000

        # Summary
        status = "✅ PASS" if result.validation_passed else "❌ FAIL"
        print(f"\n  Status: {status}")
        print(f"  Effective size: {result.effective_size_bytes/(1024*1024):.1f} MB (size × coverage² = {result.total_size_bytes/(1024*1024):.1f} × {coverage_factor:.3f})")
        if issues:
            print(f"  Issues: {', '.join(issues)}")
        print(f"  Time: {result.total_time_ms:.0f}ms")

        return result

    def close(self):
        self.conn.close()


# Constants
IGNORED_UIDS = {162}  # Subnet owner
DATA_UNIVERSE_API_URL = "https://data-universe-api.api.macrocosmos.ai"


async def fetch_active_jobs(wallet: bt.wallet) -> Dict[str, Dict]:
    """Fetch active jobs from API"""
    print(f"\nFetching active jobs from API...")

    async with DataUniverseApiClient(
        base_url=DATA_UNIVERSE_API_URL,
        keypair=wallet.hotkey,
        timeout=60,
    ) as client:
        dd_list = await client.validator_get_latest_dd_list()

    jobs_dict = {}
    platform_counts = {'reddit': 0, 'x': 0}

    for entry in dd_list.entries:
        platform = entry.platform.lower() if entry.platform else 'other'
        jobs_dict[entry.id] = {
            'id': entry.id,
            'platform': platform,
            'label': entry.label,
            'keyword': entry.keyword,
            'post_start_datetime': entry.post_start_datetime,
            'post_end_datetime': entry.post_end_datetime,
        }
        if platform in platform_counts:
            platform_counts[platform] += 1

    print(f"  Active jobs: {len(jobs_dict)} (Reddit: {platform_counts['reddit']}, X: {platform_counts['x']})")
    return jobs_dict


def get_miners_from_metagraph(num_miners: int = 10, netuid: int = 13) -> List[Dict]:
    """Load miners sorted by incentive"""
    print(f"\nLoading metagraph...")
    subtensor = bt.subtensor(network="finney")
    metagraph = subtensor.metagraph(netuid=netuid)

    miner_uids = get_miner_uids(metagraph, vpermit_rao_limit=10_000)
    miner_uids = [uid for uid in miner_uids if uid not in IGNORED_UIDS]

    miners = []
    for uid in miner_uids:
        incentive = float(metagraph.incentive[uid])
        if incentive > 0:
            miners.append({
                'uid': uid,
                'hotkey': metagraph.hotkeys[uid],
                'incentive': incentive,
            })

    miners.sort(key=lambda x: x['incentive'], reverse=True)
    print(f"  Found {len(miners)} miners with incentive > 0")

    return miners[:num_miners]


async def main():
    parser = argparse.ArgumentParser(description="DuckDB Sampled Validation for Competition")
    parser.add_argument("--num-miners", type=int, default=10)
    parser.add_argument("--sample-percent", type=float, default=10.0,
                        help="Percent of files to sample (default: 10%)")
    parser.add_argument("--wallet-name", type=str, default="default")
    parser.add_argument("--wallet-hotkey", type=str, default="default")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    # Load wallet
    print(f"\nLoading wallet: {args.wallet_name}/{args.wallet_hotkey}")
    wallet = bt.wallet(name=args.wallet_name, hotkey=args.wallet_hotkey)
    print(f"Validator: {wallet.hotkey.ss58_address}")

    # Fetch active jobs
    expected_jobs = await fetch_active_jobs(wallet)
    if not expected_jobs:
        print("ERROR: Could not fetch active jobs")
        return

    # Get miners
    miners = get_miners_from_metagraph(args.num_miners)

    # Create validator
    validator = DuckDBSampledValidator(
        wallet=wallet,
        s3_auth_url=DATA_UNIVERSE_API_URL,
        sample_percent=args.sample_percent,
        debug=args.debug
    )

    all_results = []

    try:
        # Validate each miner
        for miner in miners:
            result = await validator.validate_miner(
                miner_hotkey=miner['hotkey'],
                uid=miner['uid'],
                expected_jobs=expected_jobs
            )
            all_results.append(result)

        # Competition comparison
        print(f"\n{'='*100}")
        print("COMPETITION RESULTS - MINER COMPARISON")
        print("Score = Size × Coverage² (penalizes miners who ignore jobs)")
        print(f"{'='*100}")

        # Calculate total EFFECTIVE size of PASSED miners
        total_effective_size = sum(r.effective_size_bytes for r in all_results if r.validation_passed)

        # Sort by effective size (bigger = better)
        all_results.sort(key=lambda x: x.effective_size_bytes, reverse=True)

        print(f"\n{'UID':>5} | {'Size (MB)':>10} | {'Cov%':>6} | {'Eff.Size':>12} | {'Dup%':>6} | {'JobM%':>6} | {'Scrp%':>6} | {'Status':>6} | {'Share':>7}")
        print("-" * 100)

        for r in all_results:
            size_mb = r.total_size_bytes / (1024*1024)
            eff_size_mb = r.effective_size_bytes / (1024*1024)
            status = "PASS" if r.validation_passed else "FAIL"

            if r.validation_passed and total_effective_size > 0:
                share = r.effective_size_bytes / total_effective_size * 100
            else:
                share = 0

            print(f"{r.uid:>5} | {size_mb:>10.1f} | {r.job_coverage_rate:>5.1f}% | {eff_size_mb:>10.1f}MB | {r.duplicate_rate_within_job:>5.1f}% | {r.job_match_rate:>5.1f}% | {r.scraper_success_rate:>5.1f}% | {status:>6} | {share:>6.2f}%")

        print("-" * 100)

        passed_count = sum(1 for r in all_results if r.validation_passed)
        total_size_mb = sum(r.total_size_bytes for r in all_results) / (1024*1024)
        total_eff_size_mb = total_effective_size / (1024*1024)

        print(f"\nPassed: {passed_count}/{len(all_results)} miners")
        print(f"Total raw size: {total_size_mb:.1f} MB")
        print(f"Total effective size: {total_eff_size_mb:.1f} MB (after coverage² penalty)")

        # Show what S3 boost distribution would look like
        S3_BOOST_POOL = 200_000_000  # 200M total pool

        print(f"\n\nPROPOSED S3 BOOST DISTRIBUTION (Pool: {S3_BOOST_POOL:,})")
        print("Based on: effective_size = raw_size × coverage²")
        print("-" * 60)

        for r in all_results:
            if r.validation_passed and total_effective_size > 0:
                share = r.effective_size_bytes / total_effective_size
                boost = int(share * S3_BOOST_POOL)
            else:
                boost = 0
                share = 0
            cov = r.job_coverage_rate
            print(f"  UID {r.uid:>3}: {boost:>15,} ({share*100:>6.2f}%) [cov={cov:.1f}%]")

        # Save results
        if args.output:
            output_data = {
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'sample_percent': args.sample_percent,
                'total_jobs': len(expected_jobs),
                's3_boost_pool': S3_BOOST_POOL,
                'total_effective_size_bytes': total_effective_size,
                'scoring_formula': 'effective_size = raw_size × coverage²',
                'results': [asdict(r) for r in all_results]
            }
            with open(args.output, 'w') as f:
                json.dump(output_data, f, indent=2, default=str)
            print(f"\nResults saved to: {args.output}")

    finally:
        validator.close()


if __name__ == "__main__":
    asyncio.run(main())
