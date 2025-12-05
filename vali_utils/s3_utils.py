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
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from scraping.provider import ScraperProvider
from scraping.scraper import ScraperId, ValidationResult
from scraping.x.model import XContent
from scraping.reddit.model import RedditContent
from scraping.youtube.model import YouTubeContent
from common.data import DataEntity, DataSource
from common.constants import FILENAME_FORMAT_REQUIRED_DATE
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


# Mapping of scrapers to use based on the data source to validate
PREFERRED_SCRAPERS = {
    DataSource.X: ScraperId.X_APIDOJO,
    DataSource.REDDIT: ScraperId.REDDIT_MC,
    DataSource.YOUTUBE: ScraperId.YOUTUBE_APIFY_TRANSCRIPT
}


def extract_count_from_filename(file_path: str) -> int:
    """
    Extract claimed record count from miner filename for dashboard metrics

    Filename format: data_{YYYYMMDD_HHMMSS}_{record_count}_{16_char_hex}.parquet
    Example: data_20250804_150058_9999_4yk9nu3ghiqjmv6c.parquet -> 9999

    Args:
        file_path: Full S3 path or just filename

    Returns:
        Record count as integer, or 0 if invalid format
    """
    # Case-insensitive hex pattern to handle both upper and lowercase
    match = re.search(r'data_\d{8}_\d{6}_(\d+)_[a-fA-F0-9]{16}\.parquet', file_path)
    return int(match.group(1)) if match else 0


def is_valid_filename_format(file_path: str) -> bool:
    """
    Check if filename matches required format

    Enforced after FILENAME_FORMAT_REQUIRED_DATE

    Args:
        file_path: Full S3 path or just filename

    Returns:
        True if valid format or before enforcement date, False otherwise
    """
    now = dt.datetime.now(dt.timezone.utc)

    # Before enforcement date, all filenames are "valid" (no enforcement)
    if now < FILENAME_FORMAT_REQUIRED_DATE:
        return True

    # After enforcement date, check format strictly
    pattern = r'data_\d{8}_\d{6}_\d+_[a-fA-F0-9]{16}\.parquet$'
    return bool(re.search(pattern, file_path))


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

                # Validate filename formats (enforced after FILENAME_FORMAT_REQUIRED_DATE)
                invalid_filenames = [f['key'] for f in all_files if not is_valid_filename_format(f['key'])]

                if invalid_filenames:
                    bt.logging.warning(
                        f"{miner_hotkey}: Found {len(invalid_filenames)} files with invalid filename format"
                    )

                    # After enforcement date, reject validation if any invalid filenames found
                    now = dt.datetime.now(dt.timezone.utc)
                    if now >= FILENAME_FORMAT_REQUIRED_DATE:
                        return self._create_failed_result(
                            f"Invalid filename format: {len(invalid_filenames)} files don't match required pattern "
                            f"(data_YYYYMMDD_HHMMSS_count_16hex.parquet)"
                        )

                # Extract claimed record counts from filenames for dashboard metrics
                total_claimed_records = sum(extract_count_from_filename(f['key']) for f in all_files)

                bt.logging.info(
                    f"{miner_hotkey}: Extracted {len(all_job_ids)} unique job IDs from {len(all_files)} files"
                )
                bt.logging.info(
                    f"{miner_hotkey}: Total claimed records from filenames: {total_claimed_records:,} across {len(all_files)} files"
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

            # Check for filename count mismatches (enforced after FILENAME_FORMAT_REQUIRED_DATE)
            now = dt.datetime.now(dt.timezone.utc)
            if now >= FILENAME_FORMAT_REQUIRED_DATE and 'count_mismatches' in duplicate_analysis:
                count_mismatches = duplicate_analysis['count_mismatches']
                if count_mismatches:
                    return self._create_failed_result(
                        f"Record count validation failed: {len(count_mismatches)} files have mismatched counts "
                        f"(claimed vs actual records)"
                    )

            # Step 5: Validate job content matching
            job_match_analysis = await self._check_job_content_match(
                wallet, s3_auth_url, miner_hotkey,
                recent_data_analysis['recent_job_files'], expected_jobs
            )

            # Step 6: Perform scraper validation
            scraper_validation = await self._perform_scraper_validation(
                wallet, s3_auth_url, miner_hotkey,
                recent_data_analysis['recent_job_files'], expected_jobs
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

                    # Validate filename record count matches actual
                    claimed_count = extract_count_from_filename(file_key)
                    actual_count = len(df)
                    if claimed_count > 0 and claimed_count != actual_count:
                        bt.logging.warning(
                            f"{miner_hotkey}: Record count mismatch found: "
                            f"claimed={claimed_count}, actual={actual_count}"
                        )

                        # After enforcement date, count mismatches are validation failures
                        now = dt.datetime.now(dt.timezone.utc)
                        if now >= FILENAME_FORMAT_REQUIRED_DATE:
                            # Track mismatch for final validation decision
                            if 'count_mismatches' not in duplicate_analysis:
                                duplicate_analysis['count_mismatches'] = []
                            duplicate_analysis['count_mismatches'].append({
                                'claimed': claimed_count,
                                'actual': actual_count
                            })

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
                            elif platform == 'youtube':
                                # For YouTube: use backwards-compatible label matching
                                # This handles underscore vs hyphen differences in channel labels
                                label_matches = YouTubeContent.labels_match(entity_label, job_label_normalized)
                                if not label_matches:
                                    # Fallback: check without @ prefix
                                    label_without_at = job_label_normalized.lstrip('@')
                                    label_matches = (
                                        YouTubeContent.labels_match(entity_label, label_without_at) or
                                        job_label_normalized in entity_label or
                                        label_without_at in entity_label
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
                            elif platform == 'youtube':
                                # IMPORTANT: match miner behavior -> title OR description
                                title_val = row.get('title', '')
                                desc_val = row.get('description', '')

                                if isinstance(title_val, float) and pd.isna(title_val):
                                    title = ''
                                else:
                                    title = str(title_val).lower()

                                if isinstance(desc_val, float) and pd.isna(desc_val):
                                    description = ''
                                else:
                                    description = str(desc_val).lower()

                                keyword_matches = (
                                    job_keyword_normalized in title or
                                    job_keyword_normalized in description
                                )
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
            sample_job_mismatches=job_match_analysis['mismatch_samples'][:5]
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
            elif platform == 'youtube':
                data_source = DataSource.YOUTUBE
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
    
    def _create_failed_result(self, reason: str) -> S3ValidationResultDetailed:
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
            sample_job_mismatches=[]
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
            elif platform == 'youtube':
                return self._create_youtube_data_entity(row)
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
    
    def _create_youtube_data_entity(self, row) -> Optional[DataEntity]:
        """Create YouTube DataEntity from parquet row"""
        try:
            # Get required fields
            video_id = row.get('video_id', '')
            title = row.get('title', '')
            channel_name = row.get('channel_name', '')
            url = row.get('url', row.get('uri', ''))
            
            if not all([video_id, title, channel_name, url]):
                return None
            
            # Get datetime from row or use current time
            datetime_val = row.get('upload_date', row.get('datetime', ''))
            if datetime_val and pd.notna(datetime_val):
                try:
                    upload_date = pd.to_datetime(datetime_val)
                    if upload_date.tzinfo is None:
                        upload_date = upload_date.replace(tzinfo=dt.timezone.utc)
                except Exception:
                    upload_date = dt.datetime.now(dt.timezone.utc)
            else:
                upload_date = dt.datetime.now(dt.timezone.utc)
            
            # Create YouTubeContent object
            youtube_content = YouTubeContent(
                video_id=str(video_id),
                title=str(title),
                channel_name=str(channel_name),
                upload_date=upload_date,
                transcript=row.get('transcript', []) if pd.notna(row.get('transcript')) else [],
                url=str(url),
                duration_seconds=int(row.get('duration_seconds', 0)) if pd.notna(row.get('duration_seconds')) else 0,
                language=str(row.get('language', 'en'))
            )
            
            return YouTubeContent.to_data_entity(youtube_content)
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
    use_enhanced_validation: bool = False, config=None, s3_reader=None
) -> S3ValidationResult:
    """
    Comprehensive S3 validation using metadata analysis and statistical methods.
    Validates file structure, job alignment, and data quality indicators.

    Args:
        wallet: Validator wallet for S3 authentication
        s3_auth_url: S3 authentication service URL
        miner_hotkey: Target miner's hotkey
        use_enhanced_validation: If True, performs enhanced validation with real scrapers
        config: Configuration object with enhanced validation settings
        s3_reader: Optional ValidatorS3Access instance for efficient file listing

    Returns:
        S3ValidationResult with validation metrics
    """

    # Load expected jobs
    expected_jobs = load_expected_jobs_from_gravity()

    if use_enhanced_validation:
        # Use enhanced validation with real scrapers
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
                enhanced_validation=enhanced_result
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
            reason=reason
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
