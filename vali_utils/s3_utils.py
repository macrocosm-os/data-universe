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
    DataSource.REDDIT: ScraperId.REDDIT_CUSTOM,
    DataSource.YOUTUBE: ScraperId.YOUTUBE_APIFY_TRANSCRIPT
}


class S3Validator:
    """
    S3 validator that performs comprehensive validation including:
    - Recent data analysis (3 hour window)
    - Duplicate detection within validation batches
    - Real scraper validation using actual scrapers
    - Composite scoring with multiple factors
    """

    def __init__(self):
        self.scraper_provider = ScraperProvider()

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
            # Step 1: Get miner job folders
            job_folders = await self._get_miner_job_folders(wallet, s3_auth_url, miner_hotkey)
            if not job_folders:
                return self._create_failed_result("Could not access miner S3 data")

            # Step 2: Filter active jobs
            active_job_folders = [
                jf for jf in job_folders
                if jf.get('job_id') in expected_jobs
            ]

            if not active_job_folders:
                return self._create_failed_result("No active jobs found")

            bt.logging.info(
                f"Found {len(active_job_folders)} active jobs out of "
                f"{len(job_folders)} total for {miner_hotkey}"
            )

            # Step 3: Analyze recent data
            recent_data_analysis = await self._analyze_recent_data(
                wallet, s3_auth_url, miner_hotkey, active_job_folders
            )

            if not recent_data_analysis['has_recent_data']:
                return self._create_failed_result(
                    "No recent data found in last 3 hours"
                )

            # Step 4: Check for duplicates
            duplicate_analysis = await self._check_for_duplicates(
                wallet, s3_auth_url, miner_hotkey, recent_data_analysis['recent_job_files']
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
            num_active_jobs = len(active_job_folders)
            job_completion_rate = (num_active_jobs / num_expected_jobs * 100) if num_expected_jobs > 0 else 100.0

            # Step 8: Calculate final validation result with job completion multiplier
            return self._calculate_final_result(
                len(active_job_folders),
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
        self, wallet, s3_auth_url: str, miner_hotkey: str, job_folders: List[Dict]
    ) -> Dict:
        """Analyze data uploaded in the recent time window"""
        now = dt.datetime.now(dt.timezone.utc)
        threshold_time = now - dt.timedelta(hours=3)  # 3 hour window

        recent_job_files = {}
        total_recent_files = 0
        total_size_bytes = 0
        recent_jobs_count = 0

        # Sample jobs to avoid overwhelming analysis
        sample_jobs = random.sample(
            job_folders, min(5, len(job_folders))
        )

        for job_folder in sample_jobs:
            job_id = job_folder['job_id']
            presigned_url = job_folder['presigned_url']

            # Get files in job
            job_files = await self._get_job_files(presigned_url)
            if not job_files:
                continue

            # Check if any file in the job is recent - if so, include entire job
            has_recent_file = False
            for file_info in job_files:
                try:
                    last_modified_str = file_info.get('last_modified', '')
                    if self._is_file_recent(last_modified_str, threshold_time):
                        has_recent_file = True
                        break
                except Exception:
                    continue

            # If any file is recent, include the entire job for validation
            if has_recent_file:
                for file_info in job_files:
                    total_size_bytes += file_info.get('size', 0)

                recent_job_files[job_id] = {
                    'files': job_files,  # Include ALL files in the job
                    'presigned_url': presigned_url
                }
                total_recent_files += len(job_files)  # Count all files
                recent_jobs_count += 1

        return {
            'has_recent_data': total_recent_files > 0,
            'recent_jobs_count': recent_jobs_count,
            'recent_files_count': total_recent_files,
            'total_size_bytes': total_size_bytes,
            'recent_job_files': recent_job_files
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

    async def _check_job_content_match(
        self, wallet, s3_auth_url: str, miner_hotkey: str,
        recent_job_files: Dict, expected_jobs: Dict
    ) -> Dict:
        """Validate that uploaded data actually matches the job requirements (labels/keywords)"""
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

                        # Check if data matches job requirements
                        # Evaluate both label and keyword - match if either passes
                        label_matches = False
                        keyword_matches = False

                        if job_label and job_label.strip():  # Only check if non-empty after strip
                            # Label-based job: check if entity label matches
                            entity_label = str(row.get('label', '')).lower().strip()
                            job_label_normalized = job_label.lower().strip()

                            # Handle different label formats
                            if platform in ['x', 'twitter']:
                                # For X: check if label is in tweet_hashtags array (handles multiple hashtags)
                                tweet_hashtags = row.get('tweet_hashtags', [])
                                if isinstance(tweet_hashtags, list):
                                    # Normalize hashtags to lowercase
                                    hashtags_lower = [str(h).lower().strip() for h in tweet_hashtags]
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
                                else:
                                    # Fallback if tweet_hashtags is not a list
                                    label_without_hash = job_label_normalized.lstrip('#')
                                    label_with_hash = f"#{label_without_hash}"
                                    label_matches = (
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
                                # For YouTube: check channel labels, removing @ prefix if present
                                label_without_at = job_label_normalized.lstrip('@')
                                label_matches = (
                                    entity_label == job_label_normalized or
                                    entity_label == label_without_at or
                                    job_label_normalized in entity_label or
                                    label_without_at in entity_label
                                )
                            else:
                                label_matches = (entity_label == job_label_normalized)

                        if job_keyword and job_keyword.strip():  # Only check if non-empty after strip
                            # Keyword-based job: check if content contains keyword
                            job_keyword_normalized = job_keyword.lower().strip()

                            # Check in relevant content fields based on platform
                            if platform == 'reddit':
                                body = str(row.get('body', '')).lower()
                                title = str(row.get('title', '')).lower()
                                keyword_matches = (job_keyword_normalized in body or job_keyword_normalized in title)
                            elif platform == 'youtube':
                                title = str(row.get('title', '')).lower()
                                keyword_matches = job_keyword_normalized in title
                            else:  # X/Twitter
                                text = str(row.get('text', '')).lower()
                                keyword_matches = job_keyword_normalized in text

                        matches_job = label_matches or keyword_matches

                        uri = self._get_uri_value(row)
                        if uri and len(checked_uris) < 20:
                            checked_uris.append(uri)

                        if matches_job:
                            total_matched += 1
                        else:
                            # Record mismatch sample
                            if len(mismatch_samples) < 10:
                                mismatch_samples.append(
                                    f"Job {job_id[:8]}: Expected {job_label or job_keyword}, got label='{row.get('label', 'N/A')}' in {uri[:50] if uri else 'unknown'}"
                                )

                except Exception as e:
                    bt.logging.debug(f"Error checking job content match: {str(e)}")
                    continue

        match_rate = (total_matched / total_checked * 100) if total_checked > 0 else 0

        # Log job match check details (same style as regular validation)
        bt.logging.info(
            f"{miner_hotkey}: S3 job match validation: Checked {total_checked} entities, {total_matched} matched job requirements ({match_rate:.1f}%)"
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
        self, total_active_jobs: int, recent_data_analysis: Dict,
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
        min_job_match_rate = 100.0  # Require 100% of data to match job requirements
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
            
            # Create XContent object with ALL uploaded fields
            x_content = XContent(
                username=str(username),
                text=str(text),
                url=str(url),
                timestamp=timestamp,
                tweet_hashtags=row.get('tweet_hashtags', []) if pd.notna(row.get('tweet_hashtags')) else [],
                media=row.get('media', None) if pd.notna(row.get('media')) else None,
                user_id=str(row.get('user_id', '')),
                user_display_name=str(row.get('user_display_name', username)),
                user_verified=bool(row.get('user_verified', False)),
                tweet_id=str(row.get('tweet_id', '')),
                is_reply=bool(row.get('is_reply', False)),
                is_quote=bool(row.get('is_quote', False)),
                conversation_id=str(row.get('conversation_id', '')),
                in_reply_to_user_id=str(row.get('in_reply_to_user_id', '')),
                # Add missing engagement metrics that miners upload
                language=str(row.get('language', '')) if pd.notna(row.get('language')) else None,
                in_reply_to_username=str(row.get('in_reply_to_username', '')) if pd.notna(row.get('in_reply_to_username')) else None,
                quoted_tweet_id=str(row.get('quoted_tweet_id', '')) if pd.notna(row.get('quoted_tweet_id')) else None,
                like_count=int(row.get('like_count', 0)) if pd.notna(row.get('like_count')) else None,
                retweet_count=int(row.get('retweet_count', 0)) if pd.notna(row.get('retweet_count')) else None,
                reply_count=int(row.get('reply_count', 0)) if pd.notna(row.get('reply_count')) else None,
                quote_count=int(row.get('quote_count', 0)) if pd.notna(row.get('quote_count')) else None,
                view_count=int(row.get('view_count', 0)) if pd.notna(row.get('view_count')) else None,
                bookmark_count=int(row.get('bookmark_count', 0)) if pd.notna(row.get('bookmark_count')) else None,
                # Add missing user profile data that miners upload
                user_blue_verified=bool(row.get('user_blue_verified', False)) if pd.notna(row.get('user_blue_verified')) else None,
                user_description=str(row.get('user_description', '')) if pd.notna(row.get('user_description')) else None,
                user_location=str(row.get('user_location', '')) if pd.notna(row.get('user_location')) else None,
                profile_image_url=str(row.get('profile_image_url', '')) if pd.notna(row.get('profile_image_url')) else None,
                cover_picture_url=str(row.get('cover_picture_url', '')) if pd.notna(row.get('cover_picture_url')) else None,
                user_followers_count=int(row.get('user_followers_count', 0)) if pd.notna(row.get('user_followers_count')) else None,
                user_following_count=int(row.get('user_following_count', 0)) if pd.notna(row.get('user_following_count')) else None
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
    use_enhanced_validation: bool = False, config=None
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
    
    Returns:
        S3ValidationResult with validation metrics
    """
    
    # Load expected jobs
    expected_jobs = load_expected_jobs_from_gravity()
    
    if use_enhanced_validation:
        # Use enhanced validation with real scrapers
        try:
            validator = S3Validator()
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
