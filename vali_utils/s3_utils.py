"""
S3 validation utilities for enhanced miner data validation.
Provides comprehensive validation of S3-stored miner data using metadata analysis.
"""

import hashlib
import random
import re
import requests
import pandas as pd
import pyarrow
import json
import os
import shutil
import tempfile
import time
import threading
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
from urllib.parse import urlparse


def normalize_url_for_dedup(url_str: str) -> str:
    """Platform-aware URL normalization that extracts the canonical content ID for dedup.

    Approach: define what a VALID canonical URL looks like, ignore everything else.
    This is not a blacklist of known exploits — it's a whitelist of valid URL structure.

    X:      Only the numeric tweet ID matters. Username is lowercased (decorative — X resolves by ID).
    Reddit: Only post_id and comment_id (base36, exactly 7 chars) matter.

    Canonical forms produced:
      X tweet:        https://x.com/{user_lower}/status/{tweet_id}
      Reddit post:    https://www.reddit.com/r/{sub}/comments/{post_id}/{slug}
      Reddit comment: https://www.reddit.com/r/{sub}/comments/{post_id}/{slug}/{comment_id}
    """
    url = str(url_str).strip()
    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    path = parsed.path

    # --- X / Twitter ---
    if "x.com" in netloc or "twitter.com" in netloc:
        # Extract /username/status/DIGITS — lowercase username to prevent case-fudging
        m = re.match(r"^/([^/]+)/status/(\d+)", path)
        if m:
            username = m.group(1).lower()
            tweet_id = m.group(2)
            return f"https://x.com/{username}/status/{tweet_id}"
        return f"https://x.com{path.rstrip('/').lower()}"

    # --- Reddit ---
    if "reddit.com" in netloc:
        # /r/{sub}/comments/{post_id}/{slug}/{comment_id}
        m = re.match(
            r"^/r/([^/]+)/comments/([a-z0-9]+)(?:/([^/]*)(?:/([a-z0-9]+))?)?",
            path,
        )
        if m:
            sub, post_id, slug, comment_id = m.group(1), m.group(2), m.group(3) or "", m.group(4)
            # Real Reddit IDs are base36, exactly 7 chars (post and comment).
            # Verified against 240K+ real comment IDs and 704 post IDs — all 7 chars.
            # Fake IDs (f1, _f1, aaaaaa0, etc.) fail this check.
            if comment_id and re.match(r"^[a-z0-9]{7}$", comment_id):
                return f"https://www.reddit.com/r/{sub}/comments/{post_id}/{slug}/{comment_id}"
            return f"https://www.reddit.com/r/{sub}/comments/{post_id}/{slug}"
        return f"https://www.reddit.com{path.rstrip('/')}"

    # --- Fallback: strip query/fragment, lowercase ---
    return f"{parsed.scheme}://{netloc}{path.rstrip('/')}"


_PRESIGNED_URL_RE = re.compile(
    r"https?://[^\s'\"]+?\?[^\s'\"]*X-Amz-Signature=[A-Fa-f0-9]+[^\s'\"]*"
)


def scrub_log(text: object) -> str:
    """Strip presigned-URL query strings (signatures, credentials, expiry) from log messages.

    R2/S3 presigned URLs embed an HMAC signature in the query string. The signature
    itself is not the secret (it's a one-way HMAC, the secret key never leaves the
    server), but the URL is still replayable within its TTL and the X-Amz-Credential
    leaks the access key ID. We never want either in validator logs that may be
    aggregated, pasted, or screenshotted.
    """
    s = str(text)
    return _PRESIGNED_URL_RE.sub(lambda m: m.group(0).split("?", 1)[0] + "?[scrubbed]", s)


def _weighted_sample_without_replacement(items, weights, k):
    """Sample k items without replacement, probability proportional to weight.
    Uses the exponential-rank trick: key = -log(U) / w, take k smallest keys."""
    if k <= 0 or not items:
        return []
    k = min(k, len(items))
    keys = []
    for it, w in zip(items, weights):
        w_eff = max(w, 1)
        keys.append((-math.log(max(random.random(), 1e-12)) / w_eff, it))
    keys.sort(key=lambda x: x[0])
    return [it for _, it in keys[:k]]


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
    MIN_SCRAPER_SUCCESS = 80.0  # 80% min scraper success rate
    MIN_ENGAGEMENT_RATE = 95.0  # 95% of X rows must have non-null view_count
    MIN_UNIQUE_CONTENT_RATIO = 10.0  # 10% min unique tweet_ids / total rows

    # File size limits - prevent empty file exploit and oversized file OOM
    MIN_FILE_SIZE_BYTES = 15_000                   # 15KB - empty parquet header ≈ 8KB
    MAX_FILE_SIZE_BYTES = 1024 * 1024 * 1024       # 1GB - single file cap
    MIN_BYTES_PER_ROW = 50                         # Legit: 80-1300 B/row. Exploits: 8-25 B/row.

    # Wall-clock bounds on the DuckDB/S3 phase. A worker thread cannot be killed
    # in Python, so the eval batch joins to completion (see run_next_eval_batch);
    # for that to be safe every blocking op inside the thread must be bounded.
    # These cap the synchronous DuckDB scans + range reads (which asyncio.wait_for
    # cannot preempt). On timeout we raise duckdb.InterruptException — a
    # duckdb.Error subclass — so the existing handler fails the file closed and
    # update_validation_info still fires (no perpetual re-skip / free pass).
    # Sized from LIVE measurement: honest large miners (110k-file, ~36GB) take the
    # full per-miner DuckDB phase ~1200-1280s under 5x concurrency — so a 1200s
    # budget CLIPPED them, interrupting a legitimate scan and mis-failing them as
    # "corrupt data pages". Raised to 3600s so an honest large miner finishes; this
    # only fires on a genuinely stuck scan. (The real fix is sampling, which cuts the
    # phase to ~1min; until then these must sit ABOVE the observed honest worst case.)
    PER_FILE_DUCKDB_TIMEOUT_SECS = 180    # max wall-clock for one file's DuckDB work
    PER_MINER_DUCKDB_BUDGET_SECS = 3600   # total S3/DuckDB wall-clock per miner (~60 min)

    # Each sampled file is bulk-downloaded to a temp file, then parsed locally.
    # DuckDB read_parquet over R2 httpfs issues many small serial HTTP Range GETs
    # (latency-bound on Cloudflare's edge) and reads a 522MB file's url column in
    # ~24s; one bulk GET of the whole file is ~7s, so download-then-parse-local is
    # ~3x faster per file with identical validation output (same DuckDB queries,
    # same PyArrow read, just against a local path). Files are cleaned in a finally;
    # a startup sweep clears anything orphaned by a crash so it can't grow.
    #
    # Lives inside the repo dir (on the data disk), NOT under /tmp: on the validator
    # host /tmp is a tmpfs (RAM-backed), so writing 520MB files there would consume
    # RAM (~520MB x up-to-5 concurrent miners) — reintroducing memory pressure.
    LOCAL_VALIDATION_TEMP_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".s3_validation_tmp",
    )

    # Scraper validation window — only files uploaded within this window are scraper-validated.
    # Older files rely on credibility from previous validation cycles.
    SCRAPER_MAX_AGE_HOURS = 96

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
        start_time = time.time()

        try:
            # Step 1: List ALL files (for size calculation - no reading needed)
            bt.logging.info(f"{miner_hotkey}: DuckDB validation - listing files...")

            if not self.s3_reader:
                return self._create_failed_result("S3 reader not available")

            all_files = await self.s3_reader.list_all_files_with_metadata(miner_hotkey)

            if not all_files:
                return self._create_failed_result("No files found")

            # Group files by job, filtering out empty/oversized/suspicious files
            files_by_job = {}
            empty_files_skipped = 0
            oversized_files_skipped = 0
            low_bpr_files_skipped = 0
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
                    # Skip suspiciously compressed files (duplicated content compresses to <50 B/row)
                    if filename_rows is not None and filename_rows > 0:
                        bpr = file_size / filename_rows
                        if bpr < self.MIN_BYTES_PER_ROW:
                            low_bpr_files_skipped += 1
                            continue
                    job_id = key.split('/job_id=')[1].split('/')[0]
                    if job_id not in files_by_job:
                        files_by_job[job_id] = []
                    files_by_job[job_id].append(f)

            # Per-job latest-only: keep only the newest upload per job_id.
            # Miners reupload whole-job snapshots; older files in the same job_id are
            # stale and will be reaped by the external cleanup script.
            files_by_job, stale_files_dropped = self._keep_latest_per_job(files_by_job)
            if stale_files_dropped > 0:
                bt.logging.info(
                    f"{miner_hotkey}: latest-only filter dropped {stale_files_dropped} "
                    f"stale files across {len(files_by_job)} jobs"
                )

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
            if low_bpr_files_skipped > 0:
                bt.logging.warning(
                    f"{miner_hotkey}: {low_bpr_files_skipped} suspiciously compressed files detected "
                    f"(< {self.MIN_BYTES_PER_ROW} B/row)"
                )
                return self._create_failed_result(
                    f"{low_bpr_files_skipped} files with suspiciously low bytes/row "
                    f"(< {self.MIN_BYTES_PER_ROW} B/row — likely duplicated content)"
                )

            bt.logging.info(
                f"{miner_hotkey}: {len(all_files)} total files, "
                f"{len(active_job_ids)} active jobs, "
                f"{total_size_bytes/(1024*1024):.1f}MB, "
                f"{job_coverage_rate:.1f}% coverage"
            )

            # Step 2: Sample files from active jobs — size-weighted to ensure big files
            # (which drive effective_size) are always represented in validation.
            active_files = []
            for job_id in active_job_ids:
                active_files.extend([(f, job_id) for f in files_by_job[job_id]])

            if not active_files:
                return self._create_failed_result("No files in active jobs")

            # Cap jobs/cycle. Post-snapshot each file == one whole job (up to
            # 1GB / 3M rows). The economic-detection math is r/N ≤ D: with the
            # top-5 force-include collapsing per-fab-job reward inflation to r≈10
            # and a single failed validation costing ≥1 cycle of effective_size,
            # this makes any fab strategy net-negative EV.
            #
            # Cut 30 -> 15 to halve the per-miner DuckDB phase (live-measured ~25s
            # per whole-job file; 30 files = ~14.5 min, which overran the batch join
            # and stacked concurrent DuckDB scans -> OOM). The top-5 by claimed rows
            # are still force-included below, so the big effective_size-driving files
            # are always inspected; only long-tail coverage slows (~8%/cycle ->
            # ~4%/cycle, full coverage in ~2 days instead of ~1 at hourly runs).
            FILE_SAMPLE_CAP = 15
            sample_count = max(10, int(len(active_files) * self.sample_percent / 100))
            sample_count = min(sample_count, len(active_files), FILE_SAMPLE_CAP)

            # Byte-weighted sampling: file selection probability proportional to its
            # byte share of the total submitted size. Prior 50:50 big/small split let
            # miners hide fab content behind a thin layer of real "bait" big files —
            # uniform sampling across thousands of fab small files gave each fab file
            # a ~0% chance of inspection. Byte-weighted weights tie validation effort
            # to where the bytes (and the effective_size claim) actually live.
            weights = [max(1, f.get('size', 0)) for f, _ in active_files]
            sampled_files_with_job = _weighted_sample_without_replacement(
                active_files, weights, sample_count
            )

            # Force-include top-N by claimed rows (these drive effective_size and must
            # always be inspected even if byte-weights would have skipped them).
            def _claimed_rows(item):
                f, _ = item
                r = self._parse_row_count_from_filename(f.get('key', ''))
                return r if r and r > 0 else 1

            top_n = sorted(active_files, key=lambda item: -_claimed_rows(item))[:5]
            already_keys = {f.get('key') for f, _ in sampled_files_with_job}
            forced = [item for item in top_n if item[0].get('key') not in already_keys]

            sampled_files_with_job = sampled_files_with_job + forced
            random.shuffle(sampled_files_with_job)
            sampled_files = [f for f, _ in sampled_files_with_job]

            sampled_bytes = sum(f.get('size', 0) for f, _ in sampled_files_with_job)
            bt.logging.info(
                f"{miner_hotkey}: Sampled {len(sampled_files)} files "
                f"({sampled_bytes/(1024*1024):.1f}MB / {total_size_bytes/(1024*1024):.1f}MB, "
                f"{sampled_bytes/max(total_size_bytes,1)*100:.1f}% of bytes)"
            )

            # Step 3: Get presigned URLs for sample
            file_keys = [f['key'] for f in sampled_files]
            presigned_urls = await self._get_presigned_urls_batch(miner_hotkey, file_keys)

            if not presigned_urls:
                return self._create_failed_result("Failed to get presigned URLs")

            # Step 4: Lightweight DuckDB validation (sampled - memory safe)
            bt.logging.info(f"{miner_hotkey}: Running sampled DuckDB validation...")
            duckdb_result = await self._sampled_duckdb_validation(
                sampled_files, expected_jobs, presigned_urls, samples_per_file=100,
                miner_hotkey=miner_hotkey,
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

                # Step 6: Scraper validation — sampled_files is already latest-per-job,
                # so no recent/older split is needed.
                scraper_files = list(sampled_files)
                random.shuffle(scraper_files)

                if scraper_files:
                    bt.logging.info(
                        f"{miner_hotkey}: Running scraper validation on {len(scraper_files)} files..."
                    )
                    scraper_result = await self._perform_scraper_validation(
                        miner_hotkey, scraper_files, expected_jobs, presigned_urls, num_entities=20
                    )
                else:
                    bt.logging.info(
                        f"{miner_hotkey}: No files available for scraper validation"
                    )
                    scraper_result = {'entities_validated': 0, 'entities_passed': 0, 'success_rate': None, 'sample_results': []}

            # Step 7: Validation decision
            issues = []
            duplicate_rate = duckdb_result["duplicate_rate_within_job"]
            empty_rate = duckdb_result["empty_rate"]
            job_match_rate = job_match_result['match_rate']
            scraper_success_rate = scraper_result['success_rate']  # None when no recent files
            scraper_success_rate_display = scraper_success_rate if scraper_success_rate is not None else -1.0
            compression_failures = duckdb_result.get("compression_failures", 0)
            row_count_mismatches = duckdb_result.get("row_count_mismatches", 0)
            decode_ratio = duckdb_result.get("decode_ratio", 1.0)

            # Check for schema validation failure
            if duckdb_result.get("reason"):
                issues.append(duckdb_result["reason"])

            if duplicate_rate > self.MAX_DUPLICATE_RATE:
                issues.append(f"High duplicates: {duplicate_rate:.1f}%")
            if empty_rate > self.MAX_EMPTY_RATE:
                issues.append(f"High empty content: {empty_rate:.1f}%")
            if job_match_rate < self.MIN_JOB_MATCH_RATE:
                issues.append(f"Low job match: {job_match_rate:.1f}%")
            if scraper_success_rate is not None and scraper_success_rate < self.MIN_SCRAPER_SUCCESS:
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
                    effective_size_bytes = row_based_size * (coverage_ratio ** 2) * decode_ratio
                    bt.logging.info(
                        f"{miner_hotkey}: effective_size: "
                        f"{total_rows_from_filenames} rows × {self.STANDARD_BYTES_PER_ROW} B/row "
                        f"= {row_based_size/(1024*1024):.1f}MB × coverage²({coverage_ratio:.2f}) "
                        f"× decode_ratio({decode_ratio:.2f}) "
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
                    f"dup={duplicate_rate:.1f}%, job_match={job_match_rate:.1f}%, scraper={scraper_success_rate_display:.1f}%"
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
                    f"scraper={scraper_success_rate_display:.1f}% (min {self.MIN_SCRAPER_SUCCESS}%), "
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
                scraper_success_rate=scraper_success_rate if scraper_success_rate is not None else 0.0,
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
            bt.logging.error(scrub_log(f"{miner_hotkey}: DuckDB validation error: {str(e)}"))
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
                # TODO: decrease back to 1h once validation runtimes stabilise. Bumped to 3h
                # because long validation passes (large miners + retries) were hitting 403s
                # on presigned URLs minted at the start of the run.
                payload = {
                    "miner_hotkey": miner_hotkey,
                    "file_keys": batch,
                    "expiry_hours": 3
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
                bt.logging.warning(scrub_log(f"Presigned URL batch error: {e}"))

        return all_urls

    # Max allowed rows per row group (must match uploader's row_group_size)
    MAX_ROW_GROUP_SIZE = 10_000

    def _check_file_metadata(self, presigned_url: str, conn) -> Optional[Dict]:
        """Read parquet metadata footer only (~1-10KB network read).
        Returns metadata dict or None on error.

        Row-group shape: pandas/pyarrow writing with row_group_size=10_000
        produces groups of exactly 10K rows, with at most a single shorter
        final group holding the remainder. Anything else (interior short
        groups, or millions of tiny groups) is non-standard and an attacker
        could weaponize it to inflate row-group count and slow validator
        scans.
        """
        try:
            result = conn.execute(f"""
                SELECT
                    SUM(row_group_num_rows),
                    LIST(DISTINCT compression),
                    MAX(row_group_num_rows),
                    COUNT(*),
                    SUM(CASE WHEN row_group_num_rows < {self.MAX_ROW_GROUP_SIZE} THEN 1 ELSE 0 END),
                    MAX(CASE WHEN row_group_num_rows < {self.MAX_ROW_GROUP_SIZE} THEN row_group_id ELSE -1 END)
                FROM parquet_metadata('{presigned_url}')
                WHERE column_id = 0
            """).fetchone()
            if result is None or result[0] is None:
                return None
            return {
                'total_rows': int(result[0]),
                'codecs': set(result[1]) if result[1] else set(),
                'has_uncompressed': 'UNCOMPRESSED' in (set(result[1]) if result[1] else set()),
                'max_rg_rows': int(result[2]),
                'num_row_groups': int(result[3]),
                'short_rg_count': int(result[4]),
                'last_short_rg_id': int(result[5]),
            }
        except Exception as e:
            bt.logging.warning(scrub_log(f"parquet_metadata error: {e}"))
            return None

    @staticmethod
    def _keep_latest_per_job(files_by_job: Dict[str, List[dict]]) -> tuple:
        """Keep only the newest file per job_id (by last_modified desc).

        Returns (filtered_files_by_job, stale_files_dropped).
        """
        def _newest_key(f):
            return f.get('last_modified', '')
        stale_dropped = 0
        out: Dict[str, List[dict]] = {}
        for job_id, files in files_by_job.items():
            if len(files) > 1:
                out[job_id] = [max(files, key=_newest_key)]
                stale_dropped += len(files) - 1
            else:
                out[job_id] = files
        return out, stale_dropped

    def _parse_row_count_from_filename(self, key: str) -> Optional[int]:
        """Extract row count from new filename format: data_YYYYMMDD_HHMMSS_{count}_{hex16}.parquet"""
        filename = key.rsplit('/', 1)[-1]
        match = self._FILENAME_ROW_COUNT_RE.match(filename)
        return int(match.group(1)) if match else None

    @classmethod
    def _sweep_local_temp_dir(cls):
        """Remove any temp files orphaned by a previous crash, then recreate the dir.
        Called once at the start of the DuckDB phase so a hard kill (the validator
        restarts every few hours) can never let /tmp grow unbounded."""
        try:
            shutil.rmtree(cls.LOCAL_VALIDATION_TEMP_DIR, ignore_errors=True)
            os.makedirs(cls.LOCAL_VALIDATION_TEMP_DIR, exist_ok=True)
        except OSError as e:
            bt.logging.warning(f"Could not reset local validation temp dir: {e}")

    def _download_to_temp(self, presigned_url: str, declared_size: int) -> str:
        """Bulk-download one file to a temp file and return its local path.

        Replaces DuckDB's slow per-column HTTP Range reads with a single GET. The
        caller MUST unlink the returned path in a finally. The write is capped at
        the declared size (+8MB margin) so a miner that under-reports its size in the
        listing cannot balloon the disk; the size guard at the call site already
        rejects anything over MAX_FILE_SIZE_BYTES before we get here.
        """
        cap = min(declared_size, self.MAX_FILE_SIZE_BYTES) + (8 * 1024 * 1024)
        fd, path = tempfile.mkstemp(suffix=".parquet", dir=self.LOCAL_VALIDATION_TEMP_DIR)
        written = 0
        try:
            with os.fdopen(fd, "wb") as out, requests.get(
                presigned_url, stream=True, timeout=300
            ) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=1 << 20):
                    written += len(chunk)
                    if written > cap:
                        raise IOError(
                            f"download exceeded size cap ({written} > {cap} bytes)"
                        )
                    out.write(chunk)
        except Exception:
            try:
                os.unlink(path)
            except OSError:
                pass
            raise
        return path

    async def _sampled_duckdb_validation(
        self,
        sampled_files: List[Dict],
        expected_jobs: Dict[str, Dict],
        presigned_urls: Dict[str, str],
        samples_per_file: int = 100,
        miner_hotkey: str = "unknown",
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

        # v3 page-decode failures (valid footer, corrupt data pages)
        page_decode_failures = 0

        # L3: decodable rows vs claimed rows (sampled-file scope)
        sum_decodable_rows = 0
        sum_sampled_claimed_rows = 0

        # Metadata-based checks
        compression_failures = 0
        row_count_mismatches = 0

        # Per-file URL dedup (presence-only — actual hash set lives locally
        # in the loop iteration so memory is bounded). After
        # _keep_latest_per_job each job has exactly one file, so within-job
        # dedup is structurally within-file dedup.
        dedup_hashes_by_job: Dict[str, bool] = {}
        dedup_total = 0
        dedup_duplicates = 0

        # Engagement & content uniqueness tracking
        engagement_total_rows = 0
        engagement_missing_rows = 0
        unique_content_ids: set = set()
        total_content_id_rows = 0

        # Deep-check every sampled file. The previous 20-50 second subsample
        # was sized when N=200 made per-file URL fetchall expensive; with N=30
        # and the streaming URL pass, the bound no longer pays for itself and
        # silently capped detection probability below what the sample size
        # supports (P_detect ∝ 1-(1-θ)^k, so dropping k from 30 to 20 at θ=5%
        # cuts per-cycle detection from 78.5% to 64.2%).
        files_to_check = sampled_files

        # Per-miner wall-clock budget for the whole S3/DuckDB phase (all sampled
        # files). Bounds the range-read path too — read_random_row_group uses its
        # own requests.Session and is NOT reachable by conn.interrupt(), so it is
        # gated by a deadline recheck below.
        miner_deadline = time.monotonic() + self.PER_MINER_DUCKDB_BUDGET_SECS

        # Timing instrumentation: this phase has been observed to run up to ~2h
        # for some miners. Log per-file + per-stage durations (with hotkey) so the
        # slow segment can be identified.
        phase_start = time.monotonic()
        bt.logging.info(
            f"{miner_hotkey}: DuckDB phase START — {len(files_to_check)} files to check"
        )

        # Clear anything a prior crashed run left behind before we start writing.
        self._sweep_local_temp_dir()

        for file_idx, file_info in enumerate(files_to_check):
            if schema_failures > 0:
                break

            file_start = time.monotonic()
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
            watchdog = None
            local_path = None
            try:
                # Fail closed if the per-miner budget is already spent. Raising
                # InterruptException (a duckdb.Error) routes through the same
                # handler as a mid-scan interrupt -> file fails -> miner is
                # fail-scored and the validation gate still advances.
                remaining = miner_deadline - time.monotonic()
                if remaining <= 0:
                    raise duckdb.InterruptException(
                        f"Per-miner DuckDB budget exhausted "
                        f"({self.PER_MINER_DUCKDB_BUDGET_SECS}s)"
                    )

                # Bulk-download the file once, then point every stage below at the
                # local copy. DuckDB read_parquet / parquet_schema / _check_file_metadata
                # and the PyArrow read_random_row_group all accept a local path
                # identically to a URL, so validation logic is unchanged — only the
                # slow per-column httpfs Range reads are replaced by one GET. A
                # download error raises into the same handlers below (transient ->
                # skip, else -> fail), and the finally unlinks the temp file.
                t_dl = time.monotonic()
                local_path = self._download_to_temp(presigned_url, file_size)
                presigned_url = local_path
                dl_secs = time.monotonic() - t_dl

                conn = duckdb.connect(':memory:')
                conn.execute("SET memory_limit='1GB';")
                conn.execute("SET threads=1;")

                # Watchdog: bound this file's DuckDB work to min(per-file cap,
                # remaining budget). conn.interrupt() from a timer thread raises
                # duckdb.InterruptException in the scanning thread. The shim
                # swallows exceptions because interrupt() on an already-closed
                # conn raises ConnectionException (it is not a no-op) — the
                # finally below cancels the timer, so this only matters for the
                # fire-after-close race.
                def _interrupt(c=conn):
                    try:
                        c.interrupt()
                    except Exception:
                        pass

                file_timeout = min(self.PER_FILE_DUCKDB_TIMEOUT_SECS, remaining)
                watchdog = threading.Timer(file_timeout, _interrupt)
                watchdog.daemon = True
                watchdog.start()

                # --- Metadata check (footer-only read, ~1-10KB) ---
                t_meta = time.monotonic()
                metadata = self._check_file_metadata(presigned_url, conn)
                meta_secs = time.monotonic() - t_meta
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

                    # Row-group shape: only the last group may be shorter than
                    # MAX_ROW_GROUP_SIZE. Blocks the "2M rows in 2M groups of 1"
                    # attack that would otherwise pass the MAX check.
                    short_rg = metadata['short_rg_count']
                    num_rg = metadata['num_row_groups']
                    if short_rg > 1 or (
                        short_rg == 1 and metadata['last_short_rg_id'] != num_rg - 1
                    ):
                        schema_failures += 1
                        bt.logging.warning(
                            f"Non-uniform row groups: {short_rg} short of {num_rg} "
                            f"(only the last may be < {self.MAX_ROW_GROUP_SIZE}) in {file_key}"
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
                t_schema = time.monotonic()
                schema_result = conn.execute(
                    f"SELECT name, type FROM parquet_schema('{presigned_url}')"
                ).fetchall()
                schema_secs = time.monotonic() - t_schema
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

                # Missing required columns — fabricated files drop columns to dodge checks
                missing_columns = expected_columns - available_columns
                if missing_columns:
                    bt.logging.warning(
                        f"Invalid schema: missing required columns {sorted(missing_columns)[:10]} "
                        f"(has {len(available_columns)}/{len(expected_columns)} expected)"
                    )
                    schema_failures += 1
                    break

                # scraped_at / scrapedAt must be stored as a string (BYTE_ARRAY),
                # not a parquet timestamp dtype.
                target_col = 'scraped_at' if platform in ['x', 'twitter'] else 'scrapedat'
                bad_dtype = any(
                    name.lower() == target_col and str(ptype).upper() != 'BYTE_ARRAY'
                    for name, ptype in schema_result
                )
                if bad_dtype:
                    bt.logging.warning(
                        f"scraped_at must be string: {file_key}"
                    )
                    schema_failures += 1
                    break

                # --- Per-file URL dedup: stream the url column via DuckDB fetchmany.
                # Post-snapshot files can be 3M rows × ~150 B/URL ≈ 450 MB as a
                # single Python list; fetchall() OOM-crashed validators. Stream in
                # 10K-row batches so per-file Python heap stays at ~1.5 MB.
                #
                # _keep_latest_per_job collapses each job to one file, so the
                # hash set is per-FILE in practice (not cross-file per-job).
                # Building it locally and discarding when the file's loop
                # iteration ends bounds cumulative dedup state to one set at a
                # time. dedup_hashes_by_job is retained only for the cross-job
                # log line in _check_dedup at the end of the pass.
                if job_id not in dedup_hashes_by_job:
                    dedup_hashes_by_job[job_id] = True  # presence marker for log only
                file_hashes: set = set()

                # Hard iteration cap so a misbehaving cursor cannot loop forever.
                # Use footer total_rows when available (cheap, already validated);
                # otherwise fall back to MAX_FILE_SIZE_BYTES / MIN_BYTES_PER_ROW
                # which is the largest possible row count for any accepted file.
                BATCH_SIZE = 10_000
                if metadata and metadata.get('total_rows'):
                    expected_rows = int(metadata['total_rows'])
                else:
                    expected_rows = self.MAX_FILE_SIZE_BYTES // self.MIN_BYTES_PER_ROW
                max_iterations = (expected_rows // BATCH_SIZE) + 2  # +2 = remainder + EOF probe

                file_decodable = 0
                t_dedup = time.monotonic()
                cur = conn.execute(
                    f"SELECT url FROM read_parquet('{presigned_url}')"
                )
                cursor_overran = False
                for _ in range(max_iterations):
                    batch = cur.fetchmany(BATCH_SIZE)
                    if not batch:
                        break
                    for (url_val,) in batch:
                        if url_val and str(url_val).strip():
                            file_decodable += 1
                        normalized = normalize_url_for_dedup(url_val)
                        h = hashlib.blake2b(normalized.encode(), digest_size=8).digest()
                        dedup_total += 1
                        if h in file_hashes:
                            dedup_duplicates += 1
                        else:
                            file_hashes.add(h)
                    del batch
                else:
                    # Loop hit the iteration cap without seeing EOF — cursor is
                    # returning more rows than the footer claimed. Treat as
                    # malformed/adversarial and fail the file.
                    cursor_overran = True
                if cursor_overran:
                    bt.logging.warning(
                        f"URL dedup cursor overran metadata row count "
                        f"({expected_rows} expected) in {file_key}"
                    )
                    schema_failures += 1
                    break

                dedup_secs = time.monotonic() - t_dedup

                # L3: decodable rows vs filename-claimed rows
                file_claimed = self._parse_row_count_from_filename(file_key)
                if file_claimed is not None and file_claimed > 0:
                    sum_decodable_rows += file_decodable
                    sum_sampled_claimed_rows += file_claimed

                # --- PyArrow row-group read for empty/missing content check ---
                if platform in ['x', 'twitter']:
                    read_cols = ['url', 'text', 'view_count', 'tweet_id', 'username']
                else:
                    read_cols = ['url', 'body', 'title', 'id']
                read_cols = [c for c in read_cols if c in available_columns]
                if 'url' not in read_cols:
                    read_cols.append('url')

                # Per-miner deadline recheck: the row-group read uses its own
                # requests.Session (not the DuckDB conn), so the watchdog's
                # conn.interrupt() cannot bound it. Gate it on the budget and cap
                # each range GET at the remaining time so a slow link cannot blow
                # past the budget.
                if time.monotonic() >= miner_deadline:
                    raise duckdb.InterruptException(
                        f"Per-miner budget exhausted before row-group read "
                        f"({self.PER_MINER_DUCKDB_BUDGET_SECS}s)"
                    )
                remaining_for_rg = max(1, int(miner_deadline - time.monotonic()))
                t_rg = time.monotonic()
                rg_df = read_random_row_group(
                    presigned_url, file_size, columns=read_cols,
                    request_timeout=min(30, remaining_for_rg)
                )
                rg_secs = time.monotonic() - t_rg
                # Per-file timing breakdown (hotkey + which stage was slow).
                # dedup = full SELECT url scan; rg = PyArrow row-group read.
                file_rows_dbg = (metadata or {}).get('total_rows', '?')
                bt.logging.info(
                    f"{miner_hotkey}: file {file_idx+1}/{len(files_to_check)} "
                    f"({file_size/1e6:.0f}MB, {file_rows_dbg} rows) timing: "
                    f"dl={dl_secs:.1f}s meta={meta_secs:.1f}s schema={schema_secs:.1f}s "
                    f"dedup={dedup_secs:.1f}s rg={rg_secs:.1f}s "
                    f"total={time.monotonic()-file_start:.1f}s key={file_key.split('/')[-1]}"
                )
                if rg_df is not None and len(rg_df) > 0:
                    rg_df.columns = [c.lower() for c in rg_df.columns]
                    total_rows += len(rg_df)

                    # Missing URL check — instant fail
                    url_missing = rg_df['url'].isna() | (rg_df['url'].astype(str).str.strip() == '')
                    missing_urls = int(url_missing.sum())
                    if missing_urls > 0:
                        bt.logging.warning(f"File has {missing_urls} rows with missing URL")
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

                    # X-only: empty username = instant fail (fabricated data)
                    if platform in ['x', 'twitter'] and 'username' in rg_df.columns:
                        empty_user = rg_df['username'].fillna('').astype(str).str.strip() == ''
                        empty_user_pct = empty_user.sum() / len(rg_df) * 100
                        if empty_user_pct > 50:
                            bt.logging.warning(f"Empty username: {empty_user_pct:.0f}% rows have no username")
                            return {
                                "success": False,
                                "duplicate_rate_within_job": 100.0,
                                "empty_rate": 100.0,
                                "total_rows": 0,
                                "reason": f"Empty username in {empty_user_pct:.0f}% of X rows"
                            }

                    # X-only: engagement, uniqueness, and URL↔tweet_id consistency
                    if platform in ['x', 'twitter']:
                        # Engagement: view_count must be non-null
                        if 'view_count' in rg_df.columns:
                            views = pd.to_numeric(rg_df['view_count'], errors='coerce')
                            engagement_total_rows += len(views)
                            engagement_missing_rows += int(views.isna().sum())

                        # Unique tweet_id ratio
                        if 'tweet_id' in rg_df.columns:
                            ids = rg_df['tweet_id'].dropna().astype(str)
                            total_content_id_rows += len(ids)
                            unique_content_ids.update(ids.tolist())

                            # URL↔tweet_id consistency: status ID in URL must match tweet_id
                            if 'url' in rg_df.columns:
                                url_id_mismatches = 0
                                check_df = rg_df[['url', 'tweet_id']].dropna()
                                for _, r in check_df.head(500).iterrows():
                                    url_str = str(r['url'])
                                    tid = str(r['tweet_id'])
                                    if '/status/' in url_str:
                                        url_status_id = url_str.split('/status/')[-1].split('?')[0].split('/')[0]
                                        if url_status_id != tid:
                                            url_id_mismatches += 1
                                if url_id_mismatches > 0:
                                    mismatch_rate = url_id_mismatches / min(len(check_df), 500) * 100
                                    bt.logging.warning(
                                        f"URL↔tweet_id mismatch: {url_id_mismatches} rows have "
                                        f"different status ID in URL vs tweet_id ({mismatch_rate:.1f}%)"
                                    )
                                    return {
                                        "success": False,
                                        "duplicate_rate_within_job": 100.0,
                                        "empty_rate": 100.0,
                                        "total_rows": 0,
                                        "reason": f"URL↔tweet_id mismatch in {url_id_mismatches} rows ({mismatch_rate:.1f}%)"
                                    }

                    # Reddit: unique id ratio
                    elif platform == 'reddit':
                        if 'id' in rg_df.columns:
                            ids = rg_df['id'].dropna().astype(str)
                            total_content_id_rows += len(ids)
                            unique_content_ids.update(ids.tolist())

            except (duckdb.IOException, duckdb.HTTPException, duckdb.ConnectionException) as e:
                # Transient S3/network errors — log and skip, do NOT fail the miner.
                # scrub_log strips R2 presigned-URL query strings (signature + key id).
                bt.logging.warning(scrub_log(
                    f"{miner_hotkey}: Transient S3/IO error after "
                    f"{time.monotonic()-file_start:.1f}s on {file_key.split('/')[-1]}: "
                    f"{type(e).__name__}: {e}"
                ))
                continue
            except (duckdb.Error, pyarrow.lib.ArrowInvalid, pyarrow.lib.ArrowIOError) as e:
                page_decode_failures += 1
                bt.logging.warning(scrub_log(
                    f"{miner_hotkey}: Page-decode failure after "
                    f"{time.monotonic()-file_start:.1f}s on {file_key.split('/')[-1]}: "
                    f"{type(e).__name__}: {e}"
                ))
                continue
            except Exception as e:
                bt.logging.warning(scrub_log(
                    f"{miner_hotkey}: Sampled validation error after "
                    f"{time.monotonic()-file_start:.1f}s on {file_key.split('/')[-1]}: {e}"
                ))
                continue
            finally:
                # Cancel the timer first (no-op if it already fired); the shim
                # swallows the ConnectionException from a fire-after-close race.
                if watchdog is not None:
                    watchdog.cancel()
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
                # Always remove the downloaded temp file — on success, validation
                # failure, timeout interrupt, or any exception above.
                if local_path is not None:
                    try:
                        os.unlink(local_path)
                    except OSError:
                        pass

        bt.logging.info(
            f"{miner_hotkey}: DuckDB phase DONE in {time.monotonic()-phase_start:.1f}s "
            f"— {files_checked} files checked, {schema_failures} schema_fail, "
            f"{page_decode_failures} page_decode_fail, {dedup_total} urls hashed"
        )

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

        # Zero tolerance for v3 page-decode failures (valid footer, corrupt data pages)
        if page_decode_failures > 0:
            bt.logging.warning(
                f"Page-decode FAILED: {page_decode_failures} files have unreadable data pages"
            )
            return {
                "success": False,
                "duplicate_rate_within_job": 100.0,
                "empty_rate": 100.0,
                "total_rows": 0,
                "reason": f"Corrupt data pages in {page_decode_failures} files"
            }

        # Per-job duplicate rate
        duplicate_rate = (dedup_duplicates / dedup_total * 100) if dedup_total > 0 else 0.0
        if duplicate_rate > self.MAX_DUPLICATE_RATE:
            bt.logging.warning(
                f"Per-job dedup FAILED: {duplicate_rate:.1f}% duplicates "
                f"({dedup_duplicates}/{dedup_total} urls across {len(dedup_hashes_by_job)} jobs)"
            )
            return {
                "success": False,
                "duplicate_rate_within_job": duplicate_rate,
                "empty_rate": 100.0,
                "total_rows": 0,
                "reason": f"High duplicate rate: {duplicate_rate:.1f}% (max {self.MAX_DUPLICATE_RATE}%)"
            }

        # Engagement check (X only): legit scrapers always return view_count
        engagement_rate = 0.0
        if engagement_total_rows > 0:
            engagement_rate = ((engagement_total_rows - engagement_missing_rows) / engagement_total_rows * 100)
            if engagement_rate < self.MIN_ENGAGEMENT_RATE:
                bt.logging.warning(
                    f"Engagement FAILED: {engagement_rate:.1f}% rows have view_count "
                    f"(min {self.MIN_ENGAGEMENT_RATE}%)"
                )
                return {
                    "success": False,
                    "duplicate_rate_within_job": duplicate_rate,
                    "empty_rate": 100.0,
                    "total_rows": 0,
                    "reason": f"Missing engagement: only {engagement_rate:.1f}% rows have view_count"
                }

        # Unique content ratio: catches bulk-duplicated content (X: tweet_id, Reddit: id)
        unique_ratio = 0.0
        if total_content_id_rows > 0:
            unique_ratio = (len(unique_content_ids) / total_content_id_rows * 100)
            if unique_ratio < self.MIN_UNIQUE_CONTENT_RATIO:
                bt.logging.warning(
                    f"Unique content FAILED: {unique_ratio:.1f}% unique content IDs "
                    f"({len(unique_content_ids)}/{total_content_id_rows})"
                )
                return {
                    "success": False,
                    "duplicate_rate_within_job": duplicate_rate,
                    "empty_rate": 100.0,
                    "total_rows": 0,
                    "reason": f"Low content uniqueness: {unique_ratio:.1f}% unique content IDs"
                }

        # L3: decodable-rows ratio across sampled files
        decode_ratio = (
            min(1.0, sum_decodable_rows / sum_sampled_claimed_rows)
            if sum_sampled_claimed_rows > 0 else 1.0
        )

        if total_rows == 0:
            return {
                "success": True,
                "duplicate_rate_within_job": duplicate_rate,
                "empty_rate": 0.0,
                "total_rows": 0,
                "compression_failures": compression_failures,
                "row_count_mismatches": row_count_mismatches,
                "decode_ratio": decode_ratio,
                "sum_decodable_rows": sum_decodable_rows,
                "sum_sampled_claimed_rows": sum_sampled_claimed_rows,
            }

        empty_rate = (empty_count / total_rows * 100) if total_rows > 0 else 0

        bt.logging.info(
            f"Sampled validation: {total_rows} rows, "
            f"dup={duplicate_rate:.1f}% (per-job, {len(dedup_hashes_by_job)} jobs), "
            f"empty={empty_rate:.1f}%, engagement={engagement_rate:.1f}%, "
            f"unique_ids={unique_ratio:.1f}%, compression_fails={compression_failures}, "
            f"decode_ratio={decode_ratio:.3f} "
            f"({sum_decodable_rows}/{sum_sampled_claimed_rows} sampled-claimed rows)"
        )

        return {
            "success": True,
            "duplicate_rate_within_job": duplicate_rate,
            "empty_rate": empty_rate,
            "total_rows": total_rows,
            "compression_failures": compression_failures,
            "row_count_mismatches": row_count_mismatches,
            "decode_ratio": decode_ratio,
            "sum_decodable_rows": sum_decodable_rows,
            "sum_sampled_claimed_rows": sum_sampled_claimed_rows,
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
                    conn.execute("SET memory_limit='1GB';")
                    conn.execute("SET threads=1;")
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

                    # Skip files missing required columns (fabricated data)
                    available_cols = set(all_column_names)
                    if platform in ['x', 'twitter']:
                        required = {c.lower() for c in self.EXPECTED_COLUMNS_X}
                    else:
                        required = {c.lower() for c in self.EXPECTED_COLUMNS_REDDIT}
                    if required - available_cols:
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
        """Validate random entities using real scrapers.

        Invariant: a row sampled from a parquet file must either become a
        DataEntity and be checked by the scraper, or count as a failed
        scraper validation. Rows must never disappear silently.

        Previously, `_create_data_entity` returning None (e.g., fab files
        with `bookmark_count = array([N, 0])` crashing Pydantic field
        validation) silently dropped the row. Fab files contributed 0
        entities, the bait files filled `all_entities`, and the miner
        passed at 100% scraper success. Now the first None returns
        immediate scraper failure → MIN_SCRAPER_SUCCESS gate kills the
        miner. Honest miners produce scalar columns and never trip this.
        """
        all_entities = []

        # Byte-weighted random ordering: each file's rank is exponential(weight=size),
        # the same trick used for file sampling. Larger files come first on average
        # without deterministic ordering — and tiny fab files retain a real probability
        # of being inspected, preventing bait-saturation of the entity pool.
        file_weights = [max(f.get('size', 0), 1) for f in sampled_files]
        ranked = _weighted_sample_without_replacement(
            sampled_files, file_weights, len(sampled_files)
        )
        sampled_files = ranked

        # Size-proportional per-file row budget: a file holding 50% of the bytes
        # contributes ~50% of the rows we sample. Total entity budget is
        # `num_entities * 2` (the existing pool-size target); allocate it pro-rata.
        sample_total_bytes = max(sum(file_weights), 1)
        entity_budget = num_entities * 2
        MIN_ROWS_PER_FILE = 2
        MAX_ROWS_PER_FILE = 20

        def _rows_for_file(f):
            share = f.get('size', 0) / sample_total_bytes
            target = int(round(share * entity_budget))
            return max(MIN_ROWS_PER_FILE, min(MAX_ROWS_PER_FILE, target))

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
                conn.execute("SET memory_limit='1GB';")
                conn.execute("SET threads=1;")
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

                # Skip files missing required columns (fabricated data)
                available_cols = set(all_column_names)
                if platform in ['x', 'twitter']:
                    required = {c.lower() for c in self.EXPECTED_COLUMNS_X}
                else:
                    required = {c.lower() for c in self.EXPECTED_COLUMNS_REDDIT}
                if required - available_cols:
                    continue

                # Read 1 random row group; sample size scales with file's byte share.
                df = read_random_row_group(
                    presigned_url, file_size,
                    columns=None, max_rows=_rows_for_file(file_info)
                )

                if df is None or len(df) == 0:
                    continue

                for _, row in df.iterrows():
                    entity = self._create_data_entity(row, platform)
                    if entity is None:
                        # The invariant: a sampled row must never disappear
                        # silently. None means entity construction failed
                        # (e.g., fab files with array-typed numeric columns
                        # crash Pydantic's int() coercion, swallowed by the
                        # bare except in _create_twitter_entity). Treat as
                        # immediate scraper failure — honest miners produce
                        # scalar columns and never hit this path.
                        bt.logging.warning(
                            f"{miner_hotkey}: _create_data_entity returned None "
                            f"for sampled {platform} row "
                            f"(likely malformed/fab data). Failing scraper "
                            f"validation immediately."
                        )
                        return {
                            'entities_validated': 1,
                            'entities_passed': 0,
                            'success_rate': 0.0,
                            'sample_results': [
                                f"❌ {platform} ({job_id[:16]}): "
                                f"Failed to create DataEntity for sampled row"
                            ],
                        }
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
                user_verified=bool(row.get('user_verified', False)) if pd.notna(row.get('user_verified', None)) else None,
                tweet_id=str(row.get('tweet_id', '')),
                is_reply=bool(row.get('is_reply', False)),
                is_quote=bool(row.get('is_quote', False)),
                conversation_id=str(row.get('conversation_id', '')),
                in_reply_to_user_id=str(row.get('in_reply_to_user_id', '')) if pd.notna(row.get('in_reply_to_user_id', None)) else None,
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
        except Exception as e:
            bt.logging.warning(f"_create_twitter_entity failed: {type(e).__name__}: {e}")
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

            # Keep body as-is; don't substitute title into body.
            # Title-only posts (empty body) are valid — title is passed separately.
            content_str = body_str

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

            # Handle media array field (same robust handling as _create_twitter_entity)
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
                media=media_value,
                is_nsfw=bool(row.get('is_nsfw', False)) if pd.notna(row.get('is_nsfw')) else None,
                score=int(row.get('score', 0)) if pd.notna(row.get('score')) else None,
                upvote_ratio=float(row.get('upvote_ratio', 0.0)) if pd.notna(row.get('upvote_ratio')) else None,
                num_comments=int(row.get('num_comments', 0)) if pd.notna(row.get('num_comments')) else None,
                scrapedAt=pd.to_datetime(row.get('scrapedAt')).to_pydatetime() if row.get('scrapedAt') is not None and pd.notna(row.get('scrapedAt')) else None,
            )
            return RedditContent.to_data_entity(reddit_content)
        except Exception as e:
            bt.logging.warning(f"_create_reddit_entity failed: {type(e).__name__}: {e}")
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
