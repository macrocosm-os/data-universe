#!/usr/bin/env python3
"""
Test Script: Full DuckDB Validation with ALL Files

This script tests comprehensive validation by:
1. Listing ALL files for a miner (using get-miner-list)
2. Getting presigned URLs for ALL files (batch request)
3. Using DuckDB to read and analyze ALL files at once
4. Running comprehensive anti-cheat validation

This is the "best ever validation" approach - no sampling, full coverage.

Usage:
    python scripts/test_full_duckdb_validation.py \
        --miner-hotkey "5H2..." \
        --wallet-name "validator" \
        --wallet-hotkey "default"
"""

import argparse
import asyncio
import json
import os
import sys
import time
import requests
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bittensor as bt

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    print("ERROR: DuckDB required. Install with: pip install duckdb")
    sys.exit(1)

from vali_utils.validator_s3_access import ValidatorS3Access


@dataclass
class FullValidationResult:
    """Comprehensive validation result"""
    miner_hotkey: str
    timestamp: str

    # File metrics
    total_files: int = 0
    total_rows: int = 0
    total_size_bytes: int = 0

    # URL generation
    urls_requested: int = 0
    urls_received: int = 0
    url_generation_time_ms: float = 0

    # DuckDB read
    duckdb_read_time_ms: float = 0
    duckdb_success: bool = False

    # Validation metrics
    unique_uris: int = 0
    duplicate_count: int = 0
    duplicate_rate: float = 0.0
    cross_job_duplicates: int = 0

    empty_content_count: int = 0
    empty_content_rate: float = 0.0
    short_content_count: int = 0
    avg_content_length: float = 0.0

    missing_datetime_count: int = 0
    missing_uri_count: int = 0

    # Job analysis
    jobs_found: int = 0
    rows_per_job: Dict[str, int] = field(default_factory=dict)

    # Verdict
    is_valid: bool = False
    validation_score: float = 0.0
    issues: List[str] = field(default_factory=list)

    # Timing
    total_time_ms: float = 0.0


class FullDuckDBValidator:
    """
    Full validation using DuckDB with ALL miner files.
    No sampling - complete coverage.
    """

    # Validation thresholds
    MAX_DUPLICATE_RATE = 0.05  # 5% max duplicates
    MAX_EMPTY_RATE = 0.10  # 10% max empty content
    MAX_MISSING_URI_RATE = 0.05  # 5% max missing URIs

    def __init__(
        self,
        wallet: bt.wallet,
        s3_auth_url: str,
        debug: bool = True
    ):
        self.wallet = wallet
        self.s3_auth_url = s3_auth_url
        self.debug = debug

        self.s3_reader = ValidatorS3Access(
            wallet=wallet,
            s3_auth_url=s3_auth_url,
            debug=debug
        )

        self.conn = duckdb.connect(':memory:')
        self._setup_duckdb()

    def _setup_duckdb(self):
        """Configure DuckDB"""
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        self.conn.execute("SET http_timeout=120000;")  # 2 min timeout
        self.conn.execute("SET enable_progress_bar=false;")
        self._log("DuckDB configured with httpfs")

    def _log(self, msg: str):
        if self.debug:
            print(f"[DEBUG] {msg}")

    async def get_all_presigned_urls(
        self,
        miner_hotkey: str,
        file_keys: List[str],
        batch_size: int = 500
    ) -> Dict[str, str]:
        """
        Get presigned URLs for ALL files in batches.

        Args:
            miner_hotkey: Miner to validate
            file_keys: List of all file keys
            batch_size: How many URLs to request per API call

        Returns:
            Dict mapping file_key -> presigned_url
        """
        all_urls = {}
        total_batches = (len(file_keys) + batch_size - 1) // batch_size

        self._log(f"Requesting {len(file_keys)} URLs in {total_batches} batches...")

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
                        if 'presigned_url' in data:
                            all_urls[key] = data['presigned_url']
                    self._log(f"    Got {len(file_urls)} URLs")
                else:
                    self._log(f"    Batch {batch_num} failed: {response.status_code}")

            except Exception as e:
                self._log(f"    Batch {batch_num} error: {e}")

        self._log(f"Total: Got {len(all_urls)} presigned URLs")
        return all_urls

    def validate_with_duckdb(
        self,
        presigned_urls: List[str],
    ) -> Dict[str, Any]:
        """
        Run comprehensive validation using DuckDB on all files.

        Args:
            presigned_urls: List of presigned URLs for all files

        Returns:
            Validation metrics dictionary
        """
        if not presigned_urls:
            return {"success": False, "error": "No URLs provided"}

        # Create URL list for DuckDB
        url_list_str = ", ".join([f"'{url}'" for url in presigned_urls])

        try:
            # Query 1: Basic counts and duplicate detection
            self._log("Running duplicate detection query...")
            dup_start = time.perf_counter()

            dup_result = self.conn.execute(f"""
                WITH all_data AS (
                    SELECT
                        COALESCE(uri, url, link, source_url) as entity_uri,
                        COALESCE(text, body, '') as content,
                        datetime
                    FROM read_parquet([{url_list_str}])
                ),
                uri_counts AS (
                    SELECT
                        entity_uri,
                        COUNT(*) as cnt
                    FROM all_data
                    WHERE entity_uri IS NOT NULL AND entity_uri != ''
                    GROUP BY entity_uri
                )
                SELECT
                    (SELECT COUNT(*) FROM all_data) as total_rows,
                    COUNT(*) as unique_uris,
                    SUM(cnt) as total_with_uri,
                    SUM(CASE WHEN cnt > 1 THEN cnt - 1 ELSE 0 END) as duplicate_count,
                    MAX(cnt) as max_duplicates
                FROM uri_counts
            """).fetchone()

            dup_time = (time.perf_counter() - dup_start) * 1000
            self._log(f"  Duplicate query took {dup_time:.1f}ms")

            total_rows = dup_result[0] or 0
            unique_uris = dup_result[1] or 0
            total_with_uri = dup_result[2] or 0
            duplicate_count = dup_result[3] or 0
            max_duplicates = dup_result[4] or 0

            # Query 2: Content quality
            self._log("Running content quality query...")
            quality_start = time.perf_counter()

            quality_result = self.conn.execute(f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN COALESCE(text, body, '') = '' THEN 1 ELSE 0 END) as empty_content,
                    SUM(CASE WHEN LENGTH(COALESCE(text, body, '')) < 10 THEN 1 ELSE 0 END) as short_content,
                    SUM(CASE WHEN datetime IS NULL THEN 1 ELSE 0 END) as missing_datetime,
                    SUM(CASE WHEN COALESCE(uri, url, link, source_url, '') = '' THEN 1 ELSE 0 END) as missing_uri,
                    AVG(LENGTH(COALESCE(text, body, ''))) as avg_length
                FROM read_parquet([{url_list_str}])
            """).fetchone()

            quality_time = (time.perf_counter() - quality_start) * 1000
            self._log(f"  Quality query took {quality_time:.1f}ms")

            empty_content = quality_result[1] or 0
            short_content = quality_result[2] or 0
            missing_datetime = quality_result[3] or 0
            missing_uri = quality_result[4] or 0
            avg_length = quality_result[5] or 0

            return {
                "success": True,
                "total_rows": total_rows,
                "unique_uris": unique_uris,
                "total_with_uri": total_with_uri,
                "duplicate_count": duplicate_count,
                "duplicate_rate": (duplicate_count / total_with_uri * 100) if total_with_uri > 0 else 0,
                "max_duplicates_per_uri": max_duplicates,
                "empty_content_count": empty_content,
                "empty_content_rate": (empty_content / total_rows * 100) if total_rows > 0 else 0,
                "short_content_count": short_content,
                "missing_datetime_count": missing_datetime,
                "missing_uri_count": missing_uri,
                "missing_uri_rate": (missing_uri / total_rows * 100) if total_rows > 0 else 0,
                "avg_content_length": float(avg_length) if avg_length else 0,
                "query_times": {
                    "duplicate_query_ms": dup_time,
                    "quality_query_ms": quality_time
                }
            }

        except Exception as e:
            self._log(f"DuckDB error: {e}")
            return {"success": False, "error": str(e)}

    async def run_full_validation(
        self,
        miner_hotkey: str,
        max_files: Optional[int] = None
    ) -> FullValidationResult:
        """
        Run complete validation on ALL miner files.

        Args:
            miner_hotkey: Miner to validate
            max_files: Optional limit on files (for testing)

        Returns:
            FullValidationResult with all metrics
        """
        result = FullValidationResult(
            miner_hotkey=miner_hotkey,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
        )

        total_start = time.perf_counter()

        print(f"\n{'='*70}")
        print(f"FULL DUCKDB VALIDATION - 100% COVERAGE")
        print(f"Miner: {miner_hotkey}")
        print(f"{'='*70}")

        # Step 1: List ALL files
        print("\n[Step 1] Listing ALL files for miner...")
        step_start = time.perf_counter()

        all_files = await self.s3_reader.list_all_files_with_metadata(miner_hotkey)
        list_time = (time.perf_counter() - step_start) * 1000

        if not all_files:
            result.issues.append("No files found for miner")
            print(f"  ERROR: No files found")
            return result

        result.total_files = len(all_files)
        result.total_size_bytes = sum(f.get('size', 0) for f in all_files)

        # Extract job IDs
        job_ids = set()
        for f in all_files:
            key = f.get('key', '')
            if '/job_id=' in key:
                job_id = key.split('/job_id=')[1].split('/')[0]
                job_ids.add(job_id)

        result.jobs_found = len(job_ids)

        print(f"  Files found: {result.total_files:,}")
        print(f"  Total size: {result.total_size_bytes / (1024*1024):.2f} MB")
        print(f"  Jobs found: {result.jobs_found}")
        print(f"  Time: {list_time:.1f}ms")

        # Limit files if specified (for testing)
        files_to_process = all_files
        if max_files and len(all_files) > max_files:
            print(f"\n  [TEST MODE] Limiting to {max_files} files...")
            files_to_process = all_files[:max_files]

        # Step 2: Get presigned URLs for ALL files
        print(f"\n[Step 2] Getting presigned URLs for {len(files_to_process)} files...")
        step_start = time.perf_counter()

        file_keys = [f['key'] for f in files_to_process]
        presigned_urls = await self.get_all_presigned_urls(miner_hotkey, file_keys)

        result.urls_requested = len(file_keys)
        result.urls_received = len(presigned_urls)
        result.url_generation_time_ms = (time.perf_counter() - step_start) * 1000

        print(f"  URLs requested: {result.urls_requested:,}")
        print(f"  URLs received: {result.urls_received:,}")
        print(f"  Time: {result.url_generation_time_ms:.1f}ms")

        if result.urls_received > 0:
            print(f"  Rate: {result.url_generation_time_ms / result.urls_received:.2f}ms per URL")

        if not presigned_urls:
            result.issues.append("Failed to get presigned URLs")
            print(f"  ERROR: Failed to get presigned URLs")
            return result

        # Step 3: DuckDB validation on ALL files
        print(f"\n[Step 3] Running DuckDB validation on {len(presigned_urls)} files...")
        print(f"  This reads ALL data and runs comprehensive checks...")
        step_start = time.perf_counter()

        url_list = list(presigned_urls.values())
        validation = self.validate_with_duckdb(url_list)

        result.duckdb_read_time_ms = (time.perf_counter() - step_start) * 1000
        result.duckdb_success = validation.get("success", False)

        if result.duckdb_success:
            result.total_rows = validation["total_rows"]
            result.unique_uris = validation["unique_uris"]
            result.duplicate_count = validation["duplicate_count"]
            result.duplicate_rate = validation["duplicate_rate"]
            result.empty_content_count = validation["empty_content_count"]
            result.empty_content_rate = validation["empty_content_rate"]
            result.short_content_count = validation["short_content_count"]
            result.missing_datetime_count = validation["missing_datetime_count"]
            result.missing_uri_count = validation["missing_uri_count"]
            result.avg_content_length = validation["avg_content_length"]

            print(f"\n  RESULTS:")
            print(f"  ├─ Total rows analyzed: {result.total_rows:,}")
            print(f"  ├─ Unique URIs: {result.unique_uris:,}")
            print(f"  ├─ Duplicates: {result.duplicate_count:,} ({result.duplicate_rate:.2f}%)")
            print(f"  ├─ Empty content: {result.empty_content_count:,} ({result.empty_content_rate:.2f}%)")
            print(f"  ├─ Short content (<10 chars): {result.short_content_count:,}")
            print(f"  ├─ Missing URIs: {result.missing_uri_count:,}")
            print(f"  ├─ Missing datetime: {result.missing_datetime_count:,}")
            print(f"  ├─ Avg content length: {result.avg_content_length:.1f} chars")
            print(f"  └─ DuckDB time: {result.duckdb_read_time_ms:.1f}ms")
        else:
            error = validation.get('error', 'Unknown error')
            print(f"  ERROR: {error}")
            result.issues.append(f"DuckDB validation failed: {error}")

        # Step 4: Calculate verdict
        print(f"\n[Step 4] Calculating validation verdict...")

        if result.duckdb_success:
            # Check thresholds
            if result.duplicate_rate > self.MAX_DUPLICATE_RATE * 100:
                result.issues.append(f"High duplicate rate: {result.duplicate_rate:.2f}% (max: {self.MAX_DUPLICATE_RATE*100}%)")

            if result.empty_content_rate > self.MAX_EMPTY_RATE * 100:
                result.issues.append(f"High empty content: {result.empty_content_rate:.2f}% (max: {self.MAX_EMPTY_RATE*100}%)")

            missing_uri_rate = (result.missing_uri_count / result.total_rows * 100) if result.total_rows > 0 else 0
            if missing_uri_rate > self.MAX_MISSING_URI_RATE * 100:
                result.issues.append(f"High missing URIs: {missing_uri_rate:.2f}% (max: {self.MAX_MISSING_URI_RATE*100}%)")

            # Calculate score (100 = perfect)
            score = 100.0
            score -= min(30, result.duplicate_rate * 6)  # -30 max for duplicates
            score -= min(20, result.empty_content_rate * 2)  # -20 max for empty
            score -= min(20, missing_uri_rate * 4)  # -20 max for missing URIs
            # Remaining 30 points reserved for other checks

            result.validation_score = max(0, score)
            result.is_valid = len(result.issues) == 0 and result.validation_score >= 70

        result.total_time_ms = (time.perf_counter() - total_start) * 1000

        # Final Summary
        print(f"\n{'='*70}")
        print("VALIDATION SUMMARY")
        print(f"{'='*70}")
        print(f"  Miner: {miner_hotkey[:20]}...")
        print(f"  Files analyzed: {result.urls_received:,}")
        print(f"  Rows analyzed: {result.total_rows:,}")
        print(f"  Coverage: 100% (all files)")
        print(f"")
        print(f"  Validation Score: {result.validation_score:.1f}/100")
        print(f"  Status: {'PASS' if result.is_valid else 'FAIL'}")

        if result.issues:
            print(f"\n  Issues found:")
            for issue in result.issues:
                print(f"    - {issue}")

        print(f"\n  Timing:")
        print(f"    ├─ List files: {list_time:.1f}ms")
        print(f"    ├─ Get URLs: {result.url_generation_time_ms:.1f}ms")
        print(f"    ├─ DuckDB validation: {result.duckdb_read_time_ms:.1f}ms")
        print(f"    └─ Total: {result.total_time_ms:.1f}ms ({result.total_time_ms/1000:.1f}s)")
        print(f"{'='*70}\n")

        return result

    def close(self):
        self.conn.close()


async def main():
    parser = argparse.ArgumentParser(
        description="Full DuckDB validation with ALL miner files - 100% coverage"
    )

    parser.add_argument("--miner-hotkey", type=str, required=True,
                        help="Miner hotkey to validate")
    parser.add_argument("--wallet-name", type=str, default="default",
                        help="Validator wallet name")
    parser.add_argument("--wallet-hotkey", type=str, default="default",
                        help="Validator wallet hotkey")
    parser.add_argument("--s3-auth-url", type=str,
                        default=os.getenv("S3_AUTH_URL", "https://s3-auth.sn13.io"),
                        help="S3 Auth API URL")
    parser.add_argument("--max-files", type=int, default=None,
                        help="Limit number of files (for testing)")
    parser.add_argument("--output", type=str, default=None,
                        help="Save results to JSON file")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug output")

    args = parser.parse_args()

    # Load wallet
    print(f"\nLoading wallet: {args.wallet_name}/{args.wallet_hotkey}")
    wallet = bt.wallet(name=args.wallet_name, hotkey=args.wallet_hotkey)
    print(f"Validator hotkey: {wallet.hotkey.ss58_address}")
    print(f"S3 Auth URL: {args.s3_auth_url}")

    # Create validator
    validator = FullDuckDBValidator(
        wallet=wallet,
        s3_auth_url=args.s3_auth_url,
        debug=args.debug
    )

    try:
        # Run full validation
        result = await validator.run_full_validation(
            miner_hotkey=args.miner_hotkey,
            max_files=args.max_files
        )

        # Save results if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(asdict(result), f, indent=2, default=str)
            print(f"Results saved to: {args.output}")

        return result.is_valid

    finally:
        validator.close()


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
