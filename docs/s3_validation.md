# S3 Storage & Validation

## System Architecture

Miners upload scraped data to S3 via presigned URLs obtained from an auth server. Validators retrieve and validate this data through pagination-supported S3 access, performing comprehensive checks on format, content, and quality.

---

## Filename Format Specification

**Required Format:**
```
data_{YYYYMMDD_HHMMSS}_{record_count}_{16_char_hex}.parquet
```

**Components:**
- `YYYYMMDD`: Date (e.g., `20250804`)
- `HHMMSS`: Time (e.g., `150058`)
- `record_count`: Integer representing actual row count in file
- `16_char_hex`: 16-character hexadecimal string

**Enforcement Date:** December 2, 2025

---

## S3 Storage Structure

```
s3://bucket/data/hotkey={miner_hotkey}/job_id={job_id}/data_{timestamp}_{count}_{hash}.parquet
```

Files are organized by miner hotkey and job ID. Each job corresponds to a specific data collection task (e.g., scraping Reddit politics, X bittensor hashtag).

---

## Upload Flow

### Miner Side

1. **Authentication**
   - Request presigned URL from auth server (`/get-folder-access`)
   - Provide signature: `s3:data:access:{coldkey}:{hotkey}:{timestamp}`
   - Receive credentials with folder path

2. **Data Preparation**
   - Create parquet file from scraped data
   - Generate filename: `data_{timestamp}_{len(df)}_{secrets.token_hex(8)}.parquet`
   - Record count must match actual DataFrame length

3. **Upload**
   - Construct S3 path: `job_id={job_id}/{filename}`
   - Upload via presigned URL with form fields
   - Auth server handles actual S3 credentials

**Implementation:** `upload_utils/s3_uploader.py`, `upload_utils/s3_utils.py`

---

## Validation Flow

### Validator Side

1. **File Discovery** (Step 1)
   - List all files for miner via `list_all_files_with_metadata(miner_hotkey)`
   - Supports pagination (1000 files per page, DigitalOcean Spaces limit)
   - Extract metadata: file path, size, last modified time

2. **Filename Format Check** (Step 1)
   - Validate each filename against pattern: `data_\d{8}_\d{6}_\d+_[a-fA-F0-9]{16}\.parquet$`
   - Collect invalid filenames
   - **After Dec 2, 2025:** Fail validation if any invalid filenames found

3. **Dashboard Metrics Extraction** (Step 1)
   - Parse filenames to extract claimed record counts
   - Sum across all files: `total_claimed_records`
   - Log for monitoring and dashboard display

4. **Job Identification** (Step 1)
   - Extract job IDs from file paths
   - Match against expected jobs from Gravity
   - Calculate job completion rate

5. **Content Sampling** (Step 4)
   - Download sample files via presigned URLs
   - Load parquet files into DataFrames

6. **Record Count Validation** (Step 4)
   - Extract claimed count from filename
   - Compare with actual: `len(df)`
   - **After Dec 2, 2025:** Track mismatches, fail validation if any found

7. **Quality Checks** (Steps 4-6)
   - **Duplicate Detection:** Check for duplicate URIs across files
   - **Job Content Matching:** Verify data matches job requirements (hashtags, subreddits, etc.)
   - **Scraper Validation:** Re-scrape sample URIs to verify authenticity

**Implementation:** `vali_utils/s3_utils.py` (S3Validator class)

---

## Validation Rules

| Check | Requirement | Enforcement | Implementation |
|-------|-------------|-------------|----------------|
| Filename Format | `data_YYYYMMDD_HHMMSS_count_16hex.parquet` | Dec 2, 2025 | `is_valid_filename_format()` |
| Record Count | Claimed count = `len(df)` | Dec 2, 2025 | `extract_count_from_filename()` |
| Duplicate Rate | ≤10% | Active | URI deduplication across samples |
| Scraper Success | ≥80% | Active | Re-scrape sampled entities |
| Job Match Rate | ≥95% | Active | Content matching job criteria |

### Validation Logic

```python
# Step 1: Format check
if invalid_filenames and now >= FILENAME_FORMAT_REQUIRED_DATE:
    return _create_failed_result("Invalid filename format")

# Step 4: Count check (during duplicate detection)
if claimed_count != actual_count and now >= FILENAME_FORMAT_REQUIRED_DATE:
    track_mismatch()

# After Step 4: Check tracked mismatches
if count_mismatches and now >= FILENAME_FORMAT_REQUIRED_DATE:
    return _create_failed_result("Record count validation failed")

# Continue to content validation
if duplicate_percentage > 10%:
    return _create_failed_result("Too many duplicates")

if scraper_success_rate < 80%:
    return _create_failed_result("Low scraper success")

if job_match_rate < 95%:
    return _create_failed_result("Poor job content match")
```

---

## Scoring & Incentives

### Validation as Gate

Validation acts as a binary gate. All checks must pass for a miner to receive any rewards:

```
IF validation_passes:
    miner_score = calculated_score
ELSE:
    miner_score = 0
```

### Score Calculation

For miners passing validation:

```
raw_score = data_type_scale_factor × time_scalar × scorable_bytes
credibility_boost = credibility^2.5
job_completion_multiplier = active_jobs / expected_jobs

final_score = raw_score × credibility_boost × job_completion_multiplier
```

### Reward Distribution

```
miner_reward = (miner_score / Σ(all_miner_scores)) × total_reward_pool
```

### Key Parameters

- **Data Type Weights:** Reddit: 0.55, X: 0.35, YouTube: 0.10
- **Credibility Exponent:** 2.5
- **Min Evaluation Period:** 60 minutes
- **Data Age Limit:** 30 days (or job-specific range)

**See:** `docs/scoring.md` for detailed scoring mechanism

---

## Impact Analysis

### Without Filename Validation

- Miners can inflate metrics with incorrect counts
- Dashboard shows inaccurate data volumes
- No way to verify claimed record counts without downloading all files
- Potential for gaming through filename manipulation

### With Filename Validation

- Record counts verifiable without downloading (efficient)
- Dashboard metrics accurate for planning and analysis
- Fraud detection via count mismatch warnings
- Enforced standards improve data quality

### Example Scenario

**Miner claims large dataset:**
```
Filename: data_20250804_150058_999999_4yk9nu3ghiqjmv6c.parquet
Claimed: 999,999 records
Actual: 100 records
```

**Before Dec 2, 2025:** Warning logged, no penalty
**After Dec 2, 2025:** Validation fails, miner_score = 0, reward = 0

---

## Implementation Reference

### Miner Implementation
- **Upload Logic:** `upload_utils/s3_uploader.py` (lines 472-515)
- **Auth & Credentials:** `upload_utils/s3_utils.py`
- **Filename Generation:** Line 487-490

### Validator Implementation
- **S3 Access:** `vali_utils/validator_s3_access.py`
- **Validation Logic:** `vali_utils/s3_utils.py` (S3Validator class)
- **Helper Functions:** Lines 93-131 (`extract_count_from_filename`, `is_valid_filename_format`)
- **Format Validation:** Lines 186-200
- **Count Validation:** Lines 513-544, 256-263

### Configuration
- **Enforcement Date:** `common/constants.py` line 49
- **Thresholds:** `vali_utils/s3_utils.py` (duplicate: 10%, scraper: 80%, job_match: 95%)

---

## Technical Notes

### Pagination Support

Validators handle large datasets through pagination:
- S3 list operations limited to 1000 objects per page
- Continuation tokens used for multi-page retrieval
- Total file counts can exceed 10,000+ per miner

**Fix Applied:** PR #690 corrected pagination bug where continuation tokens were mishandled


### Performance Optimizations

- Filename validation performed on metadata only (no downloads)
- Record count validation only on sampled files (duplicate check)
- Dashboard metrics extracted via regex on file paths (O(n) complexity)
