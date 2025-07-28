"""
S3 validation utilities for enhanced miner data validation.
Provides comprehensive validation of S3-stored miner data using metadata analysis.
"""

import asyncio
import requests
import time
import xml.etree.ElementTree as ET
import urllib.parse
import pandas as pd
import json
import os
import duckdb
import datetime as dt
import bittensor as bt
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


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
            timeout=30
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
        xml_response = requests.get(miner_url)
        
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


async def validate_s3_miner_data(wallet, s3_auth_url: str, miner_hotkey: str) -> S3ValidationResult:
    """
    Comprehensive S3 validation using metadata analysis and statistical methods.
    Validates file structure, job alignment, and data quality indicators.
    """
    
    # Get miner file data
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
    
    # Load expected jobs
    expected_jobs = load_expected_jobs_from_gravity()
    
    # Perform comprehensive analysis
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
        
        # Validate jobs against expected jobs
        valid_jobs = 0
        for job_id, file_count, job_size in job_stats:
            if job_id in expected_jobs:
                valid_jobs += 1
        
        job_validity_score = (valid_jobs / job_count * 100) if job_count > 0 else 0
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
        
        # 6. Validation decision criteria
        is_valid = (
            validation_percentage >= 60.0 and
            job_validity_score >= 50.0 and
            recent_files >= 10
        )
        
        # Collect issues
        if validation_percentage < 60.0:
            issues.append(f"Overall quality score: {validation_percentage:.1f}%")
        if job_validity_score < 50.0:
            issues.append(f"Job validity: {job_validity_score:.1f}%")
        if recent_files < 10:
            issues.append(f"Recent files: {recent_files}")
        
        reason = f"S3 validation: {validation_percentage:.1f}% quality, {valid_jobs}/{job_count} valid jobs, {recent_files} recent files"
        
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
    """Generate a summary string for S3 validation result"""
    if result.is_valid:
        return f"✅ S3 Valid ({result.validation_percentage:.1f}%): {result.valid_jobs}/{result.job_count} jobs, {result.total_files} files"
    else:
        return f"❌ S3 Invalid ({result.validation_percentage:.1f}%): {', '.join(result.issues[:2])}"