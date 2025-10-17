import asyncio
import json
import os
import subprocess
import time
from typing import Dict, Optional, Any
import logging
import shutil
from datetime import datetime
import bittensor as bt
from dynamic_desirability.chain_utils import ChainPreferenceStore, add_args
from common import constants
from common.data import DataLabel, DataSource
from common.utils import get_validator_data, is_validator, time_bucket_id_from_datetime, parse_iso_date
from rewards.data import DataSourceDesirability, DataDesirabilityLookup, Job, JobMatcher
from scraping.youtube.model import YouTubeContent
from dynamic_desirability.constants import (REPO_URL,
                                            PREFERENCES_FOLDER,
                                            DEFAULT_JSON_PATH,
                                            AGGREGATE_JSON_PATH,
                                            TOTAL_VALI_WEIGHT,
                                            DEFAULT_SCALE_FACTOR,
                                            AMPLICATION_FACTOR,
                                            MINIMUM_DD_SCALE,
                                            )


def get_json(commit_sha: str, filename: str) -> Optional[Dict[str, Any]]:
    """Retrieve JSON content from a specific commit in the repository."""
    original_dir = os.getcwd()
    repo_name = REPO_URL.split('/')[-1].replace('.git', '')
    repo_path = os.path.join(original_dir, repo_name)

    try:
        if not os.path.exists(repo_name):
            bt.logging.info(f"Cloning repository: {REPO_URL}")
            subprocess.run(['git', 'clone', REPO_URL], check=True, capture_output=True)
        os.chdir(repo_name)

        bt.logging.info("Fetching latest changes")
        subprocess.run(['git', 'fetch', '--all'], check=True, capture_output=True)

        bt.logging.info(f"Checking out commit: {commit_sha}")
        subprocess.run(['git', 'checkout', commit_sha], check=True, capture_output=True)

        file_path = os.path.join(PREFERENCES_FOLDER, filename)

        if os.path.exists(file_path):
            bt.logging.info(f"File '{file_path}' found. Reading contents...")
            with open(file_path, 'r') as file:
                content = json.load(file)
            return content
        else:
            bt.logging.error(f"File '{file_path}' not found in this commit.")
            return None

    except subprocess.CalledProcessError as e:
        bt.logging.error(f"An error occurred during Git operations: {e}")
        return None
    except IOError as e:
        bt.logging.error(f"An error occurred while reading the file: {e}")
        return None
    finally:
        os.chdir(original_dir)


def create_job_key(job_params: Dict[str, Any]) -> tuple[Optional[str], str, Optional[str], Optional[str], Optional[str]]:
    """Create a unique key that includes all job parameters including datetime fields."""
    key = (
        job_params.get("keyword"),
        job_params.get("platform"), 
        job_params.get("label"),
        job_params.get("post_start_datetime"),
        job_params.get("post_end_datetime")
    )
    return key


def upload_aggregate_to_github(total_json_path: str) -> bool:
    """Upload the aggregate total.json file to the gravity repository."""
    original_dir = os.getcwd()
    repo_name = REPO_URL.split("/")[-1].replace(".git", "")
    
    try:
        # Clone or navigate to existing repo
        if os.path.exists(repo_name):
            bt.logging.info(f"Repo already exists: {repo_name}.")
        else:
            bt.logging.info(f"Cloning repository: {REPO_URL}")
            subprocess.run(["git", "clone", REPO_URL], check=True, capture_output=True)

        os.chdir(repo_name)

        bt.logging.info(f"Checking out and updating branch: main")
        subprocess.run(["git", "checkout", "main"], check=True, capture_output=True)
        subprocess.run(["git", "pull", "origin", "main"], check=True, capture_output=True)

        # Copy the total.json file to the repo root
        destination_file = "total.json"
        shutil.copy2(total_json_path, destination_file)
        bt.logging.info(f"Copied {total_json_path} to {destination_file}")

        # Stage, commit, and push changes
        try:
            subprocess.run(["git", "add", destination_file], check=True, capture_output=True)
            subprocess.run(["git", "commit", "-m", "Update aggregate dynamic desirability total.json"], check=True, capture_output=True)
            subprocess.run(["git", "push", "origin", "main"], check=True, capture_output=True)
            bt.logging.info("Successfully uploaded aggregate total.json to gravity repo")
            return True
        except subprocess.CalledProcessError as e:
            if "nothing to commit" in e.stderr.decode().lower():
                bt.logging.info("No changes to upload - total.json is already up to date")
                return True
            else:
                bt.logging.error(f"Failed to commit/push: {e}")
                return False

    except subprocess.CalledProcessError as e:
        bt.logging.error(f"Git operation failed: {e}")
        return False
    except Exception as e:
        bt.logging.error(f"Failed to upload aggregate to GitHub: {e}")
        return False
    finally:
        os.chdir(original_dir)

def calculate_total_weights(validator_data: Dict[str, Dict[str, Any]], default_json_path: str = DEFAULT_JSON_PATH,
                            total_vali_weight: float = TOTAL_VALI_WEIGHT, upload_to_gravity: bool = False) -> None:
    """Calculate total weights and write to total.json using the new job-based format.
    Compatible with both old label_weights format and new job-based format.
    Preserves first submitter's custom IDs (or uses default)."""
    aggregated_jobs = {}  # Using complete job key to track unique jobs
    job_ids = {}  # Track the first ID for each job key
    subnet_weight = 1 - total_vali_weight
    normalizer = subnet_weight / AMPLICATION_FACTOR

    # Adding default jobs
    try:
        with open(default_json_path, 'r') as f:
            default_jobs = json.load(f)
            
        # Handle both formats for default jobs
        if default_jobs and isinstance(default_jobs, list):
            # Check if it's in the old format (list of dicts with source_name and label_weights)
            if "source_name" in default_jobs[0] and "label_weights" in default_jobs[0]:
                # Convert old format to new job-based format
                converted_jobs = []
                for source in default_jobs:
                    platform = source["source_name"]
                    for label, weight in source["label_weights"].items():
                        converted_jobs.append({
                            "id": f"default_{len(converted_jobs)}",  # Generate ID for converted jobs
                            "params": {
                                "keyword": None,
                                "platform": platform,
                                "label": label,
                                "post_start_datetime": None,
                                "post_end_datetime": None
                            },
                            "weight": weight
                        })
                default_jobs = converted_jobs
                
        # Add default jobs to the aggregation
        for job in default_jobs:
            if "params" not in job or not all(k in job["params"] for k in ["keyword", "platform", "label"]):
                bt.logging.warning(f"Skipping malformed default job: {job}")
                continue
            
            # Apply channel label conversion for YouTube jobs
            job_params = _apply_channel_label_conversion(job["params"])
                
            job_key = create_job_key(job_params)
            # Store the ID from default jobs (or generate one if missing)
            job_ids[job_key] = job.get("id", f"default_{len(job_ids)}")
            aggregated_jobs[job_key] = {
                "weight": job.get("weight", 1.0),
                "params": job_params
            }
    except FileNotFoundError:
        bt.logging.error(f"Warning: {default_json_path} not found. Proceeding without default jobs.")

    # Calculate the total stake of validators who have voted
    total_stake = sum(v.get('percent_stake', 1) for v in validator_data.values() if v.get('json'))
    
    # Process each validator's job submissions
    for hotkey, data in validator_data.items():
        if not data.get('json'):
            continue
            
        stake_percentage = data.get('percent_stake', 1) / total_stake
        vali_weight = total_vali_weight * stake_percentage
        
        # Check if this validator's JSON is in old format or new format
        validator_json = data['json']
        
        # Convert old format to new format if needed
        if validator_json and isinstance(validator_json, list):
            if "source_name" in validator_json[0] and "label_weights" in validator_json[0]:
                # Old format detected, convert to new job-based format
                converted_jobs = []
                for source in validator_json:
                    platform = source["source_name"]
                    for label, weight in source["label_weights"].items():
                        converted_jobs.append({
                            "id": f"aggregate_{len(converted_jobs)}",  # Generate ID for converted jobs
                            "params": {
                                "keyword": None,
                                "platform": platform,
                                "label": label,
                                "post_start_datetime": None,
                                "post_end_datetime": None
                            },
                            "weight": weight
                        })
                validator_json = converted_jobs
        
        # Process each job from the validator
        for job in validator_json:
            if "params" not in job or not all(k in job["params"] for k in ["keyword", "platform", "label"]):
                bt.logging.warning(f"Skipping malformed job from {hotkey}: {job}")
                continue
                
            # Skip labels that are longer than MAX_LABEL_LENGTH
            if job["params"]["label"] and len(job["params"]["label"]) > constants.MAX_LABEL_LENGTH:
                continue
                
            job_weight = job.get("weight", 1.0)
            
            # Apply channel label conversion for YouTube jobs
            job_params = _apply_channel_label_conversion(job["params"])
            
            job_key = create_job_key(job_params)
            
            # Store the first ID we encounter for this job key (to preserve custom IDs)
            if job_key not in job_ids:
                job_ids[job_key] = job.get("id", f"aggregate_{len(job_ids)}")
            
            weighted_job_value = vali_weight * job_weight / normalizer
            
            if job_key in aggregated_jobs:
                aggregated_jobs[job_key]["weight"] += weighted_job_value
            else:
                aggregated_jobs[job_key] = {
                    "weight": weighted_job_value,
                    "params": job_params
                }
                
            # Cap job weight at 5.0
            if aggregated_jobs[job_key]["weight"] > 5.0:
                aggregated_jobs[job_key]["weight"] = 5.0

    # Apply floor weight - ensure no job is below DEFAULT_SCALE_FACTOR
    for job_key, job_data in aggregated_jobs.items():
        if job_data["weight"] < MINIMUM_DD_SCALE:
            job_data["weight"] = MINIMUM_DD_SCALE

    # Convert aggregated jobs to final format
    final_jobs = []
    for job_key, job_data in aggregated_jobs.items():
        job = {
            "id": job_ids[job_key],  # Use the preserved ID instead of generating new one
            "weight": job_data["weight"],
            "params": job_data["params"]
        }
        final_jobs.append(job)

    # Write aggregated jobs to the output file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    total_path = os.path.join(script_dir, AGGREGATE_JSON_PATH)
    with open(total_path, 'w') as f:
        json.dump(final_jobs, f, indent=4)

    bt.logging.info(f"\nTotal weights have been calculated and written to {AGGREGATE_JSON_PATH}")
    
    # Upload to gravity repo if requested
    if upload_to_gravity:
        bt.logging.info("Uploading aggregate total.json to gravity repository...")
        upload_aggregate_to_github(total_path)


def to_lookup(json_path: str):
    """Converts a dynamic desirability json list into a DataDesirabilityLookup."""
    with open(json_path, 'r') as f:
        jobs = json.load(f)
    
    distribution = {}
    
    # Get all data sources with weight > 0
    active_data_sources = [ds for ds in DataSource if ds.weight > 0]
    
    # Process jobs for each active data source
    for data_source in active_data_sources:
        # Convert enum names to lowercase for matching with platform names in jobs
        platform_name = data_source.name.lower()
        
        # Filter jobs for this platform
        platform_jobs = [job for job in jobs if job['params'].get('platform') == platform_name]
        
        # Create JobMatcher for this platform
        job_matcher = JobMatcher(jobs=[
            Job(
                id=job.get('id'),  # Preserve job ID
                keyword=job['params'].get('keyword'),  # Use get() with default None to handle missing keys
                label=job['params'].get('label'),
                job_weight=job['weight'],
                # Convert datetime strings to time bucket IDs during initial parsing
                start_timebucket=datetime_to_timebucket(job['params'].get('post_start_datetime')),
                end_timebucket=datetime_to_timebucket(job['params'].get('post_end_datetime'))
            ) for i, job in enumerate(platform_jobs)
        ])
        
        # Create DataSourceDesirability object and add to distribution
        distribution[data_source] = DataSourceDesirability(
            weight=data_source.weight,
            job_matcher=job_matcher
        )
    
    max_age_in_hours = constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24
    return DataDesirabilityLookup(
        distribution=distribution,
        max_age_in_hours=max_age_in_hours
    )

def datetime_to_timebucket(datetime_str: Optional[str]) -> Optional[int]:
    """Helper function to convert datetime string to time bucket ID."""
    if not datetime_str:
        return None
    
    try:
        dt_obj = parse_iso_date(datetime_str)
        if dt_obj is None:
            return None
        return time_bucket_id_from_datetime(dt_obj)
    except (ValueError, TypeError) as e:
        # Log an error or warning
        print(f"Warning: Could not parse datetime string: {datetime_str}. Error: {e}")
        return None

def _apply_channel_label_conversion(job_params: Dict[str, Any]) -> Dict[str, Any]:
    """Apply channel label conversion for YouTube jobs."""
    converted_params = job_params.copy()
    if converted_params.get("platform") == "youtube" and converted_params.get("label"):
        raw_label = converted_params["label"]
        converted_params["raw_label"] = raw_label
        converted_params["label"] = YouTubeContent.create_channel_label(raw_label)
    return converted_params

def get_hotkey_json_submission(subtensor: bt.subtensor, netuid: int, metagraph: bt.metagraph, hotkey: str):
    """Gets the unscaled JSON submisson for a specified validator hotkey. 
       If no hotkey is specified, returns the current aggregate desirability list. """
    try:
        if not hotkey:
            bt.logging.info(f"No hotkey specified. Returning aggeregate dynamic desirability list.")
            script_dir = os.path.dirname(os.path.abspath(__file__))
            agg_path = os.path.join(script_dir, AGGREGATE_JSON_PATH)
            with open(agg_path, "r") as file:
                agg_list = json.load(file)
            return agg_list
        
        uid = subtensor.get_uid_for_hotkey_on_subnet(hotkey_ss58=hotkey, netuid=netuid)

        if is_validator(uid=uid, metagraph=metagraph):
            bt.logging.info(f"Hotkey {hotkey} is a validator. Checking for JSON submission to return...")
            commit_sha = subtensor.get_commitment(netuid=netuid, uid=uid)
            return get_json(commit_sha=commit_sha, filename=f"{hotkey}.json")
        else:
            bt.logging.error(f"Hotkey {hotkey} is not a validator. Only validators have JSON submissions. ")
            return None

    except Exception as e:
        bt.logging.error(f"Could not retrieve JSON submission for hotkey {hotkey}. Error: {e}")
        return None


async def run_retrieval(config, upload_to_gravity: bool = False) -> DataDesirabilityLookup:
    try:
        my_wallet = bt.wallet(config=config)
        subtensor = bt.subtensor(config=config)
        metagraph = subtensor.metagraph(netuid=config.netuid)

        bt.logging.info("\nGetting validator weights from the metagraph...\n")
        validator_data = get_validator_data(metagraph=metagraph, vpermit_rao_limit=config.vpermit_rao_limit)

        bt.logging.info("\nRetrieving latest validator commit hashes from the chain (This takes ~90 secs)...\n")

        for hotkey in validator_data.keys():
            uid = subtensor.get_uid_for_hotkey_on_subnet(hotkey_ss58=hotkey, netuid=config.netuid)
            validator_data[hotkey]['github_hash'] =  subtensor.get_commitment(netuid=config.netuid, uid=uid)
            if validator_data[hotkey]['github_hash']:
                validator_data[hotkey]['json'] = get_json(commit_sha=validator_data[hotkey]['github_hash'],
                                                          filename=f"{hotkey}.json")

        bt.logging.info("\nCalculating total weights...\n")

        script_dir = os.path.dirname(os.path.abspath(__file__))
        default_path = os.path.join(script_dir, DEFAULT_JSON_PATH)
        calculate_total_weights(validator_data=validator_data, default_json_path=default_path,
                                total_vali_weight=TOTAL_VALI_WEIGHT, upload_to_gravity=upload_to_gravity)

        return to_lookup(os.path.join(script_dir, AGGREGATE_JSON_PATH))

    except Exception as e:
        bt.logging.error(f"Could not retrieve dynamic preferences. Using default.json to build lookup: {str(e)}")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return to_lookup(os.path.join(script_dir, DEFAULT_JSON_PATH))

def sync_run_retrieval(config, upload_to_gravity: bool = False):
    return asyncio.run(run_retrieval(config, upload_to_gravity))

if __name__ == "__main__":
    asyncio.run(run_retrieval(config=None))