import asyncio
import argparse
import json
import os
import shutil
import subprocess
import bittensor as bt
from typing import List, Optional, Dict, Any, Union
from decimal import Decimal, ROUND_HALF_UP
import datetime as dt
from common.constants import MAX_LABEL_LENGTH
from dynamic_desirability.chain_utils import ChainPreferenceStore, add_args
from dynamic_desirability.constants import REPO_URL, BRANCH_NAME, PREFERENCES_FOLDER, VALID_SOURCES


def run_command(command: List[str]) -> str:
    """Runs a subprocess command."""
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        bt.logging.error(f"Error executing command: {' '.join(command)}")
        bt.logging.error(f"Error message: {e.stderr.strip()}")
        raise


def is_new_format(data: List[Dict]) -> bool:
    """
    Determines if the provided data is in the new job format.
    """
    if not data:
        return False
    
    # Check if the first item has the expected structure for the new format
    first_item = data[0]
    return ("id" in first_item and 
            "weight" in first_item and 
            "params" in first_item and 
            isinstance(first_item["params"], dict))


def validate_datetime_format(date_string: str) -> bool:
    """
    Validates that a datetime string is in the correct ISO format.
    """
    if not date_string:
        return True
        
    try:
        dt.datetime.fromisoformat(date_string)
        return True
    except ValueError:
        return False


def validate_datetime_order(start_datetime: str, end_datetime: str) -> bool:
    """
    Validates that start_datetime is before end_datetime.
    """
    if not start_datetime or not end_datetime:
        return True
        
    try:
        start = dt.datetime.fromisoformat(start_datetime)
        end = dt.datetime.fromisoformat(end_datetime)
        return start < end
    except ValueError:
        return False


def normalize_new_format_jobs(jobs: List[Dict]) -> Optional[List[Dict]]:
    """
    Normalizes jobs in the new format and filters out invalid ones.
    """
    valid_jobs = []
    job_weights = {}
    
    for job in jobs:
        # Basic validation
        if not all(k in job for k in ["id", "weight", "params"]):
            bt.logging.warning(f"Skipping job, missing required fields: {job}")
            continue
            
        params = job["params"]
        if not all(k in params for k in ["job_type", "platform", "topic"]):
            bt.logging.warning(f"Skipping job, missing required parameters: {params}")
            continue
            
        # Validate job_type
        if params["job_type"] not in ["keyword", "label"]:
            bt.logging.warning(f"Skipping job with invalid job_type: {params['job_type']}")
            continue
            
        # Validate platform
        if params["platform"] not in VALID_SOURCES:
            bt.logging.warning(f"Skipping job with invalid platform: {params['platform']}")
            continue
            
        # Skip topics that are longer than MAX_LABEL_LENGTH
        if len(params["topic"]) > MAX_LABEL_LENGTH:
            bt.logging.warning(f"Skipping topic {params['topic']}: exceeds {MAX_LABEL_LENGTH} character limit")
            continue
            
        # Validate datetime formats if present
        start_datetime = params.get("post_start_datetime")
        end_datetime = params.get("post_end_datetime")
        
        if start_datetime and not validate_datetime_format(start_datetime):
            bt.logging.warning(f"Skipping job with invalid post_start_datetime format: {start_datetime}")
            continue
            
        if end_datetime and not validate_datetime_format(end_datetime):
            bt.logging.warning(f"Skipping job with invalid post_end_datetime format: {end_datetime}")
            continue
            
        # Validate datetime order if both are present
        if start_datetime and end_datetime and not validate_datetime_order(start_datetime, end_datetime):
            bt.logging.warning(f"Skipping job where post_start_datetime is not before post_end_datetime")
            continue
            
        # Only include jobs with positive weights
        weight = Decimal(str(job["weight"]))
        if weight <= Decimal('0'):
            bt.logging.warning(f"Skipping job with non-positive weight: {weight}")
            continue
            
        # Store valid job and its weight
        valid_jobs.append(job)
        job_weights[job["id"]] = weight
    
    if not valid_jobs:
        bt.logging.error("No valid jobs found after filtering.")
        return None
        
    # Calculate total weight
    total_weight = sum(job_weights.values())
    
    # Normalize weights to sum to 1
    for job in valid_jobs:
        original_weight = job_weights[job["id"]]
        job["weight"] = float(original_weight / total_weight)
    
    return valid_jobs


def convert_old_to_new_format(data: List[Dict], hotkey: str) -> List[Dict]:
    """
    Converts the old preferences format to the new job format.
    """
    new_format_jobs = []
    job_count = 0
    
    for source_dict in data:
        source_name = source_dict.get("source_name", "").lower()
        
        if source_name not in VALID_SOURCES or "label_weights" not in source_dict:
            continue
            
        for topic, weight in source_dict["label_weights"].items():
            # Skip labels that are longer than MAX_LABEL_LENGTH
            if len(topic) > MAX_LABEL_LENGTH:
                bt.logging.warning(f"Skipping label {topic}: exceeds {MAX_LABEL_LENGTH} character limit")
                continue
                
            weight_decimal = Decimal(str(weight))
            if weight_decimal <= Decimal('0'):
                continue
                
            job_count += 1
            job_id = f"{hotkey}_{job_count}"
            
            new_format_jobs.append({
                "id": job_id,
                "weight": float(weight_decimal),
                "params": {
                    "job_type": "label",
                    "platform": source_name,
                    "topic": topic,
                    "post_start_datetime": None,
                    "post_end_datetime": None
                }
            })
    
    return new_format_jobs


def normalize_preferences_json(file_path: str = None, desirability_dict: Dict = None, hotkey: str = None) -> Optional[str]:
    """
    Normalize potentially invalid preferences JSONs and filter out labels longer than 140 characters.
    Works with both old and new formats.
    """
    if file_path:
        try:
            with open(file_path, 'r') as f:
                if os.path.getsize(file_path) == 0:
                    bt.logging.info("File is empty. Pushing an empty JSON file to delete preferences.")
                    return json.dumps([])
                data = json.load(f)
        except FileNotFoundError:
            bt.logging.error(f"File not found: {file_path}.")
            return None
        except Exception as e:
            bt.logging.error(f"Unexpected error while reading file: {e}.")
            return None
    elif desirability_dict:
        data = desirability_dict
    else:
        bt.logging.info(f"Empty desirabilities submitted. Submitting empty vote.")
        return json.dumps([])

    # Check if the data is in the new format
    if is_new_format(data):
        bt.logging.info("Processing new format job configuration")
        normalized_jobs = normalize_new_format_jobs(data)
        if normalized_jobs is None:
            return None
        return json.dumps(normalized_jobs, indent=4)
    
    # Handle old format
    bt.logging.info("Processing old format preferences, will convert to new format")
    if not hotkey:
        bt.logging.error("Hotkey required for converting old format to new format")
        return None
        
    # Convert old format to new format
    new_format_jobs = convert_old_to_new_format(data, hotkey)
    
    # Apply normalization to the converted jobs
    normalized_jobs = normalize_new_format_jobs(new_format_jobs)
    if normalized_jobs is None:
        return None
        
    return json.dumps(normalized_jobs, indent=4)


def upload_to_github(json_content: str, hotkey: str) -> str:
    """Uploads the preferences json to Github."""

    repo_name = REPO_URL.split("/")[-1].replace(".git", "")
    if os.path.exists(repo_name):
        bt.logging.info(f"Repo already exists: {repo_name}.")
    else:
        bt.logging.info(f"Cloning repository: {REPO_URL}")
        run_command(["git", "clone", REPO_URL])

    os.chdir(repo_name)

    bt.logging.info(f"Checking out and updating branch: {BRANCH_NAME}")
    run_command(["git", "checkout", BRANCH_NAME])
    run_command(["git", "pull", "origin", BRANCH_NAME])

    # If for any reason folder was deleted, creates folder.
    if not os.path.exists(PREFERENCES_FOLDER):
        bt.logging.info(f"Creating folder: {PREFERENCES_FOLDER}")
        os.mkdir(PREFERENCES_FOLDER)

    file_name = f"{PREFERENCES_FOLDER}/{hotkey}.json"
    bt.logging.info(f"Creating preferences file: {file_name}")
    with open(file_name, 'w') as f:
        f.write(json_content)

    bt.logging.info("Staging, committing, and pushing changes")

    try:    
        run_command(["git", "add", file_name])
        run_command(["git", "commit", "-m", f"Add {hotkey} preferences JSON file"])
        run_command(["git", "push", "origin", BRANCH_NAME])
    except subprocess.CalledProcessError as e:
        bt.logging.warning("What you're currently trying to commit has no differences to your last commit. Proceeding with last commit...")

    bt.logging.info("Retrieving commit hash")
    local_commit_hash = run_command(["git", "rev-parse", "HEAD"])

    run_command(["git", "fetch", "origin", BRANCH_NAME])
    remote_commit_hash = run_command(["git", "rev-parse", f"origin/{BRANCH_NAME}"])

    if local_commit_hash == remote_commit_hash:
        bt.logging.info(f"Successfully pushed. Commit hash: {local_commit_hash}")
    else:
        bt.logging.warning("Local and remote commit hashes differ.")
        bt.logging.warning(f"Local commit hash: {local_commit_hash}")
        bt.logging.warning(f"Remote commit hash: {remote_commit_hash}")

    os.chdir("..")
    bt.logging.info(f"Deleting the cloned repository folder: {repo_name}")
    shutil.rmtree(repo_name)

    return remote_commit_hash


async def run_uploader(args):
    my_wallet = bt.wallet(name=args.wallet, hotkey=args.hotkey)
    my_hotkey = my_wallet.hotkey.ss58_address
    subtensor = bt.subtensor(network=args.network)
    uid = subtensor.get_uid_for_hotkey_on_subnet(hotkey_ss58=my_hotkey, netuid=args.netuid)

    try:
        json_content = normalize_preferences_json(file_path=args.file_path, hotkey=my_hotkey)
        if not json_content:
            bt.logging.error("Please see docs for correct format. Not pushing to Github or chain.")
            return

        bt.logging.info(f"JSON content:\n{json_content}")
        github_commit = upload_to_github(json_content, my_hotkey)
        subtensor.commit(wallet=my_wallet, netuid=args.netuid, data=github_commit)
        result = subtensor.get_commitment(netuid=args.netuid, uid=uid)
        bt.logging.info(f"Stored {result} on chain commit hash.")
        return result
    except Exception as e:
        bt.logging.error(f"An error occurred: {str(e)}")
        raise


def run_uploader_from_gravity(config, desirability_dict):
    wallet = bt.wallet(config=config)
    subtensor = bt.subtensor(config=config)
    uid = subtensor.get_uid_for_hotkey_on_subnet(hotkey_ss58=wallet.hotkey.ss58_address, netuid=config.netuid)
    try:
        json_content = normalize_preferences_json(
            desirability_dict=desirability_dict,
            hotkey=wallet.hotkey.ss58_address
        )
        if not json_content:
            message = "Please see docs for correct format. Not pushing to Github or chain."
            bt.logging.error(message)
            return False, message

        github_commit = upload_to_github(json_content, wallet.hotkey.ss58_address)
        subtensor.commit(wallet=wallet, netuid=config.netuid, data=github_commit)
        result = subtensor.get_commitment(netuid=config.netuid, uid=uid)
        message = f"Stored {result} on chain commit hash."
        bt.logging.info(message)
        return True, message
    except Exception as e:
        error_message = f"An error occured: {str(e)}"
        return False, error_message


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set desirabilities for Gravity.")
    add_args(parser, is_upload=True)
    args = parser.parse_args()
    asyncio.run(run_uploader(args))