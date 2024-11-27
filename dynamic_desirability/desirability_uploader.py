import asyncio
import argparse
import json
import os
import shutil
import subprocess
import bittensor as bt
from typing import List, Optional
from decimal import Decimal, ROUND_HALF_UP
from dynamic_desirability.chain_utils import ChainPreferenceStore, add_args
from constants import REPO_URL, BRANCH_NAME, PREFERENCES_FOLDER, VALID_SOURCES
from common.logger import logger


def run_command(command: List[str]) -> str:
    """Runs a subprocess command."""
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing command: {' '.join(command)}")
        logger.error(f"Error message: {e.stderr.strip()}")
        raise


def normalize_preferences_json(file_path: str) -> Optional[str]:
    """
    Normalize potentially invalid preferences JSONs
    """
    try:
        with open(file_path, 'r') as f:
            if os.path.getsize(file_path) == 0:
                logger.info("File is empty. Pushing an empty JSON file to delete preferences.")
                return {}
            data = json.load(f)  
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}.")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while reading file: {e}.")
        return None

    all_label_weights = {}
    valid_keys = {"source_name", "label_weights"}
    
    # Taking all positive label weights across all sources that are valid.
    try:
        for source_dict in data:
            source_dict["source_name"] = source_dict["source_name"].lower()
            source_name = source_dict["source_name"]
            source_prefix = VALID_SOURCES[source_name]

            if source_dict.keys() == valid_keys and source_name in VALID_SOURCES.keys(): 
                for label, weight in source_dict["label_weights"].items():
                    weight_decimal = Decimal(str(weight))
                    if weight_decimal > Decimal('0') and label.startswith(source_prefix):
                        all_label_weights[label] = all_label_weights.get(label, Decimal('0')) + weight_decimal
    except Exception as e:
        logger.error(f"Error while parsing your JSON file: {e}.")
        return None

    # If more than 10 label weights, only takes top 10.
    sorted_labels = sorted(all_label_weights.items(), key=lambda x: x[1], reverse=True)[:10]

    total_weight = sum(weight for _, weight in sorted_labels)
    if total_weight <= 0:
        logger.error(f"Cannot normalize preferences file. Please see docs for correct preferences format.")
        return None

    # Normalize weights to sum between 0.1 and 1.
    target_sum = min(max(total_weight, Decimal('0.1')), Decimal('1'))
    scale_factor = target_sum / total_weight

    normalized_weights = {
        label: (weight * scale_factor).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
        for label, weight in sorted_labels
    }

    # Remove labels that round to 0.0
    normalized_weights = {label: weight for label, weight in normalized_weights.items() if weight > Decimal('0')}

    # Final adjustment to ensure sum is 1.
    weight_sum = sum(normalized_weights.values())
    if weight_sum < Decimal('1'):
        deficit = Decimal('1') - weight_sum
        while deficit > Decimal('0'):
            for label in sorted(normalized_weights, key=normalized_weights.get):
                if normalized_weights[label] < Decimal('1'):
                    increase = min(deficit, Decimal('0.1'))
                    normalized_weights[label] += increase
                    deficit -= increase
                    if deficit <= Decimal('0'):
                        break

    # Remove sources with no label weights
    updated_data = []
    for source in data:
        updated_label_weights = {
            label: float(normalized_weights[label])
            for label in source["label_weights"]
            if label in normalized_weights
        }
        if updated_label_weights:
            source["label_weights"] = updated_label_weights
            updated_data.append(source)

    return json.dumps(updated_data, indent=4)


def upload_to_github(json_content: str, hotkey: str) -> str:
    """Uploads the preferences json to Github."""

    repo_name = REPO_URL.split("/")[-1].replace(".git", "")
    if os.path.exists(repo_name):
        logger.info(f"Repo already exists: {repo_name}.")
    else:
        logger.info(f"Cloning repository: {REPO_URL}")
        run_command(["git", "clone", REPO_URL])

    os.chdir(repo_name)

    logger.info(f"Checking out and updating branch: {BRANCH_NAME}")
    run_command(["git", "checkout", BRANCH_NAME])
    run_command(["git", "pull", "origin", BRANCH_NAME])

    # If for any reason folder was deleted, creates folder.
    if not os.path.exists(PREFERENCES_FOLDER):
        logger.info(f"Creating folder: {PREFERENCES_FOLDER}")
        os.mkdir(PREFERENCES_FOLDER)

    file_name = f"{PREFERENCES_FOLDER}/{hotkey}.json"
    logger.info(f"Creating preferences file: {file_name}")
    with open(file_name, 'w') as f:
        f.write(json_content)

    logger.info("Staging, committing, and pushing changes")

    try:    
        run_command(["git", "add", file_name])
        run_command(["git", "commit", "-m", f"Add {hotkey} preferences JSON file"])
        run_command(["git", "push", "origin", BRANCH_NAME])
    except subprocess.CalledProcessError as e:
        logger.warning("What you're currently trying to commit has no differences to your last commit. Proceeding with last commit...")

    logger.info("Retrieving commit hash")
    local_commit_hash = run_command(["git", "rev-parse", "HEAD"])

    run_command(["git", "fetch", "origin", BRANCH_NAME])
    remote_commit_hash = run_command(["git", "rev-parse", f"origin/{BRANCH_NAME}"])

    if local_commit_hash == remote_commit_hash:
        logger.info(f"Successfully pushed. Commit hash: {local_commit_hash}")
    else:
        logger.warning("Local and remote commit hashes differ.")
        logger.warning(f"Local commit hash: {local_commit_hash}")
        logger.warning(f"Remote commit hash: {remote_commit_hash}")

    os.chdir("..")
    logger.info(f"Deleting the cloned repository folder: {repo_name}")
    shutil.rmtree(repo_name)

    return remote_commit_hash


async def run_uploader(args):
    my_wallet = bt.wallet(name=args.wallet, hotkey=args.hotkey)
    my_hotkey = my_wallet.hotkey.ss58_address
    subtensor = bt.subtensor(network=args.network)
    chain_store = ChainPreferenceStore(wallet=my_wallet, subtensor=subtensor, netuid=args.netuid)

    try:

        json_content = normalize_preferences_json(args.file_path)
        if isinstance(json_content, dict):
            json_content = json.dumps(json_content, indent=4)

        if not json_content:
            logger.error("Please see docs for correct format. Not pushing to Github or chain.")
            return

        logger.info(f"JSON content:\n{json_content}")
        github_commit = upload_to_github(json_content, my_hotkey)
        await chain_store.store_preferences(github_commit)
        result = await chain_store.retrieve_preferences(hotkey=my_hotkey)
        logger.info(f"Stored {result} on chain commit hash.")
        return result
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set desirabilities for Gravity.")
    add_args(parser, is_upload=True)
    args = parser.parse_args()
    asyncio.run(run_uploader(args))