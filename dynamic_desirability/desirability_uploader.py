import asyncio
import argparse
import json
import os
import shutil
import subprocess
import bittensor as bt
from typing import List, Optional, Dict
from decimal import Decimal, ROUND_HALF_UP
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


def normalize_preferences_json(file_path: str = None, desirability_dict: Dict = None) -> Optional[str]:
    """
    Normalize potentially invalid preferences JSONs
    """
    if file_path:
        try:
            with open(file_path, 'r') as f:
                if os.path.getsize(file_path) == 0:
                    bt.logging.info("File is empty. Pushing an empty JSON file to delete preferences.")
                    return {}
                data = json.load(f)
        except FileNotFoundError:
            bt.logging.error(f"File not found: {file_path}.")
            return None
        except Exception as e:
            bt.logging.error(f"Unexpected error while reading file: {e}.")
            return None
    elif desirability_dict:
        pass
    else:
        bt.logging.error(f"File path and/or list of desirabilities is not provided.")
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
        bt.logging.error(f"Error while parsing your JSON file: {e}.")
        return None

    total_weight = sum(all_label_weights.values())
    if total_weight <= 0:
        bt.logging.error(f"Cannot normalize preferences file. Please see docs for correct preferences format.")
        return None

    # Normalize weights to sum to 1
    normalized_weights = {
        label: float(weight / total_weight)
        for label, weight in all_label_weights.items()
    }

    # Remove sources with no label weights
    updated_data = []
    for source in data:
        updated_label_weights = {
            label: normalized_weights[label]
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
    chain_store = ChainPreferenceStore(wallet=my_wallet, subtensor=subtensor, netuid=args.netuid)

    try:

        json_content = normalize_preferences_json(file_path=args.file_path)
        if isinstance(json_content, dict):
            json_content = json.dumps(json_content, indent=4)

        if not json_content:
            bt.logging.error("Please see docs for correct format. Not pushing to Github or chain.")
            return

        bt.logging.info(f"JSON content:\n{json_content}")
        github_commit = upload_to_github(json_content, my_hotkey)
        await chain_store.store_preferences(github_commit)
        result = await chain_store.retrieve_preferences(hotkey=my_hotkey)
        bt.logging.info(f"Stored {result} on chain commit hash.")
        return result
    except Exception as e:
        bt.logging.error(f"An error occurred: {str(e)}")
        raise


async def run_uploader_from_gravity(config, desirability_dict):
    wallet = bt.wallet(config=config)
    subtensor = bt.subtensor(config=config)
    chain_store = ChainPreferenceStore(wallet=wallet, subtensor=subtensor, netuid=config.netuid)
    try:

        json_content = normalize_preferences_json(desirability_dict=desirability_dict)
        if isinstance(json_content, dict):
            json_content = json.dumps(json_content, indent=4)

        if not json_content:
            message = "Please see docs for correct format. Not pushing to Github or chain."
            bt.logging.error(message)
            return False, message

        github_commit = upload_to_github(json_content, wallet.hotkey_str)
        await chain_store.store_preferences(github_commit)
        result = await chain_store.retrieve_preferences(hotkey=wallet.hotkey_str)
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