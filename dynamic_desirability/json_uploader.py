import asyncio
import json
import os
import shutil
import subprocess
import logging
import bittensor as bt
from typing import List
from decimal import Decimal, ROUND_HALF_UP
from chain_utils import ChainPreferenceStore
from constants import REPO_URL, BRANCH_NAME, PREFERENCES_FOLDER, NETWORK, NETUID
from dynamic_desirability.gravity_config import WALLET_NAME, HOTKEY_NAME, MY_JSON_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_command(command: List[str]) -> str:
    """Runs a subprocess command."""
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing command: {' '.join(command)}")
        logging.error(f"Error message: {e.stderr.strip()}")
        raise

def normalize_preferences_json(file_path: str) -> str:
    """
    Normalize potentially invalid preferences JSONs
    """
    with open(file_path, 'r') as f:
        data = json.load(f)

    all_label_weights = {}
    
    # Taking all positive label weights across all sources
    for source in data:
        for label, weight in source["label_weights"].items():
            weight_decimal = Decimal(str(weight))
            if weight_decimal > Decimal('0'):
                all_label_weights[label] = all_label_weights.get(label, Decimal('0')) + weight_decimal
    sorted_labels = sorted(all_label_weights.items(), key=lambda x: x[1], reverse=True)[:10]

    total_weight = sum(weight for _, weight in sorted_labels)
    if total_weight <= 0:
        bt.logging.error(f"Cannot normalize preferences file. Please see docs for correct preferences format.")
        return

    # Normalize weights to sum between 0.1 and 1
    target_sum = min(max(total_weight, Decimal('0.1')), Decimal('1'))
    scale_factor = target_sum / total_weight

    normalized_weights = {
        label: (weight * scale_factor).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
        for label, weight in sorted_labels
    }

    # Remove labels that round to 0.0
    normalized_weights = {label: weight for label, weight in normalized_weights.items() if weight > Decimal('0')}

    # Final adjustment to ensure sum is 1
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

def load_and_validate_preferences(file_path: str) -> str:
    """Load preferences from a JSON file and validate the label weights."""
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    label_weights = {}
    total_weight = Decimal('0')
    
    for source in data:
        for label, weight in source["label_weights"].items():
            # Convert weight to Decimal for precise comparison.
            weight_decimal = Decimal(str(weight))
            
            # Check if weight is an increment of 0.1 between 0.1 and 1.
            if weight_decimal < Decimal('0.1') or weight_decimal > Decimal('1') or \
               (weight_decimal * 10) % 1 != 0:
                raise ValueError(f"Error: Weight {weight} for label '{label}' is not a valid increment of 0.1 between 0.1 and 1")
            
            label_weights[label] = label_weights.get(label, 0) + weight_decimal
            total_weight += weight_decimal
    
    # Check if there are more than 10 total label_weights.
    if len(label_weights) > 10:
        raise ValueError("Error: More than 10 total label_weights")
    
    # Check if all weights add up to more than 1.
    if total_weight > Decimal('1'):
        raise ValueError(f"Error: Total weight {total_weight} exceeds 1")
    
    return json.dumps(data, indent=4)


def upload_to_github(json_content: str, hotkey: str) -> str:
    """Uploads the preferences json to Github."""

    repo_name = REPO_URL.split("/")[-1].replace(".git", "")
    if os.path.exists(repo_name):
        logging.info(f"Repo already exists: {repo_name}.")
    else:
        logging.info(f"Cloning repository: {REPO_URL}")
        run_command(["git", "clone", REPO_URL])

    os.chdir(repo_name)

    logging.info(f"Checking out and updating branch: {BRANCH_NAME}")
    run_command(["git", "checkout", BRANCH_NAME])
    run_command(["git", "pull", "origin", BRANCH_NAME])

    # If for any reason folder was deleted, creates folder.
    if not os.path.exists(PREFERENCES_FOLDER):
        logging.info(f"Creating folder: {PREFERENCES_FOLDER}")
        os.mkdir(PREFERENCES_FOLDER)

    file_name = f"{PREFERENCES_FOLDER}/{hotkey}.json"
    logging.info(f"Creating preferences file: {file_name}")
    with open(file_name, 'w') as f:
        f.write(json_content)

    logging.info(f"Creating preferences file: {file_name}")
    with open(file_name, 'w') as f:
        f.write(json_content)

    logging.info("Staging, committing, and pushing changes")

    try:    
        run_command(["git", "add", file_name])
        run_command(["git", "commit", "-m", f"Add {hotkey} preferences JSON file"])
        run_command(["git", "push", "origin", BRANCH_NAME])
    except subprocess.CalledProcessError as e:
        bt.logging.warning("What you're currently trying to commit has no differences to your last commit. Proceeding with last commit...")

    logging.info("Retrieving commit hash")
    local_commit_hash = run_command(["git", "rev-parse", "HEAD"])

    run_command(["git", "fetch", "origin", BRANCH_NAME])
    remote_commit_hash = run_command(["git", "rev-parse", f"origin/{BRANCH_NAME}"])

    if local_commit_hash == remote_commit_hash:
        logging.info(f"Successfully pushed. Commit hash: {local_commit_hash}")
    else:
        logging.warning("Local and remote commit hashes differ.")
        logging.warning(f"Local commit hash: {local_commit_hash}")
        logging.warning(f"Remote commit hash: {remote_commit_hash}")

    os.chdir("..")
    logging.info(f"Deleting the cloned repository folder: {repo_name}")
    shutil.rmtree(repo_name)

    return remote_commit_hash


async def run_uploader(preference_file: str):
    my_wallet = bt.wallet(name=WALLET_NAME, hotkey=HOTKEY_NAME)
    my_hotkey = my_wallet.hotkey.ss58_address
    subtensor = bt.subtensor(network=NETWORK)
    chain_store = ChainPreferenceStore(wallet=my_wallet, subtensor=subtensor, netuid=NETUID)

    try:
        #json_content = load_and_validate_preferences(preference_file)
        json_content = normalize_preferences_json(preference_file)
        print(json_content)
        if json_content:
            github_commit = upload_to_github(json_content, my_hotkey)
            await chain_store.store_preferences(github_commit)
            result = await chain_store.retrieve_preferences(hotkey=my_hotkey)
            bt.logging.info(f"Stored {result} on chain commit hash.")
            return result
        else:
            bt.logging.error("Your preferences cannot be normalized to a valid format. Please see docs for info.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(run_uploader(preference_file=MY_JSON_PATH))