import asyncio
import json
import os
import shutil
import subprocess
import logging
import bittensor as bt
from typing import List
from decimal import Decimal
from chain_utils import ChainPreferenceStore
from constants import REPO_URL, BRANCH_NAME, NETWORK, NETUID
from chain_config import WALLET_NAME, HOTKEY_NAME

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MY_JSON_NAME = 'my_preferences.json'

def run_command(command: List[str]) -> str:
    """Runs a subprocess command."""
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing command: {' '.join(command)}")
        logging.error(f"Error message: {e.stderr.strip()}")
        raise

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
    
    return json.dumps(data)


def upload_to_github(json_content: str, hotkey: str) -> str:
    """Uploads the preferences json to Github."""

    logging.info(f"Cloning repository: {REPO_URL}")
    run_command(["git", "clone", REPO_URL])

    repo_name = REPO_URL.split("/")[-1].replace(".git", "")
    os.chdir(repo_name)

    logging.info(f"Checking out and updating branch: {BRANCH_NAME}")
    run_command(["git", "checkout", BRANCH_NAME])
    run_command(["git", "pull", "origin", BRANCH_NAME])

    file_name = f"{hotkey}.json"

    logging.info(f"Creating preferences file: {file_name}")
    with open(file_name, 'w') as f:
        f.write(json_content)

    logging.info("Staging, committing, and pushing changes")
    run_command(["git", "add", file_name])
    run_command(["git", "commit", "-m", f"Add {hotkey} preferences JSON file"])
    run_command(["git", "push", "origin", BRANCH_NAME])

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
        json_content = load_and_validate_preferences(preference_file)
        github_commit = upload_to_github(json_content, my_hotkey)
        await chain_store.store_preferences(github_commit)
        result = await chain_store.retrieve_preferences(hotkey=my_hotkey)
        bt.logging.info(f"Stored {result} on chain commit hash.")
        return result
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pref_file = os.path.join(script_dir, MY_JSON_NAME)
    asyncio.run(run_uploader(preference_file=pref_file))