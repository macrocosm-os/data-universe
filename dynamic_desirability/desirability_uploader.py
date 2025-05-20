import asyncio
import argparse
import json
import os
import shutil
import subprocess
import bittensor as bt
from typing import List, Optional, Dict, Tuple
from dynamic_desirability.chain_utils import ChainPreferenceStore, add_args
from dynamic_desirability.constants import REPO_URL, BRANCH_NAME, PREFERENCES_FOLDER
from dynamic_desirability.data import normalize_preferences


def run_command(command: List[str]) -> str:
    """Runs a subprocess command."""
    try:
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        bt.logging.error(f"Error executing command: {' '.join(command)}")
        bt.logging.error(f"Error message: {e.stderr.strip()}")
        raise


def normalize_preferences_json(file_path: str = None, desirability_dict: Dict = None, hotkey: str = None) -> Optional[str]:
    """
    Normalize potentially invalid preferences JSONs using Pydantic models.
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

    return normalize_preferences(data, hotkey)


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


def run_uploader_from_gravity(config, desirability_dict) -> Tuple[bool, str]:
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
        error_message = f"An error occurred: {str(e)}"
        bt.logging.error(error_message)
        return False, error_message


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set desirabilities for Gravity.")
    add_args(parser, is_upload=True)
    args = parser.parse_args()
    asyncio.run(run_uploader(args))