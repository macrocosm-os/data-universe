import asyncio
import json
import os
import subprocess
import time
from typing import Dict, Optional, Any
import logging
import shutil
import bittensor as bt
from dynamic_desirability.chain_utils import ChainPreferenceStore, add_args
from common import constants
from common.data import DataLabel, DataSource
from common.utils import get_validator_data, is_validator
from rewards.data import DataSourceDesirability, DataDesirabilityLookup
from dynamic_desirability.constants import (REPO_URL,
                                            PREFERENCES_FOLDER,
                                            DEFAULT_JSON_PATH,
                                            AGGREGATE_JSON_PATH,
                                            TOTAL_VALI_WEIGHT,
                                            DEFAULT_SCALE_FACTOR,
                                            AMPLICATION_FACTOR
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


def calculate_total_weights(validator_data: Dict[str, Dict[str, Any]], default_json_path: str = DEFAULT_JSON_PATH,
                            total_vali_weight: float = TOTAL_VALI_WEIGHT) -> None:
    """Calculate total weights and write to total.json."""
    total_weights: Dict[str, Dict[str, float]] = {}
    subnet_weight = 1 - total_vali_weight
    normalizer = subnet_weight / AMPLICATION_FACTOR

    # Adding default weights to total.
    try:
        with open(default_json_path, 'r') as f:
            default_preferences = json.load(f)
        for source in default_preferences:
            source_name = source['source_name']
            if source_name not in total_weights:
                total_weights[source_name] = {}
            for label, weight in source['label_weights'].items():
                total_weights[source_name][label] = weight
    except FileNotFoundError:
        bt.logging.error(f"Warning: {default_json_path} not found. Proceeding without default weights.")

    # Calculating the sum of percent_stake for validators that have voted. Non-voting validators are excluded.
    total_stake = sum(v.get('percent_stake', 1) for v in validator_data.values() if v.get('json'))
    for hotkey, data in validator_data.items():
        if data['json']:
            stake_percentage = data.get('percent_stake', 1) / total_stake
            vali_weight = total_vali_weight * stake_percentage
            for source in data['json']:
                source_name = source['source_name']
                if source_name not in total_weights:
                    total_weights[source_name] = {}
                for label, weight in source['label_weights'].items():
                    # Skip labels that are longer than 140 characters
                    if len(label) > 140:
                        continue
                    if label in total_weights[source_name]:
                        total_weights[source_name][label] += vali_weight * weight / normalizer
                    else:
                        total_weights[source_name][label] = vali_weight * weight / normalizer
                        
                    if total_weights[source_name][label] > 5.0:
                        total_weights[source_name][label] = 5.0

    # At the end of aggregation, if some label weights are < default weight value, increase them to match. 
    for source_name, label_weights in total_weights.items():
        for label, weight in label_weights.items():
            if weight < DEFAULT_SCALE_FACTOR:
                label_weights[label] = DEFAULT_SCALE_FACTOR + 0.01

    total_json = [
        {
            "source_name": source_name,
            "label_weights": label_weights
        }
        for source_name, label_weights in total_weights.items()
    ]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    total_path = os.path.join(script_dir, AGGREGATE_JSON_PATH)
    with open(total_path, 'w') as f:
        json.dump(total_json, f, indent=4)

    bt.logging.info(f"\nTotal weights have been calculated and written to {AGGREGATE_JSON_PATH}")


def to_lookup(json_file: str) -> DataDesirabilityLookup:
    """Converts a json format to a LOOKUP format."""
    with open(json_file, 'r') as file:
        data = json.load(file)

    distribution = {}

    for source in data:
        source_name = source['source_name']
        label_weights = source['label_weights']

        source_weight = DataSource.REDDIT.weight if source_name.lower() == "reddit" else DataSource.X.weight

        label_scale_factors = {
            DataLabel(value=label): weight
            for label, weight in label_weights.items()
        }

        distribution[getattr(DataSource, source_name.upper())] = DataSourceDesirability(
            weight=source_weight,
            default_scale_factor=DEFAULT_SCALE_FACTOR,  # number is subject to change
            label_scale_factors=label_scale_factors
        )

    max_age_in_hours = constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS * 24
    return DataDesirabilityLookup(distribution=distribution, max_age_in_hours=max_age_in_hours)


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


async def run_retrieval(config) -> DataDesirabilityLookup:
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
                                total_vali_weight=TOTAL_VALI_WEIGHT)

        return to_lookup(os.path.join(script_dir, AGGREGATE_JSON_PATH))

    except Exception as e:
        bt.logging.error(f"Could not retrieve dynamic preferences. Using default.json to build lookup: {str(e)}")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return to_lookup(os.path.join(script_dir, DEFAULT_JSON_PATH))

def sync_run_retrieval(config):
    return asyncio.run(run_retrieval(config))

if __name__ == "__main__":
    asyncio.run(run_retrieval(config=None))