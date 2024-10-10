import unittest
import bittensor as bt
import os
import json
from unittest.mock import patch, mock_open
from dynamic_desirability.desirability_retrieval import to_lookup
from dynamic_desirability.constants import REDDIT_SOURCE_WEIGHT, X_SOURCE_WEIGHT
from rewards.data import DataDesirabilityLookup
from common.data import DataLabel, DataSource
from common import constants

"""Testing to_lookup that converts a json desirability index to a DataDesirabilityLookup object."""

def formatted_print(distribution):
    """Converts a DataDesirabilityLookup distribution to JSON-like format."""
    json_data = []
    
    for source_id, desirability in distribution.items():
        source_name = "reddit" if source_id == 1 else "x"
        label_weights = {label: weight for label, weight in desirability.label_scale_factors.items()}

        json_data.append({
            "source_name": source_name,
            "label_weights": label_weights
        })
    
    # Return as a pretty-printed JSON string
    return json.dumps(json_data, indent=4)

json_data = json.dumps([
            {
                "source_name": "reddit",
                "label_weights": {
                    "r/Bitcoin": 0.1,
                    "r/BitcoinCash": 0.1,
                    "r/Bittensor_": 0.1,
                    "r/Btc": 0.1,
                }
            },
            {
                "source_name": "x",
                "label_weights": {
                    "#bitcoin": 0.4,
                    "#bitcoincharts": 0.2,
                }
            }
        ], indent=4)

bt.logging.info(f"\njson_data:\n\n{json_data}\n")

file_name = 'data.json'
file_path = os.path.join('tests/dynamic_desirability', file_name)
with open(file_path, 'w') as json_file:
    json_file.write(json_data)

bt.logging.info(f"JSON data has been saved to {file_name}")

bt.logging.info("Converting to DataDesirabilityLookup...")
lookup = to_lookup(file_path)
primitive_lookup = DataDesirabilityLookup.to_primitive_data_desirability_lookup(lookup)

bt.logging.info(f"\n{lookup}\n")
bt.logging.info("Validating distribution...")
result = lookup.validate_distribution(primitive_lookup.distribution)
bt.logging.info(f"\n{formatted_print(result)}\nmax age in hours: {primitive_lookup.max_age_in_hours}")