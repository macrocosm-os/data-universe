# Using Gravity

## Quick Guide

### To learn more about Dynamic Desirability, see:
[Dynamic Desirability Doc](https://github.com/macrocosm-os/data-universe/blob/gravity/docs/dynamic_desirability.md)

### For validators, see:
- `json_maker.ipynb`
- `chain_config.py`
- `json_uploader.py`
- `desirability_retrieval.py`

### For miners, see:
- `chain_config.py`
- `desirability_retrieval.py`

## json_maker.ipynb

This is a tool for validators to create their `my_preferences.json`, which indicates their choices of incentivized labels. This JSON creation tool prompts the user for a source, label, and weight to insert into their JSON using the **Submit** button, allowing validators to build their preferences file. When validators have finished creating their `my_preferences.json`, they can press the **Finalize** button to save their created JSON to my_preferences.json. 

Validator preference JSONs must adhere to the following conditions:
1. Label weights must be between (0,1].
2. Label weights must sum to between 0 and 1 (across all sources).
3. Each label weight must be in an increment of 0.1.
4. Weights must be from subnet data sources: Reddit or X.

An example of a valid JSON submission is given below:
```
[
    {
        "source_name": "reddit",
        "label_weights": {
            "r/Bitcoin": 0.1,
            "r/BitcoinCash": 0.1,
            "r/Bittensor_": 0.1,
            "r/Btc": 0.1,
            "r/Cryptocurrency": 0.1,
            "r/Cryptomarkets": 0.1,
            "r/EthereumClassic": 0.1
        }
    },
    {
        "source_name": "x",
        "label_weights": {
            "#bitcoin": 0.1,
            "#bitcoincharts": 0.1,
            "#bitcoiner": 0.1
        }
    }
]
```

More information on JSON restrictions can be found in the [Dynamic Desirability Doc](https://github.com/macrocosm-os/data-universe/blob/gravity/docs/dynamic_desirability.md).



## chain_config.py

To run `json_uploader.py` or `desirability_retrieval.py`, you must first fill out the chain config. Use the wallet and hotkey associated with your validator/miner running on Subnet 13. 

```
WALLET_NAME="YOUR_WALLET_HERE"
HOTKEY_NAME="YOUR_HOTKEY_HERE"
```

## json_uploader.py

This file provides functionality for validators to upload their `my_preferences.json` file onto the Preferences Github and use the associated Github SHA to make a commit to the chain. These can then be retrieved any time from the chain using `desirability_retrieval.py`.



## desirability_retrieval.py

This file provides functionality for miners and validators to retrieve the current state of aggregated validator and subnet preferences from the chain. `run_retrieval()` outputs the aggregate label weights to `total.json` and also returns them as a DataDesirabilityLookup object with a default scale factor of 0.4. 


