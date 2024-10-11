# Using Gravity

## Quick Start

### To learn more about Dynamic Desirability (Gravity), see:
[Dynamic Desirability Doc](https://github.com/macrocosm-os/data-universe/blob/gravity/docs/dynamic_desirability.md)

### For validators, see:
- [`json_maker.ipynb`](https://colab.research.google.com/drive/1bc6OWAZ8EbKEGtc1Bnt5D_kJVKmcDo1K?usp=sharing)
- `desirability_uploader.py`
- `desirability_retrieval.py`

### For miners, see:
- `desirability_retrieval.py`

## json_maker.ipynb

The JSON maker tool is available on [Google Colab](https://colab.research.google.com/drive/1bc6OWAZ8EbKEGtc1Bnt5D_kJVKmcDo1K?usp=sharing), where validators can save a copy or download for their own use.

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


## desirability_uploader.py

This file provides functionality for validators to upload their `my_preferences.json` file onto the Preferences Github and use the associated Github SHA to make a commit to the chain. These can then be retrieved any time from the chain using `desirability_retrieval.py`.

To run the script, you will need the following arguments:
- `--wallet`
    - The name of your selected Bittensor wallet. 
- `--hotkey`
    - The name of your selected Bittensor hotkey.
- `--network`
    - The subtensor network.
- `--netuid`
    - For all uses on SN13, 13. 
- `--file_path`
    - This is the path to the preferences JSON file that will be uploaded to the shared repository and pushed to the chain. 

Example Input:
```
python dynamic_desirability/desirability_uploader.py --wallet YOUR_WALLET_NAME --hotkey YOUR_HOTKEY_NAME --network finney --netuid 13 --file_path dynamic_desirability/default.json
```

## desirability_retrieval.py

This file provides functionality for miners and validators to retrieve the current state of aggregated validator and subnet preferences from the chain. `run_retrieval()` outputs the aggregate label weights to `total.json` and also returns them as a DataDesirabilityLookup object with a default scale factor of 0.5. 

This script is called from [`validator.py`](https://github.com/macrocosm-os/data-universe/blob/gravity/neurons/validator.py#L123) once every 24 hours at 12 am UTC. The update frequency will be increased in later versions. 


