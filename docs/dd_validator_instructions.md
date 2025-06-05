# Dynamic Desirability Instructions for Validators

## Overview

### To learn more about Dynamic Desirability (Gravity), see:
[Dynamic Desirability Doc](https://github.com/macrocosm-os/data-universe/blob/gravity/docs/dynamic_desirability.md)

### Overall Process

The overall process of Dynamic Desirability is as follows:

1. Validators have voting power proportional to their percentage stake in the subnet.
2. Validators create a JSON representing their votes for incentivized labels.
3. Validators upload their JSON to the Macrocosmos Desirabilities GitHub Repository.
4. Validators take the SHA from their Github commit and commit it to the Bittensor chain.
5. The overall label weights landscape is reconstructed according to validators’ voting power and their JSON submissions.
6. This aggregated desirability lookup is used to score Miner scraped data.

### Early Entry

The total validator bandwidth is 0.7 (70% of the total bandwidth), and the total subnet owner weight is 0.3. Early entry is heavily incentivized, as only active validators are counted. The following scenarios are an example of how bandwidth is distributed amongst active validators:

- Validator A has 5% subnet stake.
- Validator B has 15% subnet stake. 

Only Validator A uploads preferences:
- Validator A has 70% of the total bandwidth to drive the subnet, and the whole of the validator bandwidth. 

Validator A and B upload preferences:
- Validator A has 17.5% of the total bandwidth, and one quarter of the validator bandwidth.
- Validator B has 52.5% of the total bandwidth, and three quarters of the validator bandwidth.

The highest a validator can incentivize one label weight (one active validator putting a label weight of 1 on a single label) is 23.333. The default label weight is 0.5 for non incentivized data.


## Getting Started

### Access

First, make sure you have filled out the [Gravity Write Access Request Form](https://forms.gle/ZJQMC6rwYY4ZWyfU7) to gain write access to the [Gravity Repo](https://github.com/macrocosm-os/gravity), where all validators store their preferences files. 

<p align="center">
  <img src="/assets/access_1.png" width="400" height="220">
</p>

Once you have completed the form, a member of the SN13 team (@arrmlet (Volodymyr Truba) or @ewekazoo) will grant and confirm your access. 

<p align="center">
  <img src="/assets/access_2.png" width="400" height="300">
</p>

### Preference JSONs

Validator preference JSONs must adhere to the following conditions:
1. Label weights must be between (0,1].
2. Label weights must sum to between 0 and 1 (across all sources).
3. Each label weight must be in an increment of 0.1.
4. Weights must be from subnet data sources: Reddit or X.

An example of a valid JSON submission is given below:
```
[
    {
        "id": "default_0",
        "weight": 0.25,
        "params": {
            "keyword": null,
            "platform": "reddit",
            "label": "r/Bitcoin",
            "post_start_datetime": null,
            "post_end_datetime": null
        }
    },
    {
        "id": "default_1",
        "weight": 0.25,
        "params": {
            "keyword": null,
            "platform": "reddit",
            "label": "r/BitcoinCash",
            "post_start_datetime": null,
            "post_end_datetime": null
        }
    },
    {
        "id": "default_2",
        "weight": 0.25,
        "params": {
            "keyword": null,
            "platform": "reddit",
            "label": "r/Bittensor_",
            "post_start_datetime": null,
            "post_end_datetime": null
        }
    },
    {
        "id": "default_3",
        "weight": 0.25,
        "params": {
            "keyword": null,
            "platform": "x",
            "label": "#macrocosmos",
            "post_start_datetime": null,
            "post_end_datetime": null
        }
    }
]
```

Invalid JSONs will be normalized during upload:

- Only positive weights will be counted.
- Individual label weights are normalized across the sum of the total weight, to add to 1.

More information on JSON restrictions can be found in the [Dynamic Desirability Doc](https://github.com/macrocosm-os/data-universe/blob/gravity/docs/dynamic_desirability.md).

## Submitting and Deleting Preferences with the API

As a validator, you can submit preferences through the SN13 Validator API using the `set_desirabilities` endpoint. To delete current preferences, simply submit an empty list `[]`.

---

<details>
  <summary>
    Non-API Preference Submission Instructions
  </summary>


## Uploading Desirabilities

### desirability_uploader.py

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
python dynamic_desirability/desirability_uploader.py --wallet YOUR_WALLET_NAME --hotkey YOUR_HOTKEY_NAME --network finney --netuid 13 --file_path YOUR_FILE_PATH
```

After running the script, your my_preferences.json will be uploaded to the Gravity Github Repo, and the associated SHA will be committed to the chain. Once this has finished, your preferences will be available for retrieval at any time. 

### Current Restrictions

Chain uploads are limited to once every 20 minutes. This is due to the chain commit hash limitations. 

Currently, all validators retrieve the latest updated preferences from the chain every 24 hours. In the future, this frequency will be increased to greater reflect real-time updates.

### Deleting Desirabilities

To delete your desirabilities, simply upload an empty JSON file. Uploading an empty JSON will remove your vote from the pool.

## Retrieving Desirabilities

### desirability_retrieval.py

This file provides functionality for miners and validators to retrieve the current state of aggregated validator and subnet preferences from the chain. This is done through automatic timed updates when validators are running - there is no need to manually run the script. 

`run_retrieval()` outputs the aggregate label weights to total.json and also returns them as a DataDesirabilityLookup object with a default scale factor of 0.5.

This script is called from [`validator.py`](https://github.com/macrocosm-os/data-universe/blob/gravity/neurons/validator.py#L123) once every 24 hours at 12 am (midnight) UTC and on a new validator run. The update frequency will be increased in later versions to better reflect real-time updates. Validator logs will be shown in wandb.

Update:

![Alt text](/assets/retrieval_1.png)

No Update:

![Alt text](/assets/retrieval_2.png)
![Alt text](/assets/retrieval_3.png)

Miners can also choose to retrieve the updated desirability lookup every day. This is done by setting the `--gravity` flag. See [`neurons/config.py`](../neurons/config.py) and [`neurons/miner.py`](../neurons/miner.py) for code references. 

</details>
