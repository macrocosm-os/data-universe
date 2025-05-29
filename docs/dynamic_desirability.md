# Dynamic Desirability (Gravity)

## Foreword 

We welcome questions, comments and feedback on Discord - message @arrmlet or @ewekazoo, or file a GitHub issue. 

The following is a conceptual overview of Dynamic Desirability (Gravity). We recommend you read this first, then the [Validator Instructions](dd_validator_instructions.md) for technical instructions. 


## Overview

[Dynamic Desirability](../dynamic_desirability) takes the previous system of manually setting subnet preferences for data scraping at scale to an organic, democratic mechanism. This introduces a voting system for both the subnet owner and participating validators, where each will set their own weight preferences through an adaptable list of incentivized labels. Validators choose weights and submit them as JSONs into the desirability repository called [Gravity](https://github.com/macrocosm-os/gravity/tree/main). These weights are then committed to the chain, where they can be later read and used to create an aggregate DataDesirabilityLookup object used for scoring. 

Dynamically incentivized jobs are given label scale factors of anywhere from (`default_scale_factor` + 1) up to 5.0, depending on the volume of requests and demand for that particular label. Dynamic Desirability jobs have the following parameters: 

1. `keyword`
    - Optionally, the incentivized keyword/phrase. (Scoring supported soon)
2. `label`
    - The actual incentivized label - the subreddit name or the hashtag.
3. `job_weight`
    - The scale factor received in scoring for providing data that completes this given job. This is at minimum (`default_scale_factor` + 1), and at maximum 5.
4. `start_date`
    - Optionally, the earliest viable date at which data scraped for this job will be rewarded.
5. `end_date`
    - Optionally, the latest viable date at which data scraped for this job will be rewarded.
  
### Important Notes for Scoring: 
If a job does not have a `start_date` and `end_date`, it will be scored as normal with a linear deprecation function and 30-day freshness period. Otherwise if a job has a time range, data returned within the time range will be given a `time_scalar` of 1.0 (number subject to change), and data outside of the job's given time range will receive a `time_scalar` of 0. 

## Gravity Repository

Dynamic Desirability uses the Gravity repo as a part of the validator preference submission pipeline. Read-access is public and encouraged! 

If you are a validator and would like to have your subreddit/hashtag preferences incentivized on SN13, please check out the [**Gravity repo**](https://github.com/macrocosm-os/gravity/tree/main) and fill out a [**request form**](https://forms.gle/BzLg4SwWgmi9xVC18) for write-access. Then, contact any Macrocosmos member on Discord notifying us of your request.


## Overall Process

The overall process of Dynamic Desirability is as follows:

1. Validators have voting power proportional to their percentage stake in the subnet. 
2. Validators create a JSON representing their votes for incentivized labels. 
3. Validators upload their JSON to the Macrocosmos Desirabilities GitHub Repository.
4. Validators take the SHA from their Github commit and commit it to the Bittensor chain. 
5. The overall label weights landscape is reconstructed according to validators’ voting power and their JSON submissions.
6. This aggregated desirability lookup is used to score Miner scraped data. 


## Implementation Details

### Validator Voting

In total, the subnet owner Macrocosmos has a total voting power of 30%, and the validators as a whole have a voting power of 70%. Each individual validator has a share of this 70% total validator voting power according to their percentage stake in the subnet. For instance, if a validator has 20% stake, they would then receive 70% * 20% = 14% of the total voting power. 

There is a provided JSON creation tool (json_maker.ipynb) located in the Data Desirability folder to assist validators in creating a valid submission. The guidelines for a valid JSON submission are outlined below. 

Validators will split their votes in JSON format normalized to the following conditions upon upload into Github: 

1. Label weights must be between (0,1]. 
    - The label weight now represents the percentage of the validator’s own voting power that they would like to place on the given label. For instance, a validator with 20% stake in the subnet for 14% total voting power placing 0.9 weight on a label would result in that label getting 12.6% of the total vote. 

2. Label weights must sum to between 0 and 1. 
    - This comes as a consequence of the previous condition: at most a validator can choose to use 100% of their voting power to specify labels and associated label weights. 

2. Weights must be from subnet data sources: Reddit, X, or YouTube.



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


### Validator Uploading

After validators have created and saved their submission JSON, they use the upload script provided in the Data Desirability folder to upload the file to the Macrocosmos Desirabilities GitHub and commit the GitHub commit SHA to the chain for later retrieval. Commits to the chain are persistent and transparent, allowing for reconstruction of the Data Desirability Lookup at any time during scoring or for individual Validator/Miner purposes. 

Validators should create or modify a chain_config.py file in the dynamic_desirability folder with their Bittensor wallet and hotkey name associated with their validator instance running on subnet 13. This will be used to commit to the chain. Chain commits can be made every 20 minutes. 

Chain commit hashes are used in addition to GitHub version control because it relieves the issue of author authentication on GitHub commits. This is preferable to Macrocosmos owning the API service as it is decentralized and open source, in alignment with Bittensor’s values. 

### Desirability Retrieval

Validators and Miners can retrieve the desirabilities committed to the chain through the desirability_retrieval.py script located in the [dynamic_desirability](../dynamic_desirability) folder. This takes approximately ~90 seconds per retrieval, as each validator JSON submission must be retrieved from the chain and aggregated into a total.json, representing the total desirability lookup of the subnet. 

To maximize score, it is recommended that miners integrate the dynamic desirability retrieval into their scraping procedures to use an up to date version of the current subnet desirabilities. 


## Action for Miners

Miners are encouraged to:
1. Stay updated with the latest desirability lists.
2. Adjust their scraping activities based on the most recent desirability scores to maximize their rewards.
3. Implement systems to quickly adapt to changes in desirability rankings.

---

Note: This feature is subject to community feedback. Details may change based on further research and development.
