# Validator

The Validator is responsible for validating the Miners and scoring them according to the [incentive mechanism](../README.md#incentive-mechanism). It runs a loop to enumerate all Miners in the network, and for each, it performs the following sequence:
1. It requests the latest [MinerIndex](../README.md#terminology) from the miner, which it stores in a in-memory database.
2. It chooses a random (sampled by size) DataEntityBucket from the MinerIndex to sample.
3. It gets that DataEntityBucket from the Miner.
4. It chooses N DataEntities from the DataEntityBucket to validate. It then scrapes the content from the appropriate DataSource to get those DataEntities.
5. It then compares those retrieved DataEntities against the ones provided by the Miner and updates the Miner Credibility, based on the result.
6. Finally, it updates the Miner's score. This is based on the total MinerIndex scaled by Freshness/Desirability/Duplication/Credibility.

Once this sequence has been performed for all Miners, the Validator waits a period of time before starting the next loop to ensure it does not evaluate a Miner more often than once per N minutes. This helps ensure the cost of running a Validator is not too high, and also protects the network against high amounts of traffic.

As of Jan 13th 2024, the expected cost number of DataItems queried via Apify is roughly: `225 Miners * 1 evals per hour * 2 sample per period * 24 hours = 10800`. Assuming this is ~50% Reddit (Free with Custom Scraper) and ~50% per X ($1 per 1000), the total cost is roughly $5.40 per day.

# System Requirements

Validators require at least 32 GB of RAM but do not require a GPU. We recommend a decent CPU (4+ cores) and sufficient network bandwidth to handle protocol traffic. Must have python >= 3.10.

# Getting Started

## Prerequisites
1. As of Jan 13th 2024, we support Twitter and Reddit scraping via Apify so you'll need to [setup your Apify API token](apify.md).
We also support Reddit scraping via a [personal reddit account](reddit.md) which is completely free. 
Validators will default to using the personal reddit account for reliability but this can be changed editing the PREFERRED_SCRAPERS map in validator.py locally.
We also support YouTube Scraping via a [official youtube api](youtube.md) which is completely free. 

2. Clone the repo

```shell
git clone https://github.com/RusticLuftig/data-universe.git
```

3. Setup your python [virtual environment](https://docs.python.org/3/library/venv.html) or [Conda environment](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands).

4. Install the requirements. From your virtual environment, run
```shell
cd data-universe
python -m pip install -e .
```

5. Make sure you've [created a Wallet](https://docs.bittensor.com/getting-started/wallets) and [registered a hotkey](https://docs.bittensor.com/subnets/register-and-participate).

6. (Optional) Setup a wandb account and login so your validator can store logs beyond 7 days. From your virtual environment, run
```shell
wandb login
```

This will prompt you to navigate to https://wandb.ai/authorize and copy your api key back into the terminal.

## Running the Validator

### With auto-updates

We highly recommend running the validator with auto-updates. This will help ensure your validator is always running the latest release, helping to maintain a high vtrust.

Prerequisites:
1. To run with auto-update, you will need to have [pm2](https://pm2.keymetrics.io/) installed.
2. Make sure your virtual environment is activated. This is important because the auto-updater will automatically update the package dependencies with pip.
3. Make sure you're using the main branch: `git checkout main`.

From the data-universe folder:
```shell
pm2 start --name net13-vali-updater --interpreter python scripts/start_validator.py -- --pm2_name net13-vali --wallet.name cold_wallet --wallet.hotkey hotkey_wallet [other vali flags]
```

This will start a process called `net13-vali-updater`. This process periodically checks for a new git commit on the current branch. When one is found, it performs a `pip install` for the latest packages, and restarts the validator process (who's name is given by the `--pm2_name` flag)


### Without auto-updates

If you'd prefer to manage your own validator updates...

From the data-universe folder:
```shell
pm2 start python -- ./neurons/validator.py --wallet.name your-wallet --wallet.hotkey your-hotkey
```

# Configuring the Validator

## Flags

The Validator offers some flags to customize properties.

You can view the full set of flags by running
```shell
python ./neurons/validator.py -h
```

# Coming Soon

We are working hard to add more features to the Subnet. For the Validators, we have plans to:

1. Have the Validator serve an Axon on the network, so neurons on other Subnets can retrieve data.
2. Add scrapers for other DataSources.
3. Add other (and cheaper) scrapers for the Validators to use.