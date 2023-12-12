# Validator

The Validator is responsible for validating the Miners and scoring them according to the [incentive mechanism](../README.md#incentive-mechanism). It runs a loop to enumerate all Miners in the network, and for each, it performs the following sequence:
1. It requests the latest [MinerIndex](../README.md#terminology) from the miner, which it stores in a local database.
2. It chooses a random (sampled by size) DataEntityBucket from the MinerIndex to sample.
3. It gets that DataEntityBucket from the Miner.
4. It chooses N DataEntities from the DataEntityBucket to validate. It then scrapes the content from the apprioriate DataSource to get those DataEntities.
5. It then compares those retrieved DataEntities agaisnt the ones provided by the Miner and updates the Miner Credibility, based on the result.
6. Finally, it updates the Miner's score.

Once this sequence has been performed for all Miners, the Validator waits a period of time before starting the next loop to ensure it does not evaluate a Miner more often than once per N minutes. This helps ensure the cost of running a Validator is not too high, and also protects the network against high amounts of traffic.

As of Dec 11th 2023, the expected cost number of DataItems queried via Apify is roughly: `225 Miners * 2 evals per hour * 1 sample per period * 24 hours = 10800`. Assuming this is ~50% Reddit ($3.50 per 1000) and ~50% per X ($1 per 1000), the total cost is roughly $24.30 per day.

# System Requirements

Validators do not require a GPU and should be able to run on a relatively low-tier machine, as long as it has sufficient network bandwidth and disk space.

# Getting Started

## Prerequisites
1. As of Dec 11th 2023, all DataSources are scraped via Apify, so you'll need to [setup your Apify API token](apify.md). This won't be a requirement in the future.

1. Clone the repo

```shell
git clone https://github.com/RusticLuftig/data-universe.git
```

1. Setup your python [virtual environment](https://docs.python.org/3/library/venv.html) or [Conda environment](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands).

1. Install the requirements. From your virtual environment, run
```shell
cd data-universe
pip install -r requirements.txt
```

1. Make sure you've [created a Wallet](https://docs.bittensor.com/getting-started/wallets) and [registered a hotkey](https://docs.bittensor.com/subnets/register-and-participate).

1. Install mySQL
```shell
sudo apt install mysql-server
sudo systemctl start mysql.service
sudo mysql
```

1. Create a password for your database user.
The Validator will automatically create the mysql database and user for you. However, you'll need to choose a password and add it to the `.env` file:

```py
DATABASE_USER_PASSWORD="MyStrongPassword"
```

Alternatively, you can provide it via the `--neuron.database_password` flag.

## Running the Validator

For this guide, we'll use [pm2](https://pm2.keymetrics.io/) to manage the Miner process, because it'll restart the Miner if it crashes. If you don't already have it, install pm2.

From the data-universe folder:
```shell
pm2 start python -- ./neurons/validator.py --wallet.name your-wallet --wallet.hotkey your-hotkey
```

# Configuring the Validator

## Flags

The Miner offers some flags to customize properties, such as the database name and the maximum amount of data to store.

You can view the full set of flags by running
```shell
python ./neurons/miner.py -h
```

# Coming Soon

We are working hard to add more features to the Subnet. For the Validators, we have plans to:

1. Implement an auto-update script.
2. Have the Validator serve an Axon on the network, so neurons on other Subnets can retrieve data.
3. Add scrapers for other DataSources.
4. Add other (and cheaper) scrapers for the Validators to use.