# Miner

Miners scrape data from various DataSources and get rewarded based on how much valuable data they have (see the [Incentive Mechanism](../README.md#incentive-mechanism) for the full details). The incentive mechanism does not require a Miner to scrape from all DataSources, allowing Miners to specialize and choose exactly what kinds of data they want to scrape. However, Miners are scored, in part, based on the total amount of data they have, so Miners should make sure they are scraping sufficient amounts of data.

The Miner stores all scraped data in a local database.

# System Requirements

Miners do not require a GPU and should be able to run on a low-tier machine, as long as it has sufficient network bandwidth and disk space. Must have python >= 3.10.

# Getting Started

## Prerequisites
1. As of Dec 17th 2023, we support Twitter and Reddit scraping via Apify so you'll need to [setup your Apify API token](apify.md). We also support Reddit scraping via a [personal reddit account](reddit.md) which is completely free. To support YouTube Scraping via a [official youtube api](youtube.md) which is completely free. 


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

5. (Optional) Run your miner in [offline mode](#offline) to scrape an initial set of data.

6. Make sure you've [created a Wallet](https://docs.bittensor.com/getting-started/wallets) and [registered a hotkey](https://docs.bittensor.com/subnets/register-and-participate).

## Running the Miner

For this guide, we'll use [pm2](https://pm2.keymetrics.io/) to manage the Miner process, because it'll restart the Miner if it crashes. If you don't already have it, install pm2.

### Online

From the data-universe folder, run:
```shell
pm2 start python -- ./neurons/miner.py --wallet.name your-wallet --wallet.hotkey your-hotkey
```

### Offline

From the data-universe folder, run:
```shell
pm2 start python -- ./neurons/miner.py --offline
```

Please note that your miner will not respond to validator requests in this mode and therefore if you have already registered to the subnet you should run in online mode.

# Configuring the Miner

## Flags

The Miner offers some flags to customize properties, such as the database name and the maximum amount of data to store.

You can view the full set of flags by running
```shell
python ./neurons/miner.py -h
```

## Configuring 

The frequency and types of data your Miner will scrape is configured in the [scraping_config.json](https://github.com/RusticLuftig/data-universe/blob/main/scraping/config/scraping_config.json) file. This file defines which scrapers your Miner will use. To customize your Miner, you either edit `scraping_config.json` or create your own file and pass its filepath via the `--neuron.scraping_config_file` flag. 

By default `scraping_config.json` is setup use both the apify actor and the personal reddit account for scraping reddit.

If you do not want to use Apify you should remove the sections where the `scraper_id` is set to either `Reddit.lite` or `X.microworlds` or `X.apidojo`.

If you do not want to use a personal Reddit account you should remove the sections where the `scraper_id` is set to either `Reddit.custom`.

If either of them is in the configuration but not setup properly in your `.env` file then your miner will log errors but still scrape using any configured scrapers that are properly setup.

For each scraper, you can define:

1. `cadence_seconds`: to control how frequently the scraper will run.
2. `labels_to_scrape`: to define how much of what type of data to scrape from this source. Each entry in this list consists of the following properties:
    1. `label_choices`: is a list of DataLabels to scrape. Each time the scraper runs, **one** of these labels is chosen at random to scrape.
    2. `max_age_hint_minutes`: provides a hint to the scraper of the maximum age of data you'd like to collect for the chosen label. Not all scrapers provide date/time filters so this is a hint, not a rule.
    3. `max_data_entities`: defines the maximum number of items to scrape for this set of labels, each time the scraper runs. This gives you full control over the maximum cost of scraping data from paid sources (e.g. Apify)

Let's walk through an example to explain how all these properties fit together.
```json
{
    "scraper_configs": [
        {
            "scraper_id": "X.apidojo",
            "cadence_seconds": 300,
            "labels_to_scrape": [
                {
                    "label_choices": [
                        "#bittensor",
                    ],
                    "max_age_hint_minutes": 1440,
                    "max_data_entities": 100
                },
                {
                    "label_choices": [
                        "#decentralizedfinance",
                        "#btc"
                    ],
                    "max_data_entities": 50
                }
            ]
        }
    ]
}
```

In this example, we configure the Miner to scrape using a single scraper, the "X.microworlds" scraper. The scraper will run every 5 minutes (300 seconds). When it runs, it'll run 2 scrapes:
1. The first will be a scrape for at most 100 items with #bittensor. The data scrape will choose a random [TimeBucket](../README.md#terminology) in (now - max_age_in_minutes, now). The probability distribution used to select a TimeBucket matches the Validator's incentive for [Data Freshness](../README.md#1-data-freshness): that is, it's weighted towards newer data.
2. The second will be a scrape for either #decentralizedfinance or #tao, chosen at random (uniformly). The scrape will scrape at most 50 items, and will use a random TimeBucket between now and the maximum data freshness threshold.

You can start your Miner with a different scraping config by passing the filepath to `--neuron.scraping_config_file`

# On Demand request handle
As described in [on demand request handle](../docs/on_demand.md)

# Choosing which data to scrape

As described in the [incentive mechanism](../README.md#incentive-mechanism), Miners are, in part, scored based on their data's desirability and uniqueness. We encourage Miners to tune their Miners to maximize their scores by scraping unique, desirable data.

For uniqueness, you can [view the dashboard](../README.md#data-universe-dashboard) to see how much data, by DataSource and DataLabel is currently on the Subnet.

For desirability, the [DataDesirabilityLookup](https://github.com/RusticLuftig/data-universe/blob/main/rewards/data_desirability_lookup.py) defines the exact rules Validators use to compute data desirability.