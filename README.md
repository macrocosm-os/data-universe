<picture>
    <source srcset="./assets/macrocosmos-white.png"  media="(prefers-color-scheme: dark)">
    <source srcset="./assets/macrocosmos-black.png"  media="(prefers-color-scheme: light)">
    <img src="macrocosmos-black.png">
</picture>

<div align="center">

# **Bittensor Subnet 13: Data Universe** <!-- omit in toc -->
[![Discord Chat](https://img.shields.io/discord/308323056592486420.svg)](https://discord.gg/bittensor)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) 

---

[HF Leaderboard](https://huggingface.co/spaces/macrocosm-os/sn13-dashboard) • [Discord](https://discord.gg/bittensor) • [Network](https://taostats.io/subnets/netuid-13/) • [Research](https://bittensor.com/whitepaper) 
</div>

---

# Introduction

Data is a critical pillar of AI and Data Universe is that pillar for Bittensor.

Data Universe is a Bittensor subnet for collecting and storing large amounts of data from across a wide-range of sources, for use by other Subnets. It was built from the ground-up with a focus on decentralization and scalability. There is no centralized entity that controls the data; the data is stored across all Miner's on the network and is queryable via the Validators. At launch, Data Universe is able to support up to 50 Petabytes of data across 200 miners, while only requiring ~10GB of storage on the Validator.

# Overview

The Data Universe documentation assumes you are familiar with basic Bittensor concepts: Miners, Validators, and incentives. If you need a primer, please check out https://docs.bittensor.com/learn/bittensor-building-blocks.

In the Data Universe, Miners scrape data from a defined set of sources, called DataSources. Each piece of data (e.g. a webpage, BTC prices), called a DataEntity, is stored in the miner's database. Each DataEntity belongs to exactly one DataEntityBucket, which is uniquely identified by its DataEntityBucketId, a tuple of: where the data came from (DataSource), when it was created (TimeBucket), and a classification of the data (DataLabel, e.g. a stock ticker symbol). The full set of DataEntityBuckets on a Miner is referred to as its MinerIndex.

Validators periodically query each Miner to fetch their latest MinerIndexes and store them in a local database. This gives the Validator a complete understanding of all data that's stored on the network, as well as which Miners to query for specific types of data. Validators also periodically verify the correctness of the data stored on Miners and reward Miners based on the amount of [valuable data](#data-value) the Miner has. Validators log to [wandb](https://wandb.ai/macrocosmos/data-universe-validators) anonymously by default.

Optionally, Miners upload their local stores to HuggingFace for public dataset access. This data is anonymized for privacy purposes to comply with the Terms of Service per each data source. See the [HuggingFace](docs/huggingface_setup.md) docs for more information on HuggingFace uploads. In the future, publicly uploading data to HuggingFace will be required.

See the [Miner](docs/miner.md) and [Validator](docs/validator.md) docs for more information about how they work, as well as setup instructions.

# Incentive Mechanism

As described above, each Miner reports its MinerIndex to the Validator. The MinerIndex details how much and what type of data the Miner has. The Miner is then scored based on 2 dimensions:
1. How much data the Miner has and how valuable that data is.
1. How credible the Miner is.

## Data Value

Not all data is equally valuable! There are several factors used to determine data value:

### 1) Data Freshness

Fresh data is more valuable than old data, and data older than a certain threshold is not scored.

As of Dec 11th, 2023 data older than 30 days is not scored. This may increase in future.

### 2) Data Desirability

Data Universe defines a [DataDesirabilityLookup](https://github.com/RusticLuftig/data-universe/blob/main/rewards/data_desirability_lookup.py) that defines which types of data are desirable. Data deemed desirable is scored more highly. Unspecified labels get the default_scale_factor of 0.5 meaning they score half value in comparison.

The DataDesirabilityLookup will evolve over time, but each change will be announced ahead of time to give Miners adequate time to prepare for the update.

### 3) Duplication Factor

Data that's stored by many Miners is less valuable than data stored by only a few. The value of a piece of data is decreases proportional to the number of Miners storing it.

## Miner Credibility

Validators remain suspicious of Miners and so they periodically check a sample of data from each Miner's MinerIndex, to verify the data correctness. The Validator uses these checks to track a Miner's credibility, which it then uses to scale a Miner's score. The scaling is done in such a way that it is **always** worse for a Miner to misrepresent what types and how much data it has.

# Data Universe Dashboard

As you can see from the above, Data Universe rewards diversity of data (storing 200 copies of the same data isn't exactly beneficial!) 

To help understand the current data on the Subnet, the Data Universe team hosts a dashboard (https://shorturl.at/Ca5uu), showing the amount of each type of data (by DataEntityBucketId) on the Subnet. Miners are strongly encouraged to use this dashboard to customize their [Miner Configuration](./docs/miner.md#configuring-the-miner), to maximize their rewards.

# Getting Started

See [Miner Setup](docs/miner.md#miner_setup) to learn how to setup a Miner.

See [Validator Setup](docs/validator.md#validator_setup) to learn how to setup a Validator.

# Upcoming Features

1. A Validator API to allow other Subnets to query the data.
2. More data sources

# Terminology

**DataDesirabilityLookup:** A [defined list of rules](https://github.com/RusticLuftig/data-universe/blob/main/rewards/data_desirability_lookup.py) that determine how desirable data is, based on its DataSource and DataLabel.

**DataEntity:** A single "item" of data collected by a Miner. Each DataEntity has a URI, that the Validators can use to retrieve the item from its DataSource.

**DataEntityBucket:** A logical grouping of DataEntities, based on its DataEntityBucketId.

**DataEntityBucketId:** The unique identifier for a DataEntityBucket. It contains the TimeBucket, DataSource, and DataLabel.

**DataLabel:** A label associated with a DataEntity. Precisely what the label represents is unique to the DataSource. For example, for a Yahoo finance DataSource, the label is the stock ticker of the finance data.

**DataSource:** A source from which Miners scrape data.

**Miner Credibility**: A per-miner rating, based on how often they pass data validation checks. Used to heavily penalize Miner's who misrepresent their MinerIndex.

**Miner Index**: A summary of how much and what types of data a Miner has. Specifically, it's a list of DataEntityBuckets.

# Feedback

We welcome feedback! 

If you have a suggestion, please reach out to @arrmlet, @ewekazoo or any of the broader Macrocosmos Team on the Discord channel, or file an Issue.
