# Dynamic Desirability Feature

## Overview

The Dynamic Desirability feature enhances the Bittensor miner evaluation system by allowing real-time adjustments to the desirability of different data sources and labels. This feature aims to make the network more responsive to changing market conditions, emerging trends, and the evolving needs of the Bittensor ecosystem.

## Purpose

- Adapt to rapidly changing information landscapes
- Incentivize miners to quickly shift focus to newly important topics
- Discourage the collection of data that becomes less relevant over time
- Enhance the overall quality and relevance of data in the Bittensor network

## Key Components

1. **Real-time Desirability Updater**: A system that continuously analyzes various factors to update desirability scores.

2. **Flexible Data Source Weights**: Ability to dynamically adjust weights between different data sources (e.g., Reddit, X/Twitter).

3. **Adaptive Label Scale Factors**: Mechanism to modify the importance of specific labels (subreddits, hashtags) in real-time.

4. **Default Scale Factor Adjustment**: Capability to decrease the default scale factor to incentivize miners to focus on more desirable data.

5. **Trend Detection Algorithm**: AI-powered system to identify emerging trends and topics of interest.

6. **Feedback Loop**: Integration with data consumers to understand the utility of different data types.

## Implementation Details

### Desirability Update Frequency
- Updates occur every [X] hours (to be determined based on network capabilities and needs)

### Factors Influencing Dynamic Desirability
1. Current events and breaking news
2. Trading volume and price movements in crypto markets
3. Social media engagement metrics
4. User queries and data requests within the Bittensor network
5. External APIs and data feeds for trend analysis

### Default Scale Factor and Miner Incentives
- The default scale factor can be dynamically decreased to incentivize miners to focus on more desirable data.
- This adjustment encourages miners to prioritize scraping and providing data with higher desirability scores.
- Desirability is determined by:
  - Key hashtags or usernames in social media posts
  - Keywords found in the body of Reddit posts or tweets
  - Other relevant metadata associated with the data entities

### Validator Role in Determining Desirabilities
- Validators play a crucial role in determining desirabilities based on their stake and weights in the subnet.
- The influence of each validator's desirability preferences is proportional to their stake and weight within the network.
- This system ensures that desirability scores reflect the consensus of the network's most invested participants.

### Desirability List Merging
- Scripts are available to merge lists of desirabilities from different validators.
- These scripts will be accessible via a GitHub repository (coming soon).
- Miners and other network participants can use these scripts to obtain the most up-to-date, consolidated list of desirabilities.

### Integration with Existing System
- Modifies the `DataDesirabilityLookup` in real-time
- Affects the `data_type_scale_factor` in score calculations
- Requires updates to `MinerScorer` and `DataValueCalculator` to handle dynamic inputs

## Proposed Workflow

1. Validator Input: Validators submit their desirability preferences based on their analysis and stake.
2. Desirability Aggregation: The network aggregates validator inputs, weighing them by stake and subnet weights.
3. List Merging: Scripts merge the desirability lists to create a unified, network-wide desirability ranking.
4. Update Propagation: The merged desirability list is distributed to all participants in the network.
5. Miner Adaptation: Miners adjust their data collection strategies based on the updated desirability list.
6. Scoring Impact: Updated desirability scores immediately affect miner evaluations and rewards.


## Current Status

The Dynamic Desirability feature is currently in the [development] phase. It is scheduled for implementation in Q[4] 20[24].

## Action for Miners

Miners are encouraged to:
1. Stay updated with the latest desirability lists by regularly pulling from the GitHub repository (link to be provided).
2. Adjust their scraping activities based on the most recent desirability scores to maximize their rewards.
3. Implement systems to quickly adapt to changes in desirability rankings.

---

Note: This feature is subject to community review and approval processes. Details may change based on further research and development.