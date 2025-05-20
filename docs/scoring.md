# Bittensor Miner Evaluation System

## Overview

This document outlines the key components of the Bittensor miner evaluation system: Credibility, Score, and Incentive. These components work together to create a fair and effective reward mechanism for miners in the network.

## Key Components

### 1. Credibility

| Aspect | Description |
|--------|-------------|
| Range | 0 to 1 |
| Purpose | Measures the miner's long-term reliability and consistency |
| Calculation | `new_credibility = α * current_validation_result + (1 - α) * old_credibility` |
| Characteristics | Slowly changes over time, reflecting consistent performance |

### 2. Score

| Aspect | Description |
|--------|-------------|
| Range | Any non-negative number |
| Purpose | Represents the value of data provided in a single evaluation |
| Calculation | `raw_score = data_type_scale_factor * time_scalar * scorable_bytes` <br> `final_score = raw_score * (credibility ^ 2.5)` |
| Characteristics | Can vary significantly between evaluations |

### 3. Incentive (Reward)

| Aspect | Description |
|--------|-------------|
| Range | Proportional to the miner's share of the total network score |
| Purpose | Determines the actual reward (e.g., tokens) given to the miner |
| Calculation | `miner_reward = (miner_score / total_network_score) * total_reward_pool` |
| Characteristics | Directly affects the miner's earnings |

## Relationships

### Credibility → Score
- Credibility acts as a multiplier for the raw score
- Higher credibility significantly boosts the final score due to the exponential factor (2.5)

### Score → Incentive
- The miner's score determines their share of the total reward pool
- Higher scores lead to higher rewards, but it's relative to other miners' scores

### Credibility → Incentive
- Credibility indirectly affects incentives by boosting scores
- Miners with higher credibility can earn more rewards even with the same raw data value

### HuggingFace validation
- Credibility change is not enabled at this moment, but you can take a look how it is going to be implemented here:  [hugging_face_validation.md](/docs/hugging_face_validation.md) file.
## System Flow

1. **Credibility Evaluation**:
   - Miner's current credibility is assessed based on past performance.

2. **Raw Score Calculation**:
   - Data Value is determined based on content, source, and timeliness.
   - Raw Score is computed using the Data Value and other factors.

3. **Final Score Computation**:
   - Credibility is applied as a multiplier to the Raw Score.
   - Final Score = Raw Score * (Credibility ^ 2.5)

4. **Incentive/Reward Allocation**:
   - Miner's Final Score is compared to the Total Network Score.
   - Reward is proportionally allocated based on this comparison.

5. **Feedback Loop**:
   - The allocated reward indirectly motivates the miner to maintain and improve their Credibility for future evaluations.

Note: Credibility has a direct influence on the Final Score, while the Incentive/Reward indirectly influences future Credibility through miner behavior.

## Key Parameters

- Starting Credibility: 0
- Credibility Exponent: 2.5
- Credibility Alpha (α): 0.15
- Max Data Entity Bucket Size: 128 MB
- Max Data Entity Bucket Count per Miner Index: 350,000
- Data Age Limit: 30 days
- Min Evaluation Period: 60 minutes

## Data Source Weights
- Reddit: 55% (weight: 0.55)
- X (Twitter): 35% (weight: 0.35)
- Youtube: 10% (weight: 0.1)

## Desirable Data

For the current list of desirable data sources and their jobs, run with the `--gravity` tag.

## Important Notes

- Scores are relative to other miners in the network
- Credibility builds over time, rewarding consistent good performance
- Recent data is valued more highly than older data
- The system adapts to changes in data desirability through configurable lookup tables
- Negative scale factors can penalize undesirable data

This scoring system is designed to be fair while also being resistant to gaming attempts, encouraging miners to consistently provide high-quality, relevant, and timely data to the Bittensor network.
