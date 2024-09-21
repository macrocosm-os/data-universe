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

### Incentive → Credibility (indirect)
- While incentives don't directly affect credibility, they motivate miners to maintain high credibility to maximize rewards

System Flow:

1. Credibility
   |
   v
2. Raw Score <---- Data Value
   |
   v
3. Final Score
   |
   v
4. Incentive/Reward <---- Total Network Score
   |
   v
5. (Motivates maintenance of Credibility)

Key:
---> Direct influence
<--- Indirect influence

Relationships:
- Credibility multiplies Raw Score to produce Final Score
- Data Value determines Raw Score
- Final Score determines share of Incentive/Reward
- Total Network Score influences overall Incentive/Reward distribution
- Incentive/Reward motivates miners to maintain high Credibility