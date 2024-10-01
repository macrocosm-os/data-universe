# Hugging Face Dataset Validation on Bittensor Subnet 13

This document outlines the validation process for Hugging Face datasets in the context of Bittensor Subnet 13. Validators on Subnet 13 are responsible for ensuring that miners are providing accurate and up-to-date datasets, specifically for X (formerly Twitter) and Reddit. The validation process is crucial for maintaining the integrity and utility of the network.

**Note:** The functionality described here will be integrated into the `MinerEvaluator` in the future.

## Overview

The validation process involves the following key steps:

1. **Querying Hugging Face Metadata from Miners**
2. **Selecting Random Files from the Latest Data Commit**
3. **Checking Dataset Updates and Size Changes**
4. **Validating Data Samples**
5. **Adjusting Miner Credibility Based on Validation Results**

## Validation Process Details

### 1. Querying Hugging Face Metadata from Miners

Every **55,000 blocks**, validators query the `HuggingFaceMetadata` table from each miner. This metadata includes information about the datasets that miners have uploaded to Hugging Face, specifically focusing on the newest datasets for X and Reddit.

```python
# Example of querying Hugging Face metadata
async def _query_huggingface_metadata(self, hotkey: str, uid: int, miner_axon: bt.AxonInfo) -> Optional[List[HuggingFaceMetadata]]:
    # ... code to query metadata ...
```

### 2. Selecting Random Files from the Latest Data Commit

Validators select **10 random rows** from the latest data commit of the miner's Hugging Face dataset. This selection ensures that the validation covers recent data and that miners are continually updating their datasets.

```python
# Function to select random rows from a dataset
def select_random_rows_from_parquet(repo_id: str, num_rows: int = 10) -> pd.DataFrame:
    # ... code to select random rows ...
```

### 3. Checking Dataset Updates and Size Changes

Validators assess how frequently the dataset is updated and monitor changes in its size over time. This step ensures that miners are actively accumulating data in their repositories and contributing to the network's growth.

- **Update Frequency:** Validators check the timestamps of data commits to verify regular updates.
- **Size Changes:** Validators compare the sizes of successive data commits to confirm that new data is being added.

### 4. Validating Data Samples

For each selected file, validators perform the following:

- **Data Retrieval:** Fetch the data samples from the selected files.
- **Data Verification:** Use appropriate scrapers to validate the correctness of the data.

The validation criteria are:

- **Reddit Dataset:** A validation ratio of **0.5**. If at least 5 out of 10 samples are valid, the validation is considered successful.
- **X Dataset:** A validation ratio of **0.6**. If at least 6 out of 10 samples are valid, the validation is considered successful.

```python
# Example of validating data samples
async def main():
    # ... code to validate data samples ...
    valid = await scraper.validate_hf(entities=selected_rows)
    # ... process validation results ...
```

### 5. Adjusting Miner Credibility Based on Validation Results

Based on the validation outcome, validators adjust the miner's credibility:

- **Successful Validation:** Increase the miner's credibility score by **10%**.
- **Failed Validation:** Decrease the miner's credibility score by **10%**.

This adjustment incentivizes miners to provide high-quality, up-to-date datasets.

```python
# Adjusting miner credibility
if validation_successful:
    self.scorer.increase_credibility(uid, percentage=10)
else:
    self.scorer.decrease_credibility(uid, percentage=10)
```

## Future Integration into MinerEvaluator

The `MinerEvaluator` will be updated to include this validation process. The planned changes involve:

- Implementing the Hugging Face dataset validation as a separate component within the `MinerEvaluator`.
- Scheduling the validation process to occur every **55,000 blocks**.
- Incorporating the credibility adjustments based on validation outcomes.

**Note:** The existing validation steps for data entity buckets will remain, but the Hugging Face dataset validation will be handled separately to ensure a focused and efficient validation process.

## Code Structure

- **MinerEvaluator Class:** Responsible for evaluating miners and updating their scores.
- **Hugging Face Validation Module:** Contains functions to select random rows from datasets and validate them.
- **ScraperProvider:** Supplies the appropriate scraper for data validation (e.g., X or Reddit scrapers).

## Conclusion

This validation process is designed to ensure that miners on Bittensor Subnet 13 contribute valuable and accurate datasets to the network. By regularly validating datasets and adjusting miner credibility accordingly, the network maintains high data quality standards.

## References

- **Bittensor Documentation:** [https://bittensor.com/](https://bittensor.com/)
- **Hugging Face Datasets:** [https://huggingface.co/datasets](https://huggingface.co/datasets)

