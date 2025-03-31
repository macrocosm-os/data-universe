# S3 Storage Integration Guide

This guide explains how to integrate the S3 storage system into the Data Universe subnet, including phased rollout and configuration management.

## Integration with Miner

### Step 1: Update Miner Code

Update the miner initialization to use the storage factory:

```python
from storage.s3.factory import create_storage_from_config

def setup_miner(config):
    # Initialize wallet and other components...
    
    # Create storage with chain-based configuration
    storage = create_storage_from_config(
        subtensor=subtensor,
        netuid=config.netuid,
        db_path=config.db_path,
        miner_hotkey=wallet.hotkey.ss58_address,
        wallet=wallet,
        encoding_key_manager=encoding_key_manager,
        private_encoding_key_manager=private_key_manager,
        state_file=config.state_file
    )
    
    # Use the storage in the miner
    # ...
```

### Step 2: Update Miner Upload Logic

Replace direct HuggingFace uploads with the DualStorage:

```python
# Old code
# hf_metadata = hf_uploader.upload_sql_to_huggingface()

# New code
storage_metadata = storage.upload_data()
```

## Integration with Validator

### Step 1: Update Validator Code

Add S3ValidatorStorage to the validator:

```python
from storage.s3.s3_validator_storage import S3ValidatorStorage
from storage.s3.factory import create_config_from_chain_or_env

def setup_validator(config):
    # Initialize wallet and other components...
    
    # Get storage configuration from chain
    storage_config = create_config_from_chain_or_env(subtensor, config.netuid)
    
    # Create validator storage
    if storage_config.use_s3:
        s3_storage = S3ValidatorStorage(
            config=storage_config.s3_config,
            validator_wallet=wallet
        )
        
    # Set up HF storage as before
    # ...
```

### Step 2: Update Data Validation Logic

Modify validation to check both systems during transition:

```python
def validate_miner_data(miner_hotkey, entity_uris):
    results = {}
    
    # Check S3 if enabled
    if hasattr(self, 's3_storage'):
        s3_results = self.s3_storage.validate_miner_data(miner_hotkey, entity_uris)
        results["s3"] = s3_results
    
    # Check HuggingFace
    hf_results = self.hf_storage.validate_miner_data(miner_hotkey, entity_uris)
    results["hf"] = hf_results
    
    # During transition, consider valid if found in either system
    combined_results = {}
    for uri in entity_uris:
        s3_valid = results.get("s3", {}).get(uri, False)
        hf_valid = results.get("hf", {}).get(uri, False)
        combined_results[uri] = s3_valid or hf_valid
    
    return combined_results
```

## Phased Rollout Management

### Setup Rollout Configuration

Use the provided CLI tool to manage the rollout:

```bash
# View current configuration
python -m storage.s3.config_cli get --netuid 13

# Start with a small percentage (e.g., 5%)
python -m storage.s3.config_cli rollout --netuid 13 --percentage 5 --wallet.name subnet_owner

# Increase percentage as confidence grows
python -m storage.s3.config_cli rollout --netuid 13 --percentage 25 --wallet.name subnet_owner

# Later, move to 100%
python -m storage.s3.config_cli rollout --netuid 13 --percentage 100 --wallet.name subnet_owner
```

### Initial S3 Configuration

Set up the initial S3 configuration:

```bash
python -m storage.s3.config_cli set \
  --netuid 13 \
  --use_s3 true \
  --use_huggingface true \
  --bucket_name "data-universe-subnet" \
  --auth_endpoint "https://your-api-gateway-url.amazonaws.com/auth" \
  --region "us-east-1" \
  --rollout_percentage 0 \
  --wallet.name subnet_owner
```

## Monitoring the Transition

### Add Telemetry to Track Storage Usage

Add monitoring to see how many miners are using each storage system:

```python
# In validator code
def monitor_storage_usage():
    miners_using_s3 = 0
    miners_using_hf = 0
    total_miners = 0
    
    for uid in metagraph.uids:
        hotkey = metagraph.hotkeys[uid]
        
        # Check if miner is using S3
        s3_validation = s3_storage.validate_miner_data(hotkey, test_uris)
        if any(s3_validation.values()):
            miners_using_s3 += 1
            
        # Check if miner is using HuggingFace
        hf_validation = hf_storage.validate_miner_data(hotkey, test_uris)
        if any(hf_validation.values()):
            miners_using_hf += 1
            
        total_miners += 1
    
    # Log metrics
    s3_percentage = (miners_using_s3 / total_miners) * 100 if total_miners > 0 else 0
    hf_percentage = (miners_using_hf / total_miners) * 100 if total_miners > 0 else 0
    
    bt.logging.info(f"Storage usage - S3: {s3_percentage:.1f}%, HF: {hf_percentage:.1f}%")
    
    # Optionally log to wandb or other monitoring system
    if wandb.run:
        wandb.log({
            "miners_using_s3": miners_using_s3,
            "miners_using_hf": miners_using_hf,
            "s3_percentage": s3_percentage,
            "hf_percentage": hf_percentage
        })
```

## Timeline for Transition

### Phase 1: Infrastructure Setup (Week 1-2)
- Deploy S3 bucket and authentication Lambda
- Update code to include S3 storage capability
- Configure initial settings with 0% rollout

### Phase 2: Testing Rollout (Week 3-4)
- Enable S3 for 5% of miners
- Monitor data integrity and performance
- Address any issues discovered

### Phase 3: Expanded Rollout (Week 5-6)
- Increase rollout to 25%
- Continue monitoring and fine-tuning

### Phase 4: General Availability (Week 7-8)
- Expand to 50%, then 100% of miners
- Continue dual-writing to both systems

### Phase 5: HuggingFace Deprecation (Week 9-12)
- Announce deprecation timeline
- Eventually disable HuggingFace uploads by default

## Handling Errors and Fallbacks

Implement graceful fallbacks to maintain service during transition:

```python
def upload_data_with_fallback():
    try:
        # Try S3 first if enabled
        if storage.use_s3:
            result = storage.s3_storage.upload_data_to_s3()
            if result:
                return result
                
    except Exception as e:
        bt.logging.error(f"Error uploading to S3: {str(e)}")
    
    # Fall back to HuggingFace if S3 fails or is disabled
    if storage.use_huggingface:
        try:
            return storage.hf_uploader.upload_sql_to_huggingface()
        except Exception as e:
            bt.logging.error(f"Error uploading to HuggingFace: {str(e)}")
    
    # All options failed
    bt.logging.error("All storage options failed")
    return []
```

## Communication with Miners

Ensure miners are informed about the transition:

1. Add clear logs to the miner software:
   ```python
   if storage.use_s3:
       bt.logging.info("S3 storage enabled - uploading data to AWS S3")
   if storage.use_huggingface:
       bt.logging.info("HuggingFace storage enabled - uploading data to HF")
   ```

2. Add status command to display storage configuration:
   ```python
   def print_storage_status():
       bt.logging.info("=== Storage Configuration ===")
       bt.logging.info(f"S3 Storage: {'ENABLED' if storage.use_s3 else 'DISABLED'}")
       bt.logging.info(f"HuggingFace Storage: {'ENABLED' if storage.use_huggingface else 'DISABLED'}")
       if storage.use_s3:
           bt.logging.info(f"S3 Bucket: {storage.s3_storage.bucket_name}")
           bt.logging.info(f"S3 Region: {storage.s3_storage.region}")
   ```