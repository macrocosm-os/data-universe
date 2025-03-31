# S3 Storage for Data Universe

This module provides blockchain-authenticated S3 storage for the Data Universe project. It enables miners to upload data to S3 buckets and validators to read that data, all while maintaining the trustless nature of the blockchain environment.

## Architecture

The S3 storage system consists of several components:

1. **S3MinerStorage**: Storage implementation for miners to upload data to S3
2. **S3ValidatorStorage**: Storage implementation for validators to read data from S3
3. **DualStorage**: Migration path supporting both HuggingFace and S3 storage
4. **Authentication Lambda**: AWS Lambda function for blockchain-based authentication

## Authentication Flow

The system uses blockchain signatures for authentication:

1. Miners/validators sign requests using their wallet's private key
2. A Lambda function verifies these signatures against blockchain identities
3. Upon successful verification, temporary AWS credentials are issued
4. These credentials are limited to specific paths/operations in S3

## Setup Instructions

### 1. Create AWS Resources

#### S3 Bucket

```bash
aws s3api create-bucket --bucket data-universe-storage --region us-east-1
```

#### IAM Roles

Create two IAM roles:
- `DataUniverseUploadRole`: For miners to upload data
- `DataUniverseReadRole`: For validators to read data

#### Lambda Function

Deploy the `auth_lambda.py` function to AWS Lambda with appropriate permissions to assume IAM roles.

### 2. Configure Data Universe

Update your config file to include S3 settings:

```yaml
storage:
  type: "dual"
  use_s3: true
  use_huggingface: true  # Set to false to use only S3
  s3:
    bucket_name: "data-universe-storage"
    region: "us-east-1"
    auth_endpoint: "https://your-api-gateway-url.amazonaws.com/auth"
```

## Usage Examples

### Miner Configuration

```python
from storage.s3.config import DualStorageConfig, S3Config
from storage.s3.dual_storage import DualStorage

# Create config from dictionary
config_dict = {
    "use_s3": True,
    "use_huggingface": True,
    "s3": {
        "bucket_name": "data-universe-storage",
        "auth_endpoint": "https://your-api-gateway-url.amazonaws.com/auth",
        "region": "us-east-1"
    }
}
config = DualStorageConfig.from_dict(config_dict)

# Create storage
storage = DualStorage.create(
    config=config,
    db_path="path/to/sqlite.db",
    miner_hotkey=wallet.hotkey.ss58_address,
    wallet=wallet,
    encoding_key_manager=encoding_key_manager,
    private_encoding_key_manager=private_key_manager,
    state_file="path/to/state_file"
)

# Upload data
metadata = storage.upload_data()
```

### Validator Configuration

```python
from storage.s3.s3_validator_storage import S3ValidatorStorage

# Create validator storage
storage = S3ValidatorStorage(
    config=validator_config,
    validator_wallet=wallet
)

# Read data from a miner
data_entities = storage.read_miner_data(
    miner_hotkey="5F5...",
    source=DataSource.X.value,
    max_entities=100
)
```

## Security Considerations

- The system authenticates users based on their blockchain identity
- Access is limited to specific paths based on hotkey (miner/validator)
- Credentials are temporary and limited in scope
- No central authentication server or shared secrets are required
- Content hashing ensures data integrity

## Migrating from HuggingFace

The `DualStorage` class provides a migration path:

1. Enable both S3 and HuggingFace storage
2. Upload data to both systems for a transition period
3. Once confident in S3 storage, disable HuggingFace
4. Update validators to use S3ValidatorStorage

This ensures a smooth transition without data loss or interruption to the network.