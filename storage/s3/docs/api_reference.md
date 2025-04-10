# S3 Storage API Reference

This document provides detailed API reference for the S3 storage implementation.

## S3MinerStorage

### Constructor

```python
def __init__(self, 
             db_path: str,
             miner_hotkey: str,
             wallet: Any,
             encoding_key_manager: EncodingKeyManager,
             private_encoding_key_manager: EncodingKeyManager,
             state_file: str,
             s3_config: Dict[str, Any],
             output_dir: str = 'temp_s3_storage',
             chunk_size: int = 1_000_000):
```

**Parameters:**
- `db_path`: Path to the SQLite database
- `miner_hotkey`: The miner's hotkey address
- `wallet`: The miner's bittensor wallet
- `encoding_key_manager`: Manager for encoding usernames
- `private_encoding_key_manager`: Manager for encoding URLs
- `state_file`: Path to store the state file
- `s3_config`: Configuration for S3 storage
- `output_dir`: Local directory for temporary storage
- `chunk_size`: Size of chunks for processing data

### Methods

#### compute_file_hash

```python
def compute_file_hash(self, file_path: str) -> str:
```

Computes the SHA-256 hash of a file.

**Parameters:**
- `file_path`: Path to the file

**Returns:**
- Hash of the file as a hexadecimal string

#### get_upload_credentials

```python
def get_upload_credentials(self, file_path: str, source: int) -> Dict[str, Any]:
```

Gets blockchain-authenticated credentials for uploading to S3.

**Parameters:**
- `file_path`: Path to the file being uploaded
- `source`: Data source ID

**Returns:**
- Dictionary with upload credentials

#### upload_file_to_s3

```python
def upload_file_to_s3(self, file_path: str, source: int) -> str:
```

Uploads a single file to S3 using blockchain-based auth.

**Parameters:**
- `file_path`: Path to the local file
- `source`: Data source ID

**Returns:**
- S3 path where the file was uploaded

#### upload_data_to_s3

```python
def upload_data_to_s3(self) -> List[Dict[str, Any]]:
```

Uploads all new data to S3.

**Returns:**
- List of metadata for uploaded data

#### upload_batch

```python
def upload_batch(self, file_paths: List[str], source: int) -> None:
```

Uploads a batch of files to S3 in parallel.

**Parameters:**
- `file_paths`: List of file paths to upload
- `source`: Data source ID

## S3ValidatorStorage

### Constructor

```python
def __init__(self, config: Any, validator_wallet: Any):
```

**Parameters:**
- `config`: Configuration containing S3 settings
- `validator_wallet`: The validator's bittensor wallet

### Methods

#### get_read_credentials

```python
def get_read_credentials(self, miner_hotkey: str) -> Dict[str, Any]:
```

Gets blockchain-authenticated credentials for reading from S3.

**Parameters:**
- `miner_hotkey`: The miner's hotkey address to access data from

**Returns:**
- Dictionary with AWS credentials

#### read_miner_files

```python
def read_miner_files(self, miner_hotkey: str, source: int, max_files: int = 10) -> List[str]:
```

Lists parquet files available for a miner.

**Parameters:**
- `miner_hotkey`: The miner's hotkey
- `source`: Data source ID
- `max_files`: Maximum number of files to list

**Returns:**
- List of file keys in S3

#### read_miner_data

```python
def read_miner_data(self, miner_hotkey: str, source: int, 
                   date_range: Optional[DateRange] = None, 
                   max_entities: int = 100,
                   labels: Optional[List[DataLabel]] = None) -> List[DataEntity]:
```

Reads data entities from a miner's S3 storage.

**Parameters:**
- `miner_hotkey`: The miner's hotkey
- `source`: Data source ID
- `date_range`: Optional date range to filter data
- `max_entities`: Maximum number of entities to return
- `labels`: Optional list of labels to filter by

**Returns:**
- List of DataEntity objects

#### validate_miner_data

```python
def validate_miner_data(self, miner_hotkey: str, entity_uris: List[str]) -> Dict[str, bool]:
```

Validates that a miner has the specified data entities.

**Parameters:**
- `miner_hotkey`: The miner's hotkey
- `entity_uris`: List of URIs to validate

**Returns:**
- Dictionary mapping URIs to validation results (True if found)

#### get_miner_stats

```python
def get_miner_stats(self, miner_hotkey: str) -> Dict[str, Any]:
```

Gets statistics about a miner's data.

**Parameters:**
- `miner_hotkey`: The miner's hotkey

**Returns:**
- Dictionary with statistics about the miner's data

## DualStorage

### Constructor

```python
def __init__(self, config: DualStorageConfig, 
             hf_uploader: Optional[HuggingFaceUploader] = None,
             s3_storage: Optional[S3MinerStorage] = None):
```

**Parameters:**
- `config`: Configuration for dual storage
- `hf_uploader`: Optional HuggingFace uploader instance
- `s3_storage`: Optional S3 storage instance

### Methods

#### upload_data

```python
def upload_data(self) -> List[Dict[str, Any]]:
```

Uploads data to enabled storage systems.

**Returns:**
- Combined list of metadata from all storage systems

#### create (class method)

```python
@classmethod
def create(cls, 
           config: DualStorageConfig, 
           db_path: str,
           miner_hotkey: str,
           wallet: Any,
           encoding_key_manager: Any,
           private_encoding_key_manager: Any,
           state_file: str,
           output_dir: str = 'storage_data') -> 'DualStorage':
```

Factory method to create a DualStorage instance with appropriate uploader instances.

**Parameters:**
- `config`: Dual storage configuration
- `db_path`: Path to the SQLite database
- `miner_hotkey`: The miner's hotkey
- `wallet`: The miner's bittensor wallet
- `encoding_key_manager`: Manager for encoding usernames
- `private_encoding_key_manager`: Manager for encoding URLs
- `state_file`: Path to store the state file
- `output_dir`: Local directory for temporary storage

**Returns:**
- Configured DualStorage instance

## Configuration Classes

### S3Config

```python
@dataclass
class S3Config:
    # Required settings
    bucket_name: str
    auth_endpoint: str
    
    # Optional settings with defaults
    region: str = "us-east-1"
    use_s3: bool = True
    
    # Optional advanced settings
    endpoint_url: Optional[str] = None
    max_chunk_size: int = 1_000_000
    max_thread_workers: int = 5
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'S3Config':
        """Create an S3Config from a dictionary."""
        # Implementation...
```

### DualStorageConfig

```python
@dataclass
class DualStorageConfig:
    s3_config: S3Config
    use_s3: bool = True
    use_huggingface: bool = True
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DualStorageConfig':
        """Create a DualStorageConfig from a dictionary."""
        # Implementation...
```