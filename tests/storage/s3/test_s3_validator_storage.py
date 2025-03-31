"""
Tests for S3ValidatorStorage implementation.
"""
import os
import json
import pytest
import tempfile
import datetime as dt
import pandas as pd
import unittest.mock as mock
from unittest.mock import MagicMock, patch, ANY
import bittensor as bt

from storage.s3.s3_validator_storage import S3ValidatorStorage
from common.data import DataSource, DataEntity, DataLabel
from common.date_range import DateRange


# Mock classes and functions
class MockWallet:
    def __init__(self):
        self.hotkey = MagicMock()
        self.hotkey.ss58_address = "5FTyhanxkXXGYhKL7L5UGTYmUBTk3vPFLJKNZZ9tN2eZ9Zcp"
        
    def sign(self, message):
        return b"mock_signature"


class MockResponse:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self.json_data = json_data or {}
        
    def json(self):
        return self.json_data


class MockConfig:
    def __init__(self):
        self.s3_bucket_name = "test-bucket"
        self.s3_region = "us-east-1"
        self.s3_auth_endpoint = "https://mock-auth-endpoint.example.com/auth"


@pytest.fixture
def mock_s3_creds():
    return {
        "credentials": {
            "AccessKeyId": "mock_access_key",
            "SecretAccessKey": "mock_secret_key",
            "SessionToken": "mock_session_token",
            "Expiration": "2023-12-31T23:59:59Z"
        }
    }


@pytest.fixture
def mock_s3_objects():
    return {
        'Contents': [
            {
                'Key': 'data/2/5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv/file1.parquet',
                'LastModified': dt.datetime(2023, 1, 1),
                'Size': 1024
            },
            {
                'Key': 'data/2/5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv/file2.parquet',
                'LastModified': dt.datetime(2023, 1, 2),
                'Size': 2048
            }
        ]
    }


@pytest.fixture
def mock_parquet_df():
    return pd.DataFrame({
        'url': ['https://x.com/user/status/123', 'https://x.com/user/status/456'],
        'uri': ['https://x.com/user/status/123', 'https://x.com/user/status/456'],
        'datetime': [dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 2)],
        'label': ['#test', 'NULL'],
        'content': ['{"text": "test content 1"}', '{"text": "test content 2"}']
    })


@pytest.fixture
def s3_validator_storage():
    # Create a mock config and wallet
    config = MockConfig()
    wallet = MockWallet()
    
    # Create S3ValidatorStorage instance
    storage = S3ValidatorStorage(
        config=config,
        validator_wallet=wallet
    )
    
    # Initialize cache
    storage._credentials_cache = {}
    storage._cache_expiry = {}
    
    yield storage


@patch("requests.post")
def test_get_read_credentials(mock_post, s3_validator_storage, mock_s3_creds):
    """Test that read credentials are fetched correctly."""
    # Mock the response from auth endpoint
    mock_post.return_value = MockResponse(status_code=200, json_data=mock_s3_creds)
    
    # Set test parameters
    miner_hotkey = "5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv"
    
    # Get read credentials
    creds = s3_validator_storage.get_read_credentials(miner_hotkey)
    
    # Verify request was made with correct data
    mock_post.assert_called_once()
    call_args = mock_post.call_args[1]["json"]
    assert call_args["action"] == "read"
    assert call_args["validator_hotkey"] == s3_validator_storage.hotkey
    assert call_args["miner_hotkey"] == miner_hotkey
    assert "timestamp" in call_args
    assert "expiry" in call_args
    assert "signature" in call_args
    
    # Verify response is returned correctly
    assert creds == mock_s3_creds["credentials"]
    
    # Test caching: second call should not make another request
    mock_post.reset_mock()
    creds2 = s3_validator_storage.get_read_credentials(miner_hotkey)
    assert not mock_post.called
    assert creds2 == mock_s3_creds["credentials"]


@patch("boto3.client")
@patch("storage.s3.s3_validator_storage.S3ValidatorStorage.get_read_credentials")
def test_read_miner_files(mock_get_creds, mock_boto3_client, s3_validator_storage, mock_s3_creds, mock_s3_objects):
    """Test listing miner files from S3."""
    # Mock credentials
    mock_get_creds.return_value = mock_s3_creds["credentials"]
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = mock_s3_objects
    mock_boto3_client.return_value = mock_s3
    
    # Set test parameters
    miner_hotkey = "5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv"
    source = DataSource.X.value
    
    # List files
    files = s3_validator_storage.read_miner_files(miner_hotkey, source)
    
    # Verify S3 client was created with correct credentials
    mock_boto3_client.assert_called_once_with(
        's3',
        aws_access_key_id=mock_s3_creds["credentials"]["AccessKeyId"],
        aws_secret_access_key=mock_s3_creds["credentials"]["SecretAccessKey"],
        aws_session_token=mock_s3_creds["credentials"]["SessionToken"],
        region_name=s3_validator_storage.region
    )
    
    # Verify list_objects_v2 was called with correct parameters
    mock_s3.list_objects_v2.assert_called_once_with(
        Bucket=s3_validator_storage.bucket_name,
        Prefix=f"data/{source}/{miner_hotkey}/"
    )
    
    # Verify files are returned correctly (sorted by last modified)
    assert len(files) == 2
    assert files[0] == 'data/2/5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv/file2.parquet'
    assert files[1] == 'data/2/5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv/file1.parquet'


@patch("tempfile.NamedTemporaryFile")
@patch("boto3.client")
@patch("storage.s3.s3_validator_storage.S3ValidatorStorage.read_miner_files")
@patch("storage.s3.s3_validator_storage.S3ValidatorStorage.get_read_credentials")
def test_read_miner_data(mock_get_creds, mock_read_files, mock_boto3_client, mock_temp_file, 
                          s3_validator_storage, mock_s3_creds, mock_parquet_df):
    """Test reading miner data from S3."""
    # Mock credentials
    mock_get_creds.return_value = mock_s3_creds["credentials"]
    
    # Mock file listing
    mock_read_files.return_value = [
        'data/2/5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv/file1.parquet'
    ]
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Mock temporary file
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/mock_temp_file"
    mock_temp.__enter__.return_value = mock_temp
    mock_temp_file.return_value = mock_temp
    
    # Mock pandas read_parquet
    with patch("pandas.read_parquet", return_value=mock_parquet_df):
        # Set test parameters
        miner_hotkey = "5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv"
        source = DataSource.X.value
        date_range = DateRange(
            start=dt.datetime(2023, 1, 1),
            end=dt.datetime(2023, 1, 3)
        )
        
        # Read data
        entities = s3_validator_storage.read_miner_data(
            miner_hotkey=miner_hotkey,
            source=source,
            date_range=date_range,
            max_entities=10
        )
        
        # Verify S3 download was called
        mock_s3.download_file.assert_called_once_with(
            s3_validator_storage.bucket_name,
            'data/2/5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv/file1.parquet',
            mock_temp.name
        )
        
        # Verify correct number of entities returned
        assert len(entities) == 2
        
        # Verify entity properties
        assert isinstance(entities[0], DataEntity)
        assert entities[0].uri == 'https://x.com/user/status/123'
        assert entities[0].label.value == '#test'
        assert entities[1].uri == 'https://x.com/user/status/456'
        assert entities[1].label is None


@patch("tempfile.NamedTemporaryFile")
@patch("boto3.client")
@patch("storage.s3.s3_validator_storage.S3ValidatorStorage.read_miner_files")
@patch("storage.s3.s3_validator_storage.S3ValidatorStorage.get_read_credentials")
def test_validate_miner_data(mock_get_creds, mock_read_files, mock_boto3_client, mock_temp_file, 
                             s3_validator_storage, mock_s3_creds, mock_parquet_df):
    """Test validating miner data from S3."""
    # Mock credentials
    mock_get_creds.return_value = mock_s3_creds["credentials"]
    
    # Mock file listing for both sources
    mock_read_files.side_effect = [
        ['data/1/5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv/reddit_file.parquet'],  # Reddit
        ['data/2/5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv/twitter_file.parquet']   # Twitter
    ]
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Mock temporary file
    mock_temp = MagicMock()
    mock_temp.name = "/tmp/mock_temp_file"
    mock_temp.__enter__.return_value = mock_temp
    mock_temp_file.return_value = mock_temp
    
    # Mock pandas read_parquet
    with patch("pandas.read_parquet", return_value=mock_parquet_df):
        # Set test parameters
        miner_hotkey = "5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv"
        uris_to_validate = ['https://x.com/user/status/123', 'https://x.com/other/status/789']
        
        # Validate data
        results = s3_validator_storage.validate_miner_data(miner_hotkey, uris_to_validate)
        
        # Verify S3 download was called twice (once for each source)
        assert mock_s3.download_file.call_count == 2
        
        # Verify validation results are correct
        assert results['https://x.com/user/status/123'] == True
        assert results['https://x.com/other/status/789'] == False


@patch("boto3.client")
@patch("storage.s3.s3_validator_storage.S3ValidatorStorage.get_read_credentials")
def test_get_miner_stats(mock_get_creds, mock_boto3_client, s3_validator_storage, mock_s3_creds, mock_s3_objects):
    """Test getting miner statistics from S3."""
    # Mock credentials
    mock_get_creds.return_value = mock_s3_creds["credentials"]
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = mock_s3_objects
    mock_boto3_client.return_value = mock_s3
    
    # Set test parameters
    miner_hotkey = "5G5KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv1KEYv"
    
    # Get stats
    stats = s3_validator_storage.get_miner_stats(miner_hotkey)
    
    # Verify S3 client was created with correct credentials
    assert mock_boto3_client.call_count == 2  # Once for each source
    
    # Verify list_objects_v2 was called for both sources
    assert mock_s3.list_objects_v2.call_count == 2
    
    # Verify stats structure
    assert stats["miner_hotkey"] == miner_hotkey
    assert "sources" in stats
    assert "twitter" in stats["sources"]  # We mocked X/Twitter source
    
    # Verify Twitter stats
    twitter_stats = stats["sources"]["twitter"]
    assert twitter_stats["file_count"] == 2
    assert twitter_stats["total_size_bytes"] == 3072  # 1024 + 2048
    assert twitter_stats["last_modified"] is not None