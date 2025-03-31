"""
Tests for S3MinerStorage implementation.
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

from storage.s3.s3_miner_storage import S3MinerStorage
from huggingface_utils.encoding_system import EncodingKeyManager
from common.data import DataSource


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


@pytest.fixture
def mock_s3_creds():
    return {
        "credentials": {
            "AccessKeyId": "mock_access_key",
            "SecretAccessKey": "mock_secret_key",
            "SessionToken": "mock_session_token",
            "Expiration": "2023-12-31T23:59:59Z"
        },
        "s3_path": "data/1/5FTyhanxkXXGYhKL7L5UGTYmUBTk3vPFLJKNZZ9tN2eZ9Zcp/test_file.parquet"
    }


@pytest.fixture
def mock_s3_config():
    return {
        "bucket_name": "test-bucket",
        "region": "us-east-1",
        "auth_endpoint": "https://mock-auth-endpoint.example.com/auth"
    }


@pytest.fixture
def mock_encoding_key_manager():
    key_manager = MagicMock(spec=EncodingKeyManager)
    key_manager.sym_key = b"mock_encoding_key"
    return key_manager


@pytest.fixture
def s3_miner_storage(mock_s3_config, mock_encoding_key_manager):
    with tempfile.NamedTemporaryFile() as db_file, tempfile.NamedTemporaryFile() as state_file:
        # Create a mock wallet
        wallet = MockWallet()
        
        # Create S3MinerStorage instance with mocked dependencies
        storage = S3MinerStorage(
            db_path=db_file.name,
            miner_hotkey=wallet.hotkey.ss58_address,
            wallet=wallet,
            encoding_key_manager=mock_encoding_key_manager,
            private_encoding_key_manager=mock_encoding_key_manager,
            state_file=state_file.name,
            s3_config=mock_s3_config,
            output_dir=tempfile.mkdtemp()
        )
        
        # Mock the _create_connection method to avoid actual DB connections
        conn_mock = MagicMock()
        storage.get_db_connection = MagicMock(return_value=conn_mock)
        
        yield storage


@pytest.mark.parametrize("source", [DataSource.REDDIT.value, DataSource.X.value])
def test_compute_file_hash(s3_miner_storage, source):
    """Test that file hash computation works correctly."""
    # Create a temporary file with known content
    with tempfile.NamedTemporaryFile(mode="w+") as test_file:
        test_file.write("test content")
        test_file.flush()
        
        # Compute the hash
        file_hash = s3_miner_storage.compute_file_hash(test_file.name)
        
        # Verify hash is a non-empty string
        assert isinstance(file_hash, str)
        assert len(file_hash) > 0
        
        # Verify hash is deterministic
        assert file_hash == s3_miner_storage.compute_file_hash(test_file.name)


@patch("requests.post")
def test_get_upload_credentials(mock_post, s3_miner_storage, mock_s3_creds):
    """Test that upload credentials are fetched correctly."""
    # Mock the response from auth endpoint
    mock_post.return_value = MockResponse(status_code=200, json_data=mock_s3_creds)
    
    # Create a test file
    with tempfile.NamedTemporaryFile() as test_file:
        # Get upload credentials
        creds = s3_miner_storage.get_upload_credentials(test_file.name, DataSource.X.value)
        
        # Verify request was made with correct data
        mock_post.assert_called_once()
        call_args = mock_post.call_args[1]["json"]
        assert call_args["hotkey"] == s3_miner_storage.miner_hotkey
        assert "timestamp" in call_args
        assert "expiry" in call_args
        assert "file_hash" in call_args
        assert "signature" in call_args
        assert "s3_path" in call_args
        
        # Verify response is returned correctly
        assert creds == mock_s3_creds


@patch("boto3.client")
@patch("requests.post")
def test_upload_file_to_s3(mock_post, mock_boto3_client, s3_miner_storage, mock_s3_creds):
    """Test that file upload to S3 works correctly."""
    # Mock the response from auth endpoint
    mock_post.return_value = MockResponse(status_code=200, json_data=mock_s3_creds)
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Create a test file
    with tempfile.NamedTemporaryFile() as test_file:
        test_file.write(b"test content")
        test_file.flush()
        
        # Upload the file
        s3_path = s3_miner_storage.upload_file_to_s3(test_file.name, DataSource.X.value)
        
        # Verify S3 client was created with correct credentials
        mock_boto3_client.assert_called_once_with(
            's3',
            aws_access_key_id=mock_s3_creds["credentials"]["AccessKeyId"],
            aws_secret_access_key=mock_s3_creds["credentials"]["SecretAccessKey"],
            aws_session_token=mock_s3_creds["credentials"]["SessionToken"],
            region_name=s3_miner_storage.region
        )
        
        # Verify upload_file was called with correct parameters
        mock_s3.upload_file.assert_called_once_with(
            test_file.name,
            s3_miner_storage.bucket_name,
            mock_s3_creds["s3_path"]
        )
        
        # Verify correct S3 path is returned
        assert s3_path == mock_s3_creds["s3_path"]


@patch("storage.s3.s3_miner_storage.S3MinerStorage.upload_batch")
@patch("storage.s3.s3_miner_storage.S3MinerStorage.preprocess_data")
@patch("storage.s3.s3_miner_storage.S3MinerStorage.get_data_for_upload")
def test_upload_data_to_s3(mock_get_data, mock_preprocess, mock_upload_batch, s3_miner_storage):
    """Test the full upload workflow."""
    # Create mock data with two sources
    reddit_df = pd.DataFrame({
        "datetime": [dt.datetime.now()],
        "label": ["test_label"],
        "content": ["test_content"]
    })
    
    x_df = pd.DataFrame({
        "datetime": [dt.datetime.now()],
        "label": ["test_label"],
        "content": ["test_content"]
    })
    
    # Set up mocks
    mock_get_data.side_effect = [
        [reddit_df],  # For Reddit
        [x_df]        # For X
    ]
    mock_preprocess.return_value = pd.DataFrame({
        "datetime": [dt.datetime.now()],
        "label": ["test_label"],
        "content": ["processed_content"],
        "username": ["test_user"]
    })
    
    # Mock state file operations
    s3_miner_storage.load_state = MagicMock(return_value={
        'last_upload': {'1': None, '2': None},
        'total_rows': {'1': 0, '2': 0}
    })
    s3_miner_storage.save_state = MagicMock()
    
    # Call the method
    result = s3_miner_storage.upload_data_to_s3()
    
    # Verify data was fetched for both sources
    assert mock_get_data.call_count == 2
    
    # Verify preprocessing was called
    assert mock_preprocess.call_count == 2
    
    # Verify batch upload was called
    assert mock_upload_batch.call_count == 2
    
    # Verify state was saved
    assert s3_miner_storage.save_state.call_count == 2
    
    # Verify result contains metadata for both sources
    assert len(result) == 2
    assert result[0]['source'] == DataSource.REDDIT.value
    assert result[1]['source'] == DataSource.X.value


@patch("threading.Thread")
@patch("storage.s3.s3_miner_storage.S3MinerStorage.upload_file_to_s3")
def test_upload_batch(mock_upload_file, mock_thread, s3_miner_storage):
    """Test parallel batch upload."""
    # Create temporary test files
    temp_files = []
    for i in range(3):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.write(f"test content {i}".encode())
        temp_file.close()
        temp_files.append(temp_file.name)
    
    # Configure mock to return expected S3 paths
    mock_upload_file.side_effect = [
        f"data/1/{s3_miner_storage.miner_hotkey}/file_{i}.parquet" for i in range(3)
    ]
    
    # Call the batch upload method
    s3_miner_storage.upload_batch(temp_files, DataSource.X.value)
    
    # Verify upload was called for each file
    assert mock_upload_file.call_count == 3
    
    # Clean up test files
    for file_path in temp_files:
        try:
            os.remove(file_path)
        except:
            pass