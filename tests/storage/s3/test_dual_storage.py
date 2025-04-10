"""
Tests for DualStorage implementation.
"""
import pytest
import tempfile
from unittest.mock import MagicMock, patch

from storage.s3.dual_storage import DualStorage
from storage.s3.config import DualStorageConfig, S3Config
from storage.s3.s3_miner_storage import S3MinerStorage
from huggingface_utils.huggingface_uploader import HuggingFaceUploader


@pytest.fixture
def mock_config():
    s3_config = S3Config(
        bucket_name="test-bucket",
        auth_endpoint="https://mock-auth-endpoint.example.com/auth",
        region="us-east-1"
    )
    return DualStorageConfig(
        s3_config=s3_config,
        use_s3=True,
        use_huggingface=True
    )


@pytest.fixture
def mock_hf_uploader():
    uploader = MagicMock(spec=HuggingFaceUploader)
    uploader.upload_sql_to_huggingface.return_value = [
        {"repo_name": "test/reddit_dataset", "source": 1},
        {"repo_name": "test/x_dataset", "source": 2}
    ]
    return uploader


@pytest.fixture
def mock_s3_storage():
    storage = MagicMock(spec=S3MinerStorage)
    storage.upload_data_to_s3.return_value = [
        {"bucket": "test-bucket", "source": 1},
        {"bucket": "test-bucket", "source": 2}
    ]
    return storage


def test_init(mock_config, mock_hf_uploader, mock_s3_storage):
    """Test DualStorage initialization."""
    storage = DualStorage(
        config=mock_config,
        hf_uploader=mock_hf_uploader,
        s3_storage=mock_s3_storage
    )
    
    assert storage.config == mock_config
    assert storage.hf_uploader == mock_hf_uploader
    assert storage.s3_storage == mock_s3_storage
    assert storage.use_huggingface == mock_config.use_huggingface
    assert storage.use_s3 == mock_config.use_s3


def test_upload_data_both_enabled(mock_config, mock_hf_uploader, mock_s3_storage):
    """Test upload_data with both storage systems enabled."""
    storage = DualStorage(
        config=mock_config,
        hf_uploader=mock_hf_uploader,
        s3_storage=mock_s3_storage
    )
    
    result = storage.upload_data()
    
    # Verify both uploaders were called
    mock_s3_storage.upload_data_to_s3.assert_called_once()
    mock_hf_uploader.upload_sql_to_huggingface.assert_called_once()
    
    # Verify results were combined
    assert len(result) == 4
    

def test_upload_data_s3_only(mock_config, mock_hf_uploader, mock_s3_storage):
    """Test upload_data with only S3 enabled."""
    mock_config.use_huggingface = False
    
    storage = DualStorage(
        config=mock_config,
        hf_uploader=mock_hf_uploader,
        s3_storage=mock_s3_storage
    )
    
    result = storage.upload_data()
    
    # Verify only S3 uploader was called
    mock_s3_storage.upload_data_to_s3.assert_called_once()
    mock_hf_uploader.upload_sql_to_huggingface.assert_not_called()
    
    # Verify only S3 results were returned
    assert len(result) == 2


def test_upload_data_hf_only(mock_config, mock_hf_uploader, mock_s3_storage):
    """Test upload_data with only HuggingFace enabled."""
    mock_config.use_s3 = False
    
    storage = DualStorage(
        config=mock_config,
        hf_uploader=mock_hf_uploader,
        s3_storage=mock_s3_storage
    )
    
    result = storage.upload_data()
    
    # Verify only HF uploader was called
    mock_s3_storage.upload_data_to_s3.assert_not_called()
    mock_hf_uploader.upload_sql_to_huggingface.assert_called_once()
    
    # Verify only HF results were returned
    assert len(result) == 2


def test_upload_data_handles_exceptions(mock_config, mock_hf_uploader, mock_s3_storage):
    """Test that upload_data handles exceptions gracefully."""
    # Make S3 storage raise an exception
    mock_s3_storage.upload_data_to_s3.side_effect = Exception("S3 error")
    
    storage = DualStorage(
        config=mock_config,
        hf_uploader=mock_hf_uploader,
        s3_storage=mock_s3_storage
    )
    
    result = storage.upload_data()
    
    # Verify both uploaders were called
    mock_s3_storage.upload_data_to_s3.assert_called_once()
    mock_hf_uploader.upload_sql_to_huggingface.assert_called_once()
    
    # Verify only HF results were returned
    assert len(result) == 2


@patch("storage.s3.s3_miner_storage.S3MinerStorage")
@patch("huggingface_utils.huggingface_uploader.HuggingFaceUploader")
def test_create_factory_method(mock_hf_uploader_class, mock_s3_storage_class, mock_config):
    """Test the create factory method."""
    # Mock the constructors
    mock_hf_uploader_instance = MagicMock(spec=HuggingFaceUploader)
    mock_s3_storage_instance = MagicMock(spec=S3MinerStorage)
    
    mock_hf_uploader_class.return_value = mock_hf_uploader_instance
    mock_s3_storage_class.return_value = mock_s3_storage_instance
    
    # Mock dependencies
    db_path = "/path/to/db"
    miner_hotkey = "test_hotkey"
    wallet = MagicMock()
    encoding_key_manager = MagicMock()
    private_encoding_key_manager = MagicMock()
    state_file = "/path/to/state"
    
    # Create using factory method
    storage = DualStorage.create(
        config=mock_config,
        db_path=db_path,
        miner_hotkey=miner_hotkey,
        wallet=wallet,
        encoding_key_manager=encoding_key_manager,
        private_encoding_key_manager=private_encoding_key_manager,
        state_file=state_file
    )
    
    # Verify correct initialization
    assert isinstance(storage, DualStorage)
    assert storage.config == mock_config
    assert storage.hf_uploader == mock_hf_uploader_instance
    assert storage.s3_storage == mock_s3_storage_instance
    
    # Verify HF uploader was constructed correctly
    mock_hf_uploader_class.assert_called_once()
    hf_args = mock_hf_uploader_class.call_args[1]
    assert hf_args["db_path"] == db_path
    assert hf_args["miner_hotkey"] == miner_hotkey
    assert hf_args["encoding_key_manager"] == encoding_key_manager
    assert hf_args["private_encoding_key_manager"] == private_encoding_key_manager
    assert hf_args["state_file"] == f"{state_file}_hf"
    
    # Verify S3 storage was constructed correctly
    mock_s3_storage_class.assert_called_once()
    s3_args = mock_s3_storage_class.call_args[1]
    assert s3_args["db_path"] == db_path
    assert s3_args["miner_hotkey"] == miner_hotkey
    assert s3_args["wallet"] == wallet
    assert s3_args["encoding_key_manager"] == encoding_key_manager
    assert s3_args["private_encoding_key_manager"] == private_encoding_key_manager
    assert s3_args["state_file"] == f"{state_file}_s3"