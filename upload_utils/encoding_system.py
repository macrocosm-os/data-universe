"""Module for URL encoding and decoding using Fernet encryption."""

import base64
import json
import os
import time
from typing import Tuple, Optional

import pandas as pd
from cryptography.fernet import Fernet

class EncodingKeyManager:
    """Manages the encryption key for URL encoding and decoding."""

    def __init__(self, key_path: str = 'encoding_key.json'):
        """Initialize the EncodingKeyManager with a key file path."""
        self.key_path = key_path
        self.sym_key = self._load_or_generate_key()
        self.fernet = Fernet(self.sym_key)

    def _load_or_generate_key(self) -> bytes:
        """Load an existing key or generate a new one if it doesn't exist."""
        if os.path.exists(self.key_path):
            with open(self.key_path, 'r', encoding='utf-8') as f:
                key_data = json.load(f)
                return key_data['sym_key'].encode()
        else:
            sym_key = Fernet.generate_key()
            self._save_key(sym_key)
            return sym_key

    def _save_key(self, sym_key: bytes) -> None:
        """Save the symmetric key to a JSON file."""
        key_data = {
            'sym_key': sym_key.decode()
        }
        with open(self.key_path, 'w', encoding='utf-8') as f:
            json.dump(key_data, f)

    def get_fernet(self) -> Fernet:
        """Get the Fernet instance for encryption/decryption."""
        return self.fernet


class SymKeyEncodingKeyManager(EncodingKeyManager):
    """A subclass of EncodingKeyManager that uses a symmetric key directly."""

    def __init__(self, sym_key: str):
        """
        Initialize the SymKeyEncodingKeyManager with a symmetric key.

        Args:
            sym_key (str): A base64-encoded symmetric key string.
        """
        self.sym_key = self._validate_and_encode_key(sym_key)
        self.fernet = Fernet(self.sym_key)

    def _validate_and_encode_key(self, sym_key: str) -> bytes:
        """Validate the provided key and return it as bytes."""
        try:
            # Attempt to create a Fernet instance to validate the key
            Fernet(sym_key.encode())
            return sym_key.encode()
        except Exception as e:
            raise ValueError(f"Invalid symmetric key provided: {str(e)}")

    def _load_or_generate_key(self) -> bytes:
        """Override to return the provided symmetric key."""
        return self.sym_key

    def _save_key(self, sym_key: bytes) -> None:
        """Override to do nothing, as we don't want to save the key to a file."""
        pass


def encode_url(url: str, fernet: Fernet) -> Optional[str]:
    """Encode a URL using Fernet encryption."""
    try:
        encoded = fernet.encrypt(url.encode())
        return base64.urlsafe_b64encode(encoded).decode()
    except Exception as e:
        print(f"Encryption failed for URL: {url}")
        print(f"Error: {str(e)}")
        return None


def decode_url(encoded_url: str, fernet: Fernet) -> Optional[str]:
    """Decode an encoded URL using Fernet decryption."""
    try:
        decoded = fernet.decrypt(base64.urlsafe_b64decode(encoded_url.encode()))
        return decoded.decode()
    except Exception as e:
        print(f"Decryption failed for encoded URL: {encoded_url}")
        print(f"Error: {str(e)}")
        return None


def encode_dataframe_column(df: pd.DataFrame, column_name: str, key_manager: EncodingKeyManager) -> pd.DataFrame:
    """Encode a column of URLs in a DataFrame."""
    fernet = key_manager.get_fernet()
    df[f'{column_name}_encoded'] = df[column_name].apply(lambda url: encode_url(url, fernet))
    return df


def decode_dataframe_column(df: pd.DataFrame, column_name: str, key_manager: EncodingKeyManager) -> pd.DataFrame:
    """Decode a column of encoded URLs in a DataFrame."""
    fernet = key_manager.get_fernet()
    original_column_name = column_name.replace('_encoded', '')
    df[original_column_name] = df[column_name].apply(lambda url: decode_url(url, fernet))
    return df


def main():
    """Main function to demonstrate URL encoding and decoding."""
    # Initialize EncodingKeyManager
    key_manager = EncodingKeyManager()

    # Create a larger sample DataFrame (1 million rows)
    n_rows = 1_000_000
    urls = [
        'https://example.com/short_url',
        'https://example.com/medium_length_url_with_some_parameters?param1=value1&param2=value2',
        'https://example.com/very_long_url_with_many_parameters_and_some_special_characters?param1=value1&param2=value2&param3=value3&param4=value4&special=!@#$%^&*()'
    ]
    df = pd.DataFrame({
        'url': urls * (n_rows // len(urls) + 1)
    }).head(n_rows)

    # Measure encoding time
    start_time = time.time()
    df_encoded = encode_dataframe_column(df, 'url', key_manager)
    encode_time = time.time() - start_time
    print(f"Encoding time for {n_rows} rows: {encode_time:.2f} seconds")

    # Measure decoding time
    start_time = time.time()
    df_decoded = decode_dataframe_column(df_encoded, 'url_encoded', key_manager)
    decode_time = time.time() - start_time
    print(f"Decoding time for {n_rows} rows: {decode_time:.2f} seconds")

    # Verify that the decoded URLs match the original
    print("\nVerification:")
    print(df['url'].equals(df_decoded['url']))

    # Calculate and print rows processed per second
    print(f"\nEncoding speed: {n_rows / encode_time:.2f} rows/second")
    print(f"Decoding speed: {n_rows / decode_time:.2f} rows/second")


if __name__ == "__main__":
    main()