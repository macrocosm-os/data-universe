import base64
import json
import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization
import pandas as pd


class EncodingKeyManager:
    def __init__(self, key_path: str = 'encoding_key.json'):
        self.key_path = key_path
        self.public_key, self.private_key = self.load_or_generate_key()

    def load_or_generate_key(self):
        if os.path.exists(self.key_path):
            with open(self.key_path, 'r') as f:
                key_data = json.load(f)
                public_key = serialization.load_pem_public_key(key_data['public_key'].encode())
                private_key = serialization.load_pem_private_key(
                    key_data['private_key'].encode(),
                    password=None
                )
        else:
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
            )
            public_key = private_key.public_key()
            self.save_key(private_key, public_key)

        return public_key, private_key

    def save_key(self, private_key, public_key):
        key_data = {
            'private_key': private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            ).decode(),
            'public_key': public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ).decode()
        }
        with open(self.key_path, 'w') as f:
            json.dump(key_data, f)

    def get_public_key(self):
        return self.public_key

    def get_private_key(self):
        return self.private_key


def encode_url(url, public_key):
    encoded = public_key.encrypt(
        url.encode(),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return base64.b64encode(encoded).decode()


def decode_url(encoded_url, private_key):
    decoded = private_key.decrypt(
        base64.b64decode(encoded_url),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return decoded.decode()


def encode_dataframe_column(df, column_name, key_manager):
    public_key = key_manager.get_public_key()
    df[f'{column_name}_encoded'] = df[column_name].apply(lambda url: encode_url(url, public_key))
    return df


def decode_dataframe_column(df, column_name, key_manager):
    private_key = key_manager.get_private_key()
    column_name = column_name.split('_encoded')[0]
    df[column_name] = df[column_name].apply(lambda url: decode_url(url, private_key))
    return df


# Usage example
if __name__ == "__main__":
    # Initialize EncodingKeyManager
    key_manager = EncodingKeyManager()

    # Create a sample DataFrame
    df = pd.DataFrame({
        'url': [
            'https://example1.com',
            'https://example2.com',
            'https://example3.com'
        ]
    })

    # Encode the 'url' column
    df_encoded = encode_dataframe_column(df, 'url', key_manager)

    print("Encoded DataFrame:")
    print(df_encoded)

    # Demonstrate decoding
    print("\nDecoded URLs:")
    df_decoded = decode_dataframe_column(df_encoded, 'url_encoded', key_manager)