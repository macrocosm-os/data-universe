import unittest
import bittensor as bt
import pydantic
from common import constants
from common.protocol import DecodeURLRequest
from huggingface_utils.encoding_system import EncodingKeyManager, encode_url
import json
from typing import Type
import base64

def serialize_like_dendrite(synapse: bt.Synapse) -> str:
    """Serializes a synapse like a Dendrite would."""
    d = synapse.dict()
    return json.dumps(d)


def serialize_like_axon(synapse: bt.Synapse) -> str:
    """Serializes a synapse like an Axon would."""
    return serialize_like_dendrite(synapse)


def deserialize(json_str: str, cls: Type) -> bt.Synapse:
    """Deserializes the same way a dendrite/axon does."""
    d = json.loads(json_str)
    return cls(**d)


class TestDecodeURLRequest(unittest.TestCase):
    def setUp(self):
        """Setup test environment with encoding keys."""
        # Create two key managers to simulate miner's setup
        self.private_key_manager = EncodingKeyManager(key_path='test_private_key.json')
        self.public_key_manager = EncodingKeyManager(key_path='test_public_key.json')

        # Sample URLs for testing
        self.test_urls = [
            "https://example.com/post/123",
            "https://example.com/user/456",
            "https://example.com/thread/789"
        ]

    def tearDown(self):
        """Cleanup test files."""
        import os
        # Remove test key files
        for file in ['test_private_key.json', 'test_public_key.json']:
            if os.path.exists(file):
                os.remove(file)

    def test_decode_url_request_round_trip(self):
        """Tests the complete request-response cycle with actual encoded URLs."""
        # Encode URLs using private key
        encoded_urls = [
            encode_url(url, self.private_key_manager.get_fernet())
            for url in self.test_urls
        ]

        # Create request
        request = DecodeURLRequest(
            encoded_urls=encoded_urls
        )

        # Test serialization
        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, DecodeURLRequest)
        self.assertEqual(request, deserialized)

        # Create response with decoded URLs
        response = DecodeURLRequest(
            encoded_urls=encoded_urls,
            decoded_urls=self.test_urls
        )

        # Test response serialization
        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, DecodeURLRequest)
        self.assertEqual(response, deserialized)

    def test_url_limit_validation(self):
        """Tests the 10 URL limit with actual encoded URLs."""
        # Create more than 10 test URLs
        many_urls = ["https://example.com/" + str(i) for i in range(12)]

        # Encode them
        encoded_urls = [
            encode_url(url, self.private_key_manager.get_fernet())
            for url in many_urls
        ]

        # Test with exactly 10 URLs (should pass)
        request = DecodeURLRequest(
            encoded_urls=encoded_urls[:10]
        )
        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, DecodeURLRequest)
        self.assertEqual(request, deserialized)

        # Test with more than 10 URLs (should fail)
        with self.assertRaises(ValueError):  # Changed from pydantic.ValidationError to ValueError
            DecodeURLRequest(
                encoded_urls=encoded_urls
            )

    def test_private_and_public_key_encoding(self):
        """Tests encoding with private key and decoding attempts with both keys."""
        # Encode a URL with private key
        test_url = "https://example.com/private/123"
        encoded_url = encode_url(test_url, self.private_key_manager.get_fernet())

        # Create request
        request = DecodeURLRequest(
            encoded_urls=[encoded_url]
        )

        # Test decode with private key (should work)
        decoded_private = self.private_key_manager.get_fernet().decrypt(
            base64.urlsafe_b64decode(encoded_url.encode())
        ).decode()
        self.assertEqual(test_url, decoded_private)

        # Test decode with public key (should fail)
        with self.assertRaises(Exception):
            self.public_key_manager.get_fernet().decrypt(
                base64.urlsafe_b64decode(encoded_url.encode())
            ).decode()

    def test_empty_and_invalid_responses(self):
        """Tests handling of empty and invalid URL responses."""
        # Create request with mix of valid and invalid encoded URLs
        encoded_valid = encode_url(
            "https://example.com/valid",
            self.private_key_manager.get_fernet()
        )

        request = DecodeURLRequest(
            encoded_urls=[
                encoded_valid,
                "invalid_format",  # Invalid encoding
                ""  # Empty string
            ]
        )

        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, DecodeURLRequest)
        self.assertEqual(request, deserialized)

        # Create response with mix of successful and failed decodings
        response = DecodeURLRequest(
            encoded_urls=request.encoded_urls,
            decoded_urls=[
                "https://example.com/valid",
                "",  # Failed decode
                ""  # Failed decode
            ]
        )

        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, DecodeURLRequest)
        self.assertEqual(response, deserialized)

    def test_mixed_key_decoding(self):
        """Tests handling of URLs encoded with different keys."""
        # Create URLs encoded with both private and public keys
        test_url1 = "https://example.com/private/123"
        test_url2 = "https://example.com/public/456"

        # Encode same URLs with different keys
        encoded_private = encode_url(test_url1, self.private_key_manager.get_fernet())
        encoded_public = encode_url(test_url2, self.public_key_manager.get_fernet())

        # Create request with mix of private and public encoded URLs
        request = DecodeURLRequest(
            encoded_urls=[encoded_private, encoded_public]
        )

        # Test request serialization
        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, DecodeURLRequest)
        self.assertEqual(request, deserialized)

        # Create response with correctly decoded URLs
        response = DecodeURLRequest(
            encoded_urls=[encoded_private, encoded_public],
            decoded_urls=[test_url1, test_url2]
        )

        # Test response serialization
        serialized = serialize_like_axon(response)
        deserialized = deserialize(serialized, DecodeURLRequest)
        self.assertEqual(response, deserialized)

    def test_real_encoded_urls(self):
        """Tests with actual encoded URLs from production."""
        # Example of real encoded URLs from your system
        real_encoded_private = "Z0FBQUFBQm5HWHVLMnU1T2pqTlc5bXY0dnZ3c0ZPQUZzbHZuQlRMMWdiSzI2MF9RUURIbWNyWS0xSU9tenRsbkxfMU9XRTZxU21ZRkltUXlfcXd6c1U5VS1wdTJLdHBZX2lOOXVoUy03dDAzSHpWMm42U2piS1pEMzF5RHVFQkp3dW1XWDZPMGFFbHR3M3lFMGRhYko4QU11a29IaDk3dDdPNWdrWFpUZ1VSRFhpRGQ0QUtiUUIwPQ=="
        real_encoded_public = "Z0FBQUFBQm5HWHVLMnU1T2pqTlc5bXY0dnZ3c0ZPQUZzbHZuQlRMMWdiSzI2MF9RUURIbWNyWS0xSU9tenRsbkxfMU9XRTZxU21ZRkltUXlfcXd6c1U5VS1wdTJLdHBZX2lOOXVoUy03dDAzSHpWMm42U2piS1pEMzF5RHVFQkp3dW1XWDZPMGFFbHR3M3lFMGRhYko4QU11a29IaDk3dDdPNWdrWFpUZ1VSRFhpRGQ0QUtiUUIwPQ=="

        request = DecodeURLRequest(
            encoded_urls=[real_encoded_private, real_encoded_public]
        )

        # Test full serialization
        serialized = serialize_like_dendrite(request)
        deserialized = deserialize(serialized, DecodeURLRequest)
        self.assertEqual(request, deserialized)


if __name__ == "__main__":
    unittest.main()