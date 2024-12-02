import unittest
import json
import bittensor as bt
from typing import Dict, Tuple
from organic.gravity_organic import OrganicProtocol
from organic.gravity_organic import blacklist_organic_fn


class TestOrganicProtocol(unittest.TestCase):
    def setUp(self):
        bt.logging(trace=True)
        self.ip = bt.utils.networking.get_external_ip()
        self.wallet = bt.wallet(name="unit_test", hotkey="unit_test")
        self.wallet.create_if_non_existent(
            coldkey_use_password=False, hotkey_use_password=False
        )

    def test_round_trip(self):
        port = 1234
        axon = bt.axon(
            wallet=self.wallet,
            ip="0.0.0.0",
            port=port,
            external_port=port,
        )

        try:
            test_data = {
                "gravity": {
                    "key1": "value1",
                    "key2": "value2"
                }
            }
            request = OrganicProtocol(**test_data)

            axon.attach(
                forward_fn=async_mock_organic_handler,
                blacklist_fn=async_mock_blacklist_fn,  # Fixed function name
                priority_fn=async_mock_priority
            )
            axon.start()

            dendrite = bt.dendrite(wallet=self.wallet)
            response = dendrite.query(
                axons=bt.AxonInfo(
                    version=1,
                    ip=self.ip,
                    port=port,
                    ip_type=1,
                    hotkey=self.wallet.hotkey.ss58_address,
                    coldkey=self.wallet.coldkey.ss58_address,
                ),
                synapse=request,
                timeout=12000,
            )

            self.assertTrue(response.is_success)
            self.assertEqual(response.gravity, test_data["gravity"])

        finally:
            axon.stop()

    def test_json_serialization(self):
        test_data = {
            "gravity": {
                "key1": "value1",
                "key2": "value2"
            }
        }

        protocol = OrganicProtocol(**test_data)
        test_file = "test_protocol.json"
        protocol.to_json(test_file)
        loaded_protocol = OrganicProtocol.from_json(test_file)
        self.assertEqual(protocol.gravity, loaded_protocol.gravity)

    def test_blacklist(self):
        protocol = OrganicProtocol()
        protocol.dendrite.hotkey = "wrong_key"

        result = await_result(blacklist_organic_fn(protocol))
        self.assertTrue(result)  # Check the boolean directly

        protocol.dendrite.hotkey = "sn13_test"
        result = await_result(blacklist_organic_fn(protocol))
        self.assertFalse(result)


async def async_mock_organic_handler(synapse: OrganicProtocol) -> OrganicProtocol:
    return synapse


async def async_mock_blacklist_fn(synapse: OrganicProtocol) -> Tuple[bool, str]:  # Fixed signature
    return False, ""


async def async_mock_priority(synapse: OrganicProtocol) -> float:
    return 1.0


def await_result(coroutine):
    import asyncio
    return asyncio.get_event_loop().run_until_complete(coroutine)


if __name__ == "__main__":
    unittest.main()