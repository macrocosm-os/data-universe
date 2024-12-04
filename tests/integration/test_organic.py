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
            test_data = json.dumps([
                {
                    "source_name": "reddit",
                    "label_weights": {
                        "r/testing": 1.0
                    }
                },
                {
                    "source_name": "x",
                    "label_weights": {
                        "#test": 1.0
                    }
                }
            ])
            request = OrganicProtocol(gravity=test_data)

            axon.attach(
                forward_fn=async_mock_organic_handler,
                blacklist_fn=async_mock_blacklist_fn,
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
            self.assertEqual(response.gravity, test_data)

        finally:
            axon.stop()

    def test_json_serialization(self):
        test_data = json.dumps([
            {
                "source_name": "reddit",
                "label_weights": {
                    "r/testing": 1.0
                }
            },
            {
                "source_name": "x",
                "label_weights": {
                    "#test": 1.0
                }
            }
        ])

        protocol = OrganicProtocol(gravity=test_data)
        test_file = "test_protocol.json"
        protocol.to_json(test_file)
        loaded_protocol = OrganicProtocol.from_json(test_file)
        self.assertEqual(protocol.gravity, loaded_protocol.gravity)

    def test_parse_gravity(self):
        test_data = json.dumps([
            {
                "source_name": "reddit",
                "label_weights": {
                    "r/testing": 1.0
                }
            }
        ])
        protocol = OrganicProtocol(gravity=test_data)
        parsed = protocol.parse_gravity()
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].source_name, "reddit")
        self.assertEqual(parsed[0].label_weights["r/testing"], 1.0)

    def test_empty_gravity(self):
        protocol = OrganicProtocol(gravity="")
        parsed = protocol.parse_gravity()
        self.assertEqual(parsed, [])

    def test_blacklist(self):
        protocol = OrganicProtocol(gravity="")
        protocol.dendrite.hotkey = "wrong_key"

        result = await_result(blacklist_organic_fn(protocol))
        self.assertTrue(result)

        protocol.dendrite.hotkey = "sn13_test"
        result = await_result(blacklist_organic_fn(protocol))
        self.assertFalse(result)


async def async_mock_organic_handler(synapse: OrganicProtocol) -> OrganicProtocol:
    return synapse


async def async_mock_blacklist_fn(synapse: OrganicProtocol) -> Tuple[bool, str]:
    return False, ""


async def async_mock_priority(synapse: OrganicProtocol) -> float:
    return 1.0


def await_result(coroutine):
    import asyncio
    return asyncio.get_event_loop().run_until_complete(coroutine)


if __name__ == "__main__":
    unittest.main()