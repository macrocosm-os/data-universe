import sys
import unittest
from unittest.mock import patch

from neurons.miner import Miner


class TestMinerConfig(unittest.TestCase):
    def test_miner_config(self):
        with patch.object(
            sys,
            "argv",
            [
                "miner.py",
                "--neuron.database_name",
                "mydb",
                "--subtensor.network",
                "test",
            ],
        ):
            miner = Miner()
            config = miner.get_config_for_test()

            self.assertEqual(config.neuron.database_name, "mydb")
            # Check the default values are still there.
            self.assertEqual(config.neuron.max_database_size_gb_hint, 250)


if __name__ == "__main__":
    unittest.main()
