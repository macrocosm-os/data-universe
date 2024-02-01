import sys
import unittest
from unittest.mock import patch
from neurons.config import NeuronType, create_config


class TestValidatorConfig(unittest.TestCase):
    def test_validator_config(self):
        with patch.object(
            sys,
            "argv",
            [
                "validator.py",
                "--subtensor.network",
                "test",
            ],
        ):
            config = create_config(NeuronType.VALIDATOR)

            # Check the default values are still there.
            self.assertEqual(config.neuron.axon_off, False)
            self.assertEqual(config.subtensor.network, "test")


if __name__ == "__main__":
    unittest.main()
