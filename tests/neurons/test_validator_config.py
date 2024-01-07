import sys
import unittest
from unittest.mock import patch

from neurons.validator import Validator


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
            validator = Validator()
            config = validator.get_config_for_test()

            # Check the default values are still there.
            self.assertEqual(config.neuron.axon_off, False)


if __name__ == "__main__":
    unittest.main()
