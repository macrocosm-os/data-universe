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
                "--neuron.database_user",
                "foobar",
                "--neuron.database_password",
                "P@ssw0rd",
            ],
        ):
            validator = Validator()
            config = validator.get_config_for_test()

            self.assertEqual(config.neuron.database_user, "foobar")
            self.assertEqual(config.neuron.database_password, "P@ssw0rd")

            # Check the default values are still there.
            self.assertEqual(config.neuron.database_name, "ValidatorStorage")


if __name__ == "__main__":
    unittest.main()
