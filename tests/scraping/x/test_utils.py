import unittest

from scraping.x.utils import is_valid_twitter_url


class TestUtils(unittest.TestCase):
    def test_is_valid_twitter_url(self):
        """Tests is_valid_twitter_url with various URLs."""

        self.assertFalse(is_valid_twitter_url(None))
        self.assertFalse(is_valid_twitter_url(""))
        self.assertFalse(is_valid_twitter_url("https://www.google.com"))

        self.assertTrue(
            is_valid_twitter_url(
                "https://twitter.com/bittensor_alert/status/1733247372950397060"
            )
        )


if __name__ == "__main__":
    unittest.main()
