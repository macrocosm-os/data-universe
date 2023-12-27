import unittest

from scraping.reddit.utils import is_valid_reddit_url, normalize_permalink


class TestUtils(unittest.TestCase):
    def test_is_valid_reddit_url(self):
        """Tests is_valid_reddit_url with various URLs."""

        self.assertFalse(is_valid_reddit_url(None))
        self.assertFalse(is_valid_reddit_url(""))
        self.assertFalse(is_valid_reddit_url("https://www.google.com"))

        self.assertTrue(
            is_valid_reddit_url(
                "https://www.reddit.com/r/bittensor_/comments/18e1fl6/wrappedtao_is_it_safe/"
            )
        )

    def test_normalize_permalink(self):
        """Tests that normalize_permalink correctly adds a leading '/' if necessary."""
        good_permalink = "/r/bittensor_/comments/18e1fl6/wrappedtao_is_it_safe/"

        bad_permalink = "r/bittensor_/comments/18e1fl6/wrappedtao_is_it_safe/"

        self.assertTrue(normalize_permalink(bad_permalink) == good_permalink)
        self.assertTrue(normalize_permalink(good_permalink) == good_permalink)


if __name__ == "__main__":
    unittest.main()
