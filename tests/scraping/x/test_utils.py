import unittest

from scraping.x import utils


class TestUtils(unittest.TestCase):
    maxDiff = None

    def test_is_valid_twitter_url(self):
        """Tests is_valid_twitter_url with various URLs."""

        self.assertFalse(utils.is_valid_twitter_url(None))
        self.assertFalse(utils.is_valid_twitter_url(""))
        self.assertFalse(utils.is_valid_twitter_url("https://www.google.com"))

        self.assertTrue(
            utils.is_valid_twitter_url(
                "https://twitter.com/bittensor_alert/status/1733247372950397060"
            )
        )
        self.assertTrue(
            utils.is_valid_twitter_url(
                "https://www.twitter.com/bittensor_alert/status/1733247372950397060"
            )
        )

    def test_sanitize_text(self):
        """Tests sanitize_text with various tweets."""

        tweet_text = "@eepdllc @ShannonTFortune  You can’t tell me if Randle finally shows up these playoffs this team can’t win it all:\n\nStarters\nPG: Brunson\nSG: Donovan\nSF: OG\nPF: Randle\nC: IHart\n\nBackups:\nPG: McBride\nSG: Brogdon\nSF: Donte\nPF: Hart\nC: Mitch\nhttps://t.co/dEqmDVcJng"
        expected = "You can’t tell me if Randle finally shows up these playoffs this team can’t win it all:\n\nStarters\nPG: Brunson\nSG: Donovan\nSF: OG\nPF: Randle\nC: IHart\n\nBackups:\nPG: McBride\nSG: Brogdon\nSF: Donte\nPF: Hart\nC: Mitch"

        self.assertEqual(expected, utils.sanitize_scraped_tweet(tweet_text))

    def test_extract_hashtags(self):
        """Tests hashtags are extracted correctly from a tweet text."""

        tweet_text = "#bitcoin $btc\n some insightful text here #Bitcoin #bitcoin\n"
        # Expect the order is retained and dupes are removed.
        expected = ["#bitcoin", "#btc", "#Bitcoin"]
        self.assertEqual(expected, utils.extract_hashtags(tweet_text))

    def test_extract_user(self):
        """Tests the user is extracted correctly from a tweet URL."""

        tweet_url = "https://twitter.com/bittensor_alert/status/1733247372950397060"
        self.assertEqual("@bittensor_alert", utils.extract_user(tweet_url))

    def test_extract_user_invalid_url_format(self):
        """Tests the user is extracted correctly from a tweet URL."""

        tweet_url = "https://twitter.com/status/user/1733247372950397060"
        with self.assertRaises(ValueError):
            utils.extract_user(tweet_url)


if __name__ == "__main__":
    unittest.main()
