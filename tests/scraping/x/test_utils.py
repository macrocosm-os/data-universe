import unittest

import datetime as dt

from common.data import DataEntity, DataLabel, DataSource
from scraping.x import utils
from scraping.x.model import XContent


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
        self.assertTrue(
            utils.is_valid_twitter_url(
                "https://www.x.com/bittensor_alert/status/1733247372950397060"
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

    def test_validate_tweet_content(self):
        """Validates a correct tweet passes validation."""
        actual_tweet = XContent(
            username="@bittensor_alert",
            text="🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC",
            url="https://twitter.com/bittensor_alert/status/1748585332935622672",
            timestamp=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            tweet_hashtags=["#Bittensor", "#TAO", "#MEXC"],
        )

        entity_to_validate = DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"],"model_config":{"extra": "ignore"}}',
            content_size_bytes=291,
        )

        validation_result = utils.validate_tweet_content(
            actual_tweet, entity_to_validate
        )
        self.assertTrue(validation_result.is_valid)

    def test_validate_tweet_content_prevents_extra_fields(self):
        """Validates that extra fields in the content fail validation."""
        actual_tweet = XContent(
            username="@bittensor_alert",
            text="🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC",
            url="https://twitter.com/bittensor_alert/status/1748585332935622672",
            timestamp=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            tweet_hashtags=["#Bittensor", "#TAO", "#MEXC"],
        )

        entity_to_validate = DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"extra_field":"look Ma, bigger content!","username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"],"model_config":{"extra": "ignore"}}',
            content_size_bytes=331,
        )

        validation_result = utils.validate_tweet_content(
            actual_tweet, entity_to_validate
        )
        self.assertFalse(validation_result.is_valid)

    def test_validate_tweet_content_validates_model_config(self):
        """Validates that the model_config is validated, if provided."""
        actual_tweet = XContent(
            username="@bittensor_alert",
            text="🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC",
            url="https://twitter.com/bittensor_alert/status/1748585332935622672",
            timestamp=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            tweet_hashtags=["#Bittensor", "#TAO", "#MEXC"],
        )

        entity_to_validate = DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"model_config":{"extra": "ignore123456"},"username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"]}',
            content_size_bytes=296,
        )

        validation_result = utils.validate_tweet_content(
            actual_tweet, entity_to_validate
        )
        self.assertFalse(validation_result.is_valid)

    def test_validate_tweet_content_requires_obfuscated_date(self):
        """Validates a tweet validation requires an obfuscated date."""
        actual_tweet = XContent(
            username="@bittensor_alert",
            text="🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC",
            url="https://twitter.com/bittensor_alert/status/1748585332935622672",
            timestamp=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            tweet_hashtags=["#Bittensor", "#TAO", "#MEXC"],
        )

        entity_to_validate = DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:45Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"],"model_config":{"extra": "ignore"}}',
            content_size_bytes=291,
        )

        validation_result = utils.validate_tweet_content(
            actual_tweet, entity_to_validate
        )
        self.assertFalse(validation_result.is_valid)
        self.assertIn("was not obfuscated", validation_result.reason)

    def test_validate_tweet_content_extra_bytes_below_limit(self):
        """Validates a tweet with extra bytes below the limit passes validation."""
        actual_tweet = XContent(
            username="@bittensor_alert",
            text="🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC",
            url="https://twitter.com/bittensor_alert/status/1748585332935622672",
            timestamp=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            tweet_hashtags=["#Bittensor", "#TAO", "#MEXC"],
        )

        entity_to_validate = DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            # Extra spaces in the content.
            content='{          "username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"],"model_config":{"extra": "ignore"}}',
            content_size_bytes=326,
        )

        validation_result = utils.validate_tweet_content(
            actual_tweet, entity_to_validate
        )
        self.assertTrue(validation_result.is_valid)

    def test_validate_tweet_content_extra_bytes_above_limit(self):
        """Validates a tweet with extra bytes above the limit fails validation."""
        actual_tweet = XContent(
            username="@bittensor_alert",
            text="🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC",
            url="https://twitter.com/bittensor_alert/status/1748585332935622672",
            timestamp=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            tweet_hashtags=["#Bittensor", "#TAO", "#MEXC"],
        )

        entity_to_validate = DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            # Extra spaces in the content.
            content='{           "username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"],"model_config":{"extra": "ignore"}}',
            content_size_bytes=327,
        )

        validation_result = utils.validate_tweet_content(
            actual_tweet, entity_to_validate
        )
        self.assertFalse(validation_result.is_valid)

    def test_validate_tweet_content_extra_bytes_above_limit_no_config(self):
        """Validates a tweet with extra bytes above the limit and no model config fails validation."""
        actual_tweet = XContent(
            username="@bittensor_alert",
            text="🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC",
            url="https://twitter.com/bittensor_alert/status/1748585332935622672",
            timestamp=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            tweet_hashtags=["#Bittensor", "#TAO", "#MEXC"],
        )

        entity_to_validate = DataEntity(
            uri="https://twitter.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            # Extra spaces in the content.
            content='{           "username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://twitter.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"]}',
            content_size_bytes=292,
        )

        validation_result = utils.validate_tweet_content(
            actual_tweet, entity_to_validate
        )
        self.assertFalse(validation_result.is_valid)

    def test_validate_tweet_content_url_normalized(self):
        """Validates tweet validation passes when using both twitter.com and x.com."""
        actual_tweet = XContent(
            username="@bittensor_alert",
            text="🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC",
            url="https://twitter.com/bittensor_alert/status/1748585332935622672",
            timestamp=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            tweet_hashtags=["#Bittensor", "#TAO", "#MEXC"],
        )

        entity_to_validate = DataEntity(
            uri="https://x.com/bittensor_alert/status/1748585332935622672",
            datetime=dt.datetime(2024, 1, 20, 5, 56, 45, tzinfo=dt.timezone.utc),
            source=DataSource.X,
            label=DataLabel(value="#Bittensor"),
            content='{"username":"@bittensor_alert","text":"🚨 #Bittensor Alert: 500 $TAO ($122,655) deposited into #MEXC","url":"https://x.com/bittensor_alert/status/1748585332935622672","timestamp":"2024-01-20T5:56:00Z","tweet_hashtags":["#Bittensor", "#TAO", "#MEXC"],"model_config":{"extra": "ignore"}}',
            content_size_bytes=291,
        )

        validation_result = utils.validate_tweet_content(
            actual_tweet, entity_to_validate
        )
        self.assertTrue(validation_result.is_valid)

    def test_normalize_url_https(self):
        """Validates twitter URLs are normalized to the x.com domain."""
        self.assertEqual(
            "https://twitter.com/bittensor_alert/status/1748585332935622672",
            utils.normalize_url(
                "https://x.com/bittensor_alert/status/1748585332935622672"
            ),
        )

    def test_normalize_url_http(self):
        """Validates twitter URLs are normalized to the x.com domain."""
        self.assertEqual(
            "http://twitter.com/bittensor_alert/status/1748585332935622672",
            utils.normalize_url(
                "http://x.com/bittensor_alert/status/1748585332935622672"
            ),
        )

    def test_normalize_url_non_twitter_url(self):
        """Validates a non twitter URL is not normalized."""
        self.assertEqual(
            "https://reddit.com/r/bittensor/soiu8324",
            utils.normalize_url("https://reddit.com/r/bittensor/soiu8324"),
        )


if __name__ == "__main__":
    unittest.main()
