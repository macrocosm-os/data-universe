import datetime as dt
import unittest
from common import constants

from sympy import timed
from scraping.x.model import XContent


class TestModel(unittest.TestCase):
    def test_equality(self):
        """Tests validation of equivalent XContent instances."""
        timestamp = dt.datetime.now()
        # Create two XContent instances with the same values
        xcontent1 = XContent(
            username="user1",
            text="Hello world",
            url="https://twitter.com/123",
            timestamp=timestamp,
            tweet_hashtags=["#bittensor", "$TAO"],
        )
        xcontent2 = XContent(
            username="user1",
            text="Hello world",
            url="https://twitter.com/123",
            timestamp=timestamp,
            tweet_hashtags=["#bittensor", "$TAO"],
        )

        # Check if the two instances are equivalent
        self.assertTrue(xcontent1 == xcontent2)
        self.assertTrue(xcontent2 == xcontent1)

    def test_equality_not_equivalent(self):
        """Tests validation of non-equivalent XContent instances."""
        timestamp = dt.datetime.now()
        content = XContent(
            username="user1",
            text="Hello world",
            url="https://twitter.com/123",
            timestamp=timestamp,
            tweet_hashtags=["#bittensor", "$TAO"],
        )

        non_matching_content = [
            content.copy(update={"username": "user2"}),
            content.copy(update={"text": "Hello world!"}),
            content.copy(update={"url": "https://twitter.com/456"}),
            content.copy(update={"timestamp": timestamp + dt.timedelta(seconds=1)}),
            # Hashtag ordering needs to be deterministic. Verify changing the order of the hashtags makes the content non-equivalent.
            content.copy(update={"tweet_hashtags": ["#TAO", "#bittensor"]}),
        ]

        for c in non_matching_content:
            self.assertFalse(content == c)
            self.assertFalse(c == content)

    def test_label_truncation(self):
        """Tests that XContents correctly truncate labels to 32 characters when converting to DataEntities"""
        timestamp = dt.datetime.now()
        content = XContent(
            username="user1",
            text="Hello world",
            url="https://twitter.com/123",
            timestamp=timestamp,
            tweet_hashtags=["#loooooooooooooooooooooooonghashtag", "$TAO"],
        )
        entity = XContent.to_data_entity(content)

        self.assertEqual(len(entity.label.value), constants.MAX_LABEL_LENGTH)
        self.assertEqual(entity.label.value, "#loooooooooooooooooooooooonghash")


if __name__ == "__main__":
    unittest.main()
