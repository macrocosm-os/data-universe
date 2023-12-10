import datetime as dt
import unittest

from sympy import timed
from scraping.x.model import XContent


class TestModel(unittest.TestCase):
    def test_is_equivalent_to_equivalent(self):
        """Tests validation of equivalent XContent instances."""
        timestamp = dt.datetime.now()
        # Create two XContent instances with the same values
        xcontent1 = XContent(
            username="user1",
            text="Hello world",
            replies=10,
            retweets=5,
            quotes=2,
            likes=20,
            url="https://twitter.com/123",
            timestamp=timestamp,
            tweet_hashtags=["#bittensor", "$TAO"],
        )
        xcontent2 = XContent(
            username="user1",
            text="Hello world",
            # Use different values for the dynamic fields, to ensure they're ignored.
            replies=15,
            retweets=55,
            quotes=2,
            likes=22834,
            url="https://twitter.com/123",
            timestamp=timestamp,
            tweet_hashtags=["#bittensor", "$TAO"],
        )

        # Check if the two instances are equivalent
        self.assertTrue(xcontent1.is_equivalent_to(xcontent2))
        self.assertTrue(xcontent2.is_equivalent_to(xcontent1))

    def test_is_equivalent_to_not_equivalent(self):
        """Tests validation of non-equivalent XContent instances."""
        timestamp = dt.datetime.now()
        content = XContent(
            username="user1",
            text="Hello world",
            replies=10,
            retweets=5,
            quotes=2,
            likes=20,
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
            self.assertFalse(content.is_equivalent_to(c))
            self.assertFalse(c.is_equivalent_to(content))


if __name__ == "__main__":
    unittest.main()
