import unittest

import datetime as dt

from common.data import DataEntity, DataLabel, DataSource
from scraping.reddit import utils
from scraping.reddit.model import RedditContent, RedditDataType
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

    def test_validate_reddit_content(self):
        """Performs a validation on a matching RedditContent and verifies validate_reddit_content passes."""
        actual_content = RedditContent(
            id="t1_kc3w8lk",
            url="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            username="KOOLBREEZE144",
            communityName="r/bittensor_",
            body="Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?",
            createdAt=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            dataType=RedditDataType.COMMENT,
            title=None,
            parentId="t1_kc3vd3n",
        )

        entity_to_validate = DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            datetime=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:00+00:00", "dataType": "comment", "parentId": "t1_kc3vd3n"}',
            content_size_bytes=392,
        )

        validation_result = utils.validate_reddit_content(
            actual_content, entity_to_validate
        )
        self.assertTrue(validation_result.is_valid)

    def test_validate_reddit_content_obfuscated_date_required(self):
        """Performs a validation on a RedditContent that hasn't obfuscated the date and verifies
        the validation fails."""
        actual_content = RedditContent(
            id="t1_kc3w8lk",
            url="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            username="KOOLBREEZE144",
            communityName="r/bittensor_",
            body="Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?",
            createdAt=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            dataType=RedditDataType.COMMENT,
            title=None,
            parentId="t1_kc3vd3n",
        )

        entity_to_validate = DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            datetime=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:16+00:00", "dataType": "comment", "parentId": "t1_kc3vd3n"}',
            content_size_bytes=392,
        )

        validation_result = utils.validate_reddit_content(
            actual_content, entity_to_validate
        )
        self.assertFalse(validation_result.is_valid)
        self.assertIn(
            "was not obfuscated",
            validation_result.reason,
        )

    def test_validate_reddit_content_extra_fields_prohibited(self):
        """Verifies the RedditContent doesn't allow extra fields"""
        actual_content = RedditContent(
            id="t1_kc3w8lk",
            url="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            username="KOOLBREEZE144",
            communityName="r/bittensor_",
            body="Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?",
            createdAt=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            dataType=RedditDataType.COMMENT,
            title=None,
            parentId="t1_kc3vd3n",
        )

        entity_to_validate = DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            datetime=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            content=b'{"some_extra_stuff":"I love these extra bytes $$$","id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:00+00:00", "dataType": "comment", "parentId": "t1_kc3vd3n"}',
            content_size_bytes=418,
        )

        validation_result = utils.validate_reddit_content(
            actual_content, entity_to_validate
        )
        self.assertFalse(validation_result.is_valid)
        self.assertIn("Failed to decode data entity", validation_result.reason)

    def test_validate_reddit_content_extra_bytes_below_limit(self):
        """Verifies the RedditContent allows a small amount of additional bytes on the content."""
        actual_content = RedditContent(
            id="t1_kc3w8lk",
            url="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            username="KOOLBREEZE144",
            communityName="r/bittensor_",
            body="Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?",
            createdAt=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            dataType=RedditDataType.COMMENT,
            title=None,
            parentId="t1_kc3vd3n",
        )

        entity_to_validate = DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            datetime=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            # Extra spaces in the content.
            content=b'{          "id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:00+00:00", "dataType": "comment", "parentId": "t1_kc3vd3n"}',
            content_size_bytes=410,
        )

        validation_result = utils.validate_reddit_content(
            actual_content, entity_to_validate
        )
        self.assertTrue(validation_result.is_valid)

    def test_validate_reddit_content_extra_bytes_above_limit(self):
        """Verifies the RedditContent forbids too many additional bytes on the content."""
        actual_content = RedditContent(
            id="t1_kc3w8lk",
            url="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            username="KOOLBREEZE144",
            communityName="r/bittensor_",
            body="Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?",
            createdAt=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            dataType=RedditDataType.COMMENT,
            title=None,
            parentId="t1_kc3vd3n",
        )

        entity_to_validate = DataEntity(
            uri="https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/",
            datetime=dt.datetime(2023, 12, 5, 16, 35, 16, tzinfo=dt.timezone.utc),
            source=DataSource.REDDIT,
            label=DataLabel(value="r/bittensor_"),
            # Extra spaces in the content.
            content=b'{            "id": "t1_kc3w8lk", "url": "https://www.reddit.com/r/bittensor_/comments/18bf67l/how_do_you_add_tao_to_metamask/kc3w8lk/", "username": "KOOLBREEZE144", "communityName": "r/bittensor_", "body": "Thanks for responding. Do you recommend a wallet or YT video on setting this up? What do you use?", "createdAt": "2023-12-05T16:35:00+00:00", "dataType": "comment", "parentId": "t1_kc3vd3n"}',
            content_size_bytes=411,
        )

        validation_result = utils.validate_reddit_content(
            actual_content, entity_to_validate
        )
        self.assertFalse(validation_result.is_valid)


if __name__ == "__main__":
    unittest.main()
