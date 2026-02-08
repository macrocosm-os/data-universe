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


class TestValidateScrapedAt(unittest.TestCase):
    """Tests for scraped_at validation on RedditContent."""

    def _make_content_and_entity(self, scraped_at=None):
        """Helper to create a valid RedditContent and DataEntity pair with optional scraped_at."""
        content = RedditContent(
            id="t3_test123",
            url="https://www.reddit.com/r/test/comments/test123/test_post/",
            username="test_user",
            communityName="r/test",
            body="Test body text",
            createdAt=dt.datetime(2025, 1, 10, 12, 0, 0, tzinfo=dt.timezone.utc),
            dataType=RedditDataType.POST,
            title="Test post",
            scrapedAt=scraped_at,
        )
        entity = RedditContent.to_data_entity(content)
        return content, entity

    def test_scraped_at_valid(self):
        """scraped_at that is obfuscated, >= created_at, and <= now passes."""
        scraped = dt.datetime(2025, 1, 10, 12, 5, 0, tzinfo=dt.timezone.utc)
        content, entity = self._make_content_and_entity(scraped_at=scraped)
        parsed = RedditContent.from_data_entity(entity)
        result = utils.validate_scraped_at(parsed, entity)
        self.assertIsNone(result)

    def test_scraped_at_not_obfuscated(self):
        """scraped_at with non-zero seconds fails validation."""
        scraped = dt.datetime(2025, 1, 10, 12, 5, 30, tzinfo=dt.timezone.utc)
        content = RedditContent(
            id="t3_test123",
            url="https://www.reddit.com/r/test/comments/test123/test_post/",
            username="test_user",
            communityName="r/test",
            body="Test body text",
            createdAt=dt.datetime(2025, 1, 10, 12, 0, 0, tzinfo=dt.timezone.utc),
            dataType=RedditDataType.POST,
            title="Test post",
            scrapedAt=scraped,
        )
        # Build entity manually to bypass obfuscation
        entity = DataEntity(
            uri=content.url,
            datetime=content.created_at,
            source=DataSource.REDDIT,
            label=DataLabel(value="r/test"),
            content=content.json(by_alias=True).encode("utf-8"),
            content_size_bytes=len(content.json(by_alias=True).encode("utf-8")),
        )
        parsed = RedditContent.from_data_entity(entity)
        result = utils.validate_scraped_at(parsed, entity)
        self.assertIsNotNone(result)
        self.assertFalse(result.is_valid)
        self.assertIn("obfuscated", result.reason)

    def test_scraped_at_before_created_at(self):
        """scraped_at before created_at fails validation."""
        scraped = dt.datetime(2025, 1, 9, 12, 0, 0, tzinfo=dt.timezone.utc)
        content, entity = self._make_content_and_entity(scraped_at=scraped)
        parsed = RedditContent.from_data_entity(entity)
        result = utils.validate_scraped_at(parsed, entity)
        self.assertIsNotNone(result)
        self.assertFalse(result.is_valid)
        self.assertIn("before", result.reason)

    def test_scraped_at_in_future(self):
        """scraped_at in the future fails validation."""
        future = dt.datetime(2099, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
        content, entity = self._make_content_and_entity(scraped_at=future)
        parsed = RedditContent.from_data_entity(entity)
        result = utils.validate_scraped_at(parsed, entity)
        self.assertIsNotNone(result)
        self.assertFalse(result.is_valid)
        self.assertIn("future", result.reason)

    def test_scraped_at_none_before_deadline(self):
        """scraped_at=None is allowed before the deadline."""
        content, entity = self._make_content_and_entity(scraped_at=None)
        parsed = RedditContent.from_data_entity(entity)
        result = utils.validate_scraped_at(parsed, entity)
        # Before the deadline this should return None (skip)
        self.assertIsNone(result)

    def test_scraped_at_equal_to_created_at(self):
        """scraped_at equal to created_at (obfuscated) is valid."""
        ts = dt.datetime(2025, 1, 10, 12, 0, 0, tzinfo=dt.timezone.utc)
        content, entity = self._make_content_and_entity(scraped_at=ts)
        parsed = RedditContent.from_data_entity(entity)
        result = utils.validate_scraped_at(parsed, entity)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
