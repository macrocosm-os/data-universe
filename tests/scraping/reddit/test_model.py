import datetime as dt
import unittest

from common import constants
from scraping.reddit.model import RedditContent, RedditDataType


class TestModel(unittest.TestCase):
    def test_label_truncation(self):
        """Tests that RedditContents correctly truncate labels to 32 characters when converting to DataEntities"""
        timestamp = dt.datetime.now(tz=dt.timezone.utc)
        content = RedditContent(
            id="postId",
            url="https://reddit.com/123",
            username="user1",
            communityName="r/looooooooooooooooooooooooongSubreddit",
            body="Hello world",
            createdAt=timestamp,
            dataType=RedditDataType.POST,
            title="Title text",
        )
        entity = RedditContent.to_data_entity(
            content=content, obfuscate_content_date=False
        )

        self.assertEqual(len(entity.label.value), constants.MAX_LABEL_LENGTH)
        self.assertEqual(entity.label.value, "r/looooooooooooooooooooooooongsu")

    def test_to_data_entity_non_obfuscated(self):
        timestamp = dt.datetime(
            year=2024,
            month=1,
            day=2,
            hour=3,
            minute=4,
            second=5,
            microsecond=6,
            tzinfo=dt.timezone.utc,
        )
        content = RedditContent(
            id="postId",
            url="https://reddit.com/123",
            username="user1",
            communityName="r/bitcoin",
            body="Hello world",
            createdAt=timestamp,
            dataType=RedditDataType.POST,
            title="Title text",
        )

        # Convert to entity and back to check granularity of the content timestamp.
        entity = RedditContent.to_data_entity(
            content=content, obfuscate_content_date=False
        )
        content_roundtrip = RedditContent.from_data_entity(entity)

        # Both the entity datetime and the entity content datetime should have full granularity.
        self.assertEqual(entity.datetime, timestamp)
        self.assertEqual(content_roundtrip.created_at, timestamp)

    def test_to_data_entity_obfuscated(self):
        timestamp = dt.datetime(
            year=2024,
            month=3,
            day=1,
            hour=1,
            minute=1,
            second=1,
            microsecond=1,
            tzinfo=dt.timezone.utc,
        )
        content = RedditContent(
            id="postId",
            url="https://reddit.com/123",
            username="user1",
            communityName="r/bitcoin",
            body="Hello world",
            createdAt=timestamp,
            dataType=RedditDataType.POST,
            title="Title text",
        )

        # Convert to entity and back to check granularity of the content timestamp.
        entity = RedditContent.to_data_entity(
            content=content, obfuscate_content_date=True
        )
        content_roundtrip = RedditContent.from_data_entity(entity)

        # The entity datetime should have full granularity but the roundtripped content should not.
        self.assertEqual(entity.datetime, timestamp)
        self.assertEqual(
            content_roundtrip.created_at,
            dt.datetime(
                year=2024,
                month=3,
                day=1,
                hour=1,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=dt.timezone.utc,
            ),
        )


if __name__ == "__main__":
    unittest.main()
