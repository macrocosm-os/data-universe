import datetime as dt
import unittest

from scraping.reddit.model import RedditContent, RedditDataType


class TestModel(unittest.TestCase):
    def test_label_truncation(self):
        """Tests that RedditContents correctly truncate labels to 32 characters when converting to DataEntities"""
        timestamp = dt.datetime.now()
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
        entity = RedditContent.to_data_entity(content)

        self.assertEqual(len(entity.label.value), 32)


if __name__ == "__main__":
    unittest.main()
