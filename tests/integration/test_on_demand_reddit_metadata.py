import datetime as dt
import unittest

from scraping.reddit.model import RedditContent, RedditDataType
from vali_utils.on_demand.output_models import validate_metadata_completeness


def _build_reddit_entity(data_type, upvote_ratio=None, num_comments=None):
    """Build a real Reddit DataEntity for the given data_type.

    The submission-only fields (upvote_ratio, num_comments) default to None to
    mirror how comments are produced by the scrapers.
    """
    content = RedditContent(
        id="t1_abc123",
        url="https://www.reddit.com/r/test/comments/abc/def/ghi/",
        username="alice",
        communityName="r/test",
        body="hello world",
        createdAt=dt.datetime(2026, 6, 20, 12, 0, 0, tzinfo=dt.timezone.utc),
        dataType=data_type,
        is_nsfw=False,
        score=5,
        upvote_ratio=upvote_ratio,
        num_comments=num_comments,
    )
    return RedditContent.to_data_entity(content)


class TestRedditMetadataCompleteness(unittest.TestCase):
    """Regression tests for Reddit on-demand metadata completeness.

    upvote_ratio and num_comments are submission-only fields and are None by
    design for comments, so they must not be required for comments.
    """

    def test_comment_with_none_submission_fields_passes(self):
        """A comment with None upvote_ratio/num_comments must pass validation."""
        entity = _build_reddit_entity(RedditDataType.COMMENT)
        is_valid, missing_fields = validate_metadata_completeness(entity)
        self.assertTrue(
            is_valid,
            f"Comment should pass metadata completeness, missing: {missing_fields}",
        )
        self.assertEqual(missing_fields, [])

    def test_post_with_none_submission_fields_still_fails(self):
        """A post missing upvote_ratio/num_comments must still fail (fix is scoped)."""
        entity = _build_reddit_entity(RedditDataType.POST)
        is_valid, missing_fields = validate_metadata_completeness(entity)
        self.assertFalse(is_valid)
        self.assertIn("upvote_ratio", missing_fields)
        self.assertIn("num_comments", missing_fields)

    def test_post_with_submission_fields_passes(self):
        """A complete post must still pass validation."""
        entity = _build_reddit_entity(
            RedditDataType.POST, upvote_ratio=0.95, num_comments=12
        )
        is_valid, missing_fields = validate_metadata_completeness(entity)
        self.assertTrue(is_valid, f"Post should pass, missing: {missing_fields}")
        self.assertEqual(missing_fields, [])


if __name__ == "__main__":
    unittest.main()
