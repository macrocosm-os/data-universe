"""Fresh-post validation against an archive snapshot.

The validator re-fetches Reddit through an archive that captures a post at
creation (score 0/1, no comments, pre-edit body) and re-crawls it ~36 h later.
A miner's honest later copy must not fail against that snapshot, while
fabricated content still must.
"""
import datetime as dt
import unittest

from common.data import DataEntity, DataLabel, DataSource
from scraping.reddit import utils
from scraping.reddit.model import RedditContent, RedditDataType


def _post(body="hello world, this is the body of the post", score=None, num_comments=None, age=dt.timedelta(hours=2)):
    created = dt.datetime.now(dt.timezone.utc) - age
    return RedditContent(
        id="t3_abc123", url="https://www.reddit.com/r/test/comments/abc123/x/",
        username="u1", communityName="r/test", body=body,
        createdAt=created.replace(second=0, microsecond=0), dataType=RedditDataType.POST,
        title="t", parentId=None, score=score, num_comments=num_comments,
        scrapedAt=(created + dt.timedelta(minutes=1)).replace(second=0, microsecond=0),
    )


def _entity(content: RedditContent) -> DataEntity:
    return RedditContent.to_data_entity(content)


class SnapshotAwareScoreAndComments(unittest.TestCase):
    def test_live_engagement_passes_against_creation_snapshot(self):
        submitted = _post(score=70, num_comments=12, age=dt.timedelta(hours=2))
        archive = _post(score=1, num_comments=0, age=dt.timedelta(hours=2))
        self.assertTrue(utils.validate_score_content(submitted, archive, _entity(submitted)).is_valid)
        self.assertTrue(utils.validate_comment_count(submitted, archive, _entity(submitted)).is_valid)

    def test_outlier_still_fails_when_archive_has_engagement(self):
        submitted = _post(score=5000, num_comments=900, age=dt.timedelta(hours=2))
        archive = _post(score=20, num_comments=3, age=dt.timedelta(hours=2))
        self.assertFalse(utils.validate_score_content(submitted, archive, _entity(submitted)).is_valid)
        self.assertFalse(utils.validate_comment_count(submitted, archive, _entity(submitted)).is_valid)

    def test_old_post_with_dead_archive_copy_is_not_exempt(self):
        # After the archive's second crawl a 1/0 copy means the post really is dead.
        submitted = _post(score=5000, num_comments=900, age=dt.timedelta(days=5))
        archive = _post(score=1, num_comments=0, age=dt.timedelta(days=5))
        self.assertFalse(utils.validate_score_content(submitted, archive, _entity(submitted)).is_valid)
        self.assertFalse(utils.validate_comment_count(submitted, archive, _entity(submitted)).is_valid)


class EditTolerantBody(unittest.TestCase):
    def test_exact_body_passes(self):
        c = _post(); self.assertTrue(utils.validate_reddit_content(c, _entity(c)).is_valid)

    def test_edited_continuation_passes(self):
        archive = _post(body="I've been making games with the two. Claude is very good at the animation side of things.")
        served = _post(body="I've been making games with the two. Claude is very good at the animation side of things. Edit: typo.")
        self.assertTrue(utils.validate_reddit_content(archive, _entity(served)).is_valid)

    def test_small_in_place_edit_passes(self):
        archive = _post(body="The quick brown fox jumps over the lazy dog and keeps running through the field.")
        served = _post(body="The quick brown fox jumped over the lazy dog and keeps running through the field.")
        self.assertTrue(utils.validate_reddit_content(archive, _entity(served)).is_valid)

    def test_different_body_fails(self):
        archive = _post(body="The quick brown fox jumps over the lazy dog and keeps running through the field.")
        served = _post(body="Buy cheap followers now, best prices, DM for details and bulk discounts today.")
        r = utils.validate_reddit_content(archive, _entity(served))
        self.assertFalse(r.is_valid); self.assertEqual(r.reason, "Reddit bodies do not match")


if __name__ == "__main__":
    unittest.main()
