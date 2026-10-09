"""RedditMCScraper: empty actor result is retried once; archive-unavailable is neutral."""
import asyncio
import datetime as dt
import unittest
from unittest.mock import AsyncMock, patch

from scraping.reddit.model import RedditContent, RedditDataType
from scraping.reddit.reddit_mc_scraper import RedditMCScraper


def _content():
    created = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).replace(second=0, microsecond=0)
    return RedditContent(
        id="t3_abc123", url="https://www.reddit.com/r/test/comments/abc123/x/", username="u1",
        communityName="r/test", body="body text here", createdAt=created, dataType=RedditDataType.POST,
        title="t", parentId=None, score=1, num_comments=0, scrapedAt=created,
    )


def _actor_item(c: RedditContent):
    return {"id": c.id, "url": c.url, "username": c.username, "communityName": c.community, "body": c.body,
            "createdAt": c.created_at.isoformat(), "dataType": "post", "title": c.title, "parentId": None,
            "score": 1, "num_comments": 0, "media": None, "isNsfw": False}


class RetryTests(unittest.TestCase):
    def setUp(self):
        self.scraper = RedditMCScraper(apify_api_token="x")
        self.scraper.EMPTY_RETRY_DELAY_S = 0
        self.content = _content()
        self.entity = RedditContent.to_data_entity(self.content)

    def _run(self, lookups):
        with patch.object(RedditMCScraper, "_lookup", new=AsyncMock(side_effect=lookups)) as m:
            res = asyncio.run(self.scraper.validate([self.entity]))
            return res[0], m.call_count

    def test_empty_then_found_passes(self):
        r, calls = self._run([None, _actor_item(self.content)])
        self.assertTrue(r.is_valid, r.reason); self.assertEqual(calls, 2)

    def test_empty_twice_is_not_found(self):
        r, calls = self._run([None, None])
        self.assertFalse(r.is_valid); self.assertEqual(r.reason, "URL not found or inaccessible."); self.assertEqual(calls, 2)

    def test_unavailable_twice_is_neutral(self):
        r, calls = self._run([{"error": "archive_unavailable"}, {"error": "archive_unavailable"}])
        self.assertFalse(r.is_valid); self.assertTrue(r.reason.startswith("UNFETCHABLE")); self.assertEqual(calls, 2)

    def test_found_first_time_does_not_retry(self):
        r, calls = self._run([_actor_item(self.content)])
        self.assertTrue(r.is_valid, r.reason); self.assertEqual(calls, 1)


if __name__ == "__main__":
    unittest.main()
