"""RedditMCScraper.validate(): an EMPTY actor dataset is retried once before it becomes
"URL not found or inaccessible."

Why: on 2026-09-14 the validator's Reddit actor returned an empty dataset for a post that
was (and still is) live and unchanged; the miner was scored "URL not found" and lost 30% of
its on-demand boost. Over 28 h on the largest validator, 3/3 "URL not found" verdicts were
posts that exist today. One retry separates an actor miss from a genuinely deleted post:
a deleted post is still empty on the second look and still fails.

No network, no wallet, no Apify token: the client is a scripted fake.
"""
import asyncio
import datetime as dt
import unittest
from unittest import mock

from scraping.reddit.model import RedditContent, RedditDataType
from scraping.reddit.reddit_mc_scraper import RedditMCScraper


def _entity():
    content = RedditContent(
        id="t3_abc123",
        url="https://www.reddit.com/r/test/comments/abc123/hello/",
        username="user1",
        communityName="r/test",
        body="Hello world",
        createdAt=dt.datetime(2026, 9, 12, 23, 48, tzinfo=dt.timezone.utc),
        dataType=RedditDataType.POST,
        title="Hello",
        scrapedAt=dt.datetime(2026, 9, 14, 2, 18, tzinfo=dt.timezone.utc),
    )
    return RedditContent.to_data_entity(content=content)


def _matching_item():
    """An actor item that matches _entity() field-for-field (so a retry that finds it PASSES)."""
    return {
        "id": "t3_abc123",
        "url": "https://www.reddit.com/r/test/comments/abc123/hello/",
        "username": "user1",
        "communityName": "r/test",
        "body": "Hello world",
        "createdAt": "2026-09-12T23:48:00Z",
        "dataType": "post",
        "title": "Hello",
        "parentId": None,
        "media": None,
        "isNsfw": False,
    }


class _FakeClient:
    """Scripted ApifyClientAsync: each actor call pops the next dataset from `script`."""

    def __init__(self, script):
        self.script = list(script)      # list of lists-of-items, one per actor call
        self.calls = 0

    def actor(self, actor_id):
        client = self

        class _Actor:
            async def call(self, run_input, timeout_secs):
                client.calls += 1
                return {"defaultDatasetId": f"ds{client.calls}"}
        return _Actor()

    def dataset(self, dataset_id):
        items = self.script.pop(0) if self.script else []

        class _Dataset:
            async def iterate_items(self):
                for it in items:
                    yield it
        return _Dataset()


class _FailingClient(_FakeClient):
    def actor(self, actor_id):
        client = self

        class _Actor:
            async def call(self, run_input, timeout_secs):
                client.calls += 1
                raise RuntimeError("actor exploded")
        return _Actor()


def _run(script, client_cls=_FakeClient):
    scraper = RedditMCScraper(apify_api_token="test-token")
    scraper.client = client_cls(script)
    with mock.patch("scraping.reddit.reddit_mc_scraper.asyncio.sleep", new=mock.AsyncMock()) as slp:
        results = asyncio.run(scraper.validate([_entity()]))
    return results[0], scraper.client.calls, slp


class TestEmptyResultRetry(unittest.TestCase):
    def test_items_on_first_call_means_one_actor_call_and_pass(self):
        res, calls, slp = _run([[_matching_item()]])
        self.assertTrue(res.is_valid, res.reason)
        self.assertEqual(calls, 1)
        slp.assert_not_awaited()

    def test_empty_then_items_is_retried_once_and_passes(self):
        res, calls, slp = _run([[], [_matching_item()]])
        self.assertTrue(res.is_valid, res.reason)
        self.assertEqual(calls, 2)
        slp.assert_awaited_once()

    def test_empty_twice_is_url_not_found_after_exactly_two_calls(self):
        res, calls, _ = _run([[], []])
        self.assertFalse(res.is_valid)
        self.assertEqual(res.reason, "URL not found or inaccessible.")
        self.assertEqual(calls, 2)

    def test_actor_exception_path_is_unchanged_single_call(self):
        res, calls, slp = _run([], client_cls=_FailingClient)
        self.assertFalse(res.is_valid)
        self.assertTrue(res.reason.startswith("Validation error:"), res.reason)
        self.assertEqual(calls, 1)
        slp.assert_not_awaited()

    def test_retry_delay_is_the_module_constant(self):
        _, _, slp = _run([[], []])
        from scraping.reddit import reddit_mc_scraper as m
        slp.assert_awaited_once_with(m.EMPTY_RESULT_RETRY_DELAY_S)
        self.assertGreater(m.EMPTY_RESULT_RETRY_DELAY_S, 0)


if __name__ == "__main__":
    unittest.main()
