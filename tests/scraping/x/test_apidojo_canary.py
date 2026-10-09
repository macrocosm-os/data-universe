"""An ApiDojo actor outage must not be scored as a miner failure.

When the validation run raises on both attempts, the scraper runs the actor once
on a known-good post. If the canary run also raises, the actor is down and the
result is UNFETCHABLE (neutral on the on-demand path via
SCRAPER_ERROR_REASON_PREFIXES). If the canary run completes, whatever it
returned, the failure is specific to the miner's URI and still counts.
"""

import asyncio
import unittest
from unittest.mock import MagicMock

from common.data import DataEntity
from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
from vali_utils.on_demand.on_demand_validation import SCRAPER_ERROR_REASON_PREFIXES

MINER_URI = "https://x.com/someone/status/1234567890"
CANARY_OK = [{"id": "20", "url": "https://x.com/jack/status/20"}]


class FakeRunner:
    """Raises for the miner's URI; returns `canary` (or raises it) for the canary URL."""

    def __init__(self, canary, delay=0.0):
        self.canary = canary
        self.delay = delay
        self.canary_calls = 0

    async def run(self, run_config, run_input):
        url = run_input["startUrls"][0]
        if url == ApiDojoTwitterScraper.CANARY_TWEET_URL:
            self.canary_calls += 1
            await asyncio.sleep(self.delay)
            if isinstance(self.canary, Exception):
                raise self.canary
            return self.canary
        raise TimeoutError("actor run timed out")


def _entity():
    return MagicMock(spec=DataEntity, uri=MINER_URI, content_size_bytes=100)


def _reset():
    ApiDojoTwitterScraper._canary_checked_at = 0.0
    ApiDojoTwitterScraper._canary_healthy = True


class TestApiDojoCanary(unittest.TestCase):
    setUp = tearDown = staticmethod(_reset)

    def _validate(self, runner, n=1):
        scraper = ApiDojoTwitterScraper(runner=runner)
        return asyncio.run(scraper.validate([_entity() for _ in range(n)]))

    def _assert_unfetchable(self, result):
        self.assertFalse(result.is_valid)
        self.assertTrue(result.reason.startswith(SCRAPER_ERROR_REASON_PREFIXES))

    def _assert_charged(self, result):
        self.assertFalse(result.is_valid)
        self.assertTrue(result.reason.startswith("Failed to run Actor"))
        self.assertFalse(result.reason.startswith(SCRAPER_ERROR_REASON_PREFIXES))

    def test_canary_raises_is_unfetchable(self):
        self._assert_unfetchable(self._validate(FakeRunner(TimeoutError("down")))[0])

    def test_canary_ok_failure_stands(self):
        self._assert_charged(self._validate(FakeRunner(CANARY_OK))[0])

    def test_canary_completes_without_the_post_failure_stands(self):
        # A completed run proves the actor is up; an odd result must not excuse the miner.
        for dataset in ([], [{"noResults": True}], [{"id": "21"}], ["not a dict"]):
            with self.subTest(dataset=dataset):
                _reset()
                self._assert_charged(self._validate(FakeRunner(dataset))[0])

    def test_concurrent_failures_share_one_canary(self):
        runner = FakeRunner(TimeoutError("down"), delay=0.05)
        results = self._validate(runner, n=10)
        self.assertEqual(runner.canary_calls, 1)
        for r in results:
            self._assert_unfetchable(r)

    def test_canary_result_is_cached(self):
        runner = FakeRunner(TimeoutError("down"))
        self._validate(runner)
        self._validate(runner)
        self.assertEqual(runner.canary_calls, 1)

    def test_canary_rechecked_after_ttl(self):
        runner = FakeRunner(TimeoutError("down"))
        self._validate(runner)
        ApiDojoTwitterScraper._canary_checked_at -= ApiDojoTwitterScraper.CANARY_TTL_SECS + 1
        runner.canary = CANARY_OK
        self._assert_charged(self._validate(runner)[0])
        self.assertEqual(runner.canary_calls, 2)


if __name__ == "__main__":
    unittest.main()
