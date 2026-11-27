"""On-demand: a count above a possibly cached ApiDojo re-scrape is no verdict on young tweets."""

import datetime as dt
import unittest

from scraping.x.model import XContent
from scraping.x.utils import OD_STALE_REFERENCE_PREFIX, validate_tweet_content
from vali_utils.on_demand.on_demand_validation import SCRAPER_ERROR_REASON_PREFIXES


def _tweet(age: dt.timedelta, likes: int) -> XContent:
    ts = (dt.datetime.now(dt.timezone.utc) - age).replace(second=0, microsecond=0)
    return XContent(
        username="@joshuakingtl", text="t", url="https://x.com/joshuakingtl/status/2123691968278774173",
        timestamp=ts, tweet_hashtags=[], scraped_at=ts + dt.timedelta(hours=1),
        like_count=likes, retweet_count=2932, reply_count=266, view_count=125781,
    )


def _validate(age: dt.timedelta, submitted_likes: int, cached_likes: int, strict: bool):
    entity = XContent.to_data_entity(_tweet(age, submitted_likes))
    return validate_tweet_content(
        actual_tweet=_tweet(age, cached_likes), entity=entity, is_retweet=False, strict_engagement=strict
    )


class TestStaleReference(unittest.TestCase):
    def test_young_tweet_above_cached_count_is_no_verdict(self):
        """Live case: cached 5,579 likes, miner's newer scrape 5,930."""
        r = _validate(dt.timedelta(hours=6), 5930, 5579, strict=True)
        self.assertFalse(r.is_valid)
        self.assertTrue(r.reason.startswith(OD_STALE_REFERENCE_PREFIX))
        self.assertTrue(r.reason.startswith(SCRAPER_ERROR_REASON_PREFIXES))  # OD path: no verdict

    def test_old_tweet_still_fails(self):
        r = _validate(dt.timedelta(hours=48), 5930, 5579, strict=True)
        self.assertFalse(r.is_valid)
        self.assertIn("decreased too much", r.reason)
        self.assertFalse(r.reason.startswith(OD_STALE_REFERENCE_PREFIX))

    def test_non_od_paths_unchanged(self):
        r = _validate(dt.timedelta(hours=6), 5930, 5579, strict=False)
        self.assertIn("decreased too much", r.reason)
        self.assertFalse(r.reason.startswith(OD_STALE_REFERENCE_PREFIX))


if __name__ == "__main__":
    unittest.main()
