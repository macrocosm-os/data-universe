"""On-demand engagement strictness: stale snapshots and under-reported counts."""

import datetime as dt
import unittest

from scraping.x.model import XContent
from scraping.x.utils import check_engagement_underreport

UTC = dt.timezone.utc
TWEET_TIME = dt.datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
JOB_TIME = dt.datetime(2026, 11, 19, 12, 0, tzinfo=UTC)
REAL = dict(likes=344, retweets=40, replies=30, views=47567)


def tweet(scraped_at=None, likes=None, retweets=None, replies=None, views=None):
    return XContent(
        username="@FT", text="t", url="https://x.com/FT/status/1", timestamp=TWEET_TIME,
        tweet_hashtags=[], scraped_at=scraped_at, like_count=likes,
        retweet_count=retweets, reply_count=replies, view_count=views,
    )


def check(sub, job=JOB_TIME):
    return check_engagement_underreport(sub, tweet(**REAL), job_created_at=job)


class TestEngagementUnderreport(unittest.TestCase):
    def test_capped_counts_fail_and_honest_counts_pass(self):
        scraped = JOB_TIME - dt.timedelta(minutes=5)
        self.assertIsNotNone(check(tweet(scraped, likes=20, retweets=20, replies=20, views=20)))
        self.assertIsNone(check(tweet(scraped, likes=330, retweets=39, replies=29, views=46000)))

    def test_tiers_by_age_at_scrape(self):
        for hours, ok, bad in ((30, 271, 260), (8, 170, 160), (2, 65, 60)):
            scraped = TWEET_TIME + dt.timedelta(hours=hours)
            job = scraped + dt.timedelta(minutes=5)
            self.assertIsNone(check(tweet(scraped, likes=ok), job), hours)
            self.assertIsNotNone(check(tweet(scraped, likes=bad), job), hours)

    def test_under_1h_not_checked(self):
        scraped = TWEET_TIME + dt.timedelta(minutes=30)
        self.assertIsNone(check(tweet(scraped, likes=1, views=20), scraped + dt.timedelta(minutes=5)))

    def test_stale_snapshot_fails(self):
        self.assertIn("stale", check(tweet(JOB_TIME - dt.timedelta(hours=25), **REAL)))

    def test_backdated_scraped_at_does_not_skip_the_check(self):
        job = TWEET_TIME + dt.timedelta(hours=4)
        self.assertIsNotNone(check(tweet(TWEET_TIME + dt.timedelta(minutes=30), likes=20), job))

    def test_missing_counts_skipped(self):
        self.assertIsNone(check(tweet(TWEET_TIME + dt.timedelta(hours=30), likes=None, views=None)))


if __name__ == "__main__":
    unittest.main()
