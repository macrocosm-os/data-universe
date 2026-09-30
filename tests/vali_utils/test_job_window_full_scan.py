"""Job-window check must cover every row, not just the 10-row sample.

A miner file whose tail runs past the job's post_end_datetime is real
content, so the scraper phase passes it, and a 10-row draw from one row
group misses a 7% tail most of the time. The whole-file datetime scan
makes the match rate reflect the tail regardless of the draw.
"""
import asyncio
import datetime as dt
import os
import random
import tempfile
import unittest

import pandas as pd

from vali_utils.s3_utils import DuckDBSampledValidator


def _reddit_frame(times):
    n = len(times)
    iso = [t.isoformat() for t in times]
    return pd.DataFrame({
        'datetime': iso,
        'label': ['r/redditgames'] * n,
        'id': [f't1_{i:06d}' for i in range(n)],
        'username': ['u'] * n,
        'communityName': ['r/RedditGames'] * n,
        'body': ['hello'] * n,
        'title': [None] * n,
        'createdAt': iso,
        'dataType': ['comment'] * n,
        'parentId': ['t3_x'] * n,
        'url': [f'https://www.reddit.com/r/RedditGames/comments/x/y/{i}/' for i in range(n)],
        'media': [None] * n,
        'is_nsfw': [False] * n,
        'score': [1] * n,
        'upvote_ratio': [None] * n,
        'num_comments': [None] * n,
        'scrapedAt': [iso[0]] * n,
    })


class JobWindowFullScanTest(unittest.TestCase):
    START = dt.datetime(2026, 5, 1, tzinfo=dt.timezone.utc)
    END = dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc)

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.validator = object.__new__(DuckDBSampledValidator)
        self.validator._rng = random.Random(1)
        self.validator._local_files = {}
        self.validator._cached_bytes = 0

    def _file(self, job_id, times):
        name = f'data_20260922_200217_{len(times)}_{"a" * 16}.parquet'
        key = f'data/hotkey=hk/job_id={job_id}/{name}'
        path = os.path.join(self.tmpdir.name, f'{job_id}.parquet')
        # Many row groups so the random draw rarely lands on the tail.
        _reddit_frame(times).to_parquet(path, row_group_size=10)
        self.validator._local_files[key] = path
        return {'key': key, 'size': os.path.getsize(path)}

    def _job(self):
        return {'params': {
            'platform': 'reddit', 'label': 'r/redditgames', 'keyword': None,
            'post_start_datetime': self.START.isoformat(),
            'post_end_datetime': self.END.isoformat(),
        }}

    def _run(self, files, jobs):
        return asyncio.run(self.validator._perform_job_content_matching(
            files, jobs, {f['key']: 'http://presigned' for f in files}))

    def test_tail_past_end_is_counted_regardless_of_sample(self):
        inside = [self.START + dt.timedelta(hours=i) for i in range(930)]
        tail = [self.END + dt.timedelta(minutes=i + 1) for i in range(70)]
        f = self._file('job1', inside + tail)  # tail is the last 7 row groups
        res = self._run([f], {'job1': self._job()})
        self.assertEqual(res['window_rows'], 1000)
        self.assertEqual(res['window_rows_outside'], 70)
        self.assertAlmostEqual(res['match_rate'], 93.0, places=5)
        self.assertTrue(any('70/1000 rows outside' in m for m in res['mismatch_samples']))

    def test_all_rows_in_window_is_full_match(self):
        inside = [self.START + dt.timedelta(hours=i) for i in range(200)]
        f = self._file('job2', inside)
        res = self._run([f], {'job2': self._job()})
        self.assertEqual(res['window_rows_outside'], 0)
        self.assertAlmostEqual(res['match_rate'], 100.0, places=5)

    def test_rows_before_start_count_too(self):
        early = [self.START - dt.timedelta(days=d + 1) for d in range(10)]
        inside = [self.START + dt.timedelta(hours=i) for i in range(90)]
        f = self._file('job3', early + inside)
        res = self._run([f], {'job3': self._job()})
        self.assertEqual(res['window_rows_outside'], 10)
        self.assertAlmostEqual(res['match_rate'], 90.0, places=5)

    def test_job_without_window_skips_scan(self):
        inside = [self.START + dt.timedelta(hours=i) for i in range(50)]
        f = self._file('job4', inside)
        job = {'params': {'platform': 'reddit', 'label': 'r/redditgames'}}
        res = self._run([f], {'job4': job})
        self.assertEqual(res['window_rows'], 0)
        self.assertAlmostEqual(res['match_rate'], 100.0, places=5)


if __name__ == '__main__':
    unittest.main()
