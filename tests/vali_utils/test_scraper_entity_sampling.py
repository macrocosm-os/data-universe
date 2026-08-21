"""Tests for the entity draw in DuckDBSampledValidator._perform_scraper_validation.

The draw must not be steerable by the row counts a miner declares. Before the
uniform-file / fixed-quota rule, the phase ordered files by claimed rows and gave
each file a row-proportional budget (up to 20 rows), so a miner could shrink its X
jobs until Reddit held nearly all claimed rows: two huge Reddit files then filled
the whole 40-row pool and all 20 scraper-validated entities were Reddit. The X leg
went unchecked, and the per-platform bar was vacuous because no X entity ever
entered the pool.

These tests run against real parquet files on disk (registered in the validator's
local-file cache, exactly as the download phase does) with only the scraper call
itself mocked.
"""

import asyncio
import datetime as dt
import os
import random
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from scraping.scraper import ValidationResult
from vali_utils.s3_utils import DuckDBSampledValidator


def _reddit_frame(n: int, tag: str) -> pd.DataFrame:
    now = dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc)
    return pd.DataFrame({
        'datetime': [now] * n,
        'label': ['r/test'] * n,
        'id': [f't3_{tag}{i}' for i in range(n)],
        'username': [f'user{i}' for i in range(n)],
        'communityName': ['r/test'] * n,
        'body': [f'body {tag} {i}' for i in range(n)],
        'title': [f'title {tag} {i}' for i in range(n)],
        'createdAt': [now] * n,
        'dataType': ['post'] * n,
        'parentId': [None] * n,
        'url': [f'https://reddit.com/r/test/comments/{tag}{i}' for i in range(n)],
        'media': [None] * n,
        'is_nsfw': [False] * n,
        'score': [1] * n,
        'upvote_ratio': [0.9] * n,
        'num_comments': [0] * n,
        'scrapedAt': [now] * n,
    })


def _x_frame(n: int, tag: str) -> pd.DataFrame:
    now = dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc)
    return pd.DataFrame({
        'datetime': [now] * n,
        'label': ['#test'] * n,
        'username': [f'@user{i}' for i in range(n)],
        'text': [f'tweet {tag} {i}' for i in range(n)],
        'tweet_hashtags': [['#test'] for _ in range(n)],
        'timestamp': [now] * n,
        'url': [f'https://x.com/user{i}/status/19{tag}{i:04d}' for i in range(n)],
        'media': [None] * n,
        'user_id': [f'{i}' for i in range(n)],
        'user_display_name': [f'User {i}' for i in range(n)],
        'user_verified': [False] * n,
        'tweet_id': [f'19{tag}{i:04d}' for i in range(n)],
        'is_reply': [False] * n,
        'is_quote': [False] * n,
        'conversation_id': [f'19{tag}{i:04d}' for i in range(n)],
        'in_reply_to_user_id': [None] * n,
        'language': ['en'] * n,
        'in_reply_to_username': [None] * n,
        'quoted_tweet_id': [None] * n,
        'like_count': [1] * n,
        'retweet_count': [0] * n,
        'reply_count': [0] * n,
        'quote_count': [0] * n,
        'view_count': [10] * n,
        'bookmark_count': [0] * n,
        'user_blue_verified': [False] * n,
        'user_description': ['bio'] * n,
        'user_location': ['earth'] * n,
        'profile_image_url': ['https://x.com/img.png'] * n,
        'cover_picture_url': ['https://x.com/cover.png'] * n,
        'user_followers_count': [5] * n,
        'user_following_count': [5] * n,
        'scraped_at': [now] * n,
    })


class ScraperSamplingFixture(unittest.TestCase):
    """Builds a miner layout on disk: a few huge Reddit files plus small X files."""

    # 3M claimed Reddit rows vs 1k claimed X rows — the skew a manipulating
    # miner would arrange. The draw must still reach the X files.
    REDDIT_CLAIMED_ROWS = 3_000_000
    X_CLAIMED_ROWS = 1_000

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        self.validator = object.__new__(DuckDBSampledValidator)
        self.validator._rng = random.Random(1234)
        self.validator._local_files = {}
        self.validator._cached_bytes = 0

        self.expected_jobs = {}
        self.files = []

        # 3 big Reddit files (the ballast) + 6 small X files.
        for i in range(3):
            self._add_file('reddit', f'redditjob{i}', self.REDDIT_CLAIMED_ROWS,
                           _reddit_frame(60, f'rd{i}'))
        for i in range(6):
            self._add_file('x', f'xjob{i}', self.X_CLAIMED_ROWS,
                           _x_frame(60, f'{i}'))

    def _add_file(self, platform: str, job_id: str, claimed_rows: int, df: pd.DataFrame):
        name = f'data_20260801_120000_{claimed_rows}_{"a" * 16}.parquet'
        key = f'data/hotkey=hk/job_id={job_id}/{name}'
        path = os.path.join(self.tmpdir.name, f'{job_id}_{name}')
        # Small row groups so read_random_row_group has several to choose from.
        df.to_parquet(path, row_group_size=20)
        self.files.append({'key': key, 'size': os.path.getsize(path)})
        self.validator._local_files[key] = path
        self.expected_jobs[job_id] = {'params': {'platform': platform}}

    def _run(self, num_entities: int = 20):
        """Run the phase with the scraper stubbed to pass everything."""
        async def _all_valid(entities, platform):
            return [ValidationResult(is_valid=True, reason='ok',
                                     content_size_bytes_validated=10)
                    for _ in entities]

        with patch.object(DuckDBSampledValidator, '_validate_with_scraper',
                          new=AsyncMock(side_effect=_all_valid)):
            return asyncio.run(self.validator._perform_scraper_validation(
                'hk', list(self.files), self.expected_jobs,
                # Presigned URLs are unused: every key is in the local cache.
                {f['key']: 'https://unused.invalid' for f in self.files},
                num_entities=num_entities,
            ))


class TestPlatformCoverage(ScraperSamplingFixture):
    def test_x_leg_is_checked_despite_reddit_row_dominance(self):
        """Reddit claims 99.98% of the rows; X must still be scraper-validated."""
        result = self._run()
        stats = result['platform_stats']
        self.assertIn('x', stats, f"X was never scraper-checked: {stats}")
        self.assertGreaterEqual(
            stats['x']['validated'],
            DuckDBSampledValidator.SCRAPER_PLATFORM_MIN_ENTITIES,
            f"X below the per-platform floor, so the bar is vacuous: {stats}",
        )
        self.assertIn('reddit', stats)
        self.assertEqual(result['entities_validated'], 20)

    def test_coverage_holds_across_seeds(self):
        """Not a lucky seed: every RNG seed must reach both platforms."""
        for seed in range(12):
            with self.subTest(seed=seed):
                self.validator._rng = random.Random(seed)
                stats = self._run()['platform_stats']
                self.assertGreaterEqual(
                    stats.get('x', {}).get('validated', 0),
                    DuckDBSampledValidator.SCRAPER_PLATFORM_MIN_ENTITIES,
                )
                self.assertGreaterEqual(stats.get('reddit', {}).get('validated', 0), 1)

    def test_no_single_file_owns_the_pool(self):
        """The entity pool must be spread over files, not filled by one or two.

        Each file yields at most SCRAPER_ROWS_PER_FILE rows, so filling a pool of
        2 x num_entities takes at least 8 distinct files.
        """
        seen_files = []
        real_reader = DuckDBSampledValidator._perform_scraper_validation

        import vali_utils.s3_utils as s3_utils
        original = s3_utils.read_random_row_group

        # NB: never assert inside the spy — _perform_scraper_validation wraps the
        # read in a bare `except: continue`, which swallows AssertionError and
        # turns a real failure into a silently skipped file (a test that passes
        # for the wrong reason). Collect here, assert after the run.
        def _spy(source, size, **kwargs):
            seen_files.append(source)
            return original(source, size, **kwargs)

        with patch.object(s3_utils, 'read_random_row_group', side_effect=_spy):
            self._run()

        self.assertGreaterEqual(len(set(seen_files)), 8)
        self.assertIs(real_reader, DuckDBSampledValidator._perform_scraper_validation)


class TestPerFileQuota(ScraperSamplingFixture):
    def _quotas_seen(self):
        """Every max_rows the phase asks for, one entry per file read."""
        import vali_utils.s3_utils as s3_utils
        original = s3_utils.read_random_row_group
        quotas = []

        def _spy(source, size, **kwargs):
            quotas.append(kwargs.get('max_rows'))
            return original(source, size, **kwargs)

        with patch.object(s3_utils, 'read_random_row_group', side_effect=_spy):
            self._run()
        return quotas

    def test_no_file_is_starved_by_another_files_claim(self):
        """The exploitable property was a per-file budget PROPORTIONAL to
        claimed rows: a tiny-claim file got ~2 rows while a mega file took 20,
        so mega files filled the pool. Every file must now get at least the base
        quota no matter what any other file claims."""
        quotas = self._quotas_seen()
        self.assertGreater(len(quotas), 1)
        self.assertTrue(
            all(q >= DuckDBSampledValidator.SCRAPER_ROWS_PER_FILE for q in quotas),
            f"a file was starved below the base quota: {quotas}",
        )

    def test_surplus_is_bounded_and_only_for_top_claim_files(self):
        """Claimed rows may only ADD depth, to a bounded number of files at a
        bounded size — never scale a budget continuously the way the exploited
        version did."""
        quotas = self._quotas_seen()
        base = DuckDBSampledValidator.SCRAPER_ROWS_PER_FILE
        elevated = [q for q in quotas if q > base]
        self.assertTrue(
            all(q == DuckDBSampledValidator.SCRAPER_TOP_FILE_ROWS for q in elevated),
            f"surplus quota is not a fixed size: {quotas}",
        )
        self.assertLessEqual(
            len(elevated), DuckDBSampledValidator.SCRAPER_TOP_FILE_COUNT,
            f"more files elevated than SCRAPER_TOP_FILE_COUNT: {quotas}",
        )

    def test_small_miner_still_gets_a_full_sample(self):
        """A miner with too few files to fill the pool at 5 rows each must not
        end up with a SMALLER scraper sample than the row-proportional rule
        used to give it. The quota rises; it stays uniform across files."""
        self.files = self.files[:3]  # the 3 Reddit files — 3 x 5 < 40-row pool
        quotas = self._quotas_seen()
        self.assertTrue(all(q >= DuckDBSampledValidator.SCRAPER_ROWS_PER_FILE
                            for q in quotas), quotas)
        # 3 files x quota must still cover the 2 x num_entities pool.
        self.assertGreaterEqual(sum(quotas), 40)
        self.assertEqual(self._run()['entities_validated'], 20)


class TestBudgetInvariants(ScraperSamplingFixture):
    def test_never_validates_more_than_requested(self):
        result = self._run(num_entities=7)
        self.assertLessEqual(result['entities_validated'], 7)

    def test_success_rate_matches_tallies(self):
        result = self._run()
        expected = result['entities_passed'] / result['entities_validated'] * 100
        self.assertAlmostEqual(result['success_rate'], expected, places=6)

    def test_seeded_draw_is_reproducible(self):
        """Committed sampling: the same seed must replay the same entity draw."""
        self.validator._rng = random.Random(99)
        first = self._run()
        self.validator._rng = random.Random(99)
        second = self._run()
        self.assertEqual(first['sample_results'], second['sample_results'])


class TestSelectionPlatformFloor(unittest.TestCase):
    """The SELECTION stage (validate_miner_s3_data) must put files from every
    active platform into the sample, even when a miner shrinks one platform's
    claimed rows to near-zero. Without the per-platform file floor, the
    row-weighted draw and the top-5 force-include pick only the high-volume
    platform, so the minority platform's files never reach the scraper phase
    at all — the observed '20/20 entities Reddit, no X' hole.

    The read phases are mocked; the test asserts on the files handed to
    _perform_scraper_validation.
    """

    N_REDDIT_JOBS = 30
    N_X_JOBS = 2
    REDDIT_CLAIMED = 100_000
    X_CLAIMED = 50  # miner shrinks X to almost nothing

    def _files(self):
        files = []
        expected_jobs = {}
        for i in range(self.N_REDDIT_JOBS):
            jid = f'redditjob{i}'
            name = f'data_20260801_120000_{self.REDDIT_CLAIMED}_{"a" * 16}.parquet'
            files.append({'key': f'data/hotkey=hk/job_id={jid}/{name}',
                          'size': 8_000_000, 'last_modified': f'2026-08-01T00:00:{i:02d}Z'})
            expected_jobs[jid] = {'params': {'platform': 'reddit'}}
        for i in range(self.N_X_JOBS):
            jid = f'xjob{i}'
            name = f'data_20260801_120000_{self.X_CLAIMED}_{"b" * 16}.parquet'
            files.append({'key': f'data/hotkey=hk/job_id={jid}/{name}',
                          'size': 20_000, 'last_modified': f'2026-08-01T00:00:{i:02d}Z'})
            expected_jobs[jid] = {'params': {'platform': 'x'}}
        return files, expected_jobs

    def _validator(self, seed):
        v = DuckDBSampledValidator.__new__(DuckDBSampledValidator)
        v.wallet = MagicMock()
        v.sample_percent = 10.0
        v._seed_material = seed  # deterministic committed RNG per seed
        v._local_files = {}
        v._cached_bytes = 0
        return v

    def _sampled_files_for(self, seed):
        v = self._validator(seed)
        files, expected_jobs = self._files()

        captured = {}

        async def _capture_scraper(hotkey, sampled_files, *a, **k):
            captured['files'] = sampled_files
            return {'entities_validated': 20, 'entities_passed': 20,
                    'success_rate': 100.0, 'sample_results': [],
                    'platform_stats': {'x': {'validated': 5, 'passed': 5},
                                       'reddit': {'validated': 15, 'passed': 15}}}

        v.s3_reader = MagicMock()
        v.s3_reader.list_all_files_with_metadata = AsyncMock(return_value=files)
        v._get_presigned_urls_batch = AsyncMock(
            return_value={f['key']: 'https://unused.invalid' for f in files})
        v._sampled_duckdb_validation = AsyncMock(return_value={
            'duplicate_rate_within_job': 0.0, 'empty_rate': 0.0,
            'compression_failures': 0, 'row_count_mismatches': 0,
            'decode_ratio': 1.0,
        })
        v._perform_job_content_matching = AsyncMock(return_value={
            'total_checked': 20, 'total_matched': 20, 'match_rate': 100.0,
            'mismatch_samples': [],
        })
        v._perform_scraper_validation = _capture_scraper

        asyncio.run(v.validate_miner_s3_data('hk', expected_jobs))
        return captured['files'], expected_jobs

    def test_x_files_survive_selection_despite_tiny_claimed_rows(self):
        for seed in [f'0x{i:064x}' for i in range(15)]:
            with self.subTest(seed=seed):
                sampled, expected_jobs = self._sampled_files_for(seed)
                x_files = [f for f in sampled
                           if DuckDBSampledValidator._file_platform(f['key'], expected_jobs) == 'x']
                self.assertGreaterEqual(
                    len(x_files), DuckDBSampledValidator.SCRAPER_PLATFORM_MIN_FILES,
                    f"X shut out of the sample (seed {seed}): "
                    f"{[f['key'] for f in sampled]}",
                )


if __name__ == '__main__':
    unittest.main()
