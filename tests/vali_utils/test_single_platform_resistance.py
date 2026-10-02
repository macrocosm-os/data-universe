"""End-to-end resistance to the 'force all scraper validation onto one platform'
manipulation.

A miner shrinks the CLAIMED row counts of one platform's jobs (the number it
writes into its own filenames) so the row-weighted file draw and the top-5
force-include pick only the high-volume platform. Before the per-platform file
floor (selection) + round-robin read (scraper phase), that left the minority
platform with zero scraper-checked entities — its per-platform bar vacuous, all
20/20 landing on the majority platform (observed live: all Reddit, no X).

Unlike the focused tests in test_scraper_entity_sampling.py, these drive the
WHOLE sampling path through validate_miner_s3_data on real parquet files: the
selection stage AND the real scraper read both run, only the external scraper
call (_validate_with_scraper) and the unrelated DuckDB/job-match phases are
mocked. The spy on _validate_with_scraper records how many entities of each
platform actually reached a scraper — the exact quantity the manipulation tried
to drive to zero.

Claim skew note: the validator rejects files whose size/claimed-rows ratio is
below MIN_BYTES_PER_ROW (50), so a test cannot pair a 3M-row CLAIM with a tiny
file the way production pairs it with a GB file. The layouts here keep the ratio
valid and still make the majority platform's TOTAL claimed rows dwarf the
minority's — which is all the row-weighted draw and the top-5 force-include key
off, so the manipulation is faithfully reproduced.

Only x and reddit are used: _create_data_entity builds entities for those two
platforms (the subnet's scraper-validated sources); other labels would fail
entity construction by design.
"""

import asyncio
import datetime as dt
import os
import random
import tempfile
import unittest
from collections import Counter
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from scraping.scraper import ValidationResult
from vali_utils.s3_utils import DuckDBSampledValidator

HEX = "0123456789abcdef"
PHYSICAL_ROWS = 120  # rows actually written per file (keeps files > MIN_FILE_SIZE)


def _reddit_frame(n: int, tag: str) -> pd.DataFrame:
    now = dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc)
    return pd.DataFrame({
        'datetime': [now] * n, 'label': ['r/test'] * n,
        'id': [f't3_{tag}{i}' for i in range(n)],
        'username': [f'user{i}' for i in range(n)], 'communityName': ['r/test'] * n,
        'body': [f'body {tag} {i} lorem ipsum dolor sit amet' for i in range(n)],
        'title': [f'title {tag} {i} some longer heading text' for i in range(n)],
        'createdAt': [now] * n, 'dataType': ['post'] * n, 'parentId': [None] * n,
        'url': [f'https://reddit.com/r/test/comments/{tag}{i}' for i in range(n)],
        'media': [None] * n, 'is_nsfw': [False] * n, 'score': [1] * n,
        'upvote_ratio': [0.9] * n, 'num_comments': [0] * n, 'scrapedAt': [now] * n,
    })


def _x_frame(n: int, tag: str) -> pd.DataFrame:
    now = dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc)
    return pd.DataFrame({
        'datetime': [now] * n, 'label': ['#test'] * n,
        'username': [f'@user{i}' for i in range(n)],
        'text': [f'tweet {tag} {i} lorem ipsum dolor sit amet consectetur' for i in range(n)],
        'tweet_hashtags': [['#test'] for _ in range(n)], 'timestamp': [now] * n,
        'url': [f'https://x.com/user{i}/status/19{tag}{i:04d}' for i in range(n)],
        'media': [None] * n, 'user_id': [f'{i}' for i in range(n)],
        'user_display_name': [f'User {i}' for i in range(n)], 'user_verified': [False] * n,
        'tweet_id': [f'19{tag}{i:04d}' for i in range(n)],
        'is_reply': [False] * n, 'is_quote': [False] * n,
        'conversation_id': [f'19{tag}{i:04d}' for i in range(n)],
        'in_reply_to_user_id': [None] * n, 'language': ['en'] * n,
        'in_reply_to_username': [None] * n, 'quoted_tweet_id': [None] * n,
        'like_count': [1] * n, 'retweet_count': [0] * n, 'reply_count': [0] * n,
        'quote_count': [0] * n, 'view_count': [10] * n, 'bookmark_count': [0] * n,
        'user_blue_verified': [False] * n, 'user_description': ['bio'] * n,
        'user_location': ['earth'] * n, 'profile_image_url': ['https://x.com/i.png'] * n,
        'cover_picture_url': ['https://x.com/c.png'] * n,
        'user_followers_count': [5] * n, 'user_following_count': [5] * n,
        'scraped_at': [now] * n,
    })


class ResistanceHarness(unittest.TestCase):
    """Builds real parquet files ONCE per layout and drives
    validate_miner_s3_data with only the scraper call + DuckDB/job-match mocked."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _bundle(self, layout):
        """layout: list of (platform, claimed_rows, count). Builds the files once.

        physical rows are fixed (PHYSICAL_ROWS) so every file clears
        MIN_FILE_SIZE and keeps size/claimed >= MIN_BYTES_PER_ROW; the CLAIM in
        the filename is what the row-weighted draw and top-5 key off.
        """
        files_meta, expected_jobs, local_files = [], {}, {}
        jid_n = 0
        for platform, claimed, count in layout:
            for _ in range(count):
                jid_n += 1
                jid = f'{platform}job{jid_n}'
                hexs = ''.join(random.Random(jid_n).choice(HEX) for _ in range(16))
                name = f'data_20260801_120000_{claimed}_{hexs}.parquet'
                key = f'data/hotkey=hk/job_id={jid}/{name}'
                path = os.path.join(self.tmp.name, f'{jid}.parquet')
                frame = (_x_frame if platform == 'x' else _reddit_frame)(PHYSICAL_ROWS, f'{jid_n}')
                frame.to_parquet(path, row_group_size=20)
                files_meta.append({'key': key, 'size': os.path.getsize(path),
                                   'last_modified': f'2026-08-01T00:00:{jid_n % 60:02d}Z'})
                local_files[key] = path
                expected_jobs[jid] = {'params': {'platform': platform}}
        return files_meta, expected_jobs, local_files

    def _run(self, bundle, seed):
        files_meta, expected_jobs, local_files = bundle

        v = DuckDBSampledValidator.__new__(DuckDBSampledValidator)
        v.wallet = MagicMock()
        v.sample_percent = 10.0
        v._seed_material = seed
        v._local_files = dict(local_files)
        v._cached_bytes = 0
        v.scraper_provider = MagicMock()
        v.s3_reader = MagicMock()
        v.s3_reader.list_all_files_with_metadata = AsyncMock(return_value=files_meta)
        v._get_presigned_urls_batch = AsyncMock(
            return_value={f['key']: 'https://unused.invalid' for f in files_meta})
        v._sampled_duckdb_validation = AsyncMock(return_value={
            'duplicate_rate_within_job': 0.0, 'empty_rate': 0.0,
            'compression_failures': 0, 'row_count_mismatches': 0, 'decode_ratio': 1.0,
        })
        v._perform_job_content_matching = AsyncMock(return_value={
            'total_checked': 20, 'total_matched': 20, 'match_rate': 100.0,
            'mismatch_samples': [],
        })

        scraped = Counter()

        async def _spy_scraper(entities, platform):
            scraped[platform] += len(entities)
            return [ValidationResult(is_valid=True, reason='ok',
                                     content_size_bytes_validated=10) for _ in entities]

        with patch.object(v, '_validate_with_scraper', side_effect=_spy_scraper):
            result = asyncio.run(v.validate_miner_s3_data('hk', expected_jobs))
        return scraped, result


    def _run_capture_files(self, bundle, seed):
        """Run the real selection stage and capture the file list handed to
        _perform_scraper_validation (i.e. after suspicion picks are stripped)."""
        files_meta, expected_jobs, local_files = bundle
        v = DuckDBSampledValidator.__new__(DuckDBSampledValidator)
        v.wallet = MagicMock()
        v.sample_percent = 10.0
        v._seed_material = seed
        v._local_files = dict(local_files)
        v._cached_bytes = 0
        v.scraper_provider = MagicMock()
        v.s3_reader = MagicMock()
        v.s3_reader.list_all_files_with_metadata = AsyncMock(return_value=files_meta)
        v._get_presigned_urls_batch = AsyncMock(
            return_value={f['key']: 'https://unused.invalid' for f in files_meta})
        v._sampled_duckdb_validation = AsyncMock(return_value={
            'duplicate_rate_within_job': 0.0, 'empty_rate': 0.0,
            'compression_failures': 0, 'row_count_mismatches': 0, 'decode_ratio': 1.0,
        })
        v._perform_job_content_matching = AsyncMock(return_value={
            'total_checked': 20, 'total_matched': 20, 'match_rate': 100.0,
            'mismatch_samples': [],
        })
        captured = {}

        async def _capture(hotkey, sampled_files, *a, **k):
            captured['keys'] = [f['key'] for f in sampled_files]
            return {'entities_validated': 20, 'entities_passed': 20,
                    'success_rate': 100.0, 'sample_results': [],
                    'platform_stats': {}}

        v._perform_scraper_validation = _capture
        result = asyncio.run(v.validate_miner_s3_data('hk', expected_jobs))
        return captured.get('keys', []), result

class TestEndToEndSinglePlatformResistance(ResistanceHarness):
    FLOOR = DuckDBSampledValidator.SCRAPER_PLATFORM_MIN_ENTITIES

    def test_reproduces_all_reddit_attempt_now_checks_x(self):
        """30 Reddit jobs whose total claim dwarfs 2 X jobs. The manipulation
        targeted X → 0; X must now reach its floor, every seed."""
        bundle = self._bundle([('reddit', 400, 30), ('x', 5, 2)])
        for seed in [f'0x{i:064x}' for i in range(10)]:
            with self.subTest(seed=seed):
                scraped, result = self._run(bundle, seed)
                self.assertIn('x', scraped, f"X never scraper-checked: {dict(scraped)}")
                self.assertIn('reddit', scraped)
                self.assertGreaterEqual(scraped['x'], self.FLOOR, dict(scraped))
                # No single platform may own the whole validated budget.
                self.assertLess(scraped['reddit'], result.entities_validated)

    def test_single_x_file_is_still_checked(self):
        """A platform with only ONE file (the floor clamps to what's available)
        must still reach a scraper, not be dropped."""
        bundle = self._bundle([('reddit', 400, 30), ('x', 5, 1)])
        for seed in [f'0x{i:064x}' for i in range(8)]:
            with self.subTest(seed=seed):
                scraped, _ = self._run(bundle, seed)
                self.assertIn('x', scraped, f"single X file dropped: {dict(scraped)}")
                self.assertGreaterEqual(scraped['x'], 1)

    def test_max_claim_skew(self):
        """Widest claim skew that keeps size/claimed >= MIN_BYTES_PER_ROW: X
        claims 1 row/job. X's files still get forced in and checked."""
        bundle = self._bundle([('reddit', 500, 20), ('x', 1, 3)])
        for seed in [f'0x{i:064x}' for i in range(8)]:
            with self.subTest(seed=seed):
                scraped, _ = self._run(bundle, seed)
                self.assertGreaterEqual(scraped['x'], self.FLOOR, dict(scraped))

    def test_reverse_skew_reddit_minority(self):
        """Symmetry: shrink Reddit instead. Reddit must reach its floor too."""
        bundle = self._bundle([('x', 400, 20), ('reddit', 5, 2)])
        for seed in [f'0x{i:064x}' for i in range(8)]:
            with self.subTest(seed=seed):
                scraped, _ = self._run(bundle, seed)
                self.assertGreaterEqual(scraped['reddit'], self.FLOOR, dict(scraped))

    def test_balanced_layout_checks_both(self):
        bundle = self._bundle([('reddit', 300, 8), ('x', 300, 8)])
        for seed in [f'0x{i:064x}' for i in range(6)]:
            with self.subTest(seed=seed):
                scraped, _ = self._run(bundle, seed)
                self.assertGreaterEqual(scraped['x'], self.FLOOR)
                self.assertGreaterEqual(scraped['reddit'], self.FLOOR)


class TestResistanceFuzz(ResistanceHarness):
    FLOOR = DuckDBSampledValidator.SCRAPER_PLATFORM_MIN_ENTITIES
    MIN_FILES = DuckDBSampledValidator.SCRAPER_PLATFORM_MIN_FILES

    def test_random_adversarial_layouts(self):
        """Fuzz: random counts and claim skews. Every platform that has files —
        no matter how small its claim — must be scraper-checked, and if it has
        >= SCRAPER_PLATFORM_MIN_FILES files it must reach the entity floor."""
        for t in range(25):
            rnd = random.Random(t)
            n_reddit = rnd.randint(1, 12)
            n_x = rnd.randint(1, 6)
            # Randomly pick which platform is the shrunken minority (claim 1-3)
            # vs the majority (claim 300-500). Ratios stay within MIN_BYTES_PER_ROW.
            big, small = rnd.choice([(400, 2), (500, 1), (300, 3)]), None
            if rnd.random() < 0.5:
                reddit_claim, x_claim = big[0], big[1]
            else:
                reddit_claim, x_claim = big[1], big[0]
            layout = [('reddit', reddit_claim, n_reddit), ('x', x_claim, n_x)]
            bundle = self._bundle(layout)
            seed = f'0x{t:064x}'
            with self.subTest(t=t, layout=layout):
                scraped, _ = self._run(bundle, seed)
                for plat, count in (('reddit', n_reddit), ('x', n_x)):
                    self.assertIn(plat, scraped,
                                  f"{plat} shut out (t={t}, {layout}): {dict(scraped)}")
                    if count >= self.MIN_FILES:
                        self.assertGreaterEqual(
                            scraped[plat], self.FLOOR,
                            f"{plat} below floor (t={t}, {layout}): {dict(scraped)}")


if __name__ == '__main__':
    unittest.main()


class TestAuditFindings(ResistanceHarness):
    """Regressions for holes found auditing the platform-coverage fix itself."""

    FLOOR = DuckDBSampledValidator.SCRAPER_PLATFORM_MIN_ENTITIES

    def test_suspicion_picks_do_not_satisfy_the_platform_file_floor(self):
        """The suspicion slice is detection-only: the caller strips those picks
        out of scraper_files. Counting them toward a platform's FILE floor
        therefore satisfies the floor with files the scraper never sees.

        Reachable, not theoretical: _pick_suspicious_files ranks files sharing an
        exact byte size first, which is exactly the fingerprint of the shrunken,
        uniform minority-platform files this manipulation produces — so the more
        suspicious a platform's files look, the more of its floor gets absorbed
        by picks that are excluded from validation.

        Asserted on the COMPOSITION of scraper_files rather than on downstream
        scrape counts: the per-file row quota is generous enough that a single
        surviving file still reaches the entity floor, which masks the defect
        end-to-end while the guarantee itself is already broken.
        """
        bundle = self._bundle([('reddit', 700, 26), ('x', 5, 3)])
        files_meta, expected_jobs, _ = bundle
        x_all = [f['key'] for f in files_meta
                 if DuckDBSampledValidator._file_platform(f['key'], expected_jobs) == 'x']

        def _stub_suspicion(self_, active_files, pool, k):
            """Route X's files into the detection-only slice, as the real ranker
            would when they share a byte size."""
            picks = [it for it in pool if it[0].get('key') in set(x_all)][:k]
            if len(picks) < k:
                picks += [it for it in pool
                          if it[0].get('key') not in set(x_all)][:k - len(picks)]
            return picks

        for seed in [f'0x{i:064x}' for i in range(10)]:
            with self.subTest(seed=seed):
                with patch.object(DuckDBSampledValidator, '_pick_suspicious_files',
                                  _stub_suspicion):
                    scraper_files, result = self._run_capture_files(bundle, seed)
                x_in_scraper = [k for k in scraper_files if k in x_all]
                # Every X file the suspicion slice did NOT take is eligible, and
                # the floor must supply min(SCRAPER_PLATFORM_MIN_FILES, eligible)
                # of them to the scraper phase.
                n_suspicion_x = len(set(x_all) - set(scraper_files)
                                    & set(result.suspicion_files))
                eligible = len(x_all) - n_suspicion_x
                want = min(DuckDBSampledValidator.SCRAPER_PLATFORM_MIN_FILES, eligible)
                self.assertGreaterEqual(
                    len(x_in_scraper), want,
                    f"X has {len(x_in_scraper)} file(s) in the scraper pool but "
                    f"{eligible} were eligible — the floor was satisfied by "
                    f"detection-only suspicion picks",
                )

    def test_breadth_survives_the_big_file_surplus(self):
        """The top-claim surplus must buy depth on big files ON TOP of file
        breadth, not instead of it: 2-3 files at SCRAPER_TOP_FILE_ROWS can fill
        the entire pool alone, which would collapse coverage back to exactly the
        files a fabricator wants inspected."""
        import vali_utils.s3_utils as s3_utils
        original = s3_utils.read_random_row_group
        seen = []

        def _spy(source, size, **kwargs):
            seen.append(source)
            return original(source, size, **kwargs)

        bundle = self._bundle([('reddit', 400, 12), ('x', 300, 6)])
        with patch.object(s3_utils, 'read_random_row_group', side_effect=_spy):
            self._run(bundle, '0x' + '7' * 64)

        expected = 2 * 20 // DuckDBSampledValidator.SCRAPER_ROWS_PER_FILE
        self.assertGreaterEqual(
            len(set(seen)), expected,
            f"file breadth collapsed to {len(set(seen))} distinct files; the "
            f"surplus quota ate the pool",
        )

    def test_big_files_get_more_rows_than_the_base_quota(self):
        """The fairness rules must not leave the effective_size-driving files at
        5 sampled rows — that is where fabricated volume pays."""
        import vali_utils.s3_utils as s3_utils
        original = s3_utils.read_random_row_group
        quotas = []

        def _spy(source, size, **kwargs):
            quotas.append(kwargs.get('max_rows'))
            return original(source, size, **kwargs)

        bundle = self._bundle([('reddit', 700, 10), ('x', 60, 5)])
        with patch.object(s3_utils, 'read_random_row_group', side_effect=_spy):
            self._run(bundle, '0x' + '9' * 64)

        self.assertTrue(
            any(q == DuckDBSampledValidator.SCRAPER_TOP_FILE_ROWS for q in quotas),
            f"no file received the top-claim surplus quota: {quotas}",
        )
