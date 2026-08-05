import os
import random
import tempfile
import unittest

import pandas as pd

from vali_utils.parquet_reader import read_random_row_group
from vali_utils.s3_utils import (
    DuckDBSampledValidator,
    _derive_committed_rng,
    _split_sample_budget,
    _weighted_sample_without_replacement,
)

BLOCK_HASH = "0xabc123def4567890abc123def4567890"
MANIFEST = ["a.parquet:100", "b.parquet:200", "c.parquet:300"]


class TestCommittedRng(unittest.TestCase):
    def test_same_inputs_same_draw(self):
        r1 = _derive_committed_rng(BLOCK_HASH, "hk1", MANIFEST)
        r2 = _derive_committed_rng(BLOCK_HASH, "hk1", MANIFEST)
        self.assertEqual([r1.random() for _ in range(10)], [r2.random() for _ in range(10)])

    def test_manifest_order_does_not_matter(self):
        r1 = _derive_committed_rng(BLOCK_HASH, "hk1", MANIFEST)
        r2 = _derive_committed_rng(BLOCK_HASH, "hk1", list(reversed(MANIFEST)))
        self.assertEqual(r1.random(), r2.random())

    def test_any_input_change_reshuffles(self):
        base = _derive_committed_rng(BLOCK_HASH, "hk1", MANIFEST).random()
        self.assertNotEqual(base, _derive_committed_rng(BLOCK_HASH, "hk2", MANIFEST).random())
        self.assertNotEqual(base, _derive_committed_rng("0xother", "hk1", MANIFEST).random())
        self.assertNotEqual(
            base, _derive_committed_rng(BLOCK_HASH, "hk1", MANIFEST + ["d.parquet:1"]).random()
        )

    def test_no_seed_material_falls_back_to_nondeterministic(self):
        r = _derive_committed_rng(None, "hk1", MANIFEST)
        self.assertIsInstance(r, random.Random)

    def test_weighted_sample_deterministic_under_seeded_rng(self):
        items = list(range(100))
        weights = [i + 1 for i in items]
        s1 = _weighted_sample_without_replacement(items, weights, 10, rng=random.Random(7))
        s2 = _weighted_sample_without_replacement(items, weights, 10, rng=random.Random(7))
        self.assertEqual(s1, s2)


class TestSampleBudgetSplit(unittest.TestCase):
    def test_cap_15_splits_10_3_2(self):
        self.assertEqual(_split_sample_budget(15), (10, 3, 2))

    def test_min_sample_10_splits_7_2_1(self):
        self.assertEqual(_split_sample_budget(10), (7, 2, 1))

    def test_tiny_budgets_stay_weighted(self):
        for n in (1, 2, 3):
            self.assertEqual(_split_sample_budget(n), (n, 0, 0))

    def test_split_always_sums_to_budget(self):
        for n in range(1, 30):
            self.assertEqual(sum(_split_sample_budget(n)), n)


class TestSuspicionPicker(unittest.TestCase):
    def _validator(self):
        v = object.__new__(DuckDBSampledValidator)
        v._rng = random.Random(0)
        return v

    def test_repeated_exact_size_ranks_first(self):
        """Files sharing an exact byte size (padding/copy fingerprint) must be
        picked ahead of unique-size files."""
        padded = [({'key': f'pad{i}', 'size': 5000, 'last_modified': '2026-01-01'}, f'j{i}')
                  for i in range(3)]
        organic = [({'key': f'org{i}', 'size': 6001 + i * 37, 'last_modified': '2026-01-01'}, f'k{i}')
                   for i in range(10)]
        active = padded + organic
        picks = self._validator()._pick_suspicious_files(active, active, 3)
        self.assertEqual({p[0]['key'] for p in picks}, {'pad0', 'pad1', 'pad2'})

    def test_fills_with_random_when_nothing_suspicious(self):
        organic = [({'key': f'org{i}', 'size': 1000 + i * 37, 'last_modified': ''}, f'k{i}')
                   for i in range(10)]
        picks = self._validator()._pick_suspicious_files(organic, organic, 2)
        self.assertEqual(len(picks), 2)

    def test_empty_pool(self):
        self.assertEqual(self._validator()._pick_suspicious_files([], [], 2), [])


class TestSeededRowGroupRead(unittest.TestCase):
    def test_same_rng_seed_same_rows(self):
        df = pd.DataFrame({
            'url': [f'https://x.com/u/status/{i}' for i in range(1000)],
            'text': [f'tweet {i}' for i in range(1000)],
        })
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'sample.parquet')
            df.to_parquet(path, row_group_size=100)
            out1 = read_random_row_group(path, 0, max_rows=10, rng=random.Random(42))
            out2 = read_random_row_group(path, 0, max_rows=10, rng=random.Random(42))
            out3 = read_random_row_group(path, 0, max_rows=10, rng=random.Random(43))
        self.assertIsNotNone(out1)
        self.assertEqual(out1['url'].tolist(), out2['url'].tolist())
        self.assertNotEqual(out1['url'].tolist(), out3['url'].tolist())


if __name__ == '__main__':
    unittest.main()
