import unittest
from unittest.mock import MagicMock

import torch

from rewards.miner_scorer import MinerScorer
from vali_utils.s3_utils import DuckDBSampledValidator


class TestS3QualityScoring(unittest.TestCase):
    def setUp(self):
        self.scorer = MinerScorer(10, MagicMock())

    def test_cap_applies_before_credibility(self):
        """A miner that failed validation (cred 0.6481) with a boost (2.03B) far
        above the 2xOD cap (248.8M). Old order paid the full cap; capping the
        raw boost first makes credibility bite: 248.8M x 0.6481^2.5 ~= 84M."""
        uid = 0
        self.scorer.s3_boosts[uid] = 2031789184.0
        self.scorer.s3_credibility[uid] = 0.6481
        self.scorer.ondemand_boosts[uid] = 124406226.0
        self.scorer.ondemand_credibility[uid] = 1.0

        od_component = 124406226.0
        expected_s3 = min(2031789184.0, od_component * 2) * (0.6481 ** 2.5)
        expected_total = expected_s3 + od_component

        scores = self.scorer.get_scores_for_weights()
        self.assertAlmostEqual(float(scores[uid]), expected_total, delta=expected_total * 1e-4)
        # And it must be far below the old behavior (cap saturated at 248.8M).
        self.assertLess(expected_s3, 100_000_000)

    def test_s3_zero_without_od(self):
        uid = 1
        self.scorer.s3_boosts[uid] = 1e9
        self.scorer.s3_credibility[uid] = 1.0
        self.scorer.ondemand_boosts[uid] = 0.0
        scores = self.scorer.get_scores_for_weights()
        self.assertEqual(float(scores[uid]), 0.0)

    def test_pass_ema_targets_one_whatever_the_pass_rate(self):
        """Passing is the signal; the observed rate must not grade it again.

        MIN_SCRAPER_SUCCESS already refuses anything under 80% (per platform and
        combined), so a reported pass means the sample cleared the bar. Grading
        the EMA target by the rate on top of that made an honest 19/20 cycle —
        one deleted post, one rate-limited lookup — pull credibility DOWN.
        """
        uid = 0
        for _ in range(50):
            self.scorer.update_s3_effective_size(
                uid, effective_size=1000.0, validation_passed=True, pass_rate=0.95
            )
        self.assertAlmostEqual(float(self.scorer.s3_credibility[uid]), 1.0, delta=0.001)

    def test_pass_rate_is_telemetry_only(self):
        """Two miners that both PASS end at the same credibility."""
        graded, ungraded = 0, 1
        for _ in range(10):
            self.scorer.update_s3_effective_size(graded, 1000.0, True, pass_rate=0.8)
            self.scorer.update_s3_effective_size(ungraded, 1000.0, True, pass_rate=1.0)
        self.assertAlmostEqual(
            float(self.scorer.s3_credibility[graded]),
            float(self.scorer.s3_credibility[ungraded]),
            places=6,
        )

    def test_pass_without_scraper_sample_targets_one(self):
        uid = 0
        for _ in range(50):
            self.scorer.update_s3_effective_size(
                uid, effective_size=1000.0, validation_passed=True, pass_rate=None
            )
        self.assertAlmostEqual(float(self.scorer.s3_credibility[uid]), 1.0, delta=0.001)

    def test_failure_retains_size_and_decays_cred(self):
        uid = 0
        self.scorer.update_s3_effective_size(uid, 1000.0, True, pass_rate=1.0)
        cred_before = float(self.scorer.s3_credibility[uid])
        self.scorer.update_s3_effective_size(uid, 0.0, False, pass_rate=0.5)
        self.assertEqual(float(self.scorer.effective_sizes[uid]), 1000.0)
        self.assertAlmostEqual(
            float(self.scorer.s3_credibility[uid]),
            cred_before * (1 - self.scorer.s3_cred_alpha),
            places=5,
        )

    def test_repeated_failures_decay_credibility_geometrically(self):
        """Structural detections route through the forgiving path — a flaky
        validator-side read must not cost a miner its volume outright — so
        credibility, not quarantine, does the work over repeated cycles."""
        uid = 0
        self.scorer.update_s3_effective_size(uid, 1000.0, True, pass_rate=1.0)
        cred = float(self.scorer.s3_credibility[uid])
        for _ in range(5):
            self.scorer.update_s3_effective_size(uid, 0.0, False, pass_rate=0.0)
        expected = cred * (1 - self.scorer.s3_cred_alpha) ** 5
        self.assertAlmostEqual(float(self.scorer.s3_credibility[uid]), expected, places=5)
        # Volume is retained; the multiplier is what collapses.
        self.assertEqual(float(self.scorer.effective_sizes[uid]), 1000.0)
        self.assertLess(expected ** MinerScorer._CREDIBILITY_EXP, 0.03)

    def test_growth_does_not_scale_credibility(self):
        """Doubling claimed size on a PASS must not tax the multiplier.

        The S3 boost is capped at 2x OD before credibility (_s3_component), so
        the extra volume cannot inflate the reward it feeds — scaling cred down
        for it was a pure loss on new data.
        """
        uid = 0
        self.scorer.update_s3_effective_size(uid, 1000.0, True, pass_rate=1.0)
        cred_before = float(self.scorer.s3_credibility[uid])
        alpha = self.scorer.s3_cred_alpha

        self.scorer.update_s3_effective_size(uid, 2000.0, True, pass_rate=1.0)
        expected = min(1.0, alpha + (1 - alpha) * cred_before)
        self.assertAlmostEqual(float(self.scorer.s3_credibility[uid]), expected, places=5)
        self.assertEqual(float(self.scorer.effective_sizes[uid]), 2000.0)

    def test_growth_and_flat_volume_reach_the_same_credibility(self):
        grower, flat = 0, 1
        size = 1000.0
        for _ in range(20):
            size *= 1.2  # keeps crawling and uploading, as the subnet asks
            self.scorer.update_s3_effective_size(grower, size, True, pass_rate=1.0)
            self.scorer.update_s3_effective_size(flat, 1000.0, True, pass_rate=1.0)
        self.assertAlmostEqual(
            float(self.scorer.s3_credibility[grower]),
            float(self.scorer.s3_credibility[flat]),
            places=6,
        )
        self.assertAlmostEqual(float(self.scorer.s3_credibility[grower]), 1.0, delta=0.01)

    def test_capped_s3_component_never_grows_with_volume(self):
        """The cap is why growth needs no credibility tax: past 2x OD, more
        effective_size buys nothing, so it cannot be gamed for reward."""
        uid = 0
        self.scorer.ondemand_boosts[uid] = 100_000_000.0
        self.scorer.ondemand_credibility[uid] = 1.0
        self.scorer.s3_credibility[uid] = 1.0

        self.scorer.update_s3_effective_size(uid, 1e12, True, pass_rate=1.0)
        small = float(self.scorer.get_scores_for_weights()[uid])
        self.scorer.update_s3_effective_size(uid, 1e15, True, pass_rate=1.0)
        huge = float(self.scorer.get_scores_for_weights()[uid])

        self.assertAlmostEqual(small, huge, delta=small * 1e-6)
        self.assertAlmostEqual(huge, 3 * 100_000_000.0, delta=1.0)  # OD + 2xOD

    def test_repeated_pass_climbs_toward_one(self):
        uid = 0
        self.scorer.update_s3_effective_size(uid, 1000.0, True, pass_rate=1.0)
        cred_before = float(self.scorer.s3_credibility[uid])
        alpha = self.scorer.s3_cred_alpha
        self.scorer.update_s3_effective_size(uid, 1000.0, True, pass_rate=1.0)
        expected = min(1.0, alpha + (1 - alpha) * cred_before)
        self.assertAlmostEqual(float(self.scorer.s3_credibility[uid]), expected, places=5)
        self.assertGreater(expected, cred_before)


class TestPerPlatformBar(unittest.TestCase):
    def setUp(self):
        # _per_platform_issues only touches class constants; skip __init__.
        self.validator = object.__new__(DuckDBSampledValidator)

    def test_dirty_x_leg_cannot_hide_behind_clean_reddit(self):
        """3/5 X + 15/15 Reddit = 90% combined (passes the combined bar) but the
        X leg alone is 60% — dirty X hidden behind clean Reddit ballast."""
        issues = self.validator._per_platform_issues({
            'x': {'validated': 5, 'passed': 3},
            'reddit': {'validated': 15, 'passed': 15},
        })
        self.assertEqual(len(issues), 1)
        self.assertIn('x', issues[0])
        self.assertIn('60.0', issues[0])

    def test_clean_platforms_produce_no_issues(self):
        issues = self.validator._per_platform_issues({
            'x': {'validated': 5, 'passed': 4},       # 80% == bar
            'reddit': {'validated': 15, 'passed': 14},  # 93%
        })
        self.assertEqual(issues, [])

    def test_below_floor_sample_is_not_barred(self):
        """With fewer than SCRAPER_PLATFORM_MIN_ENTITIES validated, one bad row
        swings the rate by 20+ points — no per-platform bar applies."""
        issues = self.validator._per_platform_issues({
            'x': {'validated': 2, 'passed': 0},
        })
        self.assertEqual(issues, [])


if __name__ == '__main__':
    unittest.main()
