import unittest
from unittest.mock import MagicMock

import torch

from rewards.miner_scorer import MinerScorer
from vali_utils.s3_utils import DuckDBSampledValidator


class TestS3QualityScoring(unittest.TestCase):
    def setUp(self):
        self.scorer = MinerScorer(10, MagicMock())

    # OD large enough that the 2x OD cap (400M) never binds below NORM_MAX.
    OD_UNCAPPED = 200_000_000.0

    def _set_od(self, uid, boost=OD_UNCAPPED, cred=1.0):
        self.scorer.ondemand_boosts[uid] = boost
        self.scorer.ondemand_credibility[uid] = cred

    def _s3(self):
        with self.scorer.lock:
            return self.scorer._s3_components_normalized()

    def test_top_credible_miner_gets_norm_max(self):
        top, half = 0, 1
        self.scorer.s3_boosts[top] = 4e9
        self.scorer.s3_boosts[half] = 2e9
        self.scorer.s3_credibility[top] = 1.0
        self.scorer.s3_credibility[half] = 1.0

        s3 = self._s3()
        self.assertAlmostEqual(float(s3[top]), MinerScorer.S3_BOOST_NORM_MAX, delta=1.0)
        self.assertAlmostEqual(
            float(s3[half]), MinerScorer.S3_BOOST_NORM_MAX / 2, delta=1.0
        )

    def test_credibility_scales_normalized_share(self):
        top, low = 0, 1
        self.scorer.s3_boosts[top] = 1e9
        self.scorer.s3_boosts[low] = 1e9
        self.scorer.s3_credibility[top] = 1.0
        self.scorer.s3_credibility[low] = 0.5

        expected = MinerScorer.S3_BOOST_NORM_MAX * 0.5 ** MinerScorer._CREDIBILITY_EXP
        self.assertAlmostEqual(float(self._s3()[low]), expected, delta=expected * 1e-4)

    def test_s3_zero_without_od(self):
        """No OD activity -> 2x OD cap is 0 -> no S3 reward."""
        uid = 1
        self.scorer.s3_boosts[uid] = 1e9
        self.scorer.s3_credibility[uid] = 1.0
        self.scorer.ondemand_boosts[uid] = 0.0
        scores = self.scorer.get_scores_for_weights()
        self.assertEqual(float(scores[uid]), 0.0)

    def test_s3_capped_at_twice_od(self):
        uid = 0
        od = 10_000_000.0
        self._set_od(uid, boost=od)
        self.scorer.s3_boosts[uid] = 1e12
        self.scorer.s3_credibility[uid] = 1.0
        scores = self.scorer.get_scores_for_weights()
        expected = 2 * od + od  # capped S3 + OD
        self.assertAlmostEqual(float(scores[uid]), expected, delta=expected * 1e-6)

    def test_cap_uses_coverage_adjusted_od(self):
        uid = 0
        self._set_od(uid, boost=100_000_000.0)
        self.scorer.od_coverage_mult[uid] = 0.5
        self.scorer.s3_boosts[uid] = 1e12
        self.scorer.s3_credibility[uid] = 1.0
        scores = self.scorer.get_scores_for_weights()
        od = 50_000_000.0
        self.assertAlmostEqual(float(scores[uid]), 3 * od, delta=3 * od * 1e-6)

    def test_uncapped_when_od_is_large(self):
        uid = 0
        self._set_od(uid)
        self.scorer.s3_boosts[uid] = 1e12
        self.scorer.s3_credibility[uid] = 1.0
        scores = self.scorer.get_scores_for_weights()
        expected = MinerScorer.S3_BOOST_NORM_MAX + self.OD_UNCAPPED
        self.assertAlmostEqual(float(scores[uid]), expected, delta=expected * 1e-6)

    def test_copies_without_od_earn_no_s3(self):
        """The same dataset on many hotkeys earns S3 only where OD is served."""
        for uid in range(5):
            self.scorer.s3_boosts[uid] = 1e9
            self.scorer.s3_credibility[uid] = 1.0
        self._set_od(0)
        scores = self.scorer.get_scores_for_weights()
        self.assertGreater(float(scores[0]), 0.0)
        for uid in range(1, 5):
            self.assertEqual(float(scores[uid]), 0.0)

    def test_discredited_stale_boost_does_not_squash_others(self):
        """A failed miner keeps its effective_size; only its credibility decays.
        It must not set the normalization max for honest miners."""
        stale, honest = 0, 1
        self.scorer.s3_boosts[stale] = 1e13
        self.scorer.s3_credibility[stale] = 0.01
        self.scorer.s3_boosts[honest] = 1e9
        self.scorer.s3_credibility[honest] = 1.0

        s3 = self._s3()
        self.assertAlmostEqual(
            float(s3[honest]), MinerScorer.S3_BOOST_NORM_MAX, delta=1.0
        )
        self.assertLess(float(s3[stale]), float(s3[honest]))

    def test_no_s3_data_yields_zero_without_nan(self):
        scores = self.scorer.get_scores_for_weights()
        self.assertFalse(torch.isnan(scores).any())
        self.assertEqual(float(scores.sum()), 0.0)

    def test_s3_component_never_exceeds_norm_max(self):
        for uid, boost in enumerate([1e3, 1e9, 5e12, 7e14]):
            self.scorer.s3_boosts[uid] = boost
            self.scorer.s3_credibility[uid] = 1.0
        self.assertLessEqual(
            float(self._s3().max()), MinerScorer.S3_BOOST_NORM_MAX * (1 + 1e-6)
        )

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

        Passing the full validation gate is what admits new volume; the S3
        component is bounded by S3_BOOST_NORM_MAX after normalization.
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
