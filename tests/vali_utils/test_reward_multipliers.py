"""Tests for OD reward multiplier curves.

Covers the speed and volume formulas defined in docs/od_reward_formula.md.
"""

import datetime as dt
import math
import unittest
from unittest.mock import MagicMock

from vali_utils.on_demand.on_demand_validation import (
    OnDemandValidator,
    SPEED_HALF_LIFE_S,
    SPEED_FLOOR_S,
    VOLUME_OPEN_MAX,
    _speed_multiplier,
    _volume_multiplier,
)


TOL = 0.005


class TestSpeedMultiplier(unittest.TestCase):
    """Speed curve: S(t) = min(1, 0.5^(max(FLOOR, t)/H)) with H = 45s, FLOOR = 1s.

    The practical floor treats anything faster than FLOOR as exactly FLOOR, so
    implausible sub-second (cached) responses earn no more than an honest ~1s
    miner. The fastest achievable multiplier is therefore S(FLOOR), not 1.0.
    """

    def test_sub_floor_clamped_to_floor_value(self):
        floor_value = 0.5 ** (SPEED_FLOOR_S / SPEED_HALF_LIFE_S)
        # 0s, a tiny sub-second time, and exactly FLOOR all yield the same value.
        self.assertAlmostEqual(_speed_multiplier(0), floor_value, delta=TOL)
        self.assertAlmostEqual(_speed_multiplier(0.2), floor_value, delta=TOL)
        self.assertAlmostEqual(_speed_multiplier(SPEED_FLOOR_S), floor_value, delta=TOL)

    def test_negative_seconds_clamped_to_floor_value(self):
        floor_value = 0.5 ** (SPEED_FLOOR_S / SPEED_HALF_LIFE_S)
        self.assertAlmostEqual(_speed_multiplier(-5), floor_value, delta=TOL)

    def test_half_life_is_half(self):
        self.assertAlmostEqual(_speed_multiplier(SPEED_HALF_LIFE_S), 0.5, delta=TOL)

    def test_double_half_life_is_quarter(self):
        self.assertAlmostEqual(_speed_multiplier(2 * SPEED_HALF_LIFE_S), 0.25, delta=TOL)

    def test_monotone_non_increasing(self):
        prev = _speed_multiplier(0)
        for t in [1, 5, 10, 20, 30, 45, 60, 90, 115, 120]:
            cur = _speed_multiplier(t)
            self.assertLessEqual(cur, prev + 1e-9, f"non-monotone at t={t}")
            prev = cur

    def test_selected_values(self):
        cases = [
            (1, 0.985),
            (5, 0.926),
            (10, 0.857),
            (15, 0.794),
            (30, 0.630),
            (60, 0.397),
            (90, 0.250),
            (115, 0.170),
        ]
        for t, expected in cases:
            self.assertAlmostEqual(
                _speed_multiplier(t),
                expected,
                delta=TOL,
                msg=f"t={t}",
            )

    def test_bounded_in_unit_interval(self):
        for t in [0, 1, 30, 120, 300, 10_000]:
            v = _speed_multiplier(t)
            self.assertGreaterEqual(v, 0.0)
            self.assertLessEqual(v, 1.0)


class TestVolumeMultiplierLimited(unittest.TestCase):
    """Volume curve, limited mode: sub-linear below L, log-capped bonus above."""

    def test_zero_returned_is_zero(self):
        self.assertEqual(_volume_multiplier(0, 100), 0.0)

    def test_at_target_is_one(self):
        self.assertAlmostEqual(_volume_multiplier(100, 100), 1.0, delta=TOL)

    def test_half_target_is_below_half(self):
        # (0.5)^1.3 ≈ 0.406 — V3 invariant
        self.assertLess(_volume_multiplier(50, 100), 0.5)
        self.assertAlmostEqual(_volume_multiplier(50, 100), 0.406, delta=TOL)

    def test_over_target_caps_at_1_25(self):
        for r in [200, 500, 1000, 10_000]:
            v = _volume_multiplier(r, 100)
            self.assertLessEqual(v, 1.25 + 1e-6, f"r={r}")

    def test_monotone_non_decreasing(self):
        prev = -1.0
        for r in [0, 1, 10, 30, 50, 80, 100, 150, 200, 500, 1000, 5000]:
            cur = _volume_multiplier(r, 100)
            self.assertGreaterEqual(cur, prev - 1e-9, f"non-monotone at r={r}")
            prev = cur

    def test_selected_values_L_100(self):
        cases = [
            (10, 0.0501),
            (30, 0.2091),
            (50, 0.4061),
            (80, 0.7482),
            (100, 1.0000),
            (150, 1.0608),
            (200, 1.1040),
            (1000, 1.2500),
        ]
        for r, expected in cases:
            self.assertAlmostEqual(
                _volume_multiplier(r, 100),
                expected,
                delta=TOL,
                msg=f"r={r}",
            )

    def test_negative_returned_clamped(self):
        self.assertEqual(_volume_multiplier(-5, 100), 0.0)


class TestVolumeMultiplierOpen(unittest.TestCase):
    """Volume curve, open mode (limit=None): log-scale to absolute volume."""

    def test_zero_is_zero(self):
        self.assertEqual(_volume_multiplier(0, None), 0.0)

    def test_at_reference_is_one(self):
        self.assertAlmostEqual(_volume_multiplier(1000, None), 1.0, delta=TOL)

    def test_capped_at_max(self):
        self.assertAlmostEqual(
            _volume_multiplier(10_000_000, None), VOLUME_OPEN_MAX, delta=TOL
        )

    def test_zero_limit_treated_as_open(self):
        # L=0 should fall through to open-mode logic, not crash
        self.assertAlmostEqual(_volume_multiplier(1000, 0), 1.0, delta=TOL)

    def test_selected_values(self):
        cases = [
            (10, 0.3471),
            (100, 0.6680),
            (500, 0.8998),
            (1000, 1.0000),
            (5000, 1.2328),
            (10_000, 1.3332),
        ]
        for r, expected in cases:
            self.assertAlmostEqual(
                _volume_multiplier(r, None),
                expected,
                delta=TOL,
                msg=f"r={r}",
            )


class TestCalculateOndemandRewardMultipliers(unittest.TestCase):
    """End-to-end wrapper that takes datetimes and returns (speed, volume)."""

    def setUp(self):
        # OnDemandValidator only uses self for logging in this method;
        # a bare mock evaluator is sufficient.
        self.validator = OnDemandValidator(evaluator=MagicMock())

    def _calc(self, t_seconds, returned, limit):
        t0 = dt.datetime(2026, 5, 6, 12, 0, 0, tzinfo=dt.timezone.utc)
        t1 = t0 + dt.timedelta(seconds=t_seconds)
        return self.validator.calculate_ondemand_reward_multipliers(
            job_created_at=t0,
            submission_timestamp=t1,
            returned_count=returned,
            requested_limit=limit,
        )

    def test_cache_hit_full_volume(self):
        s, v = self._calc(1, 100, 100)
        self.assertAlmostEqual(s, 0.985, delta=TOL)
        self.assertAlmostEqual(v, 1.000, delta=TOL)

    def test_typical_scrape_full_volume(self):
        s, v = self._calc(30, 100, 100)
        self.assertAlmostEqual(s, 0.630, delta=TOL)
        self.assertAlmostEqual(v, 1.000, delta=TOL)

    def test_slow_scrape_partial_volume(self):
        s, v = self._calc(60, 50, 100)
        self.assertAlmostEqual(s, 0.397, delta=TOL)
        self.assertAlmostEqual(v, 0.406, delta=TOL)

    def test_open_mode(self):
        s, v = self._calc(5, 1000, None)
        self.assertAlmostEqual(s, 0.926, delta=TOL)
        self.assertAlmostEqual(v, 1.000, delta=TOL)

    def test_missing_timestamps_returns_default(self):
        s, v = self.validator.calculate_ondemand_reward_multipliers(
            job_created_at=None,
            submission_timestamp=None,
            returned_count=100,
            requested_limit=100,
        )
        self.assertEqual(s, 0.5)
        self.assertEqual(v, 0.0)

    def test_empty_submission_zero_volume(self):
        _, v = self._calc(10, 0, 100)
        self.assertEqual(v, 0.0)


if __name__ == "__main__":
    unittest.main()
