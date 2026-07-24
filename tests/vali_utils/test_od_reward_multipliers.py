"""Tests for the OD reward multiplier curves (speed grace window + floor)."""

import unittest

from vali_utils.on_demand.on_demand_validation import (
    SPEED_FLOOR,
    SPEED_GRACE_S,
    SPEED_HALF_LIFE_S,
    _speed_multiplier,
    _volume_multiplier,
)


class TestSpeedMultiplier(unittest.TestCase):
    def test_full_mult_within_grace_window(self):
        for t in (0.0, 0.5, 5, 15, SPEED_GRACE_S):
            self.assertEqual(_speed_multiplier(t), 1.0, f"t={t}")

    def test_half_life_decay_past_grace(self):
        self.assertAlmostEqual(
            _speed_multiplier(SPEED_GRACE_S + SPEED_HALF_LIFE_S), 0.5, places=6
        )
        self.assertAlmostEqual(
            _speed_multiplier(SPEED_GRACE_S + SPEED_HALF_LIFE_S / 2),
            0.5 ** 0.5,
            places=6,
        )

    def test_floor_never_undercut(self):
        for t in (600, 3600, 86400):
            self.assertEqual(_speed_multiplier(t), SPEED_FLOOR, f"t={t}")

    def test_negative_upload_time_clamped(self):
        """Clock skew can't produce a >1.0 multiplier."""
        self.assertEqual(_speed_multiplier(-10), 1.0)

    def test_monotonic_nonincreasing(self):
        samples = [_speed_multiplier(t) for t in range(0, 1200, 30)]
        self.assertEqual(samples, sorted(samples, reverse=True))


class TestVolumeMultiplier(unittest.TestCase):
    """Pins current volume behavior (unchanged in this PR)."""

    def test_zero_returned_is_zero(self):
        self.assertEqual(_volume_multiplier(0, 100), 0.0)

    def test_full_limit_is_one(self):
        self.assertEqual(_volume_multiplier(100, 100), 1.0)


if __name__ == "__main__":
    unittest.main()
