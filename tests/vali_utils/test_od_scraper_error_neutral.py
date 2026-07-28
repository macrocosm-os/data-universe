"""A scraper ERROR must not be scored as a miner failure.

`_validate_with_scraper` previously returned a bare bool, so an Apify actor timeout,
an empty result set or a missing scraper were indistinguishable from "the miner sent
bad data" — each costing od_boost x0.70 and od_cred -0.05. These tests pin the
three-state contract: True / False / None(no evidence), and confirm that a GENUINE
content mismatch is still reported as a failure.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from common.data import DataEntity, DataSource
from scraping.scraper import ValidationResult
from vali_utils.on_demand.on_demand_validation import (
    OnDemandValidator,
    ValidationContext,
    SCRAPER_ERROR_REASON_PREFIXES,
)


def _entity():
    return MagicMock(spec=DataEntity, content_size_bytes=100)


def _ctx():
    ctx = MagicMock(spec=ValidationContext)
    ctx.source = "reddit"
    return ctx


class TestScraperErrorIsNeutral(unittest.TestCase):
    def setUp(self):
        self.v = OnDemandValidator.__new__(OnDemandValidator)

    def _run(self, scraper):
        self.v._get_scraper = MagicMock(return_value=scraper)
        return asyncio.run(
            self.v._validate_with_scraper(_ctx(), _entity(), "t3_abc123")
        )

    def _scraper_returning(self, result):
        s = MagicMock()
        s.validate = AsyncMock(return_value=[result] if result is not None else [])
        return s

    # --- errors: must be None ("no evidence"), never False ---

    def test_scraper_raises_is_neutral(self):
        s = MagicMock()
        s.validate = AsyncMock(side_effect=TimeoutError("actor timed out"))
        self.assertIsNone(self._run(s))

    def test_empty_results_is_neutral(self):
        self.assertIsNone(self._run(self._scraper_returning(None)))

    def test_missing_scraper_is_neutral(self):
        self.v._get_scraper = MagicMock(return_value=None)
        self.assertIsNone(
            asyncio.run(self.v._validate_with_scraper(_ctx(), _entity(), "t3_abc123"))
        )

    def test_scraper_self_reported_error_is_neutral(self):
        """The real-world case: reddit_mc_scraper catches its own exception and
        returns it as an is_valid=False RESULT, not a raise."""
        for prefix in SCRAPER_ERROR_REASON_PREFIXES:
            with self.subTest(prefix=prefix):
                r = ValidationResult(
                    is_valid=False,
                    reason=f"{prefix} connection reset by peer",
                    content_size_bytes_validated=100,
                )
                self.assertIsNone(self._run(self._scraper_returning(r)))

    # --- genuine verdicts: must still be True/False ---

    def test_real_content_mismatch_still_fails(self):
        r = ValidationResult(
            is_valid=False,
            reason="Text does not match",
            content_size_bytes_validated=100,
        )
        self.assertIs(self._run(self._scraper_returning(r)), False)

    def test_removed_post_still_fails(self):
        r = ValidationResult(
            is_valid=False,
            reason="URL not found or inaccessible.",
            content_size_bytes_validated=100,
        )
        self.assertIs(self._run(self._scraper_returning(r)), False)

    def test_valid_content_passes(self):
        r = ValidationResult(
            is_valid=True, reason="", content_size_bytes_validated=100
        )
        self.assertIs(self._run(self._scraper_returning(r)), True)


if __name__ == "__main__":
    unittest.main()
