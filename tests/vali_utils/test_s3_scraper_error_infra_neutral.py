"""A VALIDATOR-side scraper outage must not zero an honest miner — but a DATA error must
still count against them.

Companion to the on-demand fix (#844) and the S3-API one (#805/#843), on the S3
scraper-validation path.

THE DEFECT. `_perform_scraper_validation` caught every exception from
`_validate_with_scraper` and did `stats['validated'] += len(entities)` without touching
`stats['passed']`. So when the VALIDATOR's own scraper timed out or lost its connection,
those entities were booked as miner scraper FAILURES.

That is now worse than when this PR was first written, because `_per_platform_issues`
holds EACH platform to `MIN_SCRAPER_SUCCESS` on its own sample. One platform's outage
drives that platform's rate to ~0 -> appends an issue -> `is_valid` False ->
`effective_size` zeroed for the WHOLE submission, both platforms, every job.

THE FIX, and why it is narrow. Infra exceptions are skipped, not counted. Everything else
still counts as a miner failure — because a bare `except` would let a miner submit rows
crafted to crash the scraper and have their own sampled batch quietly excluded. That is
the same narrowing merged PR #844 used on the on-demand path.

Run: python -m pytest tests/vali_utils/test_s3_scraper_error_infra_neutral.py
"""
import asyncio
import unittest
from unittest import mock

from scraping.scraper import ValidationResult
from vali_utils.s3_utils import DuckDBSampledValidator


def _entities(n):
    """Opaque placeholders — nothing under test inspects them."""
    return [object() for _ in range(n)]


def _validator(side_effect=None, results=None):
    """A validator with __init__ bypassed (no network/config deps)."""
    v = object.__new__(DuckDBSampledValidator)

    async def _fake(entities, platform):
        if side_effect is not None:
            raise side_effect
        return results

    v._validate_with_scraper = _fake
    return v


def _run(v, by_platform):
    """Drive the extracted per-platform scrape/tally helper directly.

    `_perform_scraper_validation` builds its own entities from parquet downloads,
    presigned URLs and duckdb, so the interesting loop was unreachable in a unit test —
    which is why the loop now lives in `_scrape_check_by_platform`.
    """
    stats, samples, errored = asyncio.run(v._scrape_check_by_platform(by_platform))
    validated = sum(s["validated"] for s in stats.values())
    passed = sum(s["passed"] for s in stats.values())
    if validated > 0:
        rate = passed / validated * 100
    elif errored:
        rate = None
    else:
        rate = 0
    return {"entities_validated": validated, "entities_passed": passed,
            "success_rate": rate, "sample_results": samples, "platform_stats": stats}


def _ok(n):
    return [ValidationResult(is_valid=True, reason="ok", content_size_bytes_validated=10)
            for _ in range(n)]


class TestInfraNeutralScraperValidation(unittest.TestCase):
    def test_infra_outage_does_not_book_miner_failures(self):
        """A timeout mid-validation must not appear as scraper failures."""
        v = _validator(side_effect=asyncio.TimeoutError("read timed out"))
        out = _run(v, {"reddit": [(e, "job1") for e in _entities(5)]})
        self.assertEqual(out["entities_validated"], 0,
                         "validator-side outage was booked against the miner")
        self.assertIsNone(out["success_rate"],
                          "all-errored must yield None so the caller skips the gate")

    def test_data_error_still_counts_against_the_miner(self):
        """The narrowing is the point — a non-infra crash is still a miner failure."""
        v = _validator(side_effect=ValueError("unparseable row"))
        out = _run(v, {"reddit": [(e, "job1") for e in _entities(5)]})
        self.assertEqual(out["entities_validated"], 5)
        self.assertEqual(out["entities_passed"], 0)
        self.assertEqual(out["success_rate"], 0)

    def test_healthy_validation_is_unchanged(self):
        v = _validator(results=_ok(4))
        out = _run(v, {"reddit": [(e, "job1") for e in _entities(4)]})
        self.assertEqual((out["entities_validated"], out["entities_passed"]), (4, 4))
        self.assertEqual(out["success_rate"], 100.0)

    def test_one_platform_outage_does_not_zero_the_healthy_platform(self):
        """THE REGRESSION TEST for the per-platform bar.

        Reddit validates cleanly; X hits an infra error. Before the fix X booked 5
        failures, dragging its own per-platform rate to 0% -> `_per_platform_issues`
        appends an issue -> is_valid False -> the miner's ENTIRE effective_size is zeroed
        including the perfectly good Reddit leg.
        """
        v = object.__new__(DuckDBSampledValidator)

        async def _fake(entities, platform):
            if platform == "x":
                raise ConnectionError("connection reset by peer")
            return _ok(len(entities))

        v._validate_with_scraper = _fake
        out = _run(v, {"reddit": [(e, "j") for e in _entities(6)],
                       "x": [(e, "j") for e in _entities(5)]})

        self.assertEqual(out["success_rate"], 100.0,
                         "the healthy platform's rate was polluted by the other's outage")
        stats = out["platform_stats"]
        self.assertEqual(stats["reddit"], {"validated": 6, "passed": 6})
        self.assertEqual(stats.get("x", {"validated": 0, "passed": 0})["validated"], 0,
                         "X's infra outage must contribute NO validated entities")
        # And the bar itself must raise nothing for X (below the 5-entity floor, so exempt).
        self.assertEqual(v._per_platform_issues(stats), [])


if __name__ == "__main__":
    unittest.main()


class TestInfraErrorActuallyReachesTheCaller(unittest.TestCase):
    """The infra branch is worth nothing unless a real infra failure can REACH it.

    `_validate_with_scraper` wrapped its whole body in `except Exception` and returned
    is_valid=False results, so nothing it did could ever raise: the caller's
    `except _INFRA_SCRAPER_ERRORS` was unreachable in production, and the tests above
    passed only because they replace the method with a fake that raises. These drive the
    REAL method, so they fail if that swallowing is ever reintroduced.
    """

    @staticmethod
    def _validator(exc):
        """Real `_validate_with_scraper`; only the scraper PROVIDER is faked, so the
        failure happens before any entity is touched — the case no miner can induce."""
        v = object.__new__(DuckDBSampledValidator)
        provider = mock.MagicMock()
        provider.get.side_effect = exc
        v.scraper_provider = provider
        return v

    def test_infra_error_propagates_out_of_validate_with_scraper(self):
        v = self._validator(ConnectionError("no route to host"))
        with self.assertRaises(ConnectionError):
            asyncio.run(v._validate_with_scraper(_entities(3), "reddit"))

    def test_data_error_is_still_swallowed_as_a_miner_failure(self):
        """The narrowing has to hold in this method too, not just in the caller."""
        v = self._validator(ValueError("unparseable scraper config"))
        results = asyncio.run(v._validate_with_scraper(_entities(3), "reddit"))
        self.assertEqual(len(results), 3)
        self.assertTrue(all(not r.is_valid for r in results))

    def test_end_to_end_outage_is_not_charged_to_the_miner(self):
        """Real `_validate_with_scraper` AND real `_scrape_check_by_platform`, nothing
        stubbed between them — this is the path a validator actually executes."""
        v = self._validator(TimeoutError("connect timed out"))
        stats, _samples, errored = asyncio.run(
            v._scrape_check_by_platform({"reddit": [(e, "j") for e in _entities(5)]})
        )
        self.assertTrue(errored, "infra outage never reached the caller's infra branch")
        self.assertEqual(stats["reddit"], {"validated": 0, "passed": 0},
                         "validator-side outage was booked against the miner")

    def test_end_to_end_data_error_still_counts(self):
        v = self._validator(ValueError("unparseable scraper config"))
        stats, _samples, errored = asyncio.run(
            v._scrape_check_by_platform({"reddit": [(e, "j") for e in _entities(5)]})
        )
        self.assertFalse(errored)
        self.assertEqual(stats["reddit"], {"validated": 5, "passed": 0})
