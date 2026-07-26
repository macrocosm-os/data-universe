"""Regression test: a validator-side scraper failure must NOT zero an honest miner.

Companion to the on-demand infra-neutral fix (#844) and the S3-API one (#805/#843),
on the S3 scraper-validation path.

Before the fix, the `except` in DuckDBSampledValidator._perform_scraper_validation did
`total_validated += len(entities)` without incrementing total_passed, so when the
VALIDATOR's own scraper timed out / 5xx'd / rate-limited, those entities counted as
scraper *failures*. With MIN_SCRAPER_SUCCESS=80%, a validator-side scraper outage then
drove success_rate below the threshold -> is_valid=False -> effective_size=0, collapsing
an honest miner's S3 score on the validator's infra failure.

After the fix, an all-errored validation returns success_rate=None, which the caller
already treats as "skip the success-rate gate and rely on prior-cycle credibility".

Run: python -m pytest tests/vali_utils/test_s3_scraper_error_infra_neutral.py
 or: python tests/vali_utils/test_s3_scraper_error_infra_neutral.py
"""
import asyncio
import pandas as pd

import vali_utils.s3_utils as s3m
from vali_utils.s3_utils import DuckDBSampledValidator
from scraping.scraper import ValidationResult


class _FakeConn:
    """Minimal duckdb connection stub: returns the expected reddit schema columns."""
    def __init__(self, names):
        self._names = names

    def execute(self, _query):
        return self

    def fetchall(self):
        return [(n,) for n in self._names]

    def close(self):
        pass


def _make_validator(scraper_raises: bool):
    v = object.__new__(DuckDBSampledValidator)  # bypass __init__ (no network deps)

    async def _validate(entities, platform):
        if scraper_raises:
            raise RuntimeError("simulated validator-side scraper outage (5xx/timeout)")
        return [ValidationResult(is_valid=True, content_size_bytes_validated=1,
                                 reason="ok") for _ in entities]

    v._validate_with_scraper = _validate
    v._create_data_entity = lambda row, platform: object()  # non-None sentinel entity
    return v


def _runsync(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def _run(scraper_raises: bool):
    v = _make_validator(scraper_raises)
    cols = list(DuckDBSampledValidator.EXPECTED_COLUMNS_REDDIT)
    # Make one reddit entity flow through the file loop without touching the network:
    s3m.duckdb.connect = lambda *a, **k: _FakeConn(cols)
    s3m.read_random_row_group = lambda *a, **k: pd.DataFrame([{c: "x" for c in cols}])
    sampled_files = [{'key': 'data/hotkey=HK/job_id=J1/f.parquet', 'size': 20000}]
    presigned = {'data/hotkey=HK/job_id=J1/f.parquet': 'http://fake/f.parquet'}
    expected_jobs = {'J1': {'params': {'platform': 'reddit'}}}
    return await v._perform_scraper_validation('HK', sampled_files, expected_jobs,
                                               presigned, num_entities=2)


def test_scraper_error_yields_none_success_rate():
    """Validator-side scraper outage -> success_rate None (gate skipped), not 0 (fail)."""
    result = _runsync(_run(scraper_raises=True))
    assert result['success_rate'] is None, (
        f"expected None on validator-side scraper error, got {result['success_rate']}")


def test_scraper_ok_yields_pass_rate():
    """Healthy scraper -> a real numeric success_rate (regression guard)."""
    result = _runsync(_run(scraper_raises=False))
    assert result['success_rate'] == 100.0, (
        f"expected 100.0 on all-valid, got {result['success_rate']}")


if __name__ == "__main__":
    ok = True
    for name, fn in [("scraper-error->None", test_scraper_error_yields_none_success_rate),
                     ("scraper-ok->100.0", test_scraper_ok_yields_pass_rate)]:
        try:
            fn()
            print(f"PASS | {name}")
        except AssertionError as e:
            ok = False
            print(f"FAIL | {name}: {e}")
    import sys
    print(f"\nRESULT: {'ALL PASS' if ok else 'FAILED'}")
    sys.exit(0 if ok else 1)
