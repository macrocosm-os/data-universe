"""Unit test for the per-job latest-only filter in DuckDBSampledValidator."""

from vali_utils.s3_utils import DuckDBSampledValidator


def test_keep_latest_per_job_picks_newest_by_last_modified():
    files_by_job = {
        "job-a": [
            {"key": "data/hotkey=H/job_id=job-a/old.parquet", "size": 100, "last_modified": "2026-06-10T10:00:00.000Z"},
            {
                "key": "data/hotkey=H/job_id=job-a/newer.parquet",
                "size": 200,
                "last_modified": "2026-06-15T09:00:00.000Z",
            },
            {
                "key": "data/hotkey=H/job_id=job-a/newest.parquet",
                "size": 300,
                "last_modified": "2026-06-18T12:00:00.000Z",
            },
        ],
        "job-b": [
            {"key": "data/hotkey=H/job_id=job-b/only.parquet", "size": 50, "last_modified": "2026-06-01T00:00:00.000Z"},
        ],
    }

    out, dropped = DuckDBSampledValidator._keep_latest_per_job(files_by_job)

    assert dropped == 2
    assert set(out) == {"job-a", "job-b"}
    assert len(out["job-a"]) == 1
    assert out["job-a"][0]["key"].endswith("newest.parquet")
    assert out["job-a"][0]["size"] == 300
    assert len(out["job-b"]) == 1
    assert out["job-b"][0]["key"].endswith("only.parquet")


def test_keep_latest_per_job_no_drops_when_single_file():
    files_by_job = {
        "job-a": [
            {"key": "data/hotkey=H/job_id=job-a/f.parquet", "size": 100, "last_modified": "2026-06-10T10:00:00.000Z"}
        ],
    }
    out, dropped = DuckDBSampledValidator._keep_latest_per_job(files_by_job)
    assert dropped == 0
    assert out == files_by_job


def test_keep_latest_per_job_empty_input():
    out, dropped = DuckDBSampledValidator._keep_latest_per_job({})
    assert out == {}
    assert dropped == 0


def test_keep_latest_per_job_handles_missing_last_modified():
    """Files with no last_modified are treated as oldest (empty string sorts last).
    The one with a real timestamp must win."""
    files_by_job = {
        "job-a": [
            {"key": "data/hotkey=H/job_id=job-a/no-ts.parquet", "size": 100},
            {
                "key": "data/hotkey=H/job_id=job-a/has-ts.parquet",
                "size": 200,
                "last_modified": "2026-06-15T09:00:00.000Z",
            },
        ],
    }
    out, dropped = DuckDBSampledValidator._keep_latest_per_job(files_by_job)
    assert dropped == 1
    assert out["job-a"][0]["key"].endswith("has-ts.parquet")
