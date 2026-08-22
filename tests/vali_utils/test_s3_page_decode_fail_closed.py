"""Regression tests for the corrupt-column ("continue-skip") stuffing exploit.

A miner inflates a parquet's footer row count, then corrupts the compressed
stream of a single column that no scored check reads (e.g. parentId). The
footer stays valid, so the row count is credited toward effective_size, while
the fabricated row groups behind the corrupt column are never decoded.

PR #910 added `_assert_pages_decodable` (a full-column decode probe) and the
post-loop `page_decode_failures` gate. This suite locks in two invariants:

  1. The probe RAISES on a file with a corrupt column and SUCCEEDS on a clean
     file — i.e. it actually forces a decode of every column.
  2. The exception it raises is a subclass of the fail-closed handler tuple
     (duckdb.Error / pyarrow.lib.ArrowInvalid / ArrowIOError), NOT of the
     transient tuple (duckdb.IOException / HTTPException / ConnectionException),
     so a decode error fails the miner instead of being skipped as transient.
"""

import struct

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from vali_utils.s3_utils import DuckDBSampledValidator


def _write_clean_parquet(path, n=200):
    tbl = pa.table({
        "url": [f"https://www.reddit.com/r/x/comments/{i:06d}/" for i in range(n)],
        "id": [f"t1_{i:06d}" for i in range(n)],
        "body": [f"body {i}" for i in range(n)],
        "parentId": [f"t3_{i:06d}" for i in range(n)],
    })
    # Small row groups so we can corrupt one column chunk in isolation.
    pq.write_table(tbl, path, row_group_size=50, compression="snappy")


def _corrupt_one_column_chunk(path):
    """Flip bytes in the middle of the file to damage a compressed column
    chunk while leaving the footer (written last) intact."""
    with open(path, "r+b") as f:
        data = bytearray(f.read())
        # Corrupt a run of bytes ~40% into the file: past the header, well
        # before the footer/metadata at the tail.
        start = int(len(data) * 0.4)
        for i in range(start, start + 64):
            data[i] ^= 0xFF
        f.seek(0)
        f.write(data)


def test_probe_succeeds_on_clean_file(tmp_path):
    p = str(tmp_path / "clean.parquet")
    _write_clean_parquet(p)
    conn = duckdb.connect(":memory:")
    # Must not raise.
    DuckDBSampledValidator._assert_pages_decodable(p, conn)


def test_probe_raises_on_corrupt_column(tmp_path):
    p = str(tmp_path / "corrupt.parquet")
    _write_clean_parquet(p)

    # Footer still reports the full row count even after corruption.
    n_before = pq.ParquetFile(p).metadata.num_rows
    _corrupt_one_column_chunk(p)
    n_after = pq.ParquetFile(p).metadata.num_rows
    assert n_after == n_before, "footer row count must survive (that's the exploit)"

    conn = duckdb.connect(":memory:")
    with pytest.raises(Exception) as excinfo:
        DuckDBSampledValidator._assert_pages_decodable(p, conn)

    # The raised type must land in the fail-closed handler, not the transient one.
    exc = excinfo.value
    assert isinstance(
        exc, (duckdb.Error, pa.lib.ArrowInvalid, pa.lib.ArrowIOError)
    ), f"decode failure must be fail-closed type, got {type(exc).__name__}"
    assert not isinstance(
        exc, (duckdb.IOException, duckdb.HTTPException, duckdb.ConnectionException)
    ), (
        f"decode failure must NOT be a transient type "
        f"(would be skipped, not failed): {type(exc).__name__}"
    )


def test_transient_types_are_duckdb_error_subclasses():
    """Guards the handler ORDER invariant: the transient exceptions subclass
    duckdb.Error, so their handler must sit above the broad duckdb.Error one.
    If this ever stops being true the ordering comment can be revisited; if it
    stays true, ordering matters and the transient handler must come first.
    """
    for t in (duckdb.IOException, duckdb.HTTPException, duckdb.ConnectionException):
        assert issubclass(t, duckdb.Error)


def test_transient_skip_rate_threshold_is_sane():
    rate = DuckDBSampledValidator.MAX_TRANSIENT_SKIP_RATE
    assert 0.0 < rate < 1.0
