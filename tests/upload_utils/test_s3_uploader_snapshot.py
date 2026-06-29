"""Tests for the whole-job-snapshot S3 uploader.

Covers:
- A job with rows in SQLite produces a single upload call.
- A job with no matching rows produces zero uploads.
- max_rows is honored: query LIMIT, no row leakage.
- processed_state records snapshot stats per job.
"""

import asyncio
import datetime as dt
import json
import os
import sqlite3
import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest

from common.data import DataSource
from upload_utils.s3_uploader import S3PartitionedUploader

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _seed_db(db_path: str, rows: list[tuple]) -> None:
    """Create a DataEntity table and insert (uri, datetime, source, label, content) rows."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE DataEntity (
            uri TEXT PRIMARY KEY,
            datetime TEXT NOT NULL,
            source INTEGER NOT NULL,
            label TEXT,
            content BLOB
        )
        """
    )
    conn.executemany(
        "INSERT INTO DataEntity (uri, datetime, source, label, content) VALUES (?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


def _make_uploader(db_path: str, state_path: str) -> S3PartitionedUploader:
    """Build an uploader without touching subtensor/wallet."""
    wallet = MagicMock()
    wallet.hotkey.ss58_address = "5HotKey_xyz"

    uploader = S3PartitionedUploader.__new__(S3PartitionedUploader)
    uploader.db_path = db_path
    uploader.wallet = wallet
    uploader.subtensor = None
    uploader.miner_hotkey = wallet.hotkey.ss58_address
    uploader.api_base_url = "http://example.test"
    uploader.keypair = wallet.hotkey
    uploader.state_file = state_path
    uploader.output_dir = tempfile.mkdtemp(prefix="snap_out_")
    uploader.processed_state = {}
    return uploader


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _reddit_content(body: str) -> bytes:
    return json.dumps(
        {
            "id": "abc",
            "username": "u",
            "communityName": "r/x",
            "body": body,
            "title": "t",
            "createdAt": "2026-06-18T00:00:00Z",
            "dataType": "comment",
            "parentId": None,
            "url": "https://reddit.com/r/x/post/1",
            "media": None,
            "is_nsfw": False,
            "score": 1,
            "upvote_ratio": 1.0,
            "num_comments": 0,
            "scrapedAt": "2026-06-18T00:00:00Z",
        }
    ).encode()


def test_snapshot_uploads_single_file_for_job(tmp_path):
    """A job with N matching rows produces exactly one _upload_job_snapshot call with N rows."""
    db = str(tmp_path / "miner.db")
    state = str(tmp_path / "state.json")
    rows = [
        (f"u{i}", f"2026-06-1{i}T00:00:00Z", DataSource.REDDIT.value, "r/python", _reddit_content(f"body {i}"))
        for i in range(5)
    ]
    _seed_db(db, rows)
    up = _make_uploader(db, state)
    up._upload_job_snapshot = AsyncMock(return_value=True)

    job_config = {
        "source": DataSource.REDDIT.value,
        "type": "label",
        "value": "r/python",
        "max_rows": 100,
    }
    client = MagicMock()
    ok = asyncio.run(up._process_job(client, "job-x", job_config))

    assert ok is True
    assert up._upload_job_snapshot.call_count == 1
    call_df = up._upload_job_snapshot.call_args.args[1]
    assert len(call_df) == 5
    assert up.processed_state["job-x"]["last_snapshot_rows"] == 5


def test_snapshot_empty_job_no_upload(tmp_path):
    """A job with zero matching rows skips upload entirely."""
    db = str(tmp_path / "miner.db")
    state = str(tmp_path / "state.json")
    _seed_db(db, [])
    up = _make_uploader(db, state)
    up._upload_job_snapshot = AsyncMock(return_value=True)

    job_config = {
        "source": DataSource.REDDIT.value,
        "type": "label",
        "value": "r/nothing",
        "max_rows": 100,
    }
    client = MagicMock()
    ok = asyncio.run(up._process_job(client, "job-empty", job_config))

    assert ok is True
    assert up._upload_job_snapshot.call_count == 0
    assert "job-empty" not in up.processed_state


def test_snapshot_respects_max_rows(tmp_path):
    """Snapshot SQL LIMIT honors max_rows; only that many rows reach the uploader."""
    db = str(tmp_path / "miner.db")
    state = str(tmp_path / "state.json")
    rows = [
        (f"u{i}", f"2026-06-{i:02d}T00:00:00Z", DataSource.REDDIT.value, "r/python", _reddit_content(f"body {i}"))
        for i in range(1, 21)
    ]
    _seed_db(db, rows)
    up = _make_uploader(db, state)
    up._upload_job_snapshot = AsyncMock(return_value=True)

    job_config = {
        "source": DataSource.REDDIT.value,
        "type": "label",
        "value": "r/python",
        "max_rows": 7,
    }
    client = MagicMock()
    ok = asyncio.run(up._process_job(client, "job-cap", job_config))

    assert ok is True
    call_df = up._upload_job_snapshot.call_args.args[1]
    assert len(call_df) == 7
    assert up.processed_state["job-cap"]["last_snapshot_rows"] == 7


def test_snapshot_uses_default_when_max_rows_missing(tmp_path, monkeypatch):
    """If job config has no max_rows, DEFAULT_JOB_MAX_ROWS is the LIMIT."""
    db = str(tmp_path / "miner.db")
    state = str(tmp_path / "state.json")
    _seed_db(db, [])
    up = _make_uploader(db, state)
    up._upload_job_snapshot = AsyncMock(return_value=True)

    seen_limits = []
    original = up._get_label_data

    def spy(source, value, limit):
        seen_limits.append(limit)
        return original(source, value, limit)

    up._get_label_data = spy

    job_config = {
        "source": DataSource.REDDIT.value,
        "type": "label",
        "value": "r/x",
    }
    asyncio.run(up._process_job(MagicMock(), "job-default", job_config))
    assert seen_limits == [S3PartitionedUploader.DEFAULT_JOB_MAX_ROWS]


def test_snapshot_propagates_upload_failure(tmp_path):
    """If the upload call returns False, _process_job returns False and state isn't updated."""
    db = str(tmp_path / "miner.db")
    state = str(tmp_path / "state.json")
    rows = [
        ("u1", "2026-06-18T00:00:00Z", DataSource.REDDIT.value, "r/python", _reddit_content("body")),
    ]
    _seed_db(db, rows)
    up = _make_uploader(db, state)
    up._upload_job_snapshot = AsyncMock(return_value=False)

    job_config = {
        "source": DataSource.REDDIT.value,
        "type": "label",
        "value": "r/python",
        "max_rows": 100,
    }
    ok = asyncio.run(up._process_job(MagicMock(), "job-fail", job_config))
    assert ok is False
    assert "job-fail" not in up.processed_state
