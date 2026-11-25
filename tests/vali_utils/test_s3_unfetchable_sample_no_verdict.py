"""An S3 scraper sample in which every entity is UNFETCHABLE gives no verdict.

#922 made UNFETCHABLE results (the Reddit archive could not be reached) neither
a pass nor a miner failure. When every sampled entity was unfetchable, the
tally was 0 validated / 0 passed and reported 0%, so the MIN_SCRAPER_SUCCESS
gate failed the miner for the validator's own outage.

The parquet snapshot is produced by the miner uploader code. Only the listing,
the presigned-URL call and the Apify actor lookup are stubbed.
"""

import asyncio
import datetime as dt
import hashlib
import os
import random
import tempfile
import unittest
from unittest import mock

import pandas as pd

from common.data import DataSource
from scraping.reddit.model import RedditContent
from scraping.reddit.reddit_mc_scraper import RedditMCScraper
from scraping.scraper import ValidationResult
from upload_utils.s3_uploader import S3PartitionedUploader
from vali_utils.s3_utils import DuckDBSampledValidator
from vali_utils.validator_s3_access import S3Inconclusive

HOTKEY = "5FakeMinerHotkey"
JOB_ID = "job_reddit_only"
N_ROWS = 600


def _reddit_snapshot(tmpdir: str) -> dict:
    """One whole-job Reddit snapshot, written the way the miner writes it."""
    created = dt.datetime(2026, 11, 20, 12, 0, 5, tzinfo=dt.timezone.utc)
    scraped = dt.datetime(2026, 11, 20, 13, 0, 0, tzinfo=dt.timezone.utc)
    rows = []
    for i in range(N_ROWS):
        body = " ".join(hashlib.sha256(f"{i}-{k}".encode()).hexdigest() for k in range(3))
        entity = RedditContent.to_data_entity(RedditContent(
            id=f"t3_{i:06d}",
            url=f"https://www.reddit.com/r/bittensor_/comments/{i:06d}/post_{i}/",
            username=f"user{i}",
            communityName="r/bittensor_",
            body=body,
            createdAt=created + dt.timedelta(seconds=i),
            dataType="post",
            title=f"title {i}",
            media=None,
            is_nsfw=False,
            score=3,
            upvote_ratio=0.9,
            num_comments=1,
            scrapedAt=scraped,
        ))
        rows.append({
            "uri": entity.uri,
            "datetime": entity.datetime,
            "label": entity.label.value,
            "content": entity.content,
        })
    uploader = object.__new__(S3PartitionedUploader)
    raw_df = uploader._create_raw_dataframe(pd.DataFrame(rows), DataSource.REDDIT.value)
    assert len(raw_df) == N_ROWS
    name = f"data_20261120_130000_{len(raw_df)}_{'ab' * 8}.parquet"
    path = os.path.join(tmpdir, name)
    # Same call as S3PartitionedUploader._upload_job_snapshot.
    raw_df.to_parquet(path, index=False, compression="snappy", row_group_size=10_000)
    key = f"data/hotkey={HOTKEY}/job_id={JOB_ID}/{name}"
    return {
        "key": key,
        "size": os.path.getsize(path),
        "last_modified": "2026-11-20T13:00:10.000Z",
        "_path": path,
    }


class _Reader:
    def __init__(self, files):
        self._files = files

    async def list_all_files_with_metadata(self, hotkey):
        return [{k: v for k, v in f.items() if not k.startswith("_")} for f in self._files]


class UnfetchableScraperSampleTest(unittest.TestCase):
    EXPECTED_JOBS = {
        JOB_ID: {"id": JOB_ID, "weight": 1.0,
                 "params": {"platform": "reddit", "label": "r/bittensor_", "keyword": None,
                            "post_start_datetime": None, "post_end_datetime": None}},
    }

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        cache = os.path.join(self.tmp.name, "cache")
        os.makedirs(cache)
        p = mock.patch.object(DuckDBSampledValidator, "LOCAL_VALIDATION_TEMP_DIR", cache)
        p.start()
        self.addCleanup(p.stop)

        self.file = _reddit_snapshot(self.tmp.name)
        self.assertGreater(self.file["size"], DuckDBSampledValidator.MIN_FILE_SIZE_BYTES)

        v = object.__new__(DuckDBSampledValidator)
        v.s3_reader = _Reader([self.file])
        v.sample_percent = 10.0
        v._seed_material = "0xseed"
        v._rng = random.Random(1)
        # The download phase registers each sampled file here; pre-registering
        # the local copy is what test_scraper_entity_sampling does too.
        v._local_files = {self.file["key"]: self.file["_path"]}
        v._cached_bytes = 0
        from scraping.provider import ScraperProvider
        v.scraper_provider = ScraperProvider()
        v._get_presigned_urls_batch = mock.AsyncMock(
            return_value={self.file["key"]: "http://presigned.invalid/f"})
        self.v = v

    def _validate(self):
        return asyncio.run(self.v.validate_miner_s3_data(HOTKEY, self.EXPECTED_JOBS))

    def _archive_outage(self):
        lookup = mock.AsyncMock(
            side_effect=lambda url: {"url": url, "error": "archive_unavailable"})
        return lookup, (
            mock.patch.object(RedditMCScraper, "_lookup", lookup),
            mock.patch.object(RedditMCScraper, "EMPTY_RETRY_DELAY_S", 0),
        )

    def test_snapshot_passes_when_scraper_confirms_it(self):
        async def ok(entities, platform):
            return [ValidationResult(is_valid=True, reason="ok",
                                     content_size_bytes_validated=e.content_size_bytes)
                    for e in entities]
        self.v._validate_with_scraper = ok
        result = self._validate()
        self.assertTrue(result.is_valid, result.reason)
        self.assertGreater(result.effective_size_bytes, 0)

    def test_all_unfetchable_sample_is_inconclusive(self):
        lookup, patches = self._archive_outage()
        with patches[0], patches[1], self.assertRaises(S3Inconclusive):
            self._validate()
        self.assertGreater(lookup.await_count, 0)

    def test_all_unfetchable_sample_still_fails_on_other_issues(self):
        self.EXPECTED_JOBS = {
            JOB_ID: {**self.EXPECTED_JOBS[JOB_ID],
                     "params": {**self.EXPECTED_JOBS[JOB_ID]["params"], "label": "r/other"}},
        }
        _, patches = self._archive_outage()
        with patches[0], patches[1]:
            result = self._validate()
        self.assertFalse(result.is_valid)
        self.assertTrue(any("job match" in i.lower() for i in result.validation_issues),
                        result.validation_issues)
        self.assertFalse(any("scraper success" in i.lower() for i in result.validation_issues),
                         result.validation_issues)

    def test_scraper_rejections_still_fail(self):
        async def not_found(entities, platform):
            return [ValidationResult(is_valid=False, reason="URL not found",
                                     content_size_bytes_validated=e.content_size_bytes)
                    for e in entities]
        self.v._validate_with_scraper = not_found
        result = self._validate()
        self.assertFalse(result.is_valid)
        self.assertIn("Low scraper success: 0.0%", result.validation_issues)


if __name__ == "__main__":
    unittest.main()
