"""A FAILED S3 listing must be "no evidence", not "No files found".

Observed on validator UID 89, 2026-09-14 06:36:14 UTC: one miner's listing call returned
nothing for a single request, the validator logged

    S3 FAILED (0.0%): 0 jobs, 0 files (0.0MB) | Issues: No files found
    ... did not pass S3 validation. Reason: No files found

and s3_credibility went 0.998 -> 0.699 (x0.7, i.e. x0.41 on the S3 component). The prefix was
not empty: the same validator had passed it with 1,046 jobs three hours earlier and the next
listing passed again. 48 other miners passed S3 in that same hour.

Cause: `ValidatorS3Access.list_all_files_with_metadata` collapsed every failure -- presigned
list URL unavailable, non-200 from S3, unparseable XML, or an exception -- into an EMPTY LIST,
and `DuckDBSampledValidator.validate_miner_s3_data` maps an empty list to a failed result.
A listing that FAILED was indistinguishable from a listing that was EMPTY.

Contract after this change:
  * a listing that did not complete returns None
  * a listing that completed with zero keys returns []
  * validate_miner_s3_data returns None on None (the evaluator already skips a None result:
    `if s3_validation_result:` in miner_evaluator.py, and retries on the next cycle because the
    validation block is not advanced), and still fails on []
"""
import asyncio
import unittest
from unittest.mock import patch

from vali_utils.validator_s3_access import ValidatorS3Access
from vali_utils.s3_utils import DuckDBSampledValidator

EMPTY_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    '<IsTruncated>false</IsTruncated></ListBucketResult>'
)
ONE_FILE_TRUNCATED_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    '<IsTruncated>true</IsTruncated><NextContinuationToken>tok</NextContinuationToken>'
    '<Contents><Key>data/hotkey=HK/job_id=j1/data_1.parquet</Key><Size>20000</Size>'
    '<LastModified>2026-09-14T05:38:00.000Z</LastModified></Contents></ListBucketResult>'
)


class _Resp:
    def __init__(self, status, text=""):
        self.status_code, self.text = status, text


def _reader(presigned_urls, responses):
    """A ValidatorS3Access with no wallet/network: presigned URLs and HTTP responses scripted."""
    r = ValidatorS3Access.__new__(ValidatorS3Access)
    urls = list(presigned_urls)
    resps = list(responses)

    async def fake_presigned(hotkey, token=None):
        return urls.pop(0) if urls else None
    r._request_presigned_list_url = fake_presigned
    r._scripted = resps
    return r


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class ListingContract(unittest.TestCase):
    def _list(self, reader):
        def fake_get(url, timeout=None):
            resp = reader._scripted.pop(0)
            if isinstance(resp, Exception):
                raise resp
            return resp
        with patch("vali_utils.validator_s3_access.requests.get", side_effect=fake_get):
            return _run(reader.list_all_files_with_metadata("HK"))

    def test_presigned_url_unavailable_is_None(self):
        self.assertIsNone(self._list(_reader([None], [])))

    def test_non_200_is_None(self):
        self.assertIsNone(self._list(_reader(["u1"], [_Resp(503, "slow down")])))

    def test_unparseable_xml_is_None(self):
        self.assertIsNone(self._list(_reader(["u1"], [_Resp(200, "<html>cloudfront error")])))

    def test_transport_exception_is_None(self):
        self.assertIsNone(self._list(_reader(["u1"], [TimeoutError("read timed out")])))

    def test_failure_on_a_later_page_is_None_not_a_truncated_list(self):
        # page 1 ok and truncated; page 2 presigned URL unavailable -> a partial list would
        # under-count effective_size, which is also not evidence about the miner.
        self.assertIsNone(self._list(_reader(["u1", None], [_Resp(200, ONE_FILE_TRUNCATED_XML)])))

    def test_completed_empty_listing_is_an_empty_list(self):
        self.assertEqual(self._list(_reader(["u1"], [_Resp(200, EMPTY_XML)])), [])


class ValidatorContract(unittest.TestCase):
    def _validator(self, listing):
        v = DuckDBSampledValidator.__new__(DuckDBSampledValidator)

        class R:
            async def list_all_files_with_metadata(self, hk):
                return listing
        v.s3_reader = R()
        return v

    def test_failed_listing_yields_no_verdict(self):
        res = _run(self._validator(None).validate_miner_s3_data("HK", {}))
        self.assertIsNone(res, "a listing that did not complete must not produce a verdict")

    def test_empty_listing_still_fails(self):
        res = _run(self._validator([]).validate_miner_s3_data("HK", {}))
        self.assertIsNotNone(res)
        self.assertFalse(res.is_valid)
        self.assertIn("No files found", res.reason)



class EvaluatorContract(unittest.TestCase):
    """`_perform_s3_validation` must pass a None verdict through untouched. Its body summarises
    the result (`get_s3_validation_summary`) before any None check; without a guard a None raises
    inside the try, the except turns it into a FAILED S3ValidationResult, and the credibility decay
    this change exists to prevent happens anyway, one layer up."""

    def test_none_from_validate_is_returned_as_none_not_a_failed_result(self):
        import threading, types
        import vali_utils.miner_evaluator as me

        async def fake_validate(*a, **k):
            return None

        class Storage:
            def __init__(self): self.calls = []
            def update_validation_info(self, *a): self.calls.append(a)

        fake_self = types.SimpleNamespace(
            uid=me.MACROCOSMOS_VALIDATOR_UID, wallet=None,
            config=types.SimpleNamespace(s3_auth_url="https://x"), s3_reader=None,
            metagraph_syncer=None,               # seed lookup fails -> warning path, not fatal
            _seed_hash_lock=threading.Lock(), _seed_hash_cache=None,
            s3_storage=Storage(), s3_results_client=None,
        )
        with patch.object(me, "validate_s3_miner_data", fake_validate):
            res = _run(me.MinerEvaluator._perform_s3_validation(fake_self, 7, "HK", 9064000))
        self.assertIsNone(res, "None (no verdict) must not be converted into a failed result")
        self.assertEqual(fake_self.s3_storage.calls, [], "no verdict -> validation block not advanced -> retried next cycle")


if __name__ == "__main__":
    unittest.main()
