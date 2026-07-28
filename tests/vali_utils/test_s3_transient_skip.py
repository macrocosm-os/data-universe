"""S3 validation must SKIP on validator-side transient failures, never FAIL a
miner and never score a PARTIAL view of its data.

Two production incidents motivate these tests:

  1. A 5xx from the presigned-URL auth API failed the whole validation run, so a
     miner was penalised for our infrastructure being down.

  2. A 5xx midway through paginating a miner's file listing silently returned the
     pages fetched so far. A miner with 30k files listed as 7k, so its coverage
     (and therefore its S3 score) collapsed toward zero.

Both are now retried, and if they still cannot complete they raise
S3ValidationSkip, which `_perform_s3_validation` converts into `None` — and
`eval_miner` only scores S3 when the result is truthy, so the miner's S3 score
and credibility are left exactly as they were.
"""
import asyncio
import xml.etree.ElementTree as ET

import pytest
import requests as real_requests

from vali_utils import s3_utils, validator_s3_access
from vali_utils.miner_evaluator import MinerEvaluator
from vali_utils.s3_utils import (
    DuckDBSampledValidator,
    S3ValidationSkip,
    http_with_retry,
)
from vali_utils.validator_s3_access import ValidatorS3Access

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


# --------------------------------------------------------------------------- #
# Doubles
# --------------------------------------------------------------------------- #
class _Resp:
    """Minimal stand-in for requests.Response."""

    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        return self._payload


class _FakeRequests:
    """Swaps out .post/.get while leaving the real exception classes reachable
    (http_with_retry catches requests.Timeout / requests.ConnectionError)."""

    def __init__(self, post=None, get=None):
        self._post = post
        self._get = get

    def post(self, *args, **kwargs):
        return self._post(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self._get(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(real_requests, name)


class _FakeSigner:
    def headers(self, body):
        return {}


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    """Retry tests should exercise the retry logic, not the sleep."""
    monkeypatch.setattr(s3_utils, "_S3_RETRY_BACKOFF", 0)


def _list_xml(keys, truncated=False, token=None, ns=S3_NS, hotkey="H"):
    """Build a ListBucketResult page the way an S3-compatible backend does."""
    xmlns = f' xmlns="{ns}"' if ns else ""
    contents = "".join(
        f"<Contents><Key>data/hotkey={hotkey}/job_id=j/{k}</Key>"
        f"<Size>{100 + i}</Size>"
        f"<LastModified>2026-07-0{(i % 9) + 1}T00:00:00.000Z</LastModified></Contents>"
        for i, k in enumerate(keys)
    )
    trunc = "true" if truncated else "false"
    next_tok = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f"<ListBucketResult{xmlns}>{contents}"
        f"<IsTruncated>{trunc}</IsTruncated>{next_tok}</ListBucketResult>"
    )


def _reader(monkeypatch, pages):
    """A ValidatorS3Access whose list pages come from `pages`.

    Each entry is either a _Resp (the page body) or an Exception to raise. The
    /get-miner-list call always succeeds; only page fetches vary.
    """
    reader = object.__new__(ValidatorS3Access)
    reader.s3_auth_url = "https://auth.test"
    reader._signer = _FakeSigner()

    calls = {"list_url": 0, "page": 0}

    def fake_post(url, **kwargs):
        calls["list_url"] += 1
        return _Resp(payload={"list_url": "https://r2.test/list?sig=x"})

    def fake_get(url, **kwargs):
        i = min(calls["page"], len(pages) - 1)
        calls["page"] += 1
        item = pages[i]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(
        validator_s3_access, "requests", _FakeRequests(post=fake_post, get=fake_get)
    )
    return reader, calls


# --------------------------------------------------------------------------- #
# http_with_retry — the shared transient-failure policy
# --------------------------------------------------------------------------- #
def test_returns_immediately_when_the_first_attempt_succeeds():
    calls = []

    def send():
        calls.append(1)
        return _Resp(200, payload={"ok": True})

    resp = asyncio.run(http_with_retry(send, what="probe"))

    assert resp.json() == {"ok": True}
    assert len(calls) == 1


def test_recovers_from_a_transient_5xx():
    responses = [_Resp(502), _Resp(503), _Resp(200, payload={"ok": True})]
    calls = []

    def send():
        calls.append(1)
        return responses[len(calls) - 1]

    resp = asyncio.run(http_with_retry(send, what="probe"))

    assert resp.status_code == 200
    assert len(calls) == 3, "should have retried past both transient failures"


def test_skips_rather_than_failing_when_5xx_persists():
    calls = []

    def send():
        calls.append(1)
        return _Resp(502)

    with pytest.raises(S3ValidationSkip) as err:
        asyncio.run(http_with_retry(send, what="get-file-presigned-urls"))

    assert len(calls) == s3_utils._S3_RETRY_ATTEMPTS
    assert "get-file-presigned-urls" in str(err.value)
    assert "502" in str(err.value)


def test_does_not_waste_retries_on_a_non_transient_status():
    calls = []

    def send():
        calls.append(1)
        return _Resp(403)

    with pytest.raises(S3ValidationSkip):
        asyncio.run(http_with_retry(send, what="probe"))

    assert len(calls) == 1, "4xx cannot be fixed by retrying"


def test_retries_network_errors_too():
    calls = []

    def send():
        calls.append(1)
        if len(calls) == 1:
            raise real_requests.ConnectionError("connection reset")
        return _Resp(200)

    assert asyncio.run(http_with_retry(send, what="probe")).status_code == 200
    assert len(calls) == 2


# --------------------------------------------------------------------------- #
# Incident 1 — presigned URLs for the sampled files
# --------------------------------------------------------------------------- #
def _validator():
    v = object.__new__(DuckDBSampledValidator)
    v.s3_auth_url = "https://auth.test"
    v._signer = _FakeSigner()
    return v


def test_presigned_urls_survive_a_transient_api_failure(monkeypatch):
    calls = []

    def fake_post(url, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            return _Resp(502)
        return _Resp(200, payload={"file_urls": {"a.parquet": "https://r2/a?sig=1"}})

    monkeypatch.setattr(s3_utils, "requests", _FakeRequests(post=fake_post))

    urls = asyncio.run(_validator()._get_presigned_urls_batch("H", ["a.parquet"]))

    assert urls == {"a.parquet": "https://r2/a?sig=1"}


def test_presigned_urls_skip_instead_of_validating_a_partial_sample(monkeypatch):
    """A batch we cannot resolve must abort the run, not shrink the sample."""

    def fake_post(url, **kwargs):
        payload = {"file_urls": {"a.parquet": "https://r2/a?sig=1"}}
        # First batch resolves, the second is permanently down.
        return _Resp(200, payload=payload) if b'"a.parquet"' in kwargs["data"] else _Resp(502)

    monkeypatch.setattr(s3_utils, "requests", _FakeRequests(post=fake_post))

    with pytest.raises(S3ValidationSkip):
        asyncio.run(
            _validator()._get_presigned_urls_batch(
                "H", ["a.parquet", "b.parquet"], batch_size=1
            )
        )


# --------------------------------------------------------------------------- #
# Incident 2 — the file listing must be complete or not used at all
# --------------------------------------------------------------------------- #
def test_listing_paginates_to_completion(monkeypatch):
    reader, _ = _reader(
        monkeypatch,
        [
            _Resp(text=_list_xml(["p1a.parquet", "p1b.parquet"], truncated=True, token="t1")),
            _Resp(text=_list_xml(["p2a.parquet"], truncated=False)),
        ],
    )

    files = asyncio.run(reader.list_all_files_with_metadata("H"))

    assert [f["key"].split("/")[-1] for f in files] == [
        "p1a.parquet",
        "p1b.parquet",
        "p2a.parquet",
    ]
    assert files[0]["size"] == 100
    assert files[0]["last_modified"] == "2026-07-01T00:00:00.000Z"


def test_listing_retries_a_transient_page_and_still_returns_everything(monkeypatch):
    reader, _ = _reader(
        monkeypatch,
        [
            _Resp(text=_list_xml(["p1.parquet"], truncated=True, token="t1")),
            _Resp(503),  # page 2 blips...
            _Resp(text=_list_xml(["p2.parquet"], truncated=False)),  # ...then recovers
        ],
    )

    files = asyncio.run(reader.list_all_files_with_metadata("H"))

    assert len(files) == 2, "a recovered page must not truncate the listing"


def test_listing_skips_instead_of_returning_the_pages_it_managed_to_fetch(monkeypatch):
    """The 30k-files-listed-as-7k incident.

    Returning the partial set understates the miner's coverage, which is scored
    as if it were the whole truth. Aborting is the only safe outcome.
    """
    reader, _ = _reader(
        monkeypatch,
        [
            _Resp(text=_list_xml(["p1.parquet"], truncated=True, token="t1")),
            _Resp(500),  # page 2 never recovers
        ],
    )

    with pytest.raises(S3ValidationSkip) as err:
        asyncio.run(reader.list_all_files_with_metadata("H"))

    assert "list page 2" in str(err.value)


def test_listing_skips_when_the_page_body_is_not_xml(monkeypatch):
    reader, _ = _reader(monkeypatch, [_Resp(text="502 Bad Gateway")])

    with pytest.raises(S3ValidationSkip) as err:
        asyncio.run(reader.list_all_files_with_metadata("H"))

    assert "unparseable" in str(err.value)


@pytest.mark.parametrize(
    "body",
    [
        "<html>502 Bad Gateway</html>",
        f'<Error xmlns="{S3_NS}"><Code>InternalError</Code></Error>',
    ],
    ids=["proxy-error-page", "s3-error-document"],
)
def test_listing_skips_when_the_page_is_well_formed_xml_but_not_a_listing(
    monkeypatch, body
):
    """An error page parses as valid XML and simply has no <Contents>. Treating
    that as an empty listing would fail the miner for a gateway hiccup."""
    reader, _ = _reader(monkeypatch, [_Resp(text=body)])

    with pytest.raises(S3ValidationSkip) as err:
        asyncio.run(reader.list_all_files_with_metadata("H"))

    assert "not a listing" in str(err.value)


def test_listing_skips_when_the_list_url_cannot_be_obtained(monkeypatch):
    reader = object.__new__(ValidatorS3Access)
    reader.s3_auth_url = "https://auth.test"
    reader._signer = _FakeSigner()

    monkeypatch.setattr(
        validator_s3_access,
        "requests",
        _FakeRequests(post=lambda url, **kw: _Resp(502)),
    )

    with pytest.raises(S3ValidationSkip) as err:
        asyncio.run(reader.list_all_files_with_metadata("H"))

    assert "get-miner-list" in str(err.value)


def test_listing_skips_when_pagination_never_terminates(monkeypatch):
    """Always-truncated pages mean we never saw the end of the listing."""
    reader, calls = _reader(
        monkeypatch, [_Resp(text=_list_xml(["p.parquet"], truncated=True, token="t"))]
    )

    with pytest.raises(S3ValidationSkip) as err:
        asyncio.run(reader.list_all_files_with_metadata("H"))

    assert "incomplete" in str(err.value)
    assert calls["page"] == 200, "should stop at max_pages"


def test_a_genuinely_empty_miner_is_not_a_skip(monkeypatch):
    """An empty listing that COMPLETED is a real answer, and the caller is
    entitled to fail the miner for it. Only incomplete listings skip."""
    reader, _ = _reader(monkeypatch, [_Resp(text=_list_xml([], truncated=False))])

    assert asyncio.run(reader.list_all_files_with_metadata("H")) == []


@pytest.mark.parametrize(
    "namespace",
    [S3_NS, "", "http://doc.s3.cloudflare.com/2006-03-01/"],
    ids=["aws-namespace", "no-namespace", "other-namespace"],
)
def test_listing_parses_the_xml_whatever_namespace_it_arrives_in(monkeypatch, namespace):
    """Pinning the AWS namespace would match zero <Contents> if a backend ever
    emitted a different one — the miner would look empty and be failed."""
    reader, _ = _reader(
        monkeypatch,
        [_Resp(text=_list_xml(["a.parquet"], truncated=False, ns=namespace))],
    )

    files = asyncio.run(reader.list_all_files_with_metadata("H"))

    assert len(files) == 1
    assert files[0]["key"].endswith("a.parquet")
    assert files[0]["size"] == 100


# --------------------------------------------------------------------------- #
# The skip has to survive all the way to the scorer
# --------------------------------------------------------------------------- #
def _evaluator(monkeypatch, validate):
    """MinerEvaluator stub exposing just what _perform_s3_validation touches."""
    ev = object.__new__(MinerEvaluator)
    ev.uid = 89  # MACROCOSMOS_VALIDATOR_UID — runs validation locally
    ev.wallet = object()
    ev.s3_reader = object()
    ev.s3_results_client = None

    class _Cfg:
        s3_auth_url = "https://auth.test"

    ev.config = _Cfg()

    stored = []

    class _Storage:
        def update_validation_info(self, *args):
            stored.append(args)

    ev.s3_storage = _Storage()

    monkeypatch.setattr(s3_utils, "_S3_RETRY_BACKOFF", 0)
    monkeypatch.setattr(
        "vali_utils.miner_evaluator.validate_s3_miner_data", validate
    )
    monkeypatch.setattr(
        "vali_utils.miner_evaluator.get_s3_validation_summary", lambda r: "summary"
    )
    return ev, stored


def test_a_skip_leaves_the_miner_completely_unscored(monkeypatch):
    async def validate(*args, **kwargs):
        raise S3ValidationSkip("listing incomplete at page 8")

    ev, stored = _evaluator(monkeypatch, validate)

    result = asyncio.run(ev._perform_s3_validation(uid=7, hotkey="H", current_block=100))

    # eval_miner guards on `if s3_validation_result:`, so None means the scorer
    # is never told about S3 this round: no reward, no penalty, nothing decayed.
    assert result is None
    assert stored == [], "a skipped run must not advance validation bookkeeping"


def test_a_real_validation_error_still_fails_the_miner(monkeypatch):
    """Only transient/infrastructure faults skip. Everything else is unchanged."""

    async def validate(*args, **kwargs):
        raise ValueError("corrupt parquet footer")

    ev, stored = _evaluator(monkeypatch, validate)

    result = asyncio.run(ev._perform_s3_validation(uid=7, hotkey="H", current_block=100))

    assert result is not None
    assert result.is_valid is False
    assert stored, "a real result still updates validation bookkeeping"
