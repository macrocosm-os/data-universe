import asyncio
import base64
from datetime import datetime, timedelta, timezone
import json
import uuid
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Dict, List, Literal, Optional, Set, Union

from pydantic import BaseModel, ConfigDict, Field
from common.data import DataEntity
from substrateinterface.keypair import Keypair
import httpx

import bittensor as bt

TEN_MB_BYTES = 10 * 1_000_000

class OnDemandMinerUpload(BaseModel):
    data_entities: List[DataEntity]

    def model_dump(self, **kwargs):
        """
        Override the dump method to use DataEntity.to_json_dict
        for each DataEntity object.
        """

        base_dump = {} # super().model_dump(**kwargs)

        base_dump["data_entities"] = [
            entity.to_json_dict() for entity in self.data_entities
        ]
        return base_dump

    def model_dump_json(self, **kwargs):
        return json.dumps(self.model_dump(**kwargs))

    @classmethod
    def model_validate(cls, obj, **kwargs) -> "OnDemandMinerUpload":
        return OnDemandMinerUpload(data_entities=[DataEntity.from_json_dict(entity_dict) for entity_dict in obj['data_entities']])


class OnDemandJobPayloadX(BaseModel):
    model_config = ConfigDict(extra="allow")

    platform: Literal["x"] = "x"

    usernames: Optional[List[str]] = None
    keywords: Optional[List[str]] = None
    url: Optional[str] = None


class OnDemandJobPayloadReddit(BaseModel):
    model_config = ConfigDict(extra="allow")

    platform: Literal["reddit"] = "reddit"

    subreddit: Optional[str] = None

    usernames: Optional[List[str]] = None
    keywords: Optional[List[str]] = None


class OnDemandJobPayloadYoutube(BaseModel):
    model_config = ConfigDict(extra="allow")

    platform: Literal["youtube"] = "youtube"

    channels: Optional[List[str]] = None
    url: Optional[str] = None


class OnDemandJob(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = ""

    created_at: Optional[datetime] = None
    expire_at: Optional[datetime] = None

    job: Union[
        OnDemandJobPayloadX, OnDemandJobPayloadReddit, OnDemandJobPayloadYoutube
    ] = Field(discriminator="platform")

    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    limit: int = Field(default=100, ge=1, le=1000)
    keyword_mode: Literal["any"] | Literal["all"] = "any"


class OnDemandJobSubmission(BaseModel):
    model_config = ConfigDict(extra="allow")

    job_id: str
    miner_hotkey: str = ""
    miner_incentive: float = 0.0

    s3_path: Optional[str] = None
    s3_presigned_url: Optional[str] = None

    s3_etag: Optional[str] = None
    s3_content_length: Optional[int] = None
    s3_last_modified: Optional[datetime] = None
    submitted_at: Optional[datetime] = None


class ListActiveJobsRequest(BaseModel):
    since: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc) - timedelta(minutes=2)
    )


class ListActiveJobsResponse(BaseModel):
    jobs: List[OnDemandJob]


class SubmitJobRequest(BaseModel):
    submission: OnDemandJobSubmission


class SubmitOnDemandJobResponse(BaseModel):
    presigned_post_upload_data: dict


class GetJobSubmissionsResponse(BaseModel):
    submissions: List[OnDemandJobSubmission]


class ListJobsWithSubmissionsForValidationRequest(BaseModel):
    expired_since: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc) - timedelta(minutes=1)
    )
    expired_until: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    limit: int = Field(default=10, ge=1, le=10)


class JobWithSubmissions(BaseModel):
    job: OnDemandJob
    submissions: List[OnDemandJobSubmission]


class ListJobsWithSubmissionForValidationResponse(BaseModel):
    jobs_with_submissions: List[JobWithSubmissions]


def _sha256_hex(data: bytes) -> str:
    return sha256(data).hexdigest()


@dataclass
class TaoSigner:
    """
    Produces BitTensor Tao v2 auth headers for a given Keypair.

    Signed message:
        sha256(body).hexdigest() + "." + nonce + "." + timestamp_ms
    Signature is base64-encoded.
    """

    keypair: Keypair

    def headers(self, body: bytes) -> Dict[str, str]:
        ts = str(int(round(time.time() * 1000)))
        nonce = str(uuid.uuid4())
        message = f"{_sha256_hex(body)}.{nonce}.{ts}"
        sig_bytes = self.keypair.sign(message)
        sig_b64 = base64.b64encode(sig_bytes).decode("ascii")
        return {
            "X-Tao-Version": "2",
            "X-Tao-Timestamp": ts,
            "X-Tao-Nonce": nonce,
            "X-Tao-Signed-By": self.keypair.ss58_address,
            "X-Tao-Signature": sig_b64,
        }


class DataUniverseApiClient:
    """
    Async client for:
      - POST /on-demand/miner/jobs/active
      - POST /on-demand/miner/jobs/submit
      - POST /on-demand/validator/jobs

    Parameters
    ----------
    base_url : str
        API base (e.g. "http://localhost:8000" or "https://api.example.com").
    keypair : Keypair
        hotkey keypair

    timeout : float
        HTTP timeout in seconds.
    verify_ssl : bool
        TLS verification for HTTPS requests (also used for S3 PUTs).

    Swagger Docs: https://data-universe-api.api.macrocosmos.ai/docs
    """

    def __init__(
        self,
        base_url: str,
        *,
        keypair: Optional[Keypair] = None,
        timeout: float = 30.0,
        verify_ssl: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._signer = TaoSigner(keypair) if keypair else None

        self._timeout = timeout
        self._verify_ssl = verify_ssl
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "DataUniverseApiClient":
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self._timeout,
            verify=self._verify_ssl,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client:
            await self._client.aclose()
        self._client = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError(
                "Use 'async with OnDemandClient(...)' or call __aenter__ manually."
            )
        return self._client

    async def _post_signed_json(
        self, path: str, signer: TaoSigner, payload_json: str
    ) -> httpx.Response:
        """
        Sends POST with a pre-serialized JSON string (so the exact bytes are used for the signature).
        """
        client = self._ensure_client()
        body_bytes = payload_json.encode("utf-8")
        headers = signer.headers(body=body_bytes)
        return await client.post(path, content=body_bytes, headers=headers)

    async def miner_list_active_jobs(
        self, req: ListActiveJobsRequest
    ) -> ListActiveJobsResponse:
        """
        POST /on-demand/miner/jobs/active
        """
        if not self._signer:
            raise RuntimeError("miner_keypair was not provided.")
        # Use Pydantic to serialize exactly as your server expects
        payload_json = req.model_dump_json()
        resp = await self._post_signed_json(
            "/on-demand/miner/jobs/active", self._signer, payload_json
        )
        _raise_for_status(resp)
        # Parse back into your response model
        return ListActiveJobsResponse.model_validate_json(resp.text)

    async def miner_submit_job(
        self, req: SubmitJobRequest
    ) -> SubmitOnDemandJobResponse:
        """
        POST /on-demand/miner/jobs/submit
        Returns the presigned_upload_url (and related fields) in SubmitOnDemandJobResponse.
        """

        if not self._signer:
            raise RuntimeError("miner_keypair was not provided.")
        payload_json = req.model_dump_json()
        resp = await self._post_signed_json(
            "/on-demand/miner/jobs/submit", self._signer, payload_json
        )
        _raise_for_status(resp)
        return SubmitOnDemandJobResponse.model_validate_json(resp.text)

    async def miner_submit_and_upload(
        self, *, job_id: str, data: OnDemandMinerUpload
    ) -> SubmitOnDemandJobResponse:
        """
        Convenience:
          1) POST /on-demand/miner/jobs/submit to get a presigned_upload_url
          2) PUT the provided `data` to that URL (S3)

        `data` needs to be a json dumpable object

        Returns the SubmitOnDemandJobResponse from step (1).
        """
        submit_req = SubmitJobRequest(submission=OnDemandJobSubmission(job_id=job_id))

        submit_resp = await self.miner_submit_job(submit_req)
        presigned = submit_resp.presigned_post_upload_data
        if not presigned or "url" not in presigned or "fields" not in presigned:
            raise RuntimeError("Server did not return a valid presigned_post_upload_data (missing url/fields).")

        # Build the multipart request:
        # - All fields from the signer must be included as regular form fields
        # - The payload goes in the "file" part
        fields = dict(presigned["fields"])  # policy, signature, key, Content-Type, etc.

        # If signer pinned Content-Type, mirror it on the file part; otherwise default to JSON
        content_type = fields.get("Content-Type", "application/json")
        files = {
            "file": ("data.json", data.model_dump_json(), content_type),
        }

        client = self._ensure_client()
        post_resp = await client.post(presigned["url"], data=fields, files=files)

        # S3 presigned POST usually returns 204; some setups return 201/200
        if post_resp.status_code not in (204, 201, 200):
            raise RuntimeError(f"Upload failed: {post_resp.status_code} {post_resp.text}")

        return submit_resp


    async def validator_list_jobs_with_submissions(
        self, req: ListJobsWithSubmissionsForValidationRequest
    ) -> ListJobsWithSubmissionForValidationResponse:

        if not self._signer:
            raise RuntimeError("validator_keypair was not provided.")

        payload_json = req.model_dump_json()
        bt.logging.debug(f"validator_list_jobs_with_submissions payload: {payload_json}")
        resp = await self._post_signed_json(
            "/on-demand/validator/jobs", self._signer, payload_json
        )
        _raise_for_status(resp)
        return ListJobsWithSubmissionForValidationResponse.model_validate_json(
            resp.text
        )

    async def validator_list_and_download_submission_json(
        self,
        req: ListJobsWithSubmissionsForValidationRequest,
        *,
        max_parallelism: int = 8,
        job_ids_to_skip_downloading: Optional[Set[str]] = None,
        file_size_limit_bytes: int = TEN_MB_BYTES,
    ) -> tuple[ListJobsWithSubmissionForValidationResponse, list[dict]]:
        job_ids_to_skip_downloading = job_ids_to_skip_downloading or set()

        validator_resp = await self.validator_list_jobs_with_submissions(req)
        bt.logging.debug(f"Validator job list with submissions:\n\n{validator_resp}")

        client = self._ensure_client()
        sem = asyncio.Semaphore(max_parallelism)
        tasks: list[asyncio.Task] = []

        async def _fetch_one(job_id: str, sub: OnDemandJobSubmission):
            # Pull fields via getattr only
            miner_hotkey = getattr(sub, "miner_hotkey", None)
            s3_path = getattr(sub, "s3_path", None)
            s3_last_modified = getattr(sub, "s3_last_modified", None)
            s3_etag = getattr(sub, "s3_etag", None)
            length = getattr(sub, "s3_content_length", None)
            url = getattr(sub, "s3_presigned_url", None)

            def base(ok: bool, status: int = 0, error: str | None = None, data=None, url_out=None):
                return {
                    "job_id": job_id,
                    "miner_hotkey": miner_hotkey,
                    "s3_path": s3_path,
                    "s3_last_modified": s3_last_modified,
                    "s3_etag": s3_etag,
                    "s3_content_length": length,
                    "url": url_out if ok else None,
                    "status": status,
                    "ok": ok,
                    "error": error,
                    "data": data,
                }

            if length is None:
                return base(False, error="missing s3 content length")
            try:
                length_int = int(length)
            except Exception:
                return base(False, error=f"invalid s3 content length: {length!r}")
            if length_int > file_size_limit_bytes:
                return base(False, error="file size above limit")
            if not url:
                return base(False, error="missing presigned url")

            async with sem:
                try:
                    r = await client.get(url, follow_redirects=True, timeout=60.0)
                    status = r.status_code
                    if 200 <= status < 300:
                        try:
                            data = r.json()
                        except Exception:
                            data = json.loads(r.text)
                        return base(True, status=status, data=data, url_out=str(r.url))
                    
                    body_snip = r.text[:500]
                    return base(False, status=status, error=body_snip)
                except Exception as e:
                    return base(False, error=str(e))

        for jws in validator_resp.jobs_with_submissions:
            job_id = jws.job.id
            if job_id in job_ids_to_skip_downloading:
                continue
            for sub in jws.submissions:
                tasks.append(asyncio.create_task(_fetch_one(job_id, sub)))

        downloads: list[dict] = []
        if tasks:
            for coro in asyncio.as_completed(tasks):
                downloads.append(await coro)

        return validator_resp, downloads

def _raise_for_status(resp: httpx.Response) -> None:
    if 200 <= resp.status_code < 300:
        return

    try:
        detail = resp.json()
    except Exception:
        detail = resp.text
    raise httpx.HTTPStatusError(
        f"HTTP {resp.status_code} for {resp.request.method} {resp.request.url}. Response: {detail!r}",
        request=resp.request,
        response=resp,
    )
