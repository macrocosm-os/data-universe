import base64
from datetime import datetime, timedelta, timezone
import json
import uuid
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from substrateinterface.keypair import Keypair
import httpx


class OnDemandJobPayloadX(BaseModel):
    model_config = ConfigDict(extra="allow")

    platform: Literal["x"] = "x"

    usernames: Optional[List[str]] = None
    keywords: Optional[List[str]] = None


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
    submitted_at: Optional[datetime]


class ListActiveJobsRequest(BaseModel):
    since: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc) - timedelta(minutes=2)
    )


class ListActiveJobsResponse(BaseModel):
    jobs: List[OnDemandJob]


class SubmitJobRequest(BaseModel):
    submission: OnDemandJobSubmission


class SubmitOnDemandJobResponse(BaseModel):
    presigned_upload_url: str


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


class OnDemandClient:
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

    async def __aenter__(self) -> "OnDemandClient":
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
        self, *, job_id: str, data: Any
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
        url = submit_resp.presigned_upload_url
        if not url:
            raise RuntimeError("Server did not return a presigned_upload_url.")

        client = self._ensure_client()
        put_resp = await client.put(url, content=json.dumps(data))
        _raise_for_status(put_resp)

        return submit_resp

    async def validator_list_jobs_with_submissions(
        self, req: ListJobsWithSubmissionsForValidationRequest
    ) -> ListJobsWithSubmissionForValidationResponse:

        if not self._signer:
            raise RuntimeError("validator_keypair was not provided.")

        payload_json = req.model_dump_json()
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
    ) -> tuple[ListJobsWithSubmissionForValidationResponse, list[dict]]:
        """
        Calls /on-demand/validator/jobs and concurrently downloads the JSON bodies
        of all returned submissions via their presigned URLs.

        Returns:
            (validator_response, downloads)
            - validator_response: your ListJobsWithSubmissionForValidationResponse
            - downloads: list of dicts with:
                {
                  "job_id": str,
                  "miner_hotkey": str | None,
                  "s3_path": str | None,
                  "s3_last_modified": str | None,
                  "s3_etag": str | None,
                  "s3_content_length": int | None,
                  "url": str,
                  "status": int,
                  "ok": bool,
                  "error": str | None,
                  "data": Any  # parsed JSON on success
                }
        Notes:
            - Skips submissions without a usable 's3_presigned_url'.
            - Respects `max_parallelism` using an asyncio.Semaphore.
            - Uses the same underlying httpx.AsyncClient (inherits timeout/verify).
        """
        # First, list jobs with submissions
        validator_resp = await self.validator_list_jobs_with_submissions(req)

        client = self._ensure_client()
        sem = asyncio.Semaphore(max_parallelism)
        tasks: list[asyncio.Task] = []

        async def _fetch_one(job_id: str, sub: OnDemandJobSubmission) -> dict:
            url = getattr(sub, "s3_presigned_url", None)
            if not url:
                return {
                    "job_id": job_id,
                    "miner_hotkey": getattr(sub, "miner_hotkey", None),
                    "s3_path": getattr(sub, "s3_path", None),
                    "s3_last_modified": getattr(sub, "s3_last_modified", None),
                    "s3_etag": getattr(sub, "s3_etag", None),
                    "s3_content_length": getattr(sub, "s3_content_length", None),
                    "url": None,
                    "status": 0,
                    "ok": False,
                    "error": "missing presigned url",
                    "data": None,
                }

            async with sem:
                try:
                    r = await client.get(url)
                    status = r.status_code
                    if 200 <= status < 300:
                        # Try JSON first; if it fails, attempt to parse text as JSON.
                        try:
                            data = r.json()
                        except Exception:
                            data = json.loads(r.text)
                        return {
                            "job_id": job_id,
                            "miner_hotkey": getattr(sub, "miner_hotkey", None),
                            "s3_path": getattr(sub, "s3_path", None),
                            "s3_last_modified": getattr(sub, "s3_last_modified", None),
                            "s3_etag": getattr(sub, "s3_etag", None),
                            "s3_content_length": getattr(sub, "s3_content_length", None),
                            "url": url,
                            "status": status,
                            "ok": True,
                            "error": None,
                            "data": data,
                        }
                    else:
                        return {
                            "job_id": job_id,
                            "miner_hotkey": getattr(sub, "miner_hotkey", None),
                            "s3_path": getattr(sub, "s3_path", None),
                            "s3_last_modified": getattr(sub, "s3_last_modified", None),
                            "s3_etag": getattr(sub, "s3_etag", None),
                            "s3_content_length": getattr(sub, "s3_content_length", None),
                            "url": url,
                            "status": status,
                            "ok": False,
                            "error": r.text,
                            "data": None,
                        }
                except Exception as e:
                    return {
                        "job_id": job_id,
                        "miner_hotkey": getattr(sub, "miner_hotkey", None),
                        "s3_path": getattr(sub, "s3_path", None),
                        "s3_last_modified": getattr(sub, "s3_last_modified", None),
                        "s3_etag": getattr(sub, "s3_etag", None),
                        "s3_content_length": getattr(sub, "s3_content_length", None),
                        "url": url,
                        "status": 0,
                        "ok": False,
                        "error": str(e),
                        "data": None,
                    }

        # Schedule downloads
        for jws in validator_resp.jobs_with_submissions:
            job_id = jws.job.id
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
