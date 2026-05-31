import sys, asyncio
sys.path.insert(0, '/tmp/du-pr-805')
import vali_utils.validator_s3_access as vsa
from vali_utils.validator_s3_access import ValidatorS3Access, S3InfraError

HK = "HK"

def xml_page(keys, is_truncated, token=None):
    contents = "".join(
        f'<Contents><Key>data/hotkey={HK}/job_id=1/{k}.parquet</Key>'
        f'<Size>20000</Size><LastModified>2026-01-01</LastModified></Contents>'
        for k in keys
    )
    trunc = 'true' if is_truncated else 'false'
    tok = f'<NextContinuationToken>{token}</NextContinuationToken>' if token else ''
    return (f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f'{contents}<IsTruncated>{trunc}</IsTruncated>{tok}</ListBucketResult>')

class FakeResp:
    def __init__(self, status, text):
        self.status_code = status
        self.text = text

_gseq = {'seq': []}
vsa.requests.get = lambda url, timeout=None: _gseq['seq'].pop(0)

def make_obj(presign_seq):
    o = object.__new__(ValidatorS3Access)
    pseq = list(presign_seq)
    async def _presign(hk, tok=None):
        return pseq.pop(0)
    o._request_presigned_list_url = _presign
    return o

async def run_case(name, presign_seq, get_seq, expect):
    o = make_obj(presign_seq)
    _gseq['seq'] = list(get_seq)
    try:
        files = await o.list_all_files_with_metadata(HK)
        outcome = ('return', len(files))
    except S3InfraError:
        outcome = ('raise', 'S3InfraError')
    except Exception as e:
        outcome = ('raise', type(e).__name__)
    ok = (outcome[0] == expect[0]) and (expect[1] is None or outcome[1] == expect[1])
    print(f"{'PASS' if ok else 'FAIL'} | {name}: got {outcome}, expected {expect}")
    return ok

async def main():
    oks = []
    # A: API unreachable on page 1 (presign None) -> infra raise (NOT empty return)
    oks.append(await run_case("A API-unreachable->raise", [None], [], ('raise','S3InfraError')))
    # B: genuine empty listing (200, IsTruncated=false, 0 files) -> return [] (NOT raise)
    oks.append(await run_case("B genuine-empty->return[]", ["u1"], [FakeResp(200, xml_page([], False))], ('return',0)))
    # C: normal 2 files, complete -> return 2
    oks.append(await run_case("C normal-2files->return2", ["u1"], [FakeResp(200, xml_page(['a','b'], False))], ('return',2)))
    # D: partial pagination (page1 truncated=true, page2 presign fails) -> infra raise  [Gemini-caught case]
    oks.append(await run_case("D partial-pagination->raise", ["u1", None],
                              [FakeResp(200, xml_page(['a'], True, 'TOK'))], ('raise','S3InfraError')))
    # E: 503 on page1 -> infra raise
    oks.append(await run_case("E 503->raise", ["u1"], [FakeResp(503, 'err')], ('raise','S3InfraError')))
    # F: unparseable XML on page1 -> infra raise
    oks.append(await run_case("F bad-xml->raise", ["u1"], [FakeResp(200, 'NOT XML <')], ('raise','S3InfraError')))
    print(f"\nRESULT: {sum(oks)}/{len(oks)} passed")
    sys.exit(0 if all(oks) else 1)

asyncio.run(main())
