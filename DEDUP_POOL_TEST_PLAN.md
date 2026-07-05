# Dedup Worker Pool — Test & Deploy Runbook

**What's being tested:** branch `feat/dedup-worker-pool` (local-only, NOT pushed) —
the URL-dedup loop moves from eval threads into a pool of worker subprocesses
(GIL fix). It carries `be760de` (the pool) + this runbook/smoke test on top of
`ad808af` (the deployed cache). `feat/s3-download-local-validate` stays at
`ad808af`, matching remote. If everything passes: merge
`feat/dedup-worker-pool` → `feat/s3-download-local-validate` → `dev`.
Full background: `VALIDATION_SPEED_PLAN.md`.

**Make sure this branch is checked out before scp'ing anything:**
```bash
cd ~/data-universe && git checkout feat/dedup-worker-pool
```

**Files involved:** `vali_utils/url_normalizer.py` (new), `vali_utils/dedup_worker.py`
(new), `vali_utils/dedup_pool.py` (new), `vali_utils/s3_utils.py` (modified),
`scripts/diag_cache_equiv.py` (new `--compare pool` mode),
`scripts/smoke_dedup_pool.py` (untracked smoke test).

**Golden rule:** the live validator is never touched until Phase 4. Phases 1–3
run from `~/testing_playground` so a pm2 restart can't accidentally pick up
untested code.

---

## Already done — local real-data battery (Jul 4, all passed)

Ran inline-vs-pool on every REAL data file available on the dev machine:

| file | rows | dups | match |
|---|---|---|---|
| real 2M-row miner upload (data_20260326_..., 200 row groups) | 2,000,000 | 995 | ✅ exact |
| bittensor_x_deduped | 894,938 | 844,047 (94% dups!) | ✅ exact |
| bittensor_reddit_deduped | 291,431 | 0 | ✅ exact |
| 2 real crawler files | 3,048,300 / 3,049,383 | ~3.04M each | ✅ exact |
| + 2 smaller real miner files | | | ✅ exact |

Edge cases (both paths byte-identical, incl. raised exception TYPES):
empty file, 1 row, exact batch boundary, unicode/emoji/RTL/NUL urls,
bytes-typed url column, missing url column (both raise BinderException —
subclass fidelity proven live), uppercase column name, file deleted
mid-scan (POSIX fd survives). Soak: 200 scans, zero fd drift, worker RSS
stable (~1.1GB total after 3M-row scans). Stress: 15 concurrent scans over
real files, all counts == sequential reference, 6.7s wall.

Remaining risk the host phases cover: the full validate_miner_s3_data
integration (metadata/schema/budget wiring around the scan) on live miners,
and Linux-vs-macOS process behavior.

---

## Phase 0 — copy to the host playground (2 min, zero risk)

From the Mac:

```bash
scp vali_utils/url_normalizer.py vali_utils/dedup_worker.py vali_utils/dedup_pool.py \
    vali_utils/s3_utils.py \
    root@<host>:~/testing_playground/vali_utils/
scp scripts/diag_cache_equiv.py scripts/smoke_dedup_pool.py \
    root@<host>:~/testing_playground/scripts/
```

On the host, sanity-check the copy landed (must print the pool constant):

```bash
grep -c "DEDUP_POOL_ENABLED" ~/testing_playground/vali_utils/s3_utils.py   # expect >= 2
```

---

## Phase 1 — smoke test (2 min, no wallet, no network)

```bash
cd ~/testing_playground && source /root/data-universe/venv/bin/activate
python scripts/smoke_dedup_pool.py
```

**PASS looks like:**
- `s3_utils: /root/testing_playground/vali_utils/s3_utils.py` ← the playground copy, not the repo one
- `1. correctness: OK` — pool counts == inline counts
- `2. corrupt->fail-closed: OK`
- `3. concurrency 5 threads: ... -> ~4-6x on this host` ← the GIL fix working on the host's cores

**If it fails here, stop** — nothing else is worth running. Likely causes:
worker spawn blocked (check `pgrep -f dedup_worker` during the run), or the
wrong `s3_utils.py` was imported (line 1 of the output tells you).

---

## Phase 1b — SEE the production speedup (~5 min, no wallet, no network)

Simulates the exact production shape: 5 concurrent eval threads each
dedup-scanning a 2M-row file, inline vs pool, with per-file `dedup=` lines
in the same format as the pm2 logs:

```bash
python scripts/sim_prod_concurrency.py
```

Reference result (dev Mac): solo=6.3s; inline ×5 threads = **36–37s per file**
(GIL queueing, ×5.8 the solo cost — this is production's `dedup=47–104s`);
pool ×5 threads = **6.8s per file**, 5.4× wall speedup, counts identical on
every file. Host numbers will differ in absolute terms, same shape.

---

## Phase 2 — real-miner verdict equivalence (~35–45 min, the decisive test)

Runs the FULL production validation twice per miner on a frozen file snapshot:
once with the inline loop (today's code path), once with the pool. Every
result field is diffed. Scraper API is stubbed (no Apify cost); DD list is
pulled fresh from the API.

```bash
cd ~/testing_playground
nohup python -u scripts/diag_cache_equiv.py \
    --wallet.name cfusion --wallet.hotkey v3 \
    --compare pool --top_n 3 --skip_uids 162 \
    > pool_equiv.log 2>&1 &
tail -f pool_equiv.log        # Ctrl-C detaches the tail, run keeps going
```

**PASS criteria (all three required):**
1. Per miner: `-> PASS`, and final line `PASS on 3/3 miners`
2. Summary table: `diffs=0` for every miner (display-only `info:` lines are fine)
3. Both runs per miner show `0 schema_fail, 0 page_decode_fail` in the
   `DuckDB phase DONE` lines (no spurious failures introduced by the pool)

**Do NOT judge speed here.** This test runs one miner at a time; solo, pool ≈
inline (±10%). The speedup only exists under concurrency (Phase 5).

**If a miner shows `DIFF`:** save the log, do not deploy, send me the
`*** N VERDICT FIELD(S) DIFFER ***` block. The frozen snapshot rules out
live-upload drift, so a strict diff here is a real bug.

**If a run dies mid-way:** check `pgrep -f dedup_worker` for stuck workers,
`kill` them, and rerun — the diag is stateless.

---

## Phase 3 — decision gate

Deploy only if: Phase 1 PASS + Phase 2 `PASS on 3/3, diffs=0`.
Anything else → stop, collect logs, we debug before any deploy.

---

## Phase 4 — deploy (only after the gate)

1. On the Mac — merge the test branch into the main working branch and push
   (everything is local-only right now):
   ```bash
   cd ~/data-universe
   git checkout feat/s3-download-local-validate
   git merge --no-ff feat/dedup-worker-pool
   git push origin feat/s3-download-local-validate
   ```
2. On the host:
   ```bash
   cd ~/data-universe && git pull && git log --oneline -2   # expect the merge + be760de
   pm2 restart 10 --update-env
   ```
3. First 10 minutes:
   ```bash
   pm2 logs 10 --lines 40 --nostream        # normal startup, batch starting
   pgrep -fc "vali_utils.dedup_worker"      # expect up to 12 once the first
                                            # dedup scan runs (lazy start)
   ```

---

## Phase 5 — post-deploy verification (the speed proof)

**Within the first eval batch (~15 min):** per-file timing lines are the
whole story. Compare `dedup=` before vs after:

```bash
pm2 logs 10 --out --nostream --lines 300 | grep "timing: dl=" | tail -10
```
- Before (5 concurrent, GIL-bound): `dedup=47–104s` on 2M-row files
- After (pool): expect `dedup=15–25s` on the same class of files

**Within the first hours:**
```bash
# batch wall time should drop toward ~8-10 min:
pm2 logs 10 --out --nostream --lines 2000 | grep "Running validation on the following batch" | tail -5
# memory sanity — validator RSS + ~12 workers ~25MB each:
pm2 ls; ps -o rss= -p $(pgrep -f dedup_worker | tr '\n' ',' | sed 's/,$//') | awk '{s+=$1} END {print s/1024 " MB workers total"}'
# no new errors:
pm2 logs 10 --err --nostream --lines 100 | grep -vE "wandb|Invalid HTTP" | tail -20
```

**Next morning:** the ledger — no new self-exits:
```bash
grep -E "exited with code|by signal" ~/.pm2/pm2.log | tail -5
```

---

## Rollback (no code edit needed)

```bash
# in the pm2 env for process 10:
DEDUP_POOL_ENABLED=false pm2 restart 10 --update-env
```
That reverts to the inline dedup path (retained verbatim). Full revert:
`git revert be760de` + restart.

---

## Phase 6 — after 24h clean: raise concurrency

`miner_evaluator.py`: `miners_to_eval = 5` → `10`. Watch for a day:
`dl=` times (bandwidth contention is the next ceiling), batch wall time
vs the 80-min watchdog, disk in `.s3_validation_tmp` (~3GB × concurrent
evals). If clean → consider 15. Expected end state: full metagraph pass
~17h → ~2.5–3.5h.
