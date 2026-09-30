# AGENTS.md

Working rules for coding agents and contributors in Data Universe (Bittensor
Subnet 13). Facts below were checked against the code on 2026-10-01. If the
code and this file disagree, the code is right: fix this file in the same PR.

## Commands

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt pytest       # pytest is not in requirements

python -m pytest tests/ -q                   # full suite, ~20 s; tests are unittest-style
python -m pytest tests/vali_utils/ -q        # validator suite, run for any validator change
ruff check .                                 # config in pyproject.toml: py310, line length 88

python neurons/miner.py     --netuid 13 --subtensor.network finney --wallet.name W --wallet.hotkey H
python neurons/validator.py --netuid 13 --subtensor.network finney --wallet.name W --wallet.hotkey H
```

Definition of done for a code change: the relevant suite passes, any new
failure is either fixed or shown to be pre-existing on the base branch, and
the PR description states whether the change affects miners, validators, or
both. There is no CI; the suite runs on your machine only.

Known broken, do not "fix" in an unrelated PR: `tests/test_all.py`, and a
handful of tests in `tests/vali_utils/` and `tests/scraping/config/` that fail
on `main`.

## Layout

| Path | Owns |
|---|---|
| `neurons/miner.py`, `neurons/validator.py` | Node entry points and background loops |
| `common/` | `DataEntity`, `DataSource`, constants, API client (`api_client.py`) |
| `scraping/` | `Scraper` interface, `ScraperId` registry (`provider.py`), X and Reddit scrapers, coordinator, JSON config |
| `upload_utils/s3_uploader.py` | Miner bulk upload contract |
| `vali_utils/s3_utils.py` | Bulk (S3) validation, `DuckDBSampledValidator` |
| `vali_utils/on_demand/` | On-demand validation, output models, multipliers |
| `vali_utils/miner_evaluator.py` | Per-miner evaluation loop; wires S3 and on-demand results into the scorer |
| `rewards/miner_scorer.py` | Credibility, boosts, coverage multipliers, final score |
| `dynamic_desirability/` | Gravity job list (`total.json`) and validator voting |
| `tests/` | Mirrors the packages above |
| `docs/` | Authoritative docs, listed at the end |

Reports, dumps, logs and parquet files at the repo root are local working
material. Never read them as truth, extend them, or commit them.

## Two reward paths

Every miner is scored on both. They multiply, not add:

```
od  = ondemand_boosts × ondemand_credibility × od_coverage_mult
s3  = min(s3_boost, 2 × od) × s3_credibility ^ 2.5      # 0 when od <= 0
score = min(p2p_score, s3 + od) + s3 + od               # then 70% emission burn
```

A miner with no on-demand activity earns nothing from bulk data. A miner
with bulk data but no live serving is capped at twice its on-demand score.
`rewards/miner_scorer.py:202-247`.

### Bulk data (S3 path)

Miners upload one Parquet snapshot per Gravity job every 2 hours. One
designated validator (`MACROCOSMOS_VALIDATOR_UID`) runs the checks; the
others fetch its published result. A miner is re-validated when its result
is older than 600 blocks.

Upload contract, enforced in `vali_utils/s3_utils.py`:

- Filename `data_YYYYMMDD_HHMMSS_<rows>_<16hex>.parquet`; `<rows>` must equal
  the file's row count. Files under 15 KB or over 1 GB are skipped; any file
  under 50 bytes per row fails the miner outright.
- Snappy or another real codec; an `UNCOMPRESSED` column fails. Row groups
  of exactly 10,000 rows (last one shorter). Every page of every row group
  must decode; a decode error anywhere fails the miner.
- Exact column set per platform (`EXPECTED_COLUMNS_X`, 33 columns;
  `EXPECTED_COLUMNS_REDDIT`, 17). No optional columns. Extra or missing
  columns fail schema. `scraped_at` must be a string column.
- Only the newest file per job counts. Older files in the same job are
  ignored, so incremental uploads score only their last chunk.
- Only jobs present in `dynamic_desirability/total.json` count.

Checks on the sampled files, in order, with the constants that set them:
duplicate normalised URLs within a job (`MAX_DUPLICATE_RATE` 1%), empty
content (`MAX_EMPTY_RATE` 10%), X `view_count` present (`MIN_ENGAGEMENT_RATE`
95%), unique content (`MIN_UNIQUE_CONTENT_RATIO` 10%), transient read
failures (`MAX_TRANSIENT_SKIP_RATE` 30%), job match on label, keyword and
date window (`MIN_JOB_MATCH_RATE` 95%), live re-scrape of 20 sampled
entities (`MIN_SCRAPER_SUCCESS` 80%, overall and per platform with at least
`SCRAPER_PLATFORM_MIN_ENTITIES` 5). Sampling is committed: seeded from a
recent block hash plus the miner's file manifest, so a miner cannot predict
or replay the draw.

Effective size on pass:

```
rows = Σ over active jobs of min(rows_in_latest_file, job.max_rows)   # max_rows default 2,000,000
effective_size = rows × 300 bytes × coverage² × decode_ratio           # coverage = active jobs / expected jobs
s3_boost = effective_size² / Σ all miners' effective_size
```

Credibility: `cred = α + (1−α)·cred` on pass, `cred × (1−α)` on fail,
α = 0.30, start 0.1. Passing never lowers credibility.

### Live queries (on-demand path)

This is the path customers actually pay for and the one most often broken by
a change elsewhere. Read `vali_utils/on_demand/` and the OD sections of
`vali_utils/miner_evaluator.py` before touching either side.

Miner side, `neurons/miner.py` (`run_on_demand`, `poll_on_demand_active_jobs`,
`scrape_on_demand_job`, `loop_poll_on_demand_active_jobs`):

1. Poll `POST /on-demand/miner/jobs/active` every 5 s for jobs created in the
   last 2 minutes. Seen ids are kept in an in-memory LRU; nothing on disk.
2. Job payload: `platform` (`x` or `reddit`), `usernames` (≤10), `keywords`
   (≤5), `url` (X only, exclusive with the others), `subreddit` (Reddit),
   `start_date`, `end_date`, `limit` (1..1000, default 100), `keyword_mode`
   (`any`|`all`), `expire_at`. Jobs within 5 s of expiry are dropped.
3. Scrape with `ApiDojoTwitterScraper.on_demand_scrape` or
   `RedditJsonScraper.on_demand_scrape`, default window last 24 h, truncate
   to `limit`.
4. Empty results are not submitted. Otherwise `POST /on-demand/miner/jobs/submit`
   returns a presigned URL and the miner uploads
   `{"data_entities": [DataEntity...]}` as JSON, each entity's `content` a
   serialised `XContent` or `RedditContent`.

Miners may replace the scrapers in step 3 with their own. Steps 1, 2 and 4
are the contract. There is no `handle_on_demand`; `docs/on_demand.md` is out
of date on that and on the reward description.

Validator side, per miner on its hourly evaluation (`_evaluate_od`):

1. List that miner's submissions for jobs that expired since the last
   evaluation, within a 3 h window, up to 1,000.
2. Zero-byte submissions earn nothing and cost nothing. Up to
   `OD_MAX_JOBS_TO_VALIDATE` (3) non-empty ones are sampled at random.
3. Per sampled submission: download; `OD_SCHEMA_SAMPLE_SIZE` (5) entities are
   checked for type, uri, in-response duplicate uris, model parse, and that
   they match the request's usernames, keywords, keyword mode, dates and url.
   Then exactly one entity is re-fetched from the live source. A payload
   with an empty `data_entities` list is checked with a `limit=1` probe:
   if the source has data, it fails.
4. Verdicts: pass, fail, or neutral. Scraper outage, unfetchable content,
   5xx on download and exceptions are neutral: no reward, no penalty.
   Never turn a validator-side failure into a miner penalty.
5. Unsampled non-empty submissions get a small credibility bump.

Rewards (`rewards/miner_scorer.py`, `vali_utils/on_demand/on_demand_validation.py`):

```
speed  = clamp(0.5 ^ ((t − 30 s) / 45 s), 0.3, 1.0)         # t = submitted_at − created_at
volume = (rows/limit)^1.3 if rows ≤ limit, else 1 + min(0.25, 0.15·ln(1 + excess/limit))
pass:  boosts = 0.3·(1e8·speed·volume) + 0.7·boosts;  cred = min(1, 0.02 + 0.98·cred)
fail:  boosts ×= 0.7;  cred −= 0.05
```

Coverage multiplier, per platform, from API stats of "doable" jobs (≥5
submitters) in the window: zero submissions with ≥3 doable jobs is
abstention, `mult × 0.3`; with ≥20 doable jobs, submitting fewer than 15% of
them scales `mult` down to a floor of 0.3. Improvements apply at once, drops
move halfway per evaluation, and a stats-fetch failure relaxes toward 1.

Not in the live path, despite what older docs and dead code suggest: no
cross-miner pooling or dedup, no byte-identity or padding detection, no
stake lottery. `vali_utils/on_demand/od_job_cache.py` and the consensus
functions in `on_demand_validation.py` are unreferenced.

## Miner scraping template

`scraping/scraper.py` defines two abstract methods, `scrape` and `validate`.
Register new scrapers in `ScraperId` and `scraping/provider.py`. The
coordinator reads `scraping/config/scraping_config.json`, picks one random
label per entry each cadence, and stores into SQLite (`--neuron.database_name`,
size hint `--neuron.max_database_size_gb_hint`, default 250 GB).

Content rules every miner must satisfy regardless of scraper, from
`scraping/x/utils.py` and `scraping/reddit/utils.py`: `scraped_at` present,
truncated to the minute, not before the post, not in the future;
`created_at` minute-truncated on Reddit; X urls on `x.com` not `twitter.com`;
Reddit `media` present; engagement counts within the age-scaled tolerance
(100% under 1 h, down to 20% after 7 days).

Known bug on `main`: the checked-in `scraping_config.json` still lists a
YouTube scraper that no longer exists, so a miner started with default flags
fails config validation. Fix the config, not the enum.

## Working here

- **Branches.** Feature branches → `dev` → release PR to `main` → tag
  `vX.Y.Z`. Validators auto-update from `main` by commit hash every 15 min
  (`scripts/start_validator.py`). There is no miner auto-updater: every
  miner-facing change needs a `docs/miner.md` update and an announcement.
- **`neurons/__init__.py` `__version__` is stale** (1.3.8 vs tags 1.18.x). It
  only feeds `version_key` in `set_weights`. Do not "fix" it casually; it
  changes the weight-set key.
- **State resets.** `MinerScorer.STATE_VERSION` gates score resets. Bump it
  only when a scoring change makes old state meaningless, in its own PR.
- **Thresholds and constants** in `common/constants.py`, `rewards/miner_scorer.py`
  and the class constants of `vali_utils/s3_utils.py` and `miner_evaluator.py`
  change every miner's income. One PR per threshold change, with the share
  of current miners affected in the description.
- **Fail closed on the validator side** for anything the validator cannot
  read or verify about a miner's file. **Stay neutral** for anything that is
  the validator's own failure. Every silent skip in this repo has become an
  exploit; every over-eager penalty has hit honest miners.
- **Tests build real Parquet on disk** and call the real validator methods.
  Follow that pattern; do not mock DuckDB.
- **Never commit** `.env`, keys, wallet files, parquet data, logs, or a
  specific miner hotkey outside a test fixture. Never print secrets in logs
  or PR text.

## Authoritative docs

`docs/miner.md`, `docs/validator.md` (running nodes) · `docs/s3_validation.md`
(bulk contract) · `docs/scoring.md` (credibility and weights) ·
`docs/on_demand.md` (request models; reward text is stale) ·
`docs/dynamic_desirability.md` (Gravity jobs) · `docs/miner_policy.md`
(content and data-protection obligations).
