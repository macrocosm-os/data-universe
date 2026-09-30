# AGENTS.md

Guidance for coding agents and new contributors working in this repository.
Keep this file short, factual and current. When something here stops being
true, fix it in the same PR that changed the behaviour.

## What this repo is

Data Universe is Bittensor Subnet 13. Miners scrape public social data
(X/Twitter and Reddit today) and upload it as Parquet files to shared object
storage. Validators sample that data, verify it against the live source, and
set on-chain weights that decide miner rewards. Customers consume the data
through Gravity (scheduled jobs) and on-demand queries served by the same
miners.

Two independent reward paths exist and both run in `neurons/validator.py`:

- **S3 path (bulk data).** Miners upload whole-job snapshots; validators
  score size, coverage and quality of the latest file per job.
- **On-demand path (live queries).** Validators forward customer requests
  to miners, pool the responses, and score speed, coverage and validity.

The legacy P2P index protocol (`common/protocol.py`) is still in the code but
no longer carries the main reward.

## Repository map

| Path | What lives there |
|---|---|
| `neurons/` | `miner.py` and `validator.py` entry points, `__init__.py` holds the version |
| `common/` | Data models (`data.py`), protocol, constants, shared utils |
| `scraping/` | Scraper interface (`scraper.py`), `provider.py` registry, per-source scrapers in `x/` and `reddit/`, JSON configs in `config/` |
| `upload_utils/` | Miner-side S3 uploader and filename/partition contract |
| `storage/` | Miner SQLite storage, validator storage |
| `vali_utils/` | Validator logic: `miner_evaluator.py`, `s3_utils.py` (S3 validation), `on_demand/` (OD validation), `parquet_reader.py` |
| `rewards/` | `miner_scorer.py` (credibility, scores, weights), desirability lookup |
| `dynamic_desirability/` | Gravity job list retrieval and validator voting |
| `docs/` | Human docs; the authoritative ones are listed at the end of this file |
| `tests/` | Pytest suites, mirrored by package (`tests/vali_utils`, `tests/scraping`, ...) |
| `scripts/` | Operator and forensic tooling, not shipped to nodes |

Anything else at the repo root (handoff notes, investigation reports, parquet
dumps, log folders) is local working material, not part of the product.
Do not depend on it, extend it, or commit it.

## Setup and commands

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# run tests (pytest, no extra config)
python -m pytest tests/ -q
python -m pytest tests/vali_utils/test_s3_latest_per_job.py -q

# run nodes
python neurons/miner.py     --netuid 13 --subtensor.network finney --wallet.name W --wallet.hotkey H
python neurons/validator.py --netuid 13 --subtensor.network finney --wallet.name W --wallet.hotkey H
```

Miners and validators read API keys and storage settings from a local `.env`.
Never commit it and never paste its contents into code, tests, docs or PRs.

## How miners work, and what they may change

Miners are free to run their own code. The reference implementation is a
template, not a requirement. What is fixed is the contract the validator
checks:

- **Upload layout and filename.** One Parquet file per upload under the
  miner's hotkey and job id, named
  `data_YYYYMMDD_HHMMSS_<rows>_<16hex>.parquet`. The row count in the name
  must equal the rows in the file. Row groups of at most 10,000 rows, standard
  Parquet with a common codec, nothing exotic.
- **Whole-job snapshots.** Each upload is the complete current dataset for
  that job. Validators keep only the newest file per job; older files in the
  same job are ignored, so incremental uploads score only their last chunk.
- **Schema.** Exactly the column set the validator expects per platform
  (see `EXPECTED_COLUMNS_X` and `EXPECTED_COLUMNS_REDDIT` in
  `vali_utils/s3_utils.py`), plus optional `uri`. Extra columns get the file
  skipped.
- **Content rules.** Real, currently retrievable posts. `scraped_at`
  obfuscated to the minute, never in the future, never before the post.
  Data inside the job's label, keyword and date window.

Customisation points in the template:

- Add a scraper by implementing `scraping/scraper.py` and registering it in
  `scraping/provider.py`. Choose what to scrape via the JSON config and the
  Gravity job list, not by editing the validator.
- Override `handle_on_demand` in `neurons/miner.py` to serve on-demand
  requests with your own scraper. Keep the request and response models.
- Replace `upload_utils/s3_uploader.py` as long as the layout, filename and
  schema contract above holds.

Legal and content obligations for miners are in `docs/miner_policy.md`.

## How validation works

**S3 validation** (`vali_utils/s3_utils.py`, `DuckDBSampledValidator`), run
per miner roughly every two hours:

1. List the miner's files, keep the newest per active job, parse row counts
   from filenames.
2. Download sampled files locally, decode every row group. Undecodable pages
   or corrupt metadata fail the miner outright.
3. Duplicate check across normalised URLs within a job (max 1%; honest miners dedup locally). Empty content max 10%.
4. Job match: rows must fit the job's label, keyword and date window (min
   95% across the sample).
5. Scraper check: re-fetch sampled entities from the live source (min 80%
   pass, per platform as well as overall).

Passing yields an effective size: latest-file rows per active job, capped by
the job's max rows, times a standard bytes-per-row constant, times coverage
squared. Effective size feeds the miner's S3 score together with a
credibility that moves toward 1 on pass and drops on fail.

**On-demand validation** (`vali_utils/on_demand/`): the validator submits the
customer job, miners respond within a TTL, responses are pooled and
deduplicated, and a sample is checked against the live source. Reward
depends on validity, coverage of the platforms the miner claims, and speed.
Abstaining from platforms a miner could serve is penalised.

Scoring constants live in `common/constants.py`, `rewards/miner_scorer.py`
and the class constants at the top of `vali_utils/s3_utils.py`. Changing any
threshold changes every miner's income. Do it in its own PR with the reason
in the description, never as a side effect of another change.

## Rewards in one paragraph

Each miner gets a score from the S3 path and the on-demand path. Scores are
relative: a miner's share of emissions is its score over the sum of all
scores, so improving one miner's score lowers everyone else's. Credibility
compounds over time; a miner that fails validation loses credibility faster
than it regains it. Gravity jobs from paying customers carry higher weight
than default data, and their date windows override the 30-day freshness
limit. Full detail: `docs/scoring.md` and `docs/s3_validation.md`.

## Working in this repo

**Branches and releases.** Feature branches target `dev`. `dev` is merged to
`main` in a release PR, tagged `vX.Y.Z`, and validators auto-update from
`main`. A validator-side change ships on the next tag; a miner-side change
also needs a docs update in `docs/miner.md` and a note for miners, since they
run their own code and must be told what the validator will start checking.

**Before you change anything.** State what the change does to miners, to
validators, or to both. Most bugs in this repo are one side assuming the
other side's behaviour. If a change alters what passes validation, say what
share of current miners would fail and why that is acceptable.

**Fail closed on the validator side.** When a check cannot run (undecodable
file, unreadable column, missing metadata), treat it as a failure, not as a
skip. Every silent skip in this codebase has become an exploit. The
counterpart: never fail a miner for a validator-side error such as a scraper
outage; those must be neutral.

**Tests.** Every validator change gets a test in `tests/vali_utils/`. The
suites build small Parquet files on disk and run the real validator methods
against them; follow that pattern rather than mocking DuckDB. Run the suite
before and after and report which failures are pre-existing. The repo has a
handful of known-failing tests; do not fix them in an unrelated PR.

**Keep changes surgical.** Touch only what the task needs, match the
surrounding style, leave unrelated dead code alone and mention it instead.
Prefer a small change to an abstraction. Delete what your change made
unused, nothing else.

**Never commit** `.env`, keys, wallet files, parquet data, logs, or anything
that identifies a specific miner hotkey outside of a test fixture.

## Where the truth is

- `docs/miner.md`, `docs/validator.md`: running a node
- `docs/s3_validation.md`: upload contract and validation rules
- `docs/scoring.md`: credibility, score and incentive
- `docs/on_demand.md`: on-demand request and response contract
- `docs/dynamic_desirability.md`: Gravity jobs and validator voting
- `docs/miner_policy.md`: content and data-protection obligations

If this file and the code disagree, the code is right and this file has a
bug. Fix it.
