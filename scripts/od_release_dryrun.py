"""OD release dry-run — shows what the new coverage + speed scoring WOULD do, read-only.

Fetches live jobs-stats and per-miner jobs from the API with a Tao-signed
request (same client/signer the validator uses), then runs the NEW code paths
(coverage numerator, coverage multiplier, speed curve) on the real responses
and prints per-miner results.

Makes NO writes: no scorer state, no weights, no submission downloads.

Usage (on the validator machine, from the repo root on this branch):
    ./venv/bin/python scripts/od_release_dryrun.py \
        --wallet.name <name> --wallet.hotkey <hotkey> \
        [--url https://data-universe-api.api.macrocosmos.ai] \
        [--hotkeys hk1,hk2,...]        # explicit list, or omit to use the metagraph
        [--netuid 13] [--network finney] [--max-miners 300]
"""
import argparse
import asyncio
import datetime as dt
import statistics
import sys

import bittensor as bt

from common.api_client import (
    DataUniverseApiClient,
    ListMinerJobsForValidationRequest,
    OnDemandJobsStatsRequest,
)
from rewards.miner_scorer import MinerScorer
from vali_utils.miner_evaluator import MinerEvaluator
from vali_utils.on_demand.on_demand_validation import (
    SPEED_FLOOR,
    SPEED_GRACE_S,
    _speed_multiplier,
)


def compute_coverage_mult(counts, stats, page_full):
    """Mirrors the coverage block in MinerEvaluator._evaluate_od (PR #888)."""
    if page_full:
        return 1.0, "full-page failsafe"
    mult, notes = 1.0, []
    for platform, pstats in stats.platforms.items():
        c = counts.get(platform, {"any": 0, "nonempty": 0})
        if c["any"] == 0:
            if pstats.doable_jobs >= MinerEvaluator.OD_ABSTAIN_MIN_PLATFORM_JOBS:
                mult *= MinerScorer.OD_ABSTAIN_MULT
                notes.append(f"{platform}:abstain")
        elif pstats.doable_jobs >= MinerEvaluator.OD_COVERAGE_MIN_PLATFORM_JOBS:
            full_at = min(
                MinerEvaluator.OD_COVERAGE_FULL_MULT_AT,
                0.5 * MinerEvaluator.OD_JOBS_FETCH_LIMIT / pstats.doable_jobs,
            )
            coverage = min(1.0, c["nonempty"] / pstats.doable_jobs)
            if coverage < full_at:
                mult *= max(MinerScorer.OD_ABSTAIN_MULT, coverage / full_at)
                notes.append(f"{platform}:cov={coverage:.3f}")
    return mult, ",".join(notes) or "full"


def compute_old_mult(counts, stats):
    """Current production rule: binary abstention only."""
    mult = 1.0
    for platform, pstats in stats.platforms.items():
        c = counts.get(platform, {"any": 0})
        if c["any"] == 0 and pstats.doable_jobs >= MinerEvaluator.OD_ABSTAIN_MIN_PLATFORM_JOBS:
            mult *= MinerScorer.OD_ABSTAIN_MULT
    return mult


async def check_miner(client, sem, hotkey, since, until):
    async with sem:
        try:
            resp = await client.validator_list_miner_jobs(
                ListMinerJobsForValidationRequest(
                    miner_hotkey=hotkey,
                    expired_since=since,
                    expired_until=until,
                    limit=MinerEvaluator.OD_JOBS_FETCH_LIMIT,
                )
            )
            return hotkey, resp.jobs, None
        except Exception as e:
            return hotkey, None, f"{type(e).__name__}: {e}"


async def run(args, hotkeys):
    now = dt.datetime.now(dt.timezone.utc)
    since = now - dt.timedelta(hours=MinerEvaluator.OD_COVERAGE_WINDOW_HOURS)

    wallet = bt.Wallet(config=args)
    async with DataUniverseApiClient(
        base_url=args.url, keypair=wallet.hotkey, timeout=60,
        verify_ssl="localhost" not in args.url,
    ) as client:
        try:
            stats = await client.validator_get_jobs_stats(
                OnDemandJobsStatsRequest(
                    expired_since=since,
                    expired_until=now,
                    min_submitters=MinerEvaluator.OD_COVERAGE_MIN_SUBMITTERS,
                )
            )
        except Exception as e:
            print(f"\nFAILED to fetch jobs-stats: {type(e).__name__}: {e}")
            print("(401/403 means the hotkey is not a registered validator; 5xx means API trouble)")
            sys.exit(1)
        print(f"\njobs-stats window {MinerEvaluator.OD_COVERAGE_WINDOW_HOURS}h:")
        for platform, p in stats.platforms.items():
            print(f"  {platform}: doable={p.doable_jobs} total={p.total_jobs}")

        sem = asyncio.Semaphore(6)
        results = await asyncio.gather(
            *[check_miner(client, sem, hk, since, now) for hk in hotkeys]
        )

    rows, errors = [], []
    upload_times = []
    missing = {"s3_content_length": 0, "expire_at": 0, "timestamps": 0}
    total_jobs = 0

    for hotkey, jobs, err in results:
        if err is not None:
            errors.append((hotkey, err))
            continue
        total_jobs += len(jobs)
        for j in jobs:
            if j.submission.s3_content_length is None:
                missing["s3_content_length"] += 1
            if j.job.expire_at is None:
                missing["expire_at"] += 1
            if j.job.created_at is not None and j.submission.submitted_at is not None:
                upload_times.append(
                    (j.submission.submitted_at - j.job.created_at).total_seconds()
                )
            else:
                missing["timestamps"] += 1

        counts = MinerEvaluator._od_submission_counts(jobs, since)
        page_full = len(jobs) >= MinerEvaluator.OD_JOBS_FETCH_LIMIT
        new_mult, notes = compute_coverage_mult(counts, stats, page_full)
        old_mult = compute_old_mult(counts, stats)
        per_platform = " ".join(
            f"{p}={c['any']}/{c['nonempty']}" for p, c in sorted(counts.items())
        ) or "-"
        rows.append((hotkey, len(jobs), per_platform, new_mult, old_mult, notes))

    print(f"\nper-miner coverage multipliers ({len(rows)} miners, window jobs any/nonempty):")
    print(f"{'hotkey':<12} {'jobs':>5}  {'submitted(any/nonempty)':<34} {'new':>6} {'old':>5}  notes")
    for hotkey, njobs, per_platform, new_mult, old_mult, notes in sorted(
        rows, key=lambda r: r[3]
    ):
        print(f"{hotkey[:10]:<12} {njobs:>5}  {per_platform:<34} {new_mult:>6.3f} {old_mult:>5.2f}  {notes}")

    buckets = {}
    for r in rows:
        buckets[round(r[3], 2)] = buckets.get(round(r[3], 2), 0) + 1
    print(f"\nnew-mult histogram: {dict(sorted(buckets.items()))}")

    if upload_times:
        upload_times.sort()
        new_at_full = sum(1 for t in upload_times if _speed_multiplier(t) >= 0.999)
        new_at_floor = sum(1 for t in upload_times if _speed_multiplier(t) <= SPEED_FLOOR)
        old_curve = lambda t: min(1.0, 0.5 ** (max(0.0, t) / 45.0))
        print(f"\nspeed ({len(upload_times)} submissions with both timestamps):")
        print(
            f"  upload_s p50={statistics.median(upload_times):.1f}"
            f" p95={upload_times[int(0.95 * (len(upload_times) - 1))]:.1f}"
            f" max={upload_times[-1]:.1f}"
        )
        print(
            f"  new curve (grace {SPEED_GRACE_S:.0f}s): {new_at_full} at 1.0,"
            f" {new_at_floor} at floor {SPEED_FLOOR}"
        )
        print(
            f"  old curve mean={statistics.mean(old_curve(t) for t in upload_times):.3f}"
            f" vs new mean={statistics.mean(_speed_multiplier(t) for t in upload_times):.3f}"
        )

    print(f"\nresponse-shape diagnostics over {total_jobs} jobs:")
    for k, v in missing.items():
        flag = "  <-- CHECK" if v else ""
        print(f"  missing {k}: {v}{flag}")
    if errors:
        print(f"\nfetch errors ({len(errors)}):")
        for hotkey, err in errors[:10]:
            print(f"  {hotkey[:10]}: {err}")
    print("\ndry-run complete — nothing was written.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="https://data-universe-api.api.macrocosmos.ai")
    ap.add_argument("--hotkeys", default=None, help="comma-separated miner hotkeys; omit to use the metagraph")
    ap.add_argument("--netuid", type=int, default=13)
    ap.add_argument("--network", default="finney")
    ap.add_argument("--max-miners", type=int, default=300)
    bt.Wallet.add_args(ap)
    args = bt.Config(ap)
    args.url = args.get("url")

    if args.get("hotkeys"):
        hotkeys = [h.strip() for h in args.get("hotkeys").split(",") if h.strip()]
    else:
        print(f"syncing metagraph netuid={args.get('netuid')} on {args.get('network')}...")
        metagraph = bt.Subtensor(network=args.get("network")).metagraph(args.get("netuid"))
        hotkeys = list(metagraph.hotkeys)[: args.get("max_miners")]
    print(f"checking {len(hotkeys)} hotkeys against {args.url}")

    asyncio.run(run(args, hotkeys))


if __name__ == "__main__":
    main()
