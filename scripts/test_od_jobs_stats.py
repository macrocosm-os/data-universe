#!/usr/bin/env python3
"""Standalone smoke test for the OD jobs-stats endpoint (data-universe-api PR #103).

Signs requests with a real validator hotkey and:
  1. calls POST /on-demand/validator/jobs/stats for the trailing window,
  2. fetches the metagraph, picks the top-N miners by incentive (plus any
     --miner hotkeys), and prints their full per-platform OD stats:
     coverage vs doable jobs, empty submissions, submitted bytes.

Usage (from repo root):
  python scripts/test_od_jobs_stats.py --wallet-name <name> --wallet-hotkey <hotkey>
  python scripts/test_od_jobs_stats.py --wallet-name <name> --wallet-hotkey <hotkey> \
      --top 5 --skip-uid 162 --miner 5Dvenu...
"""
import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def signed_post(client, signer, url: str, payload: dict):
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json", **signer.headers(body)}
    return client.post(url, content=body, headers=headers)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--wallet-name", required=True)
    p.add_argument("--wallet-hotkey", required=True)
    p.add_argument("--url", default="https://data-universe-api.api.macrocosmos.ai")
    p.add_argument("--hours", type=float, default=3.0, help="trailing window size")
    p.add_argument("--min-submitters", type=int, default=5)
    p.add_argument("--top", type=int, default=5,
                   help="check the top-N miners by incentive (0 to disable)")
    p.add_argument("--skip-uid", type=int, action="append", default=[],
                   help="UID to exclude from the top-N pick (repeatable)")
    p.add_argument("--miner", action="append", default=[],
                   help="extra miner hotkey to check (repeatable)")
    p.add_argument("--netuid", type=int, default=13)
    p.add_argument("--network", default="finney")
    args = p.parse_args()

    # Import after argparse — bittensor intercepts -h/--help at import time.
    import httpx
    import bittensor as bt
    from common.api_client import TaoSigner

    wallet = bt.Wallet(name=args.wallet_name, hotkey=args.wallet_hotkey)
    signer = TaoSigner(keypair=wallet.hotkey)
    print(f"validator hotkey: {wallet.hotkey.ss58_address}")

    # Build the miner list: top-N by incentive from the metagraph + manual ones.
    miners = []  # (label, hotkey)
    if args.top > 0:
        print(f"fetching metagraph (netuid={args.netuid}, {args.network})...")
        mg = bt.Metagraph(netuid=args.netuid, network=args.network, lite=True)
        order = sorted(range(len(mg.hotkeys)), key=lambda u: -float(mg.I[u]))
        picked = 0
        for uid in order:
            if uid in args.skip_uid:
                continue
            miners.append((f"uid={uid} inc={float(mg.I[uid]):.5f}", mg.hotkeys[uid]))
            picked += 1
            if picked >= args.top:
                break
    miners.extend((f"manual", hk) for hk in args.miner)

    n_calls = 1 + len(miners)
    if n_calls > 12:
        print(f"warning: {n_calls} calls exceeds the 12/min validator rate limit")

    now = datetime.now(timezone.utc)
    window = {
        "expired_since": (now - timedelta(hours=args.hours)).isoformat(),
        "expired_until": now.isoformat(),
    }

    with httpx.Client(timeout=60) as client:
        # 1. jobs stats (coverage denominators)
        r = signed_post(
            client, signer, f"{args.url}/on-demand/validator/jobs/stats",
            {**window, "min_submitters": args.min_submitters},
        )
        print(f"\n[stats] HTTP {r.status_code}  (window: last {args.hours:g}h)")
        if r.status_code != 200:
            print(r.text[:500])
            sys.exit(1)
        stats = r.json()["platforms"]
        for platform, s in sorted(stats.items()):
            print(f"  {platform:8s} total={s['total_jobs']:<5d} "
                  f"doable(>={args.min_submitters} miners)={s['doable_jobs']}")

        # 2. full per-miner stats over the same window
        for label, hotkey in miners:
            r = signed_post(
                client, signer, f"{args.url}/on-demand/validator/miner-jobs",
                {**window, "miner_hotkey": hotkey, "limit": 1000},
            )
            print(f"\n[miner] {hotkey}  ({label})  HTTP {r.status_code}")
            if r.status_code != 200:
                print(f"  {r.text[:300]}")
                continue
            jobs = r.json()["jobs"]

            per = {}  # platform -> dict
            for j in jobs:
                platform = j["job"]["job"]["platform"]
                d = per.setdefault(
                    platform, {"jobs": 0, "empty": 0, "bytes": 0}
                )
                d["jobs"] += 1
                size = j["submission"].get("s3_content_length") or 0
                d["bytes"] += size
                if size == 0:
                    d["empty"] += 1

            for platform, s in sorted(stats.items()):
                d = per.get(platform, {"jobs": 0, "empty": 0, "bytes": 0})
                doable = s["doable_jobs"]
                cov = f"{min(1.0, d['jobs'] / doable):6.1%}" if doable else "   n/a"
                print(f"  {platform:8s} submitted={d['jobs']:<4d} doable={doable:<4d} "
                      f"coverage={cov}  empty={d['empty']:<3d} bytes={d['bytes']:,}")
            if len(jobs) == 1000:
                print("  (warning: hit limit=1000, counts may be truncated)")


if __name__ == "__main__":
    main()
