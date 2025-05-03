#!/usr/bin/env python3
import argparse
import sys
import bittensor as bt
from pathlib import Path
import json
from vali_utils.validator_s3_access import ValidatorS3Access


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Test S3 access for validators")
    parser.add_argument("--wallet", type=str, required=True, help="Wallet name")
    parser.add_argument("--hotkey", type=str, required=True, help="Hotkey name")
    parser.add_argument("--s3_auth_url", type=str, default="https://sn13-data.api.macrocosmos.ai",
                        help="S3 authentication URL")
    parser.add_argument("--netuid", type=int, default=13, help="Network UID")
    parser.add_argument("--network", type=str, default="finney", help="Network name")
    parser.add_argument("--action", type=str, choices=['auth', 'list_sources', 'list_miners', 'list_files'],
                        default='auth', help="Action to perform")
    parser.add_argument("--source", type=str, help="Data source (x or reddit)")
    parser.add_argument("--miner", type=str, help="Miner ID (coldkey)")

    args = parser.parse_args()

    # Create config
    config = bt.config()
    config.netuid = args.netuid
    config.s3_auth_url = args.s3_auth_url

    # Create wallet and S3 access
    wallet = bt.wallet(name=args.wallet, hotkey=args.hotkey)
    s3_access = ValidatorS3Access(
        wallet=wallet,
        s3_auth_url=args.s3_auth_url
    )

    # Perform requested action
    if args.action == 'auth':
        # Test authentication
        if s3_access.ensure_access():
            print("✅ Authentication successful")
            print(f"Access data received:")

            # Print readable summary of access data
            access_data = s3_access.access_data
            print(f"  Bucket: {access_data.get('bucket')}")
            print(f"  Region: {access_data.get('region')}")
            print(f"  Expiry: {access_data.get('expiry')}")

            # Print URLs structure
            urls = access_data.get('urls', {})
            sources = urls.get('sources', {})
            print(f"  Available sources: {list(sources.keys())}")

            return 0
        else:
            print("❌ Authentication failed")
            return 1

    elif args.action == 'list_sources':
        # List available sources
        sources = s3_access.list_sources()
        if sources:
            print(f"✅ Available sources: {sources}")
            return 0
        else:
            print("❌ Failed to list sources or none available")
            return 1

    elif args.action == 'list_miners':
        # List miners for a source
        if not args.source:
            print("❌ --source is required for list_miners action")
            return 1

        miners = s3_access.list_miners(args.source)
        if miners:
            print(f"✅ Found {len(miners)} miners for source {args.source}:")
            for m in miners[:20]:  # Show first 20
                print(f"  - {m}")
            if len(miners) > 20:
                print(f"  ... and {len(miners) - 20} more")
            return 0
        else:
            print(f"❌ No miners found for source {args.source} or listing failed")
            return 1

    elif args.action == 'list_files':
        # List files for a miner
        if not args.source or not args.miner:
            print("❌ --source and --miner are required for list_files action")
            return 1

        files = s3_access.list_files(args.source, args.miner)
        if files:
            print(f"✅ Found {len(files)} files for miner {args.miner} in source {args.source}:")
            for i, f in enumerate(files[:10]):  # Show first 10
                print(f"  {i + 1}. {f['filename']} ({f['size']} bytes, modified: {f['last_modified']})")
            if len(files) > 10:
                print(f"  ... and {len(files) - 10} more")
            return 0
        else:
            print(f"❌ No files found for miner {args.miner} or listing failed")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())