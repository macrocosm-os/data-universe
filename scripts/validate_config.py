#!/usr/bin/env python3
"""Validates a scraping config JSON file without running the miner.

Usage:
    python scripts/validate_config.py path/to/scraping_config.json
    python scripts/validate_config.py path/to/scraping_config.json --verbose
"""

import argparse
import sys
from pathlib import Path

from pydantic import ValidationError

from scraping.config.config_reader import ConfigReader


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate a scraping config JSON file"
    )
    parser.add_argument(
        "config_path",
        type=Path,
        help="Path to the scraping_config.json file to validate",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed config information",
    )

    args = parser.parse_args()

    if not args.config_path.exists():
        print(f"✗ File not found: {args.config_path}", file=sys.stderr)
        return 1

    try:
        config = ConfigReader.load_config(str(args.config_path))
    except ValidationError as e:
        print(f"✗ Invalid config:", file=sys.stderr)
        for error in e.errors():
            location = ".".join(str(loc) for loc in error["loc"])
            print(f"  {location} - {error['msg']}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"✗ Failed to load config: {e}", file=sys.stderr)
        return 1

    scraper_count = len(config.scraper_configs)
    print(f"✓ Config looks good - {scraper_count} scraper{'s' if scraper_count != 1 else ''} configured")

    if args.verbose:
        print("\nConfigured scrapers:")
        for scraper_id, scraper_config in config.scraper_configs.items():
            label_count = len(scraper_config.labels_to_scrape)
            print(
                f"  - {scraper_id.name}: cadence={scraper_config.cadence_seconds}s, "
                f"{label_count} label config{'s' if label_count != 1 else ''}"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
