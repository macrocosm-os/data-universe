#!/usr/bin/env python3
"""
Simple runner script for MinIO test
Just run: python run_minio_test.py
"""

import os
import sys

# Add current directory to path to import the test module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    print("🚀 MinIO + Hive Partitioning Test")
    print("=" * 40)

    # Check if database exists
    db_file = "SqliteMinerStorage.sqlite"
    if not os.path.exists(db_file):
        print(f"❌ Database file not found: {db_file}")
        print("Please make sure you're in the directory with your database file")
        return

    print(f"✅ Found database: {db_file}")

    # Check dependencies
    try:
        import pandas as pd
        print("✅ pandas available")
    except ImportError:
        print("❌ pandas not found. Install with: pip install pandas")
        return

    try:
        import minio
        print("✅ minio available")
    except ImportError:
        print("❌ minio not found. Install with: pip install minio")
        return

    # Run the test
    from minio_storage import MinIOTester

    tester = MinIOTester(db_file)
    success = tester.run_test()

    if success:
        print("\n🎉 Test completed successfully!")
    else:
        print("\n❌ Test failed")


if __name__ == "__main__":
    main()