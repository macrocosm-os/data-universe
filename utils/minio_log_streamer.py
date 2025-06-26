#!/usr/bin/env python3
"""
Minio Log Streamer - Stream validator logs from Minio to local machine
Usage: python minio_log_streamer.py [validator_uid] [--follow]
"""

import json
import time
import argparse
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import sys

class MinioLogStreamer:
    def __init__(self, minio_endpoint="146.190.168.187:9000", 
                 access_key="miner_test_hot", 
                 secret_key="key_key_offline_secret",
                 bucket_name="validator-logs"):
        self.client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.bucket_name = bucket_name
        self.seen_files = set()
        
    def list_validators(self):
        """List all validators with logs"""
        try:
            objects = self.client.list_objects(self.bucket_name, recursive=False)
            validators = []
            for obj in objects:
                if obj.object_name.endswith('/'):
                    validators.append(obj.object_name.rstrip('/'))
            return validators
        except S3Error as e:
            print(f"Error listing validators: {e}")
            return []
    
    def list_runs(self, validator_uid):
        """List all runs for a validator"""
        try:
            prefix = f"validator_{validator_uid}/"
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=False)
            runs = []
            for obj in objects:
                if obj.object_name.endswith('/'):
                    run_id = obj.object_name.replace(prefix, '').rstrip('/')
                    runs.append(run_id)
            return sorted(runs)
        except S3Error as e:
            print(f"Error listing runs: {e}")
            return []
    
    def get_latest_run(self, validator_uid):
        """Get the latest run for a validator"""
        runs = self.list_runs(validator_uid)
        return runs[-1] if runs else None
    
    def stream_logs(self, validator_uid, run_id=None, follow=False):
        """Stream logs for a validator run"""
        current_run_id = run_id
        
        # If no specific run ID, start with latest
        if current_run_id is None:
            current_run_id = self.get_latest_run(validator_uid)
            if not current_run_id:
                print(f"No runs found for validator_{validator_uid}")
                return
        
        print(f"Streaming logs for validator_{validator_uid}")
        if not follow:
            print(f"Run: {current_run_id}")
        else:
            print(f"Following all runs (starting from: {current_run_id})")
        print("=" * 60)
        
        # Show config first
        self._show_config(validator_uid, current_run_id)
        
        while True:
            # If following, check for newer runs
            if follow and run_id is None:  # Only auto-switch if no specific run was requested
                latest_run = self.get_latest_run(validator_uid)
                if latest_run and latest_run != current_run_id:
                    print(f"\n🔄 NEW RUN DETECTED: {latest_run}")
                    print("=" * 60)
                    self._show_config(validator_uid, latest_run)
                    current_run_id = latest_run
                    # Don't clear seen_files - we want to continue from where we left off
            
            # Get all log files for current run
            prefix = f"validator_{validator_uid}/{current_run_id}/"
            try:
                objects = self.client.list_objects(self.bucket_name, prefix=prefix)
                
                log_files = []
                for obj in objects:
                    if obj.object_name.startswith(prefix + "logs_") and obj.object_name.endswith(".json"):
                        log_files.append(obj.object_name)
                
                # Sort by timestamp in filename
                log_files.sort()
                
                # Process new files
                for log_file in log_files:
                    if log_file not in self.seen_files:
                        self._process_log_file(log_file)
                        self.seen_files.add(log_file)
                        
            except S3Error as e:
                if follow:
                    # Run might not exist yet, wait and try again
                    pass
                else:
                    print(f"Error accessing run {current_run_id}: {e}")
                    break
            
            if not follow:
                break
                
            time.sleep(2)  # Check for new logs every 2 seconds
    
    def _show_config(self, validator_uid, run_id):
        """Show validator configuration"""
        try:
            config_path = f"validator_{validator_uid}/{run_id}/config.json"
            response = self.client.get_object(self.bucket_name, config_path)
            config_data = json.loads(response.read())
            
            print("🔧 VALIDATOR CONFIGURATION")
            print("-" * 30)
            run_info = config_data.get('run_info', {})
            print(f"UID: {run_info.get('uid')}")
            print(f"Hotkey: {run_info.get('hotkey', '')[:20]}...")
            print(f"Version: {run_info.get('version')}")
            print(f"Started: {run_info.get('started_at')}")
            print(f"Scrapers: {', '.join(run_info.get('scrapers', []))}")
            print()
            
        except Exception as e:
            print(f"Could not load config: {e}")
    
    def _process_log_file(self, log_file):
        """Process and display a log file"""
        try:
            response = self.client.get_object(self.bucket_name, log_file)
            log_data = json.loads(response.read())
            
            logs = log_data.get('logs', [])
            for log_entry in logs:
                timestamp = log_entry.get('timestamp', '')
                level = log_entry.get('level', 'INFO')
                message = log_entry.get('message', '')
                
                # Format timestamp
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                except:
                    formatted_time = timestamp
                
                # Color coding for levels
                color = ""
                reset = "\033[0m"
                if level == "ERROR":
                    color = "\033[91m"  # Red
                elif level == "WARNING":
                    color = "\033[93m"  # Yellow
                elif level == "INFO":
                    color = "\033[92m"  # Green
                elif level == "DEBUG":
                    color = "\033[94m"  # Blue
                elif level == "TRACE":
                    color = "\033[95m"  # Magenta
                elif level == "SUCCESS":
                    color = "\033[96m"  # Cyan
                
                print(f"{formatted_time} | {color}{level:>8}{reset} | {message}")
                
        except Exception as e:
            print(f"Error processing {log_file}: {e}")
    
    def show_summary(self, validator_uid, run_id=None):
        """Show run summary"""
        if run_id is None:
            run_id = self.get_latest_run(validator_uid)
            if not run_id:
                print(f"No runs found for validator_{validator_uid}")
                return
        
        try:
            summary_path = f"validator_{validator_uid}/{run_id}/summary.json"
            response = self.client.get_object(self.bucket_name, summary_path)
            summary_data = json.loads(response.read())
            
            print("📊 RUN SUMMARY")
            print("-" * 20)
            print(f"Run ID: {summary_data.get('run_id')}")
            print(f"Validator UID: {summary_data.get('validator_uid')}")
            print(f"Start Time: {summary_data.get('start_time')}")
            print(f"End Time: {summary_data.get('end_time')}")
            print(f"Duration: {summary_data.get('duration_seconds', 0):.1f} seconds")
            print(f"Logs Count: {summary_data.get('logs_count', 0)}")
            print(f"Metrics Count: {summary_data.get('metrics_count', 0)}")
            
        except Exception as e:
            print(f"Could not load summary: {e}")


def main():
    parser = argparse.ArgumentParser(description="Stream validator logs from Minio")
    parser.add_argument("validator_uid", nargs='?', help="Validator UID (e.g., 0)")
    parser.add_argument("--run", help="Specific run ID to stream")
    parser.add_argument("--follow", "-f", action="store_true", help="Follow logs (like tail -f)")
    parser.add_argument("--list", "-l", action="store_true", help="List available validators")
    parser.add_argument("--summary", "-s", action="store_true", help="Show run summary")
    parser.add_argument("--endpoint", default="146.190.168.187:9000", help="Minio endpoint")
    parser.add_argument("--access-key", default="miner_test_hot", help="Minio access key")
    parser.add_argument("--secret-key", default="key_key_offline_secret", help="Minio secret key")
    parser.add_argument("--bucket", default="validator-logs", help="Minio bucket name")
    
    args = parser.parse_args()
    
    streamer = MinioLogStreamer(
        minio_endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        bucket_name=args.bucket
    )
    
    if args.list:
        validators = streamer.list_validators()
        print("Available validators:")
        for validator in validators:
            runs = streamer.list_runs(validator.replace('validator_', ''))
            print(f"  {validator} ({len(runs)} runs)")
            for run in runs[-3:]:  # Show last 3 runs
                print(f"    └── {run}")
        return
    
    if not args.validator_uid:
        print("Please specify a validator UID or use --list to see available validators")
        return
    
    if args.summary:
        streamer.show_summary(args.validator_uid, args.run)
    else:
        streamer.stream_logs(args.validator_uid, args.run, args.follow)


if __name__ == "__main__":
    main()