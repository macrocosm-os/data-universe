import json
import datetime as dt
import io
import bittensor as bt
from typing import Dict, Any, Optional, List
import os
import subprocess
import time
import platform
import urllib.request
from minio import Minio
from minio.error import S3Error


class ValidatorMinioLogger:
    """
    Simple Minio logger specifically for validators to replace wandb.
    Direct connection to self-hosted Minio without S3Auth complexity.
    """
    
    def __init__(self, 
                 validator_uid: int,
                 wallet: bt.wallet,
                 minio_endpoint: str,
                 minio_access_key: str = "miner_test_hot",
                 minio_secret_key: str = "key_key_offline_secret",
                 bucket_name: str = "validator-logs",
                 project_name: str = "data-universe-validators",
                 secure: bool = False,
                 auto_start_local: bool = True):
        self.validator_uid = validator_uid
        self.wallet = wallet
        self.project_name = project_name
        self.bucket_name = bucket_name
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.secure = secure
        self.auto_start_local = auto_start_local
        
        # Local Minio process tracking
        self.local_minio_process = None
        self.local_minio_port = 9000
        
        # Initialize Minio client
        self.minio_client = self._initialize_minio_client()
        
        # Run tracking
        self.run_start = None
        self.run_id = None
        self.current_log_buffer = []
        self.metrics_buffer = []
        self.last_upload_time = dt.datetime.now()
        
        # Configuration
        self.config_data = {}
        
        # Ensure bucket exists
        if self.minio_client:
            self._ensure_bucket_exists()
    
    def _initialize_minio_client(self):
        """Initialize Minio client, with fallback to local instance"""
        # First try to connect to specified endpoint
        try:
            bt.logging.info(f"Attempting to connect to Minio at {self.minio_endpoint}")
            client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=self.secure
            )
            
            # Test connection
            client.list_buckets()
            bt.logging.info(f"✅ Connected to Minio at {self.minio_endpoint}")
            return client
            
        except Exception as e:
            bt.logging.warning(f"Failed to connect to Minio at {self.minio_endpoint}: {e}")
            
            if self.auto_start_local:
                bt.logging.info("Attempting to start local Minio instance...")
                if self._start_local_minio():
                    # Try connecting to local instance
                    try:
                        local_endpoint = f"localhost:{self.local_minio_port}"
                        client = Minio(
                            local_endpoint,
                            access_key=self.minio_access_key,
                            secret_key=self.minio_secret_key,
                            secure=False
                        )
                        
                        # Wait a bit for Minio to fully start
                        time.sleep(3)
                        client.list_buckets()
                        bt.logging.info(f"✅ Connected to local Minio at {local_endpoint}")
                        return client
                        
                    except Exception as local_e:
                        bt.logging.error(f"Failed to connect to local Minio: {local_e}")
                        
            bt.logging.error("Could not establish Minio connection")
            return None
    
    def _start_local_minio(self):
        """Start a local Minio instance"""
        try:
            # Create validator storage directory
            storage_dir = os.path.expanduser("~/validator_minio_storage")
            data_dir = os.path.join(storage_dir, "data")
            
            os.makedirs(data_dir, exist_ok=True)
            
            # Download Minio binary if needed
            minio_binary = os.path.join(storage_dir, "minio")
            if not os.path.exists(minio_binary):
                bt.logging.info("Downloading Minio binary...")
                self._download_minio_binary(minio_binary)
            
            # Find available port
            self.local_minio_port = self._find_available_port(9000)
            console_port = self._find_available_port(9001)
            
            # Start Minio process
            env = os.environ.copy()
            env["MINIO_ROOT_USER"] = self.minio_access_key
            env["MINIO_ROOT_PASSWORD"] = self.minio_secret_key
            
            cmd = [
                minio_binary, "server", data_dir,
                "--address", f"0.0.0.0:{self.local_minio_port}",
                "--console-address", f"0.0.0.0:{console_port}"
            ]
            
            bt.logging.info(f"Starting local Minio: {' '.join(cmd)}")
            self.local_minio_process = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=storage_dir
            )
            
            bt.logging.info(f"🚀 Local Minio started on port {self.local_minio_port}")
            bt.logging.info(f"🎛️ Console available on port {console_port}")
            return True
            
        except Exception as e:
            bt.logging.error(f"Failed to start local Minio: {e}")
            return False
    
    def _download_minio_binary(self, binary_path):
        """Download Minio binary for the current platform"""
        try:
            system = platform.system().lower()
            machine = platform.machine().lower()
            
            if system == "darwin":
                if "arm" in machine or "aarch64" in machine:
                    url = "https://dl.min.io/server/minio/release/darwin-arm64/minio"
                else:
                    url = "https://dl.min.io/server/minio/release/darwin-amd64/minio"
            elif system == "linux":
                if "arm" in machine or "aarch64" in machine:
                    url = "https://dl.min.io/server/minio/release/linux-arm64/minio"
                else:
                    url = "https://dl.min.io/server/minio/release/linux-amd64/minio"
            else:
                raise Exception(f"Unsupported platform: {system}-{machine}")
            
            bt.logging.info(f"Downloading from {url}")
            urllib.request.urlretrieve(url, binary_path)
            os.chmod(binary_path, 0o755)
            bt.logging.info("✅ Minio binary downloaded")
            
        except Exception as e:
            bt.logging.error(f"Failed to download Minio binary: {e}")
            raise
    
    def _find_available_port(self, start_port):
        """Find an available port starting from start_port"""
        import socket
        for port in range(start_port, start_port + 100):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('localhost', port))
                    return port
            except OSError:
                continue
        raise Exception(f"No available ports found starting from {start_port}")
    
    def _ensure_bucket_exists(self):
        """Ensure the logging bucket exists"""
        try:
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                bt.logging.info(f"Created Minio bucket: {self.bucket_name}")
            else:
                bt.logging.debug(f"Minio bucket exists: {self.bucket_name}")
        except S3Error as e:
            bt.logging.error(f"Error ensuring bucket exists: {e}")
        
    def init_run(self, config: Dict[str, Any] = None, version_tag: str = None, 
                 scraper_providers: List[str] = None):
        """Initialize a new logging run - equivalent to wandb.init()"""
        now = dt.datetime.now()
        self.run_start = now
        self.run_id = now.strftime("%Y-%m-%d_%H-%M-%S")
        
        # Store configuration
        self.config_data = {
            "uid": self.validator_uid,
            "hotkey": self.wallet.hotkey.ss58_address,
            "run_name": self.run_id,
            "type": "validator",
            "version": version_tag or "unknown",
            "scrapers": scraper_providers or [],
            "started_at": now.isoformat(),
            "project": self.project_name,
            **(config or {})
        }
        
        # Clear buffers
        self.current_log_buffer = []
        self.metrics_buffer = []
        
        bt.logging.info(f"Started Minio logging run: validator-{self.validator_uid}-{self.run_id}")
        
        # Upload initial config
        self._upload_config()
        
    def log(self, data: Dict[str, Any], step: Optional[int] = None):
        """Log metrics data - equivalent to wandb.log()"""
        log_entry = {
            "timestamp": dt.datetime.now().isoformat(),
            "step": step,
            "data": data
        }
        self.metrics_buffer.append(log_entry)
        
        # Upload metrics every 10 entries or immediately if it's important data
        if len(self.metrics_buffer) >= 10:
            self._upload_metrics()
            
    def log_stdout(self, message: str, level: str = "INFO"):
        """Log stdout/stderr messages"""
        # Skip our own upload messages to avoid recursion
        if "Uploaded" in message and "log entries to Minio" in message:
            return
            
        log_entry = {
            "timestamp": dt.datetime.now().isoformat(),
            "level": level,
            "message": message,
            "run_id": self.run_id
        }
        self.current_log_buffer.append(log_entry)
        
        # Upload logs more frequently - every 5 entries or every 10 seconds
        now = dt.datetime.now()
        time_since_upload = (now - self.last_upload_time).total_seconds()
        
        if len(self.current_log_buffer) >= 5 or time_since_upload >= 10:
            self._upload_logs()
            self.last_upload_time = now
            
    def finish(self):
        """Finish the current run - equivalent to wandb.finish()"""
        if not self.run_id:
            return
            
        # Upload any remaining data
        if self.current_log_buffer:
            self._upload_logs()
        if self.metrics_buffer:
            self._upload_metrics()
            
        # Upload final summary
        self._upload_summary()
        
        bt.logging.info(f"Finished Minio logging run: {self.run_id}")
        
        # Clear state
        self.run_id = None
        self.run_start = None
        self.current_log_buffer = []
        self.metrics_buffer = []
    
    def cleanup(self):
        """Clean up resources including local Minio process"""
        if self.local_minio_process:
            try:
                bt.logging.info("Shutting down local Minio process...")
                self.local_minio_process.terminate()
                self.local_minio_process.wait(timeout=10)
                bt.logging.info("✅ Local Minio process terminated")
            except subprocess.TimeoutExpired:
                bt.logging.warning("Local Minio process did not terminate gracefully, killing...")
                self.local_minio_process.kill()
            except Exception as e:
                bt.logging.error(f"Error shutting down local Minio: {e}")
            finally:
                self.local_minio_process = None
        
    def should_rotate_run(self) -> bool:
        """Check if run should be rotated (daily rotation)"""
        if not self.run_start:
            return False
        return (dt.datetime.now() - self.run_start) >= dt.timedelta(days=1)
        
    def _upload_config(self):
        """Upload configuration data"""
        if not self.run_id:
            return
            
        try:
            # Create config data
            config_data = {
                "run_info": self.config_data,
                "uploaded_at": dt.datetime.now().isoformat()
            }
            
            # Convert to JSON string
            config_json = json.dumps(config_data, indent=2)
            
            # Upload directly to Minio
            object_name = f"validator_{self.validator_uid}/{self.run_id}/config.json"
            
            self.minio_client.put_object(
                self.bucket_name,
                object_name,
                io.BytesIO(config_json.encode('utf-8')),
                len(config_json.encode('utf-8')),
                content_type='application/json'
            )
            
            bt.logging.debug(f"Uploaded config to Minio: {object_name}")
            
        except S3Error as e:
            bt.logging.error(f"Error uploading config to Minio: {e}")
            
    def _upload_logs(self):
        """Upload log buffer to Minio"""
        if not self.current_log_buffer or not self.run_id:
            return
            
        try:
            # Create log data
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            log_data = {
                "logs": self.current_log_buffer.copy(),
                "uploaded_at": dt.datetime.now().isoformat(),
                "run_id": self.run_id
            }
            
            # Convert to JSON string
            log_json = json.dumps(log_data, indent=2)
            
            # Upload to Minio
            object_name = f"validator_{self.validator_uid}/{self.run_id}/logs_{timestamp}.json"
            
            self.minio_client.put_object(
                self.bucket_name,
                object_name,
                io.BytesIO(log_json.encode('utf-8')),
                len(log_json.encode('utf-8')),
                content_type='application/json'
            )
            
            bt.logging.debug(f"Uploaded {len(self.current_log_buffer)} log entries to Minio")
            self.current_log_buffer = []  # Clear buffer
            
        except S3Error as e:
            bt.logging.error(f"Error uploading logs to Minio: {e}")
            
    def _upload_metrics(self):
        """Upload metrics buffer to Minio"""
        if not self.metrics_buffer or not self.run_id:
            return
            
        try:
            # Create metrics data
            timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            metrics_data = {
                "metrics": self.metrics_buffer.copy(),
                "uploaded_at": dt.datetime.now().isoformat(),
                "run_id": self.run_id
            }
            
            # Convert to JSON string
            metrics_json = json.dumps(metrics_data, indent=2)
            
            # Upload to Minio
            object_name = f"validator_{self.validator_uid}/{self.run_id}/metrics_{timestamp}.json"
            
            self.minio_client.put_object(
                self.bucket_name,
                object_name,
                io.BytesIO(metrics_json.encode('utf-8')),
                len(metrics_json.encode('utf-8')),
                content_type='application/json'
            )
            
            bt.logging.debug(f"Uploaded {len(self.metrics_buffer)} metrics to Minio")
            self.metrics_buffer = []  # Clear buffer
            
        except S3Error as e:
            bt.logging.error(f"Error uploading metrics to Minio: {e}")
            
    def _upload_summary(self):
        """Upload run summary"""
        if not self.run_id:
            return
            
        try:
            # Create summary
            summary_data = {
                "run_id": self.run_id,
                "validator_uid": self.validator_uid,
                "start_time": self.run_start.isoformat() if self.run_start else None,
                "end_time": dt.datetime.now().isoformat(),
                "duration_seconds": (dt.datetime.now() - self.run_start).total_seconds() if self.run_start else 0,
                "config": self.config_data,
                "logs_count": len(self.current_log_buffer),
                "metrics_count": len(self.metrics_buffer)
            }
            
            # Convert to JSON string
            summary_json = json.dumps(summary_data, indent=2)
            
            # Upload to Minio
            object_name = f"validator_{self.validator_uid}/{self.run_id}/summary.json"
            
            self.minio_client.put_object(
                self.bucket_name,
                object_name,
                io.BytesIO(summary_json.encode('utf-8')),
                len(summary_json.encode('utf-8')),
                content_type='application/json'
            )
            
            bt.logging.debug(f"Uploaded run summary to Minio")
            
        except S3Error as e:
            bt.logging.error(f"Error uploading summary to Minio: {e}")


class ValidatorMinioLogCapture:
    """Context manager to capture stdout/stderr and bittensor logs"""
    
    def __init__(self, minio_logger: ValidatorMinioLogger, capture_stdout: bool = True, capture_stderr: bool = True):
        self.minio_logger = minio_logger
        self.capture_stdout = capture_stdout
        self.capture_stderr = capture_stderr
        self.original_stdout = None
        self.original_stderr = None
        self.original_bt_logging_function = None
        
    def __enter__(self):
        # Capture stdout/stderr
        if self.capture_stdout:
            import sys
            self.original_stdout = sys.stdout
            sys.stdout = self._create_wrapper(self.original_stdout, "INFO")
            
        if self.capture_stderr:
            import sys
            self.original_stderr = sys.stderr
            sys.stderr = self._create_wrapper(self.original_stderr, "ERROR")
        
        # Also hook into bittensor logging directly
        try:
            import bittensor as bt
            import logging
            
            # Hook into multiple bittensor logging methods
            methods_to_hook = ['info', 'debug', 'trace', 'error', 'warning', 'success']
            self.original_bt_methods = {}
            
            for method_name in methods_to_hook:
                if hasattr(bt.logging, method_name):
                    original_method = getattr(bt.logging, method_name)
                    self.original_bt_methods[method_name] = original_method
                    
                    # Create wrapper for each method (fix closure issue)
                    def create_wrapper(orig_method, level_name, minio_logger):
                        def wrapper(message, *args, **kwargs):
                            # Call original method
                            result = orig_method(message, *args, **kwargs)
                            
                            # Also send to Minio
                            try:
                                formatted_message = str(message) % args if args else str(message)
                                minio_logger.log_stdout(formatted_message, level_name.upper())
                            except Exception:
                                pass
                            
                            return result
                        return wrapper
                    
                    # Replace the method
                    setattr(bt.logging, method_name, create_wrapper(original_method, method_name, self.minio_logger))
                    
        except Exception as e:
            print(f"Could not hook bittensor logging: {e}")
            
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.original_stdout:
            import sys
            sys.stdout = self.original_stdout
        if self.original_stderr:
            import sys
            sys.stderr = self.original_stderr
            
        # Restore bittensor logging methods
        if hasattr(self, 'original_bt_methods'):
            try:
                import bittensor as bt
                for method_name, original_method in self.original_bt_methods.items():
                    setattr(bt.logging, method_name, original_method)
            except:
                pass
    
            
    def _create_wrapper(self, original_stream, level):
        """Create a wrapper that logs to both original stream and Minio"""
        class StreamWrapper:
            def __init__(self, original, minio_logger, level):
                self.original = original
                self.minio_logger = minio_logger
                self.level = level
                
            def write(self, text):
                # Write to original stream
                self.original.write(text)
                
                # Log to Minio if it's substantial content
                if text.strip():
                    self.minio_logger.log_stdout(text.strip(), self.level)
                    
            def flush(self):
                self.original.flush()
                
        return StreamWrapper(original_stream, self.minio_logger, level)