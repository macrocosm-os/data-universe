# Local MinIO Storage for Miners

Decentralized storage system that replaces centralized S3/HF uploads. Miners store data locally by job_id, enabling direct validator querying with DuckDB.

## 🚀 Quick Setup

### 1. Install Dependencies
```bash
pip install minio duckdb
```

### 2. Run Miner with Local Storage
```bash
# For testing (offline mode)
python neurons/miner.py --offline --gravity

# For production
python neurons/miner.py --netuid 13 --wallet.name YOUR_WALLET --gravity
```

### 3. Verify Setup
```bash
# Check MinIO console
open http://localhost:9001

# Check logs for connection info
tail -f logs/miner.log | grep -i minio
```

## 🔧 How It Works

**When you run with `--gravity`:**
1. ✅ Miner automatically downloads DD jobs from `dynamic_desirability/total.json`
2. ✅ MinIO server starts on port 9000 (localhost for testing, 0.0.0.0 for production)
3. ✅ Data gets processed and stored by job_id:
   ```
   miner_storage/minio_data/miner-data/
   ├── default_0/          # Reddit r/Bitcoin
   ├── default_10/         # X #bitcoin  
   └── crawler-xxx/        # Custom jobs
   ```
4. ✅ Validators can query directly with DuckDB

## 🧪 Testing (Offline Mode)

```bash
# Start miner in offline mode
python neurons/miner.py --offline --gravity

# Test with DuckDB
python3 -c "
import duckdb
conn = duckdb.connect()
conn.execute('INSTALL httpfs')
conn.execute('LOAD httpfs')
conn.execute('SET s3_endpoint=\'localhost:9000\'')
conn.execute('SET s3_access_key_id=\'miner_test_hot\'')
conn.execute('SET s3_secret_access_key=\'key_key_offline_secret\'')
conn.execute('SET s3_use_ssl=false')

# Query data
result = conn.execute('SELECT COUNT(*) FROM read_parquet(\'s3://miner-data/*/*.parquet\')').fetchone()
print(f'Total records: {result[0]}')
"
```

## 🌐 Production Setup

**For validators to access your data:**

1. **Configure firewall** (if needed):
   ```bash
   sudo ufw allow 9000/tcp  # MinIO data access
   ```

2. **Credentials are auto-generated** from your miner hotkey:
   - Access Key: `miner_{first_8_chars_of_hotkey}`
   - Secret Key: `key_{last_12_chars_of_hotkey}_secret`

3. **Validators query your endpoint**:
   ```python
   # DuckDB query to your miner
   conn.execute("SET s3_endpoint='YOUR_MINER_IP:9000'")
   result = conn.execute("SELECT * FROM read_parquet('s3://miner-data/default_0/*.parquet') LIMIT 10").fetchdf()
   ```

## ⚙️ Configuration

**Optional configuration via environment or args:**
```bash
# Custom ports (if 9000 conflicts)
export MINIO_PORT=9010
export MINIO_CONSOLE_PORT=9011

# Custom processing settings  
export MINIO_CHUNK_SIZE=50000
export MINIO_RETENTION_DAYS=60
```

## 🔍 Monitoring

**Check if everything is working:**
```bash
# View miner logs
tail -f logs/miner.log | grep -E "(MinIO|job|Uploaded)"

# Check MinIO health
curl http://localhost:9000/minio/health/live

# List stored data
# Install mc client: https://min.io/docs/minio/linux/reference/minio-mc.html
mc alias set local http://localhost:9000 ACCESS_KEY SECRET_KEY
mc ls local/miner-data/
```

## ❓ Troubleshooting

**Common issues:**

1. **`ImportError: No module named 'minio'`**
   ```bash
   pip install minio
   ```

2. **Port 9000 already in use**
   ```bash
   export MINIO_PORT=9010
   python neurons/miner.py --offline --gravity
   ```

3. **No data being processed**
   - Check `dynamic_desirability/total.json` exists
   - Verify SQLite database has data: `sqlite3 SqliteMinerStorage.sqlite "SELECT COUNT(*) FROM DataEntity;"`

4. **Validators can't connect**
   - Check firewall allows port 9000
   - Verify miner IP is accessible: `telnet YOUR_MINER_IP 9000`

## 📊 What Replaces What

| Old System | New System |
|------------|------------|
| ❌ S3PartitionedUploader | ✅ LocalMinIOUploader |
| ❌ Centralized S3 storage | ✅ Local MinIO per miner |
| ❌ Complex S3 credentials | ✅ Simple hotkey-based auth |
| ❌ Upload to external service | ✅ Store locally, query directly |

**Result:** Fully decentralized storage with direct validator access! 🎯

## 🔒 Security Improvements (TODO)

### 1. Read-Only Validator Credentials
**Current Issue:** Validators get ROOT credentials - can DELETE all miner data! 😱

**Solution:** Create read-only credentials for validators:
```python
def create_validator_credentials(self):
    """Create read-only credentials for validators"""
    read_only_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [f"arn:aws:s3:::{self.bucket_name}", f"arn:aws:s3:::{self.bucket_name}/*"]
        }]
    }
    
    validator_access_key = f"validator_{self.miner_hotkey[:8]}"
    validator_secret_key = f"readonly_{self.miner_hotkey[-12:]}_key"
    
    # Apply read-only policy via MinIO admin API
    return {"access_key": validator_access_key, "secret_key": validator_secret_key}
```

### 2. Presigned URLs (More Secure Alternative)
**MinIO supports presigned URLs** - time-limited access without credential sharing:

```python
def generate_presigned_url(self, object_name: str, expires_hours: int = 24):
    """Generate presigned URL for object access"""
    return self.minio_client.presigned_get_object(
        bucket_name=self.bucket_name,
        object_name=object_name,
        expires=timedelta(hours=expires_hours)
    )

# Usage
presigned_url = uploader.generate_presigned_url("default_0/data_latest.parquet", expires_hours=2)

# Validator queries with presigned URL (no credentials!)
result = conn.execute(f"SELECT * FROM read_parquet('{presigned_url}') LIMIT 10").fetchdf()
```

**Benefits:**
- ✅ Time-limited access (expires after X hours)
- ✅ No permanent credentials sharing
- ✅ Granular per-file permissions
- ✅ Works with DuckDB and pandas

**Recommended:** Implement presigned URLs for validator access to eliminate credential security risks.