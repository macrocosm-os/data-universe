# S3 Storage Configuration Guide

This guide provides detailed instructions for configuring the S3 storage system for Data Universe.

## Configuration Parameters

### S3Config

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `bucket_name` | string | Yes | N/A | Name of the S3 bucket to store data |
| `auth_endpoint` | string | Yes | N/A | URL of the authentication Lambda API |
| `region` | string | No | `"us-east-1"` | AWS region for the S3 bucket |
| `use_s3` | boolean | No | `true` | Whether to enable S3 storage |
| `endpoint_url` | string | No | `null` | Custom endpoint for S3-compatible storage |
| `max_chunk_size` | integer | No | `1000000` | Maximum rows per chunk for processing |
| `max_thread_workers` | integer | No | `5` | Maximum threads for parallel uploads |

### DualStorageConfig

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `s3_config` | S3Config | Yes | N/A | Configuration for S3 storage |
| `use_s3` | boolean | No | `true` | Whether to enable S3 storage |
| `use_huggingface` | boolean | No | `true` | Whether to enable HuggingFace storage |

## Configuration File Examples

### S3-Only Configuration (YAML)

```yaml
storage:
  type: "s3"
  s3:
    bucket_name: "data-universe-storage"
    auth_endpoint: "https://your-api-gateway-url.amazonaws.com/auth"
    region: "us-east-1"
    max_chunk_size: 2000000  # 2 million rows per chunk
    max_thread_workers: 8    # Use 8 threads for parallel uploads
```

### Dual Storage Configuration (YAML)

```yaml
storage:
  type: "dual"
  use_s3: true
  use_huggingface: true
  s3:
    bucket_name: "data-universe-storage"
    auth_endpoint: "https://your-api-gateway-url.amazonaws.com/auth"
    region: "us-east-1"
```

### S3-Compatible Storage (MinIO) Configuration (YAML)

```yaml
storage:
  type: "s3"
  s3:
    bucket_name: "data-universe"
    auth_endpoint: "https://your-auth-service.example.com/auth"
    region: "us-east-1"
    endpoint_url: "https://minio.example.com"
```

## Environment Variables

The following environment variables can be used to override configuration settings:

| Variable | Purpose |
|----------|---------|
| `DATA_UNIVERSE_S3_BUCKET` | S3 bucket name |
| `DATA_UNIVERSE_S3_AUTH_ENDPOINT` | Authentication endpoint URL |
| `DATA_UNIVERSE_S3_REGION` | AWS region |
| `DATA_UNIVERSE_USE_S3` | Enable/disable S3 storage (true/false) |
| `DATA_UNIVERSE_USE_HF` | Enable/disable HuggingFace storage (true/false) |

Example usage:
```bash
export DATA_UNIVERSE_S3_BUCKET="data-universe-test"
export DATA_UNIVERSE_USE_S3="true"
export DATA_UNIVERSE_USE_HF="false"
```

## AWS IAM Policy Examples

### Upload Role Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::data-universe-storage",
        "arn:aws:s3:::data-universe-storage/data/*"
      ],
      "Condition": {
        "StringLike": {
          "s3:prefix": "data/${source}/${miner_hotkey}/*"
        }
      }
    }
  ]
}
```

### Read Role Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::data-universe-storage",
        "arn:aws:s3:::data-universe-storage/data/*"
      ]
    }
  ]
}
```

## Lambda Function Configuration

The authentication Lambda function should be configured with:

1. **Timeout**: 10 seconds (recommended)
2. **Memory**: 256 MB (minimum)
3. **Environment Variables**:
   - `S3_BUCKET_NAME`: The S3 bucket name
   - `UPLOAD_ROLE_ARN`: ARN of the upload IAM role
   - `READ_ROLE_ARN`: ARN of the read IAM role

## API Gateway Setup

1. Create a new REST API in API Gateway
2. Add routes:
   - `POST /auth` - for upload authentication
   - `POST /auth-read` - for read authentication
3. Configure Lambda proxy integration
4. Enable CORS if needed
5. Deploy the API to a stage (e.g., "prod")
6. Use the resulting URL as the `auth_endpoint` in your configuration

## Monitoring and Troubleshooting

### CloudWatch Log Groups

Monitor these CloudWatch Log Groups:
- `/aws/lambda/data-universe-auth` - Authentication Lambda logs
- `/aws/apigateway/data-universe-api` - API Gateway logs

### Common Issues and Solutions

1. **Authentication Failures**:
   - Check that wallet keys are correctly configured
   - Verify Lambda has correct IAM permissions
   - Ensure clock synchronization for timestamp validation

2. **Access Denied to S3**:
   - Check IAM role permissions
   - Verify path structure matches policy conditions
   - Check for bucket policy restrictions

3. **Performance Issues**:
   - Increase `max_thread_workers` for parallel uploads
   - Use a region closer to your miners/validators
   - Consider using S3 Transfer Acceleration for remote locations