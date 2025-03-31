# S3 Storage Implementation Details

This document provides detailed information about the S3 storage implementation for Data Universe.

## Architecture Overview

The S3 storage system consists of several key components:

![Architecture Diagram](./architecture.png)

### Components

1. **S3MinerStorage**: 
   - Handles data uploads from miners to S3
   - Uses blockchain signatures for authentication
   - Manages local state for tracking uploads

2. **S3ValidatorStorage**: 
   - Allows validators to securely read miner data
   - Provides validation methods to verify data integrity
   - Uses temporary credentials with restricted access

3. **DualStorage**: 
   - Migration bridge between HuggingFace and S3 storage
   - Can upload to both systems simultaneously
   - Configurable to gradually transition between systems

4. **Authentication Lambda**: 
   - AWS Lambda function that verifies blockchain signatures
   - Issues temporary AWS credentials with limited permissions
   - Maps blockchain identities to specific S3 paths

## Authentication Flow

The blockchain-based authentication flow works as follows:

1. **Miner/Validator Authentication Request**:
   ```
   +----------------+                +------------------+
   | Miner/Validator|                | Auth Lambda      |
   |                | 1. Sign Request|                  |
   |                +--------------->|                  |
   |                |                |                  |
   |                | 2. Temp Creds  |                  |
   |                |<---------------+                  |
   +----------------+                +------------------+
   ```

2. **Signature Creation**:
   - Miner/validator creates a message containing:
     - Their hotkey (blockchain identity)
     - Timestamp and expiry time
     - File hash (for uploads) or intended path (for reads)
   - Signs the message with their blockchain private key

3. **Signature Verification**:
   - Lambda verifies the signature against the blockchain identity
   - Ensures the timestamp is recent (prevents replay attacks)
   - Validates that the requested access matches the identity

4. **Credential Issuance**:
   - Lambda creates an IAM policy restricting access to specific S3 paths
   - Issues temporary AWS credentials valid for a limited time (typically 1 hour)
   - Credentials contain the minimal permissions necessary

5. **S3 Operations**:
   - Miner/validator uses temporary credentials to perform S3 operations
   - All operations are logged in AWS CloudTrail for auditing
   - Credentials expire automatically after the specified duration

## Security Model

The security model is based on several key principles:

1. **Trustless Authentication**:
   - No shared secrets or central authentication server
   - All authentication tied directly to blockchain identities
   - Signatures can be verified by anyone with the public key

2. **Least Privilege Access**:
   - Miners can only write to their own data paths
   - Validators can only read data they're authorized to validate
   - No user has global access to the S3 bucket

3. **Time-Limited Credentials**:
   - All AWS credentials expire automatically
   - Short validity periods minimize risk from credential exposure
   - Replay attacks prevented by timestamps in signed messages

4. **Path Isolation**:
   - Data is organized by source type and miner hotkey
   - Path structure: `data/{source_id}/{miner_hotkey}/{file_name}`
   - Ensures miners cannot access each other's data

5. **Content Integrity**:
   - File hashes included in signed upload requests
   - Validators can verify data hasn't been tampered with
   - Challenge-response mechanism for data verification

## Migration Strategy

The migration from HuggingFace to S3 involves several phases:

1. **Dual Storage Phase** (1-2 months):
   - Deploy S3 storage infrastructure
   - Configure miners to use DualStorage
   - Upload to both HuggingFace and S3 simultaneously
   - Validators continue using HuggingFace as primary source

2. **Validation Phase** (2-4 weeks):
   - Configure some validators to use S3ValidatorStorage
   - Compare validation results between HuggingFace and S3
   - Resolve any discrepancies or issues

3. **Cutover Phase** (1-2 weeks):
   - Switch all validators to S3ValidatorStorage
   - Set miners to S3-only mode (disable HuggingFace uploads)
   - Monitor system performance and reliability

4. **Legacy Cleanup** (after successful cutover):
   - Archive important data from HuggingFace
   - Remove HuggingFace-specific code paths
   - Optimize S3 storage implementation

## AWS Infrastructure Requirements

To implement this system, you'll need:

1. **S3 Bucket**:
   - Standard S3 bucket with versioning enabled
   - Server-side encryption for data at rest
   - Lifecycle policies for cost optimization

2. **Lambda Function**:
   - Python-based Lambda function for authentication
   - API Gateway for HTTPS endpoint
   - IAM role with permissions to assume other roles

3. **IAM Roles**:
   - `DataUniverseUploadRole`: For miners to upload data
   - `DataUniverseReadRole`: For validators to read data
   - Both with restrictive policies for least privilege

4. **CloudWatch**:
   - Logs for Lambda function and S3 access
   - Alarms for suspicious activity
   - Metrics for system performance

## Cost Optimization

To optimize costs, the implementation includes:

1. **Intelligent Uploads**:
   - Incremental uploads based on state tracking
   - Deduplication of data between miners
   - Compression of data before upload

2. **S3 Storage Classes**:
   - Recent data in Standard storage class
   - Older data automatically transitions to Infrequent Access
   - Historical data archived to Glacier after defined period

3. **Lifecycle Policies**:
   - Automatic cleanup of temporary files
   - Version expiration for older data
   - Size-based limits per miner to prevent abuse

4. **Lambda Optimization**:
   - Credential caching to reduce Lambda invocations
   - Batched operations to minimize API calls
   - Efficient signature verification algorithms