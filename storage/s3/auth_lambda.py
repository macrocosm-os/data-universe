"""
Example AWS Lambda function for blockchain-based S3 authentication.

This file serves as a template for implementing the AWS Lambda function that verifies
blockchain signatures and grants temporary access to S3 storage. This file should be 
deployed to AWS Lambda with appropriate IAM permissions for S3 access management.
"""
import os
import json
import time
import hashlib
import boto3
import base64
from typing import Dict, Any, Tuple
from botocore.exceptions import ClientError

# Bittensor libraries would need to be packaged with the Lambda function
# This is a simplified example showing the concept

# S3 bucket name from environment variables
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'data-universe-storage')
UPLOAD_ROLE_ARN = os.environ.get('UPLOAD_ROLE_ARN', 'arn:aws:iam::ACCOUNT_ID:role/DataUniverseUploadRole')
READ_ROLE_ARN = os.environ.get('READ_ROLE_ARN', 'arn:aws:iam::ACCOUNT_ID:role/DataUniverseReadRole')


def verify_bittensor_signature(hotkey: str, message: str, signature: str) -> bool:
    """
    Verify a signature using the Bittensor wallet.
    
    This is a placeholder - in a real implementation, this would use the Bittensor
    signature verification logic.
    
    Args:
        hotkey: The hotkey address
        message: The message that was signed
        signature: The signature to verify
        
    Returns:
        True if signature is valid, False otherwise
    """
    try:
        # In a real implementation, this would use bittensor's cryptographic verification
        # from substrateinterface import Keypair
        # keypair = Keypair(ss58_address=hotkey)
        # return keypair.verify(message.encode(), bytes.fromhex(signature))
        
        # This is a placeholder for demonstration
        print(f"Verifying signature for {hotkey} on message: {message}")
        return True  # In real implementation, this would be actual verification
    except Exception as e:
        print(f"Error verifying signature: {str(e)}")
        return False


def get_upload_credentials(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate temporary credentials for S3 file upload.
    
    Args:
        event: The Lambda event containing authentication details
        
    Returns:
        Response with temporary credentials
    """
    # Extract parameters
    hotkey = event['hotkey']
    timestamp = event['timestamp']
    expiry = event['expiry']
    file_hash = event['file_hash']
    signature = event['signature']
    s3_path = event['s3_path']
    
    # Validate timestamp and expiry
    current_time = int(time.time())
    if current_time > expiry:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Request expired"})
        }
    
    # Validate signature
    message = f"{hotkey}:{s3_path}:{file_hash}:{timestamp}:{expiry}"
    if not verify_bittensor_signature(hotkey, message, signature):
        return {
            "statusCode": 401,
            "body": json.dumps({"error": "Invalid signature"})
        }
    
    # Generate policy for specific path
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:PutObject"],
            "Resource": [f"arn:aws:s3:::{S3_BUCKET_NAME}/{s3_path}"]
        }]
    }
    
    # Assume role with temporary credentials
    sts_client = boto3.client('sts')
    
    try:
        assumed_role = sts_client.assume_role(
            RoleArn=UPLOAD_ROLE_ARN,
            RoleSessionName=f"{hotkey}-upload-{timestamp}",
            Policy=json.dumps(policy),
            DurationSeconds=3600  # 1 hour
        )
        
        # Return the credentials
        return {
            "statusCode": 200,
            "body": json.dumps({
                "credentials": assumed_role['Credentials'],
                "s3_path": s3_path
            })
        }
    except ClientError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error assuming role: {str(e)}"})
        }


def get_read_credentials(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate temporary credentials for S3 file reading.
    
    Args:
        event: The Lambda event containing authentication details
        
    Returns:
        Response with temporary credentials
    """
    # Extract parameters
    validator_hotkey = event['validator_hotkey']
    miner_hotkey = event['miner_hotkey']
    timestamp = event['timestamp']
    expiry = event['expiry']
    signature = event['signature']
    
    # Validate timestamp and expiry
    current_time = int(time.time())
    if current_time > expiry:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Request expired"})
        }
    
    # Validate signature
    message = f"read:{validator_hotkey}:{miner_hotkey}:{timestamp}:{expiry}"
    if not verify_bittensor_signature(validator_hotkey, message, signature):
        return {
            "statusCode": 401,
            "body": json.dumps({"error": "Invalid signature"})
        }
    
    # Generate policy for read access to miner's data
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                f"arn:aws:s3:::{S3_BUCKET_NAME}",
                f"arn:aws:s3:::{S3_BUCKET_NAME}/data/*/{miner_hotkey}/*"
            ]
        }]
    }
    
    # Assume role with temporary credentials
    sts_client = boto3.client('sts')
    
    try:
        assumed_role = sts_client.assume_role(
            RoleArn=READ_ROLE_ARN,
            RoleSessionName=f"{validator_hotkey}-read-{timestamp}",
            Policy=json.dumps(policy),
            DurationSeconds=3600  # 1 hour
        )
        
        # Return the credentials
        return {
            "statusCode": 200,
            "body": json.dumps({
                "credentials": assumed_role['Credentials']
            })
        }
    except ClientError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error assuming role: {str(e)}"})
        }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function.
    
    Args:
        event: Lambda event with authentication details
        context: Lambda context
        
    Returns:
        HTTP response with result
    """
    print(f"Received event: {json.dumps(event)}")
    
    # Determine action type
    action = event.get('action', 'upload')
    
    if action == 'read':
        return get_read_credentials(event)
    else:
        return get_upload_credentials(event)