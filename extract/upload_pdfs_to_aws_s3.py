#!/usr/bin/env python3
"""
Python version of upload_pdfs_to_aws_s3.sh

This script:
1. Verifies required environment variables
2. Checks AWS authentication
3. Creates S3 bucket if it doesn't exist
4. Creates IAM user and group with appropriate permissions
5. Generates access keys and updates .env file
6. Syncs local data to S3

Environment variables required:
- S3_BUCKET_NAME: Name of the S3 bucket
- AWS_REGION: AWS region for the bucket

Usage:
    uv run -m extract.upload_pdfs_to_aws_s3
"""

import os
import json
import re
from pathlib import Path
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def verify_environment_variables() -> tuple[str, str]:
    """Verify required environment variables are set."""
    bucket_name = os.getenv('S3_BUCKET_NAME')
    aws_region = os.getenv('AWS_REGION')
    
    if not bucket_name or not aws_region:
        raise ValueError("ERROR: S3_BUCKET_NAME and AWS_REGION must be set")
    
    return bucket_name, aws_region


def check_aws_authentication() -> bool:
    """Check if AWS CLI is authenticated."""
    try:
        sts_client = boto3.client('sts')
        sts_client.get_caller_identity()
        return True
    except (NoCredentialsError, ClientError) as e:
        logger.error("ERROR: AWS CLI not authenticated. Run 'aws sso login' first.")
        logger.error(f"Details: {e}")
        return False


def create_bucket_if_not_exists(bucket_name: str, region: str) -> None:
    """Create S3 bucket if it doesn't exist."""
    s3_client = boto3.client('s3', region_name=region)
    
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket doesn't exist, create it
            try:
                if region == 'us-east-1':
                    # us-east-1 doesn't need LocationConstraint
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                logger.info(f"Created bucket {bucket_name}")
            except ClientError as create_error:
                logger.error(f"Failed to create bucket: {create_error}")
                raise
        else:
            logger.error(f"Error checking bucket: {e}")
            raise


def create_iam_user(bucket_name: str) -> str:
    """Create IAM user if it doesn't exist."""
    iam_client = boto3.client('iam')
    user_name = f"{bucket_name}-user"
    
    try:
        iam_client.get_user(UserName=user_name)
        logger.info(f"IAM user {user_name} already exists")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            iam_client.create_user(UserName=user_name)
            logger.info(f"Created IAM user: {user_name}")
        else:
            logger.error(f"Error checking user: {e}")
            raise
    
    return user_name


def create_iam_group(bucket_name: str) -> str:
    """Create IAM group if it doesn't exist."""
    iam_client = boto3.client('iam')
    group_name = f"{bucket_name}-group"
    
    try:
        iam_client.get_group(GroupName=group_name)
        logger.info(f"IAM group {group_name} already exists")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            iam_client.create_group(GroupName=group_name)
            logger.info(f"Created IAM group: {group_name}")
        else:
            logger.error(f"Error checking group: {e}")
            raise
    
    return group_name


def add_user_to_group(user_name: str, group_name: str) -> None:
    """Add user to group."""
    iam_client = boto3.client('iam')
    
    try:
        iam_client.add_user_to_group(GroupName=group_name, UserName=user_name)
        logger.info(f"Added user {user_name} to group {group_name}")
    except ClientError as e:
        if e.response['Error']['Code'] != 'EntityAlreadyExists':
            logger.error(f"Error adding user to group: {e}")
            raise


def create_and_attach_policy(bucket_name: str, group_name: str) -> str:
    """Create S3 read policy and attach to group."""
    iam_client = boto3.client('iam')
    policy_name = f"{bucket_name}-read"
    
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetObject"],
            "Resource": [
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*"
            ]
        }]
    }
    
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )
        policy_arn = response['Policy']['Arn']
        logger.info(f"Created policy: {policy_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            # Policy already exists, get its ARN
            account_id = boto3.client('sts').get_caller_identity()['Account']
            policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"
            logger.info(f"Policy {policy_name} already exists")
        else:
            logger.error(f"Error creating policy: {e}")
            raise
    
    # Attach policy to group
    try:
        iam_client.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
        logger.info(f"Attached policy to group {group_name}")
    except ClientError as e:
        if e.response['Error']['Code'] != 'EntityAlreadyExists':
            logger.error(f"Error attaching policy: {e}")
            raise
    
    return policy_arn


def create_access_keys(user_name: str) -> tuple[str, str]:
    """Create access keys for the user."""
    iam_client = boto3.client('iam')
    
    try:
        response = iam_client.create_access_key(UserName=user_name)
        access_key_id = response['AccessKey']['AccessKeyId']
        secret_access_key = response['AccessKey']['SecretAccessKey']
        
        logger.info(f"AWS_ACCESS_KEY_ID: {access_key_id}")
        logger.info(f"AWS_SECRET_ACCESS_KEY: {secret_access_key}")
        
        return access_key_id, secret_access_key
    except ClientError as e:
        logger.error(f"Error creating access keys: {e}")
        raise


def update_env_file(access_key_id: str, secret_access_key: str) -> None:
    """Update .env file with new AWS credentials."""
    env_file = Path(".env")
    
    # Read existing .env file if it exists
    env_content = ""
    if env_file.exists():
        env_content = env_file.read_text()
    
    # Check and update AWS_ACCESS_KEY_ID
    if "AWS_ACCESS_KEY_ID=" in env_content:
        # Extract current value
        match = re.search(r'AWS_ACCESS_KEY_ID=(.*)$', env_content, re.MULTILINE)
        if match and match.group(1).strip():
            logger.error("ERROR: .env file already contains a non-empty AWS_ACCESS_KEY_ID; value will not be overwritten.")
            logger.error(f"Please manually add the following to your .env file:")
            logger.error(f"AWS_ACCESS_KEY_ID={access_key_id}")
        else:
            # Replace empty value
            env_content = re.sub(r'AWS_ACCESS_KEY_ID=.*$', f'AWS_ACCESS_KEY_ID={access_key_id}', env_content, flags=re.MULTILINE)
    else:
        # Add new line
        env_content += f"\nAWS_ACCESS_KEY_ID={access_key_id}"
    
    # Check and update AWS_SECRET_ACCESS_KEY
    if "AWS_SECRET_ACCESS_KEY=" in env_content:
        # Extract current value
        match = re.search(r'AWS_SECRET_ACCESS_KEY=(.*)$', env_content, re.MULTILINE)
        if match and match.group(1).strip():
            logger.error("ERROR: .env file already contains a non-empty AWS_SECRET_ACCESS_KEY; value will not be overwritten.")
            logger.error(f"Please manually add the following to your .env file:")
            logger.error(f"AWS_SECRET_ACCESS_KEY={secret_access_key}")
        else:
            # Replace empty value
            env_content = re.sub(r'AWS_SECRET_ACCESS_KEY=.*$', f'AWS_SECRET_ACCESS_KEY={secret_access_key}', env_content, flags=re.MULTILINE)
    else:
        # Add new line
        env_content += f"\nAWS_SECRET_ACCESS_KEY={secret_access_key}"
    
    # Write back to file
    env_file.write_text(env_content.strip() + "\n")
    logger.info("Updated .env file with AWS credentials")


def sync_data_to_s3(bucket_name: str) -> None:
    """Sync local data directory to S3 bucket."""
    s3_client = boto3.client('s3')
    data_dir = Path("extract/data")
    
    if not data_dir.exists():
        logger.error(f"Data directory {data_dir} does not exist")
        return
    
    logger.info("Starting S3 sync...")
    logger.info("~670 GB of data, which, at AWS storage and usage rates, likely amounts to ~$20 per month")
    
    # Walk through all files in the data directory
    total_files = 0
    uploaded_files = 0
    
    for file_path in data_dir.rglob("*"):
        if file_path.is_file():
            total_files += 1
            # Calculate S3 key (relative path from data directory)
            relative_path = file_path.relative_to(data_dir)
            s3_key = str(relative_path)
            
            try:
                # Check if file already exists in S3
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                    logger.debug(f"File already exists in S3: {s3_key}")
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # File doesn't exist, upload it
                        s3_client.upload_file(str(file_path), bucket_name, s3_key)
                        uploaded_files += 1
                        logger.info(f"Uploaded: {s3_key}")
                    else:
                        logger.error(f"Error checking file {s3_key}: {e}")
            except Exception as e:
                logger.error(f"Error uploading {file_path}: {e}")
    
    logger.info(f"S3 sync completed. {uploaded_files} new files uploaded out of {total_files} total files.")


def main():
    """Main function to run the S3 upload workflow."""
    try:
        # Verify environment variables
        bucket_name, aws_region = verify_environment_variables()
        logger.info(f"Using bucket: {bucket_name} in region: {aws_region}")
        
        # Check AWS authentication
        if not check_aws_authentication():
            return 1
        
        # Create bucket if needed
        create_bucket_if_not_exists(bucket_name, aws_region)
        
        # Create IAM user and group
        user_name = create_iam_user(bucket_name)
        group_name = create_iam_group(bucket_name)
        
        # Add user to group
        add_user_to_group(user_name, group_name)
        
        # Create and attach policy
        create_and_attach_policy(bucket_name, group_name)
        
        # Create access keys
        access_key_id, secret_access_key = create_access_keys(user_name)
        
        # Update .env file
        update_env_file(access_key_id, secret_access_key)
        
        # Sync data to S3
        sync_data_to_s3(bucket_name)
        
        logger.info("AWS S3 setup and upload completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 