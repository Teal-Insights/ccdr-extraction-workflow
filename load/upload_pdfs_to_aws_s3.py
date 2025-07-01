#!/usr/bin/env python3
"""
Python version of upload_pdfs_to_aws_s3.sh

This script:
1. Verifies required environment variables
2. Checks AWS SSO authentication
3. Creates S3 bucket if it doesn't exist
4. Syncs local PDF files to S3 (skips JSON and other non-PDF files)

Environment variables required:
- S3_BUCKET_NAME: Name of the S3 bucket
- AWS_REGION: AWS region for the bucket

Environment variables optional:
- AWS_PROFILE: AWS profile name to use (if not set, uses default credential chain)

Note: This script uses your existing AWS SSO session. Make sure to run 'aws sso login' first.
If using a specific profile, run 'aws sso login --profile <profile_name>' instead.

Usage:
    uv run -m extract.upload_pdfs_to_aws_s3
"""

import os
from pathlib import Path
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, TokenRetrievalError
from dotenv import load_dotenv
import logging

from load.schema import Document

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def verify_environment_variables() -> tuple[str, str]:
    """Verify required environment variables are set."""
    bucket_name = os.getenv("S3_BUCKET_NAME")
    aws_region = os.getenv("AWS_REGION")

    if not bucket_name or not aws_region:
        raise ValueError("ERROR: S3_BUCKET_NAME and AWS_REGION must be set")

    return bucket_name, aws_region


def get_aws_session():
    """Get AWS session using SSO profile if available."""
    # Try to use a specified AWS profile, fall back to default
    profile_name = os.getenv("AWS_PROFILE")

    if profile_name:
        try:
            session = boto3.Session(profile_name=profile_name)
            # Test the session
            sts_client = session.client("sts")
            identity = sts_client.get_caller_identity()
            logger.info(f"Using AWS profile: {profile_name}")
            logger.info(f"Authenticated as: {identity.get('Arn', 'Unknown')}")
            return session
        except Exception as e:
            logger.warning(f"Could not use AWS profile {profile_name}: {e}")
            logger.info("Falling back to default credential chain...")
    else:
        logger.info("No AWS_PROFILE specified, using default credential chain...")

    # Fall back to default session (will use SSO if configured as default)
    return boto3.Session()


def get_s3_client():
    """Initializes and returns a boto3 S3 client."""
    # This reuses the logic from the original script to get a session.
    # It assumes credentials/profile are configured via environment variables or SSO.
    try:
        session = get_aws_session()
        sts_client = session.client("sts")
        sts_client.get_caller_identity()  # Test credentials
        return session.client("s3")
    except (NoCredentialsError, Exception):
        print("Falling back to default AWS credential chain.")
        return boto3.client("s3")


def upload_file_to_s3(
    local_path: str, doc: Document, s3_client, bucket_name: str
) -> str:
    """
    Uploads a single file to S3 and returns its public URL.

    Args:
        local_path: The path to the local file to upload.
        doc: The Document object, used for naming the S3 key.
        s3_client: The boto3 S3 client instance.
        bucket_name: The name of the S3 bucket.

    Returns:
        The final S3 storage URL for the object.
    """
    local_file = Path(local_path)

    # The S3 key should mirror the local path structure for stability.
    s3_key = f"pub_{doc.publication_id}/doc_{doc.id}{local_file.suffix}"

    print(
        f"  -> Uploading '{local_file.name}' to S3 bucket '{bucket_name}' with key '{s3_key}'"
    )
    s3_client.upload_file(str(local_file), bucket_name, s3_key)

    # Construct the final S3 URL
    region = s3_client.meta.region_name
    storage_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"
    print(f"  -> Upload complete. Storage URL: {storage_url}")

    return storage_url


def check_aws_authentication() -> tuple[bool, boto3.Session | None]:
    """Check if AWS SSO is authenticated and return session."""
    try:
        session = get_aws_session()
        # Try to get caller identity using the session
        sts_client = session.client("sts")
        identity = sts_client.get_caller_identity()
        logger.info(f"Authentication successful")
        return True, session
    except (NoCredentialsError, TokenRetrievalError) as e:
        logger.error("ERROR: AWS not authenticated or session expired.")
        profile_name = os.getenv("AWS_PROFILE")
        if profile_name:
            logger.error(f"Please run 'aws sso login --profile {profile_name}' first.")
        else:
            logger.error(
                "Please run 'aws sso login' or set AWS_PROFILE environment variable."
            )
        logger.error(f"Details: {e}")
        return False, None
    except ClientError as e:
        logger.error(f"AWS authentication error: {e}")
        return False, None


def create_bucket_if_not_exists(
    bucket_name: str, region: str, session: boto3.Session
) -> None:
    """Create S3 bucket if it doesn't exist."""
    s3_client = session.client("s3", region_name=region)

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "404":
            # Bucket doesn't exist, create it
            try:
                if region == "us-east-1":
                    # us-east-1 doesn't need LocationConstraint
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={"LocationConstraint": region},
                    )
                logger.info(f"Created bucket {bucket_name}")
            except ClientError as create_error:
                logger.error(f"Failed to create bucket: {create_error}")
                raise
        else:
            logger.error(f"Error checking bucket: {e}")
            raise


def sync_data_to_s3(bucket_name: str, session: boto3.Session) -> None:
    """Sync local PDF files to S3 bucket."""
    s3_client = session.client("s3")
    data_dir = Path("extract/data")

    if not data_dir.exists():
        logger.error(f"Data directory {data_dir} does not exist")
        return

    logger.info("Starting S3 sync for PDF files only...")
    logger.info(
        "~670 GB of data, which, at AWS storage and usage rates, likely amounts to ~$20 per month"
    )

    # Walk through all files in the data directory
    total_files = 0
    uploaded_files = 0
    skipped_files = 0

    for file_path in data_dir.rglob("*"):
        if file_path.is_file():
            total_files += 1

            # Only upload PDF files
            if file_path.suffix.lower() != ".pdf":
                logger.debug(f"Skipping non-PDF file: {file_path.name}")
                skipped_files += 1
                continue

            # Calculate S3 key (use filename directly)
            s3_key = file_path.name

            try:
                # Check if file already exists in S3
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                    logger.debug(f"PDF already exists in S3: {s3_key}")
                except ClientError as e:
                    if e.response.get("Error", {}).get("Code", "") == "404":
                        # File doesn't exist, upload it
                        s3_client.upload_file(str(file_path), bucket_name, s3_key)
                        uploaded_files += 1
                        logger.info(f"Uploaded PDF: {s3_key}")
                    else:
                        logger.error(f"Error checking file {s3_key}: {e}")
            except Exception as e:
                logger.error(f"Error uploading {file_path}: {e}")

    logger.info(
        f"S3 sync completed. {uploaded_files} new PDF files uploaded out of {total_files - skipped_files} total PDF files."
    )
    logger.info(f"Skipped {skipped_files} non-PDF files.")


def cleanup_local_files() -> None:
    """
    Clean up local PDF files after S3 upload is complete.
    This removes all PDF files from the extract/data directory.
    """
    logger.info("--- Cleaning up local PDF files ---")
    
    data_dir = Path("extract/data")
    if not data_dir.exists():
        logger.info("No data directory found to clean up.")
        return
    
    pdf_files = list(data_dir.rglob("*.pdf"))
    
    if not pdf_files:
        logger.info("No PDF files found to clean up.")
        return
    
    for pdf_file in pdf_files:
        try:
            os.remove(pdf_file)
            logger.info(f"  -> Removed: {pdf_file}")
        except Exception as e:
            logger.error(f"  -> Failed to remove {pdf_file}: {e}")
    
    logger.info(f"--- Cleaned up {len(pdf_files)} PDF files ---")


def main(cleanup_after_upload: bool = False):
    """Main function to run the S3 upload workflow.
    
    Args:
        cleanup_after_upload: Whether to clean up local PDF files after successful upload.
    """
    try:
        # Verify environment variables
        bucket_name, aws_region = verify_environment_variables()
        logger.info(f"Using bucket: {bucket_name} in region: {aws_region}")

        # Check AWS SSO authentication
        auth_success, session = check_aws_authentication()
        if not auth_success or session is None:
            profile_name = os.getenv("AWS_PROFILE")
            if profile_name:
                logger.info(
                    f"To authenticate, run: aws sso login --profile {profile_name}"
                )
            else:
                logger.info(
                    "To authenticate, run: aws sso login (or set AWS_PROFILE and run aws sso login --profile <profile_name>)"
                )
            return 1

        # Create bucket if needed
        create_bucket_if_not_exists(bucket_name, aws_region, session)

        # Sync data to S3
        sync_data_to_s3(bucket_name, session)

        logger.info("AWS S3 setup and upload completed successfully!")
        
        # Clean up local files if requested
        if cleanup_after_upload:
            cleanup_local_files()

        return 0

    except Exception as e:
        logger.error(f"Script failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
