"""R2 Client Singleton for Cloud Storage Operations.

Lazily initializes boto3 S3 client only when needed (CI=true).
Provides helper functions for R2 upload/download operations.
"""

import os
import io
from typing import Optional

_s3_client = None


def is_cloud_mode() -> bool:
    """Check if running in cloud mode (CI environment)."""
    return os.environ.get('CI', '').lower() == 'true'


def get_connector_name() -> str:
    """Get the connector name from environment (set by runner.py)."""
    return os.environ.get('CONNECTOR_NAME', 'unknown')


def _get_r2_config() -> dict:
    """Get R2 configuration from environment variables."""
    return {
        'account_id': os.environ['R2_ACCOUNT_ID'],
        'access_key_id': os.environ['R2_ACCESS_KEY_ID'],
        'secret_access_key': os.environ['R2_SECRET_ACCESS_KEY'],
        'bucket_name': os.environ['R2_BUCKET_NAME'],
    }


def get_s3_client():
    """Get or create the S3 client singleton."""
    global _s3_client

    if _s3_client is None:
        import boto3

        config = _get_r2_config()
        endpoint_url = f"https://{config['account_id']}.r2.cloudflarestorage.com"

        _s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=config['access_key_id'],
            aws_secret_access_key=config['secret_access_key'],
            region_name='auto'
        )

    return _s3_client


def get_bucket_name() -> str:
    """Get the R2 bucket name."""
    return os.environ['R2_BUCKET_NAME']


def upload_bytes(data: bytes, key: str) -> str:
    """Upload bytes to R2.

    Args:
        data: Bytes to upload
        key: Full key path in bucket (e.g., 'data/raw/asset.json')

    Returns:
        S3 URI of uploaded object
    """
    client = get_s3_client()
    bucket = get_bucket_name()

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data
    )

    return f"s3://{bucket}/{key}"


def upload_file(file_path: str, key: str) -> str:
    """Upload a local file to R2.

    Args:
        file_path: Local file path
        key: Full key path in bucket

    Returns:
        S3 URI of uploaded object
    """
    client = get_s3_client()
    bucket = get_bucket_name()

    client.upload_file(file_path, bucket, key)

    return f"s3://{bucket}/{key}"


def upload_fileobj(fileobj: io.IOBase, key: str) -> str:
    """Upload a file-like object to R2.

    Args:
        fileobj: File-like object (must be seeked to start)
        key: Full key path in bucket

    Returns:
        S3 URI of uploaded object
    """
    client = get_s3_client()
    bucket = get_bucket_name()

    client.upload_fileobj(fileobj, bucket, key)

    return f"s3://{bucket}/{key}"


def download_bytes(key: str) -> Optional[bytes]:
    """Download bytes from R2.

    Args:
        key: Full key path in bucket

    Returns:
        Bytes content, or None if key doesn't exist
    """
    client = get_s3_client()
    bucket = get_bucket_name()

    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except client.exceptions.NoSuchKey:
        return None


def object_exists(key: str) -> bool:
    """Check if an object exists in R2.

    Args:
        key: Full key path in bucket

    Returns:
        True if object exists, False otherwise
    """
    client = get_s3_client()
    bucket = get_bucket_name()

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def get_storage_options() -> dict:
    """Get storage options for deltalake S3 writes.

    Returns:
        Dict with AWS credentials and endpoint for delta-rs
    """
    config = _get_r2_config()

    return {
        'AWS_ENDPOINT_URL': f"https://{config['account_id']}.r2.cloudflarestorage.com",
        'AWS_ACCESS_KEY_ID': config['access_key_id'],
        'AWS_SECRET_ACCESS_KEY': config['secret_access_key'],
        'AWS_REGION': 'auto',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',  # Required for R2 compatibility
    }


def get_delta_table_uri(dataset_name: str) -> str:
    """Get the S3 URI for a Delta table.

    Args:
        dataset_name: Name of the dataset

    Returns:
        S3 URI for the Delta table location
    """
    bucket = get_bucket_name()
    connector = get_connector_name()
    return f"s3://{bucket}/{connector}/data/subsets/{dataset_name}"
