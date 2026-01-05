"""R2 client singleton for cloud storage operations."""

import os
import io
from typing import Optional

_s3_client = None


def is_cloud_mode() -> bool:
    return os.environ.get('CI', '').lower() == 'true'


def get_connector_name() -> str:
    return os.environ.get('CONNECTOR_NAME', 'unknown')


def get_bucket_name() -> str:
    return os.environ['R2_BUCKET_NAME']


def _get_r2_config() -> dict:
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
        _s3_client = boto3.client(
            's3',
            endpoint_url=f"https://{config['account_id']}.r2.cloudflarestorage.com",
            aws_access_key_id=config['access_key_id'],
            aws_secret_access_key=config['secret_access_key'],
            region_name='auto'
        )
    return _s3_client


def upload_bytes(data: bytes, key: str) -> str:
    """Upload bytes to R2. Returns S3 URI."""
    client = get_s3_client()
    bucket = get_bucket_name()
    client.put_object(Bucket=bucket, Key=key, Body=data)
    return f"s3://{bucket}/{key}"


def upload_file(file_path: str, key: str) -> str:
    """Upload a local file to R2. Returns S3 URI."""
    client = get_s3_client()
    bucket = get_bucket_name()
    client.upload_file(file_path, bucket, key)
    return f"s3://{bucket}/{key}"


def download_bytes(key: str) -> Optional[bytes]:
    """Download bytes from R2. Returns None if not found."""
    client = get_s3_client()
    bucket = get_bucket_name()
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except client.exceptions.NoSuchKey:
        return None


def get_storage_options() -> dict:
    """Get storage options for deltalake S3 writes."""
    config = _get_r2_config()
    return {
        'AWS_ENDPOINT_URL': f"https://{config['account_id']}.r2.cloudflarestorage.com",
        'AWS_ACCESS_KEY_ID': config['access_key_id'],
        'AWS_SECRET_ACCESS_KEY': config['secret_access_key'],
        'AWS_REGION': 'auto',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    }


def get_delta_table_uri(dataset_name: str) -> str:
    """Get the S3 URI for a Delta table."""
    return f"s3://{get_bucket_name()}/{get_connector_name()}/data/subsets/{dataset_name}"
