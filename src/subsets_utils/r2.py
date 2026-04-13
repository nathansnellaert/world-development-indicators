"""R2/S3 client for direct boto3 operations (used by runner.py for log uploads)."""

import os

_s3_client = None


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
        from botocore.config import Config
        r2_config = _get_r2_config()
        # 8 attempts with exponential backoff = ~2 min total retry time
        boto_config = Config(retries={'max_attempts': 8, 'mode': 'adaptive'})
        _s3_client = boto3.client(
            's3',
            endpoint_url=f"https://{r2_config['account_id']}.r2.cloudflarestorage.com",
            aws_access_key_id=r2_config['access_key_id'],
            aws_secret_access_key=r2_config['secret_access_key'],
            region_name='auto',
            config=boto_config
        )
    return _s3_client


def upload_file(file_path: str, key: str) -> str:
    """Upload a local file to R2. Returns S3 URI."""
    client = get_s3_client()
    bucket = os.environ['R2_BUCKET_NAME']
    client.upload_file(file_path, bucket, key)
    return f"s3://{bucket}/{key}"


def upload_bytes(data: bytes, key: str) -> str:
    """Upload bytes to R2. Returns S3 URI."""
    client = get_s3_client()
    bucket = os.environ['R2_BUCKET_NAME']
    client.put_object(Bucket=bucket, Key=key, Body=data)
    return f"s3://{bucket}/{key}"


def head_object(key: str) -> dict | None:
    """Get object metadata from R2 without downloading. Returns None if not found."""
    client = get_s3_client()
    bucket = os.environ['R2_BUCKET_NAME']
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return None
        raise


def download_bytes(key: str) -> bytes | None:
    """Download bytes from R2. Returns None if not found."""
    client = get_s3_client()
    bucket = os.environ['R2_BUCKET_NAME']
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except client.exceptions.NoSuchKey:
        return None


def list_keys(prefix: str) -> list[str]:
    """List all keys in R2 with given prefix."""
    client = get_s3_client()
    bucket = os.environ['R2_BUCKET_NAME']
    keys = []

    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])

    return keys


