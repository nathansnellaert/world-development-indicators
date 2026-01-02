import os
import io
import json
import gzip
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake, DeltaTable
from . import debug
from .environment import get_data_dir


def upload_data(data: pa.Table, dataset_name: str, mode: str = "append", merge_key: str = None) -> str:
    if mode not in ("append", "overwrite", "merge"):
        raise ValueError(f"Invalid mode '{mode}'. Must be 'append', 'overwrite', or 'merge'.")

    if mode == "merge" and not merge_key:
        raise ValueError("merge_key is required when mode='merge'")

    if mode == "overwrite":
        print(f"⚠️  Warning: Overwriting {dataset_name} - all existing data will be replaced")

    if len(data) == 0:
        print(f"No data to upload for {dataset_name}")
        return ""

    size_mb = round(data.nbytes / 1024 / 1024, 2)
    columns = ', '.join([f.name for f in data.schema])
    mode_label = {"append": "Appending to", "overwrite": "Overwriting", "merge": "Merging into"}[mode]
    print(f"{mode_label} {dataset_name}: {len(data)} rows, {len(data.schema)} cols ({columns}), {size_mb} MB")

    table_path = Path(get_data_dir()) / "subsets" / dataset_name

    if mode == "merge":
        if not table_path.exists():
            write_deltalake(str(table_path), data)
            print(f"Created new table {dataset_name}")
        else:
            dt = DeltaTable(str(table_path))
            updates = {col: f"source.{col}" for col in data.column_names}
            (
                dt.merge(
                    source=data,
                    predicate=f"target.{merge_key} = source.{merge_key}",
                    source_alias="source",
                    target_alias="target"
                )
                .when_matched_update(updates=updates)
                .when_not_matched_insert(updates=updates)
                .execute()
            )
            result = dt.to_pyarrow_table()
            print(f"Merged: table now has {len(result)} total rows")
    else:
        write_deltalake(
            str(table_path),
            data,
            mode=mode,
            schema_mode="merge" if mode == "append" else "overwrite"
        )

    null_counts = {}
    for col_name in data.column_names:
        nulls = data[col_name].null_count
        if nulls > 0:
            null_counts[col_name] = nulls

    debug.log_data_output(
        dataset_name=dataset_name,
        row_count=len(data),
        size_bytes=data.nbytes,
        columns=data.column_names,
        column_count=len(data.schema),
        null_counts=null_counts,
        mode=mode
    )

    return str(table_path)


def load_state(asset: str) -> dict:
    """Load state for an asset from local filesystem.

    State is always stored locally to enable version control via Git commits
    in GitHub Actions, providing transparency and audit trail.
    """
    environment = os.environ.get('ENVIRONMENT', 'dev')
    state_file = Path(".state") / environment / f"{asset}.json"

    if state_file.exists():
        with open(state_file, 'r') as f:
            return json.load(f)
    return {}


def save_state(asset: str, state_data: dict) -> str:
    """Save state for an asset to local filesystem.

    State is always stored locally to enable version control via Git commits
    in GitHub Actions, providing transparency and audit trail.
    """
    # Load old state for comparison (for debug logging)
    old_state = load_state(asset)

    # Add metadata to state
    state_data = state_data.copy()
    state_data['_metadata'] = {
        'updated_at': datetime.now().isoformat(),
        'run_id': os.environ.get('RUN_ID', 'unknown')
    }

    # Save state
    environment = os.environ.get('ENVIRONMENT', 'dev')
    state_dir = Path(".state") / environment
    state_dir.mkdir(parents=True, exist_ok=True)

    state_file = state_dir / f"{asset}.json"
    with open(state_file, 'w') as f:
        json.dump(state_data, f, indent=2)

    # Log state change
    debug.log_state_change(asset, old_state, state_data)

    return str(state_file)


def has_changed(new_data: pa.Table, asset_name: str) -> bool:
    """Check if new data differs from the existing asset.

    Compares the new data with what's already stored in the Delta table.
    Returns True if data has changed or if no previous data exists.

    Args:
        new_data: The new PyArrow table to compare
        asset_name: The dataset/asset name

    Returns:
        bool: True if data has changed or doesn't exist, False if unchanged
    """
    table_path = Path(get_data_dir()) / "subsets" / asset_name

    if not table_path.exists():
        return True  # No existing data, so it's "changed"

    try:
        dt = DeltaTable(str(table_path))
        existing_data = dt.to_pyarrow_table()

        # Compare row counts first (fast check)
        if len(new_data) != len(existing_data):
            return True

        # Compare schemas
        if new_data.schema != existing_data.schema:
            return True

        # Compare actual data by converting to bytes and hashing
        # This is more reliable than row-by-row comparison
        new_bytes = new_data.to_pandas().to_csv(index=False)
        existing_bytes = existing_data.to_pandas().to_csv(index=False)

        return new_bytes != existing_bytes

    except Exception:
        return True  # If comparison fails, assume changed


def load_asset(asset_name: str) -> pa.Table:
    """Load a previously saved asset from local Delta table for debugging/development

    This allows loading assets from previous runs to avoid re-fetching data
    during development. Useful when debugging later stages of a pipeline.

    Args:
        asset_name: The dataset/asset name (e.g., 'indicators', 'series')

    Returns:
        pa.Table: The loaded PyArrow table

    Raises:
        FileNotFoundError: If no Delta table found
    """
    table_path = Path(get_data_dir()) / "subsets" / asset_name

    if not table_path.exists():
        raise FileNotFoundError(f"No Delta table found at {table_path}")

    dt = DeltaTable(str(table_path))
    return dt.to_pyarrow_table()


def _get_raw_path(asset_id: str, extension: str) -> Path:
    """Raw directory: DATA_DIR/raw/CONNECTOR/asset_id.ext"""
    connector = os.environ.get('CONNECTOR_NAME', 'unknown')
    path = Path(get_data_dir()) / "raw" / connector / f"{asset_id}.{extension}"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def save_raw_file(content: str | bytes, asset_id: str, extension: str = "txt") -> str:
    """Generic raw saver for CSV, XML, ZIP, etc.

    Args:
        content: String (text/csv) or Bytes (zip/pdf/binary)
        asset_id: The identifier for the asset
        extension: File extension (e.g., 'csv', 'xml', 'zip')
    """
    path = _get_raw_path(asset_id, extension)

    if isinstance(content, str):
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
    else:
        with open(path, 'wb') as f:
            f.write(content)

    print(f"  -> Raw Cache: Saved {asset_id}.{extension}")
    return str(path)


def load_raw_file(asset_id: str, extension: str = "txt") -> str | bytes:
    """Generic raw loader for CSV, XML, ZIP, etc."""
    path = _get_raw_path(asset_id, extension)

    if not path.exists():
        raise FileNotFoundError(f"Raw asset '{asset_id}.{extension}' not found.")

    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except UnicodeDecodeError:
        with open(path, 'rb') as f:
            return f.read()


def save_raw_json(data: any, asset_id: str, compress: bool = False) -> str:
    """Save raw JSON data. Accepts Dict or List.
    Use compress=True for massive datasets to save disk space.
    """
    ext = "json.gz" if compress else "json"
    path = _get_raw_path(asset_id, ext)

    if compress:
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            json.dump(data, f)
    else:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

    print(f"  -> Raw Cache: Saved {asset_id}.{ext}")
    return str(path)


def load_raw_json(asset_id: str) -> any:
    """Load raw JSON data. Auto-detects compression."""
    # Try uncompressed first
    path = _get_raw_path(asset_id, "json")
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    # Try compressed
    path = _get_raw_path(asset_id, "json.gz")
    if path.exists():
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            return json.load(f)

    raise FileNotFoundError(f"Raw asset '{asset_id}' not found.")


def upload_raw_to_r2(data: pa.Table, key: str) -> str:
    """Upload a PyArrow table to Cloudflare R2 as a parquet file.

    For uploading raw/intermediate data to R2 storage. The caller controls
    the full key path.

    Required environment variables:
        R2_ACCOUNT_ID: Cloudflare account ID
        R2_ACCESS_KEY_ID: R2 access key ID
        R2_SECRET_ACCESS_KEY: R2 secret access key
        R2_BUCKET_NAME: R2 bucket name

    Args:
        data: The PyArrow table to upload
        key: The full key path in the bucket (e.g., 'wikipedia/2016-01-01.parquet')

    Returns:
        str: The S3 URI of the uploaded file
    """
    import boto3

    if len(data) == 0:
        print(f"No data to upload for {key}")
        return ""

    account_id = os.environ['R2_ACCOUNT_ID']
    access_key_id = os.environ['R2_ACCESS_KEY_ID']
    secret_access_key = os.environ['R2_SECRET_ACCESS_KEY']
    bucket_name = os.environ['R2_BUCKET_NAME']

    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name='auto'
    )

    buffer = io.BytesIO()
    pq.write_table(data, buffer, compression='snappy')
    buffer.seek(0)

    size_mb = round(buffer.getbuffer().nbytes / 1024 / 1024, 2)
    print(f"Uploading to R2: {key} ({len(data)} rows, {size_mb} MB)")

    s3_client.upload_fileobj(buffer, bucket_name, key)

    return f"s3://{bucket_name}/{key}"