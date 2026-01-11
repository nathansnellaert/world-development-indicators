"""Data I/O operations for raw data, state, and Delta tables."""

import os
import io
import json
import gzip
import uuid
import hashlib
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake, DeltaTable
from . import debug
from .environment import get_data_dir
from .r2 import _is_cloud_mode, upload_bytes, upload_file, download_bytes, get_storage_options, get_delta_table_uri, get_bucket_name, get_connector_name


# --- Cloud mode disk cache ---
_CACHE_DIR = os.environ.get('CACHE_DIR', '/tmp/subsets_cache')
_CACHE_MIN_FREE_GB = float(os.environ.get('CACHE_MIN_FREE_GB', '1'))


def _get_cache_path(key: str) -> Path:
    """Get local cache path for an R2 key."""
    path = Path(_CACHE_DIR) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _evict_if_needed(required_bytes: int = 0):
    """Evict oldest cached files if disk space is low."""
    cache_dir = Path(_CACHE_DIR)
    if not cache_dir.exists():
        return

    min_free = _CACHE_MIN_FREE_GB * 1024 * 1024 * 1024  # Convert to bytes

    # Check current free space
    stat = shutil.disk_usage(cache_dir)
    if stat.free - required_bytes >= min_free:
        return  # Enough space

    # Get all cached files sorted by mtime (oldest first)
    files = []
    for f in cache_dir.rglob('*'):
        if f.is_file():
            files.append((f.stat().st_mtime, f.stat().st_size, f))
    files.sort()  # Oldest first

    # Delete until we have enough space
    for mtime, size, path in files:
        stat = shutil.disk_usage(cache_dir)
        if stat.free - required_bytes >= min_free:
            break
        path.unlink()
        # Clean empty parent dirs
        try:
            path.parent.rmdir()
        except OSError:
            pass


def _cache_lookup(key: str) -> Optional[Path]:
    """Check if a file is in cache. Returns path if exists, None otherwise."""
    path = Path(_CACHE_DIR) / key
    if path.exists():
        path.touch()  # Update access time for LRU
        return path
    return None


def _compute_table_hash(table: pa.Table) -> str:
    """Compute a stable hash of a PyArrow table for change detection."""
    # Write to parquet bytes for consistent hashing
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    return hashlib.sha256(buffer.getvalue()).hexdigest()[:16]


def _get_hash_state_key(dataset_name: str) -> str:
    return f"_hash_{dataset_name}"


def sync_data(data: pa.Table, dataset_name: str, mode: str = "overwrite") -> str | None:
    """Sync a PyArrow table to a Delta table, only if data has changed.

    Returns the table URI if data was synced, None if no changes detected.
    """
    if len(data) == 0:
        print(f"No data to sync for {dataset_name}")
        return None

    # Compute hash of new data
    new_hash = _compute_table_hash(data)

    # Load existing hash from state
    state = load_state(_get_hash_state_key(dataset_name))
    old_hash = state.get("hash")

    if old_hash == new_hash:
        print(f"No changes detected for {dataset_name} (hash: {new_hash})")
        return None

    # Data has changed, upload it
    size_mb = round(data.nbytes / 1024 / 1024, 2)
    columns = ', '.join([f.name for f in data.schema])
    print(f"Syncing {dataset_name}: {len(data)} rows, {len(data.schema)} cols ({columns}), {size_mb} MB")
    if old_hash:
        print(f"  Hash changed: {old_hash} -> {new_hash}")
    else:
        print(f"  New dataset (hash: {new_hash})")

    if _is_cloud_mode():
        table_uri = get_delta_table_uri(dataset_name)
        storage_options = get_storage_options()
    else:
        table_uri = str(Path(get_data_dir()) / "subsets" / dataset_name)
        storage_options = None

    write_deltalake(table_uri, data, mode=mode, storage_options=storage_options,
                    schema_mode="overwrite")

    # Save new hash to state
    save_state(_get_hash_state_key(dataset_name), {"hash": new_hash})

    # Log output
    null_counts = {col: data[col].null_count for col in data.column_names if data[col].null_count > 0}
    debug.log_data_output(dataset_name=dataset_name, row_count=len(data), size_bytes=data.nbytes,
                          columns=data.column_names, column_count=len(data.schema), null_counts=null_counts, mode=mode)
    return table_uri


# --- Delta table operations ---

def upload_data(data: pa.Table, dataset_name: str, metadata: dict = None, mode: str = "append", merge_key: str = None) -> str:
    """Upload a PyArrow table to a Delta table."""
    from .dag import track_write
    track_write(f"subsets/{dataset_name}", rows=len(data))

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

    table_name = metadata.get("title") if metadata else None
    table_description = json.dumps(metadata) if metadata else None

    if _is_cloud_mode():
        table_uri = get_delta_table_uri(dataset_name)
        storage_options = get_storage_options()
    else:
        table_uri = str(Path(get_data_dir()) / "subsets" / dataset_name)
        storage_options = None

    if mode == "merge":
        try:
            dt = DeltaTable(table_uri, storage_options=storage_options) if storage_options else DeltaTable(table_uri)
            updates = {col: f"source.{col}" for col in data.column_names}
            dt.merge(source=data, predicate=f"target.{merge_key} = source.{merge_key}",
                     source_alias="source", target_alias="target") \
              .when_matched_update(updates=updates) \
              .when_not_matched_insert(updates=updates) \
              .execute()
            print(f"Merged: table now has {len(dt.to_pyarrow_table())} total rows")
        except Exception:
            write_deltalake(table_uri, data, storage_options=storage_options, name=table_name, description=table_description)
            print(f"Created new table {dataset_name}")
    else:
        write_deltalake(table_uri, data, mode=mode, storage_options=storage_options,
                        name=table_name, description=table_description,
                        schema_mode="merge" if mode == "append" else "overwrite")

    # Log output
    null_counts = {col: data[col].null_count for col in data.column_names if data[col].null_count > 0}
    debug.log_data_output(dataset_name=dataset_name, row_count=len(data), size_bytes=data.nbytes,
                          columns=data.column_names, column_count=len(data.schema), null_counts=null_counts, mode=mode)
    return table_uri


def load_asset(asset_name: str) -> pa.Table:
    """Load a Delta table as PyArrow table."""
    if _is_cloud_mode():
        table_uri = get_delta_table_uri(asset_name)
        try:
            return DeltaTable(table_uri, storage_options=get_storage_options()).to_pyarrow_table()
        except Exception as e:
            raise FileNotFoundError(f"No Delta table found at {table_uri}") from e
    else:
        table_path = Path(get_data_dir()) / "subsets" / asset_name
        if not table_path.exists():
            raise FileNotFoundError(f"No Delta table found at {table_path}")
        return DeltaTable(str(table_path)).to_pyarrow_table()


def has_changed(new_data: pa.Table, asset_name: str) -> bool:
    """Check if new data differs from existing asset. Returns True if changed or doesn't exist."""
    try:
        existing = load_asset(asset_name)
        if len(new_data) != len(existing) or new_data.schema != existing.schema:
            return True
        return new_data.to_pandas().to_csv(index=False) != existing.to_pandas().to_csv(index=False)
    except Exception:
        return True


# --- State operations ---

def _state_key(asset: str) -> str:
    return f"{get_connector_name()}/data/state/{asset}.json"


def load_state(asset: str) -> dict:
    """Load state for an asset."""
    if _is_cloud_mode():
        data = download_bytes(_state_key(asset))
        return json.loads(data.decode('utf-8')) if data else {}
    else:
        state_file = Path(get_data_dir()) / "state" / f"{asset}.json"
        return json.load(open(state_file)) if state_file.exists() else {}


def save_state(asset: str, state_data: dict) -> str:
    """Save state for an asset."""
    old_state = load_state(asset)
    state_data = {**state_data, '_metadata': {'updated_at': datetime.now().isoformat(), 'run_id': os.environ.get('RUN_ID', 'unknown')}}

    if _is_cloud_mode():
        uri = upload_bytes(json.dumps(state_data, indent=2).encode('utf-8'), _state_key(asset))
        debug.log_state_change(asset, old_state, state_data)
        return uri
    else:
        state_dir = Path(get_data_dir()) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        state_file = state_dir / f"{asset}.json"
        json.dump(state_data, open(state_file, 'w'), indent=2)
        debug.log_state_change(asset, old_state, state_data)
        return str(state_file)


# --- Raw data operations ---

def _raw_path(asset_id: str, ext: str) -> Path:
    path = Path(get_data_dir()) / "raw" / f"{asset_id}.{ext}"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _raw_key(asset_id: str, ext: str) -> str:
    return f"{get_connector_name()}/data/raw/{asset_id}.{ext}"


def save_raw_file(content: str | bytes, asset_id: str, extension: str = "txt") -> str:
    """Save raw file (CSV, XML, ZIP, etc.)."""
    if _is_cloud_mode():
        data = content.encode('utf-8') if isinstance(content, str) else content
        key = _raw_key(asset_id, extension)

        # Evict if needed and write to cache
        _evict_if_needed(len(data))
        cache_path = _get_cache_path(key)
        cache_path.write_bytes(data)

        # Upload to R2
        uri = upload_bytes(data, key)
        print(f"  -> R2: Saved {asset_id}.{extension}")
        return uri
    else:
        path = _raw_path(asset_id, extension)
        if isinstance(content, str):
            path.write_text(content, encoding='utf-8')
        else:
            path.write_bytes(content)
        print(f"  -> Raw Cache: Saved {asset_id}.{extension}")
        return str(path)


def load_raw_file(asset_id: str, extension: str = "txt") -> str | bytes:
    """Load raw file."""
    if _is_cloud_mode():
        key = _raw_key(asset_id, extension)

        # Check cache first
        cached = _cache_lookup(key)
        if cached:
            print(f"  <- Cache hit: {asset_id}.{extension}")
            try:
                return cached.read_text(encoding='utf-8')
            except UnicodeDecodeError:
                return cached.read_bytes()

        # Download from R2
        data = download_bytes(key)
        if data is None:
            raise FileNotFoundError(f"Raw asset '{asset_id}.{extension}' not found in R2.")

        # Save to cache for next time
        _evict_if_needed(len(data))
        cache_path = _get_cache_path(key)
        cache_path.write_bytes(data)

        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            return data
    else:
        path = _raw_path(asset_id, extension)
        if not path.exists():
            raise FileNotFoundError(f"Raw asset '{asset_id}.{extension}' not found.")
        try:
            return path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            return path.read_bytes()


def save_raw_json(data: any, asset_id: str, compress: bool = False) -> str:
    """Save raw JSON data."""
    ext = "json.gz" if compress else "json"
    if compress:
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
            gz.write(json.dumps(data).encode('utf-8'))
        content = buffer.getvalue()
    else:
        content = json.dumps(data, indent=2).encode('utf-8')

    if _is_cloud_mode():
        key = _raw_key(asset_id, ext)

        # Evict if needed and write to cache
        _evict_if_needed(len(content))
        cache_path = _get_cache_path(key)
        cache_path.write_bytes(content)

        # Upload to R2
        uri = upload_bytes(content, key)
        print(f"  -> R2: Saved {asset_id}.{ext}")
        return uri
    else:
        path = _raw_path(asset_id, ext)
        path.write_bytes(content)
        print(f"  -> Raw Cache: Saved {asset_id}.{ext}")
        return str(path)


def load_raw_json(asset_id: str) -> any:
    """Load raw JSON data. Auto-detects compression."""
    if _is_cloud_mode():
        # Check cache first (both compressed and uncompressed)
        for ext in ("json", "json.gz"):
            key = _raw_key(asset_id, ext)
            cached = _cache_lookup(key)
            if cached:
                print(f"  <- Cache hit: {asset_id}.{ext}")
                if ext == "json.gz":
                    with gzip.open(cached, 'rt', encoding='utf-8') as f:
                        return json.load(f)
                return json.loads(cached.read_text(encoding='utf-8'))

        # Try R2 (uncompressed first)
        key = _raw_key(asset_id, "json")
        data = download_bytes(key)
        if data:
            _evict_if_needed(len(data))
            cache_path = _get_cache_path(key)
            cache_path.write_bytes(data)
            return json.loads(data.decode('utf-8'))

        # Try compressed
        key = _raw_key(asset_id, "json.gz")
        data = download_bytes(key)
        if data:
            _evict_if_needed(len(data))
            cache_path = _get_cache_path(key)
            cache_path.write_bytes(data)
            with gzip.GzipFile(fileobj=io.BytesIO(data), mode='rb') as gz:
                return json.load(gz)

        raise FileNotFoundError(f"Raw asset '{asset_id}' not found in R2.")
    else:
        path = _raw_path(asset_id, "json")
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
        path = _raw_path(asset_id, "json.gz")
        if path.exists():
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                return json.load(f)
        raise FileNotFoundError(f"Raw asset '{asset_id}' not found.")


def save_raw_parquet(data: pa.Table, asset_id: str, metadata: dict = None) -> str:
    """Save raw PyArrow table as Parquet."""
    from .dag import track_write
    track_write(f"raw/{asset_id}", rows=data.num_rows)

    if metadata:
        existing = data.schema.metadata or {}
        existing[b'asset_metadata'] = json.dumps(metadata).encode('utf-8')
        data = data.replace_schema_metadata(existing)

    if _is_cloud_mode():
        key = _raw_key(asset_id, "parquet")
        cache_path = _get_cache_path(key)

        # Estimate size and evict if needed
        buffer = io.BytesIO()
        pq.write_table(data, buffer, compression='snappy')
        _evict_if_needed(buffer.tell())

        # Write to cache
        cache_path.write_bytes(buffer.getvalue())

        # Upload to R2
        uri = upload_file(str(cache_path), key)
        print(f"  -> R2: Saved {asset_id}.parquet ({data.num_rows:,} rows)")
        return uri
    else:
        path = _raw_path(asset_id, "parquet")
        pq.write_table(data, path, compression='snappy')
        print(f"  -> Raw Cache: Saved {asset_id}.parquet ({data.num_rows:,} rows)")
        return str(path)


def load_raw_parquet(asset_id: str) -> pa.Table:
    """Load raw Parquet file as PyArrow table."""
    from .dag import track_read
    track_read(f"raw/{asset_id}")

    if _is_cloud_mode():
        key = _raw_key(asset_id, "parquet")

        # Check cache first
        cached = _cache_lookup(key)
        if cached:
            print(f"  <- Cache hit: {asset_id}.parquet")
            return pq.read_table(cached)

        # Download from R2
        data = download_bytes(key)
        if data is None:
            raise FileNotFoundError(f"Raw parquet asset '{asset_id}' not found in R2")

        # Save to cache for next time
        _evict_if_needed(len(data))
        cache_path = _get_cache_path(key)
        cache_path.write_bytes(data)

        return pq.read_table(io.BytesIO(data))
    else:
        path = _raw_path(asset_id, "parquet")
        if not path.exists():
            raise FileNotFoundError(f"Raw parquet asset '{asset_id}' not found at {path}")
        return pq.read_table(path)


def get_raw_path(asset_id: str, ext: str = "parquet") -> str:
    """Get path/URI for a raw asset.

    Returns S3 URI in cloud mode, local path otherwise.
    """
    if _is_cloud_mode():
        return f"s3://{get_bucket_name()}/{_raw_key(asset_id, ext)}"
    return str(_raw_path(asset_id, ext))
