"""Data I/O operations for raw data, state, and Delta tables."""

import os
import io
import json
import gzip
import uuid
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake, DeltaTable
from . import debug
from .environment import get_data_dir
from .r2 import is_cloud_mode, upload_bytes, upload_file, download_bytes, get_storage_options, get_delta_table_uri, get_bucket_name, get_connector_name


# --- Delta table operations ---

def upload_data(data: pa.Table, dataset_name: str, metadata: dict = None, mode: str = "append", merge_key: str = None) -> str:
    """Upload a PyArrow table to a Delta table."""
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

    if is_cloud_mode():
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
    if is_cloud_mode():
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
    if is_cloud_mode():
        data = download_bytes(_state_key(asset))
        return json.loads(data.decode('utf-8')) if data else {}
    else:
        state_file = Path(get_data_dir()) / "state" / f"{asset}.json"
        return json.load(open(state_file)) if state_file.exists() else {}


def save_state(asset: str, state_data: dict) -> str:
    """Save state for an asset."""
    old_state = load_state(asset)
    state_data = {**state_data, '_metadata': {'updated_at': datetime.now().isoformat(), 'run_id': os.environ.get('RUN_ID', 'unknown')}}

    if is_cloud_mode():
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
    if is_cloud_mode():
        data = content.encode('utf-8') if isinstance(content, str) else content
        print(f"  -> R2: Saved {asset_id}.{extension}")
        return upload_bytes(data, _raw_key(asset_id, extension))
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
    if is_cloud_mode():
        data = download_bytes(_raw_key(asset_id, extension))
        if data is None:
            raise FileNotFoundError(f"Raw asset '{asset_id}.{extension}' not found in R2.")
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

    if is_cloud_mode():
        print(f"  -> R2: Saved {asset_id}.{ext}")
        return upload_bytes(content, _raw_key(asset_id, ext))
    else:
        path = _raw_path(asset_id, ext)
        path.write_bytes(content)
        print(f"  -> Raw Cache: Saved {asset_id}.{ext}")
        return str(path)


def load_raw_json(asset_id: str) -> any:
    """Load raw JSON data. Auto-detects compression."""
    if is_cloud_mode():
        data = download_bytes(_raw_key(asset_id, "json"))
        if data:
            return json.loads(data.decode('utf-8'))
        data = download_bytes(_raw_key(asset_id, "json.gz"))
        if data:
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
    if metadata:
        existing = data.schema.metadata or {}
        existing[b'asset_metadata'] = json.dumps(metadata).encode('utf-8')
        data = data.replace_schema_metadata(existing)

    if is_cloud_mode():
        temp_path = f"/tmp/{uuid.uuid4()}.parquet"
        try:
            pq.write_table(data, temp_path, compression='snappy')
            uri = upload_file(temp_path, _raw_key(asset_id, "parquet"))
            print(f"  -> R2: Saved {asset_id}.parquet ({data.num_rows:,} rows)")
            return uri
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
    else:
        path = _raw_path(asset_id, "parquet")
        pq.write_table(data, path, compression='snappy')
        print(f"  -> Raw Cache: Saved {asset_id}.parquet ({data.num_rows:,} rows)")
        return str(path)


def load_raw_parquet(asset_id: str) -> pa.Table:
    """Load raw Parquet file as PyArrow table."""
    if is_cloud_mode():
        data = download_bytes(_raw_key(asset_id, "parquet"))
        if data is None:
            raise FileNotFoundError(f"Raw parquet asset '{asset_id}' not found in R2")
        return pq.read_table(io.BytesIO(data))
    else:
        path = _raw_path(asset_id, "parquet")
        if not path.exists():
            raise FileNotFoundError(f"Raw parquet asset '{asset_id}' not found at {path}")
        return pq.read_table(path)
