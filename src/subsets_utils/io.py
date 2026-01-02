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
from .r2 import is_cloud_mode, upload_bytes, upload_file, upload_fileobj, download_bytes, get_storage_options, get_delta_table_uri, get_bucket_name, get_connector_name


def upload_data(data: pa.Table, dataset_name: str, metadata: dict = None, mode: str = "append", merge_key: str = None) -> str:
    """Upload a PyArrow table to a Delta table.

    In local mode: writes to DATA_DIR/subsets/{dataset_name}
    In cloud mode: writes directly to R2 s3://{bucket}/data/subsets/{dataset_name}

    Args:
        data: The PyArrow table to upload
        dataset_name: Name of the dataset (used as directory name)
        metadata: Optional metadata dict with keys: title, description, columns
        mode: 'append', 'overwrite', or 'merge'
        merge_key: Required when mode='merge', the column to merge on
    """
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

    # Extract metadata for Delta table
    table_name = metadata.get("title") if metadata else None
    table_description = json.dumps(metadata) if metadata else None

    if is_cloud_mode():
        # Cloud mode: write directly to R2
        table_uri = get_delta_table_uri(dataset_name)
        storage_options = get_storage_options()

        if mode == "merge":
            try:
                dt = DeltaTable(table_uri, storage_options=storage_options)
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
            except Exception:
                # Table doesn't exist, create it
                write_deltalake(
                    table_uri,
                    data,
                    storage_options=storage_options,
                    name=table_name,
                    description=table_description
                )
                print(f"Created new table {dataset_name}")
        else:
            write_deltalake(
                table_uri,
                data,
                mode=mode,
                storage_options=storage_options,
                name=table_name,
                description=table_description,
                schema_mode="merge" if mode == "append" else "overwrite"
            )

        output_path = table_uri
    else:
        # Local mode: write to filesystem
        table_path = Path(get_data_dir()) / "subsets" / dataset_name

        if mode == "merge":
            if not table_path.exists():
                write_deltalake(str(table_path), data, name=table_name, description=table_description)
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
                name=table_name,
                description=table_description,
                schema_mode="merge" if mode == "append" else "overwrite"
            )

        output_path = str(table_path)

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

    return output_path


def load_state(asset: str) -> dict:
    """Load state for an asset.

    In local mode: reads from .state/{environment}/{asset}.json
    In cloud mode: reads from R2 {connector}/data/state/{asset}.json
    """
    if is_cloud_mode():
        connector = get_connector_name()
        key = f"{connector}/data/state/{asset}.json"
        data = download_bytes(key)
        if data is None:
            return {}
        return json.loads(data.decode('utf-8'))
    else:
        environment = os.environ.get('ENVIRONMENT', 'dev')
        state_file = Path(".state") / environment / f"{asset}.json"

        if state_file.exists():
            with open(state_file, 'r') as f:
                return json.load(f)
        return {}


def save_state(asset: str, state_data: dict) -> str:
    """Save state for an asset.

    In local mode: writes to .state/{environment}/{asset}.json
    In cloud mode: writes to R2 {connector}/data/state/{asset}.json
    """
    # Load old state for comparison (for debug logging)
    old_state = load_state(asset)

    # Add metadata to state
    state_data = state_data.copy()
    state_data['_metadata'] = {
        'updated_at': datetime.now().isoformat(),
        'run_id': os.environ.get('RUN_ID', 'unknown')
    }

    if is_cloud_mode():
        connector = get_connector_name()
        key = f"{connector}/data/state/{asset}.json"
        data = json.dumps(state_data, indent=2).encode('utf-8')
        uri = upload_bytes(data, key)
        debug.log_state_change(asset, old_state, state_data)
        return uri
    else:
        environment = os.environ.get('ENVIRONMENT', 'dev')
        state_dir = Path(".state") / environment
        state_dir.mkdir(parents=True, exist_ok=True)

        state_file = state_dir / f"{asset}.json"
        with open(state_file, 'w') as f:
            json.dump(state_data, f, indent=2)

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
    if is_cloud_mode():
        table_uri = get_delta_table_uri(asset_name)
        storage_options = get_storage_options()

        try:
            dt = DeltaTable(table_uri, storage_options=storage_options)
            existing_data = dt.to_pyarrow_table()

            if len(new_data) != len(existing_data):
                return True

            if new_data.schema != existing_data.schema:
                return True

            new_bytes = new_data.to_pandas().to_csv(index=False)
            existing_bytes = existing_data.to_pandas().to_csv(index=False)

            return new_bytes != existing_bytes

        except Exception:
            return True
    else:
        table_path = Path(get_data_dir()) / "subsets" / asset_name

        if not table_path.exists():
            return True

        try:
            dt = DeltaTable(str(table_path))
            existing_data = dt.to_pyarrow_table()

            if len(new_data) != len(existing_data):
                return True

            if new_data.schema != existing_data.schema:
                return True

            new_bytes = new_data.to_pandas().to_csv(index=False)
            existing_bytes = existing_data.to_pandas().to_csv(index=False)

            return new_bytes != existing_bytes

        except Exception:
            return True


def load_asset(asset_name: str) -> pa.Table:
    """Load a previously saved asset from Delta table.

    In local mode: reads from DATA_DIR/subsets/{asset_name}
    In cloud mode: reads from R2 s3://{bucket}/data/subsets/{asset_name}

    Args:
        asset_name: The dataset/asset name (e.g., 'indicators', 'series')

    Returns:
        pa.Table: The loaded PyArrow table

    Raises:
        FileNotFoundError: If no Delta table found
    """
    if is_cloud_mode():
        table_uri = get_delta_table_uri(asset_name)
        storage_options = get_storage_options()

        try:
            dt = DeltaTable(table_uri, storage_options=storage_options)
            return dt.to_pyarrow_table()
        except Exception as e:
            raise FileNotFoundError(f"No Delta table found at {table_uri}") from e
    else:
        table_path = Path(get_data_dir()) / "subsets" / asset_name

        if not table_path.exists():
            raise FileNotFoundError(f"No Delta table found at {table_path}")

        dt = DeltaTable(str(table_path))
        return dt.to_pyarrow_table()


def _get_raw_path(asset_id: str, extension: str) -> Path:
    """Raw directory: DATA_DIR/raw/asset_id.ext (local mode only)"""
    path = Path(get_data_dir()) / "raw" / f"{asset_id}.{extension}"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _get_raw_r2_key(asset_id: str, extension: str) -> str:
    """R2 key for raw data: {connector}/data/raw/asset_id.ext"""
    connector = get_connector_name()
    return f"{connector}/data/raw/{asset_id}.{extension}"


def save_raw_file(content: str | bytes, asset_id: str, extension: str = "txt") -> str:
    """Generic raw saver for CSV, XML, ZIP, etc.

    In local mode: writes to DATA_DIR/raw/{asset_id}.{extension}
    In cloud mode: uploads directly to R2 (no disk write)

    Args:
        content: String (text/csv) or Bytes (zip/pdf/binary)
        asset_id: The identifier for the asset
        extension: File extension (e.g., 'csv', 'xml', 'zip')
    """
    if is_cloud_mode():
        key = _get_raw_r2_key(asset_id, extension)
        if isinstance(content, str):
            data = content.encode('utf-8')
        else:
            data = content
        uri = upload_bytes(data, key)
        print(f"  -> R2: Saved {asset_id}.{extension}")
        return uri
    else:
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
    """Generic raw loader for CSV, XML, ZIP, etc.

    In local mode: reads from DATA_DIR/raw/{asset_id}.{extension}
    In cloud mode: downloads from R2
    """
    if is_cloud_mode():
        key = _get_raw_r2_key(asset_id, extension)
        data = download_bytes(key)
        if data is None:
            raise FileNotFoundError(f"Raw asset '{asset_id}.{extension}' not found in R2.")

        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            return data
    else:
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

    In local mode: writes to DATA_DIR/raw/{asset_id}.json[.gz]
    In cloud mode: uploads directly to R2 (no disk write)

    Use compress=True for massive datasets to save storage space.
    """
    ext = "json.gz" if compress else "json"

    if is_cloud_mode():
        key = _get_raw_r2_key(asset_id, ext)
        if compress:
            buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
                with io.TextIOWrapper(gz, encoding='utf-8') as f:
                    json.dump(data, f)
            content = buffer.getvalue()
        else:
            content = json.dumps(data, indent=2).encode('utf-8')

        uri = upload_bytes(content, key)
        print(f"  -> R2: Saved {asset_id}.{ext}")
        return uri
    else:
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
    """Load raw JSON data. Auto-detects compression.

    In local mode: reads from DATA_DIR/raw/{asset_id}.json[.gz]
    In cloud mode: downloads from R2
    """
    if is_cloud_mode():
        # Try uncompressed first
        key = _get_raw_r2_key(asset_id, "json")
        data = download_bytes(key)
        if data is not None:
            return json.loads(data.decode('utf-8'))

        # Try compressed
        key = _get_raw_r2_key(asset_id, "json.gz")
        data = download_bytes(key)
        if data is not None:
            with gzip.GzipFile(fileobj=io.BytesIO(data), mode='rb') as gz:
                with io.TextIOWrapper(gz, encoding='utf-8') as f:
                    return json.load(f)

        raise FileNotFoundError(f"Raw asset '{asset_id}' not found in R2.")
    else:
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


def save_raw_parquet(data: pa.Table, asset_id: str, metadata: dict = None) -> str:
    """Save raw PyArrow table as Parquet with optional metadata.

    In local mode: writes to DATA_DIR/raw/{asset_id}.parquet
    In cloud mode: writes to temp file, uploads to R2, then deletes temp file
        (the "Temp & Toss" pattern to avoid disk exhaustion)

    Args:
        data: PyArrow table to save
        asset_id: Identifier for the asset
        metadata: Optional metadata dict to store with the file

    Returns:
        Path or URI to the saved file
    """
    # Store metadata in the Parquet file's key-value metadata
    if metadata:
        existing_metadata = data.schema.metadata or {}
        existing_metadata[b'asset_metadata'] = json.dumps(metadata).encode('utf-8')
        data = data.replace_schema_metadata(existing_metadata)

    if is_cloud_mode():
        # Temp & Toss pattern: write to temp, upload, delete
        temp_path = f"/tmp/{uuid.uuid4()}.parquet"
        try:
            pq.write_table(data, temp_path, compression='snappy')
            key = _get_raw_r2_key(asset_id, "parquet")
            uri = upload_file(temp_path, key)
            print(f"  -> R2: Saved {asset_id}.parquet ({data.num_rows:,} rows)")
            return uri
        finally:
            # Always delete temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)
    else:
        path = _get_raw_path(asset_id, "parquet")
        pq.write_table(data, path, compression='snappy')
        print(f"  -> Raw Cache: Saved {asset_id}.parquet ({data.num_rows:,} rows)")
        return str(path)


def load_raw_parquet(asset_id: str) -> pa.Table:
    """Load raw Parquet file as PyArrow table.

    In local mode: reads from DATA_DIR/raw/{asset_id}.parquet
    In cloud mode: downloads from R2 to temp file, reads, then deletes

    Args:
        asset_id: Identifier for the asset

    Returns:
        PyArrow table
    """
    if is_cloud_mode():
        key = _get_raw_r2_key(asset_id, "parquet")
        data = download_bytes(key)
        if data is None:
            raise FileNotFoundError(f"Raw parquet asset '{asset_id}' not found in R2")

        # Read parquet from bytes
        buffer = io.BytesIO(data)
        return pq.read_table(buffer)
    else:
        path = _get_raw_path(asset_id, "parquet")
        if not path.exists():
            raise FileNotFoundError(f"Raw parquet asset '{asset_id}' not found at {path}")

        return pq.read_table(path)


