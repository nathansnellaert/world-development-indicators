"""Delta table operations: merge, overwrite, append.

Simple, explicit API for writing to Delta tables.
No hidden defaults. No escape hatches.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable, CommitProperties

from .config import get_data_dir, is_cloud, get_storage_options, subsets_uri
from . import debug
from .io import data_hash
from .tracking import record_write


def _run_commit_properties() -> CommitProperties | None:
    """Build CommitProperties with run context for Delta commits.

    Embeds run_id, connector name, and GitHub Actions metadata into the
    Delta Lake commitInfo so the server can extract lineage from the log.
    Returns None if no run context is available (e.g. ad-hoc local writes).
    """
    meta = {}
    for env_key, meta_key in [
        ("RUN_ID", "subsets.run_id"),
        ("CONNECTOR_NAME", "subsets.connector"),
        ("GITHUB_RUN_ID", "subsets.github_run_id"),
    ]:
        val = os.environ.get(env_key)
        if val:
            meta[meta_key] = val

    gh_run_id = os.environ.get("GITHUB_RUN_ID")
    gh_repo = os.environ.get("GITHUB_REPOSITORY")
    if gh_run_id and gh_repo:
        meta["subsets.github_run_url"] = (
            f"https://github.com/{gh_repo}/actions/runs/{gh_run_id}"
        )

    gh_sha = os.environ.get("GITHUB_SHA")
    if gh_sha:
        meta["subsets.git_commit"] = gh_sha

    return CommitProperties(custom_metadata=meta) if meta else None


@dataclass
class WriteResult:
    uri: str
    version: int
    hash: str
    rows: int


def validate_asset(
    name: str,
    *,
    key: Union[str, list[str]] = None,
    expected_columns: list[str] = None
) -> dict:
    """Validate an existing Delta table asset.

    Returns a report with:
    - row_count: number of rows
    - columns: list of columns
    - key_duplicates: number of duplicate key combinations (if key provided)
    - key_nulls: nulls per key column (if key provided)
    - needs_cleanup: True if issues found

    Args:
        name: Dataset name
        key: Expected key column(s) for uniqueness check
        expected_columns: Columns that should exist

    Returns:
        Validation report dict

    Raises:
        FileNotFoundError: If asset doesn't exist
    """
    uri = _get_uri(name)
    opts = _get_opts()

    try:
        dt = DeltaTable(uri, storage_options=opts)
        table = dt.to_pyarrow_table()
    except Exception as e:
        raise FileNotFoundError(f"Asset '{name}' not found: {e}")

    report = {
        "name": name,
        "row_count": len(table),
        "columns": table.column_names,
        "issues": [],
        "needs_cleanup": False
    }

    # Check expected columns
    if expected_columns:
        missing = set(expected_columns) - set(table.column_names)
        if missing:
            report["issues"].append(f"Missing columns: {missing}")
            report["needs_cleanup"] = True

    # Check key uniqueness and nulls
    if key:
        keys = [key] if isinstance(key, str) else key
        report["key"] = keys

        # Check key columns exist
        missing_keys = [k for k in keys if k not in table.column_names]
        if missing_keys:
            report["issues"].append(f"Key columns missing: {missing_keys}")
            report["needs_cleanup"] = True
        else:
            # Check nulls in key columns
            key_nulls = {}
            for k in keys:
                null_count = table[k].null_count
                if null_count > 0:
                    key_nulls[k] = null_count
            if key_nulls:
                report["key_nulls"] = key_nulls
                report["issues"].append(f"Nulls in key columns: {key_nulls}")
                report["needs_cleanup"] = True

            # Check duplicates
            import pyarrow.compute as pc
            if len(keys) == 1:
                unique_count = len(table[keys[0]].unique())
            else:
                combined = pc.binary_join_element_wise(
                    *[pc.cast(table[k], pa.string()) for k in keys],
                    "||"
                )
                unique_count = len(combined.unique())

            dup_count = len(table) - unique_count
            if dup_count > 0:
                report["key_duplicates"] = dup_count
                report["issues"].append(f"{dup_count} duplicate key combinations")
                report["needs_cleanup"] = True

    # Summary
    if report["needs_cleanup"]:
        print(f"⚠️  [{name}] Needs cleanup: {', '.join(report['issues'])}")
    else:
        print(f"✅ [{name}] Valid: {len(table):,} rows, key={key}")

    return report


def _get_uri(name: str) -> str:
    """Get Delta table URI based on environment."""
    if is_cloud():
        return subsets_uri(name)
    return str(Path(get_data_dir()) / "subsets" / name)


def _get_opts() -> dict | None:
    """Get storage options for cloud, None for local."""
    return get_storage_options() if is_cloud() else None


def _log_write(name: str, table: pa.Table, mode: str):
    """Log the write operation."""
    size_mb = round(table.nbytes / 1024 / 1024, 2)
    cols = ', '.join([f.name for f in table.schema])
    print(f"[{mode}] {name}: {len(table):,} rows, {len(table.schema)} cols ({cols}), {size_mb} MB")

    # Debug logging
    null_counts = {col: table[col].null_count for col in table.column_names if table[col].null_count > 0}
    debug.log_data_output(
        dataset_name=name,
        row_count=len(table),
        size_bytes=table.nbytes,
        columns=table.column_names,
        column_count=len(table.schema),
        null_counts=null_counts,
        mode=mode
    )


def _validate_keys(table: pa.Table, keys: list[str], name: str):
    """Validate key columns before merge.

    Checks:
    1. Key columns exist
    2. Key columns have no nulls
    3. Key combination is unique (no duplicates)
    """
    # Check columns exist
    for k in keys:
        if k not in table.column_names:
            raise ValueError(f"[{name}] Key column '{k}' not found. Columns: {table.column_names}")

    # Check for nulls in key columns
    for k in keys:
        null_count = table[k].null_count
        if null_count > 0:
            raise ValueError(f"[{name}] Key column '{k}' has {null_count} nulls. Merge keys cannot be null.")

    # Check uniqueness - group by keys and look for duplicates
    if len(keys) == 1:
        # Single key - use value_counts
        key_col = table[keys[0]]
        unique_count = len(key_col.unique())
        if unique_count != len(table):
            dup_count = len(table) - unique_count
            raise ValueError(
                f"[{name}] Key '{keys[0]}' has {dup_count} duplicate values. "
                f"Merge key must be unique. Check your data or add more columns to key."
            )
    else:
        # Composite key - need to check combination
        import pyarrow.compute as pc

        # Concatenate key columns as strings for uniqueness check
        combined = pc.binary_join_element_wise(
            *[pc.cast(table[k], pa.string()) for k in keys],
            "||"
        )
        unique_count = len(combined.unique())
        if unique_count != len(table):
            dup_count = len(table) - unique_count
            raise ValueError(
                f"[{name}] Key {keys} has {dup_count} duplicate combinations. "
                f"Merge key must be unique. Check your data or add more columns to key."
            )


def merge(
    table: pa.Table,
    name: str,
    *,
    key: Union[str, list[str]],
    partition_by: list[str] = None,
    validate: bool = True
) -> str:
    """Upsert data into a Delta table.

    - Inserts new records (key doesn't exist)
    - Updates existing records (key exists)
    - Duplicates impossible

    Args:
        table: PyArrow table to write
        name: Dataset name
        key: Column(s) that uniquely identify a record
        partition_by: Optional columns to partition by
        validate: Check key uniqueness before merge (default True)

    Returns:
        Delta table URI
    """
    if len(table) == 0:
        print(f"[merge] {name}: no data to write")
        return None

    # Normalize key to list
    keys = [key] if isinstance(key, str) else key

    # Validate keys
    if validate:
        _validate_keys(table, keys, name)

    uri = _get_uri(name)
    opts = _get_opts()

    try:
        # Try to merge into existing table
        dt = DeltaTable(uri, storage_options=opts)

        # Build merge predicate
        predicate = " AND ".join([f"target.{k} = source.{k}" for k in keys])

        # All columns for update/insert
        updates = {col: f"source.{col}" for col in table.column_names}

        dt.merge(
            source=table,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
            commit_properties=_run_commit_properties(),
        ).when_matched_update(
            updates=updates
        ).when_not_matched_insert(
            updates=updates
        ).execute()

        result_table = dt.to_pyarrow_table()
        new_count = len(result_table)
        version = dt.version()
        h = data_hash(result_table)
        _log_write(name, table, f"merge → {new_count:,} total")

    except Exception:
        # Table doesn't exist, create it
        write_deltalake(
            uri,
            table,
            mode="overwrite",
            partition_by=partition_by,
            storage_options=opts,
            commit_properties=_run_commit_properties(),
        )
        dt = DeltaTable(uri, storage_options=opts)
        new_count = len(table)
        version = dt.version()
        h = data_hash(table)
        _log_write(name, table, "merge (created)")

    record_write(f"subsets/{name}", version=version, hash=h)
    return WriteResult(uri=uri, version=version, hash=h, rows=new_count)


def overwrite(
    table: pa.Table,
    name: str,
    *,
    partition_by: list[str] = None
) -> str:
    """Replace entire Delta table with new data.

    Use ONLY for:
    - Snapshots without natural key (current prices, live status)
    - Computed aggregates
    - Data that's fully recomputed each run

    For anything with a natural key, use merge() instead.

    Args:
        table: PyArrow table to write
        name: Dataset name
        partition_by: Optional columns to partition by

    Returns:
        Delta table URI
    """
    if len(table) == 0:
        print(f"[overwrite] {name}: no data to write")
        return None

    uri = _get_uri(name)
    opts = _get_opts()

    write_deltalake(
        uri,
        table,
        mode="overwrite",
        partition_by=partition_by,
        storage_options=opts,
        schema_mode="overwrite",
        commit_properties=_run_commit_properties(),
    )

    dt = DeltaTable(uri, storage_options=opts)
    version = dt.version()
    h = data_hash(table)

    _log_write(name, table, "overwrite")
    record_write(f"subsets/{name}", version=version, hash=h)
    return WriteResult(uri=uri, version=version, hash=h, rows=len(table))


def append(
    table: pa.Table,
    name: str,
    *,
    partition_by: list[str] = None
) -> str:
    """Append data to a Delta table.

    WARNING: No deduplication! Use ONLY for:
    - Immutable events (audit logs, raw API responses)
    - Data where duplicates are impossible by design

    Always use partition_by for cleanup capability.

    Args:
        table: PyArrow table to write
        name: Dataset name
        partition_by: Columns to partition by (strongly recommended)

    Returns:
        Delta table URI
    """
    if len(table) == 0:
        print(f"[append] {name}: no data to write")
        return None

    if partition_by is None:
        print(f"⚠️  Warning: append() without partition_by makes cleanup difficult")

    uri = _get_uri(name)
    opts = _get_opts()

    write_deltalake(
        uri,
        table,
        mode="append",
        partition_by=partition_by,
        storage_options=opts,
        schema_mode="merge",  # Allow schema evolution for append
        commit_properties=_run_commit_properties(),
    )

    dt = DeltaTable(uri, storage_options=opts)
    version = dt.version()
    h = data_hash(table)

    _log_write(name, table, "append")
    record_write(f"subsets/{name}", version=version, hash=h)
    return WriteResult(uri=uri, version=version, hash=h, rows=len(table))
