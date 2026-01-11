import json
import hashlib
from pathlib import Path
from deltalake import DeltaTable
from .environment import get_data_dir, _is_cloud_mode
from .r2 import get_delta_table_uri, get_storage_options, upload_bytes, download_bytes, get_connector_name


def _metadata_state_key(dataset_name: str) -> str:
    return f"{get_connector_name()}/data/state/_metadata_{dataset_name}.json"


def _compute_metadata_hash(metadata: dict) -> str:
    """Compute a stable hash of metadata for change detection."""
    # Sort keys for consistent hashing
    serialized = json.dumps(metadata, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def sync_metadata(dataset_name: str, metadata: dict) -> bool:
    """Sync metadata to a Delta table, only if metadata has changed.

    Returns True if metadata was synced, False if no changes detected.
    """
    if 'title' not in metadata:
        raise ValueError("Missing required field: 'title'")
    if 'description' not in metadata:
        raise ValueError("Missing required field: 'description'")
    if 'column_descriptions' not in metadata:
        raise ValueError("Missing required field: 'column_descriptions'")

    # id is always derived from dataset_name (ignore any passed id)
    metadata = {k: v for k, v in metadata.items() if k != 'id'}
    metadata['id'] = dataset_name

    # Compute hash of new metadata
    new_hash = _compute_metadata_hash(metadata)

    # Load existing hash
    if _is_cloud_mode():
        old_state_bytes = download_bytes(_metadata_state_key(dataset_name))
        old_state = json.loads(old_state_bytes.decode()) if old_state_bytes else {}
    else:
        state_file = Path(get_data_dir()) / "state" / f"_metadata_{dataset_name}.json"
        old_state = json.load(open(state_file)) if state_file.exists() else {}

    old_hash = old_state.get("hash")

    if old_hash == new_hash:
        print(f"No metadata changes for {dataset_name} (hash: {new_hash})")
        return False

    # Metadata has changed, publish it
    if old_hash:
        print(f"Syncing metadata for {dataset_name} (hash: {old_hash} -> {new_hash})")
    else:
        print(f"Syncing metadata for {dataset_name} (new, hash: {new_hash})")

    if _is_cloud_mode():
        table_uri = get_delta_table_uri(dataset_name)
        dt = DeltaTable(table_uri, storage_options=get_storage_options())
    else:
        table_path = Path(get_data_dir()) / "subsets" / dataset_name
        dt = DeltaTable(str(table_path))

    if 'column_descriptions' in metadata:
        schema = dt.schema().to_pyarrow() if hasattr(dt.schema(), 'to_pyarrow') else dt.schema().to_arrow()
        actual_columns = {field.name for field in schema}
        col_descs = json.loads(metadata['column_descriptions']) if isinstance(
            metadata['column_descriptions'], str
        ) else metadata['column_descriptions']
        invalid = set(col_descs.keys()) - actual_columns
        if invalid:
            raise ValueError(f"Invalid columns in descriptions: {sorted(invalid)}")

    dt.alter.set_table_description(json.dumps(metadata))

    # Save new hash
    new_state = {"hash": new_hash}
    if _is_cloud_mode():
        upload_bytes(json.dumps(new_state).encode(), _metadata_state_key(dataset_name))
    else:
        state_dir = Path(get_data_dir()) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        state_file = state_dir / f"_metadata_{dataset_name}.json"
        json.dump(new_state, open(state_file, 'w'))

    return True


