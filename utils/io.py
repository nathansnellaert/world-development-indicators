import os
import json
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import logging
import requests
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError, CommitFailedException
from . import debug
from .environment import get_data_dir

logger = logging.getLogger(__name__)

# Storage backend singleton
_storage_backend = None
# Catalog singleton for Iceberg operations
_catalog = None


class LocalStorage:
    """Local filesystem storage for development"""
    
    def __init__(self):
        self.base_path = Path(get_data_dir())
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalStorage initialized at {self.base_path}")
    
    def upload_data(self, data: pa.Table, dataset_name: str, partition: str = None) -> str:
        connector = os.environ['CONNECTOR_NAME']
        run_id = os.environ['RUN_ID']
        
        # Build path with optional partition
        if partition:
            path = self.base_path / connector / dataset_name / partition
        else:
            path = self.base_path / connector / dataset_name
        path.mkdir(parents=True, exist_ok=True)
        
        file_path = path / f"{run_id}.parquet"
        pq.write_table(data, file_path)
        
        logger.info(f"Saved {len(data)} rows to {file_path}")
        return str(file_path)
    
    def save_snapshot(self, snapshot: dict, dataset_name: str, partition: str = None) -> str:
        connector = os.environ['CONNECTOR_NAME']
        run_id = os.environ['RUN_ID']
        
        snapshot_path = self.base_path / connector / "snapshots"
        if partition:
            snapshot_path = snapshot_path / dataset_name / partition
        snapshot_path.mkdir(parents=True, exist_ok=True)
        
        snapshot_file = snapshot_path / f"{run_id}.json"
        with open(snapshot_file, 'w') as f:
            json.dump(snapshot, f, indent=2)
        
        logger.info(f"Wrote snapshot to {snapshot_file}")
        return str(snapshot_file)
    
    def load_asset(self, connector: str, asset_name: str, run_id: str = None) -> pa.Table:
        base_path = self.base_path / connector / asset_name
        if not base_path.exists():
            raise FileNotFoundError(f"No asset data found at {base_path}")
        
        # Find available parquet files
        parquet_files = list(base_path.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {base_path}")
        
        if run_id:
            # Load specific run
            file_path = base_path / f"{run_id}.parquet"
            if not file_path.exists():
                raise FileNotFoundError(f"Run {run_id} not found for asset {asset_name}")
        else:
            # Load most recent file
            file_path = max(parquet_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"Loading most recent asset from {file_path}")
        
        return pq.read_table(file_path)


class IcebergStorage:
    """Iceberg catalog storage for production"""
    
    def __init__(self):
        self.catalog = self._get_catalog()
        logger.info(f"IcebergStorage initialized")
    
    def _get_catalog(self):
        """Get or create the Iceberg catalog connection"""
        global _catalog
        
        if _catalog is None:
            _catalog = RestCatalog(
                name="subsets",
                uri=os.environ['SUBSETS_CATALOG_URL'],
                token=os.environ['SUBSETS_API_KEY'],
                warehouse=os.environ['SUBSETS_WAREHOUSE']
            )
            logger.info(f"Iceberg catalog initialized")
        
        return _catalog
    
    def upload_data(self, data: pa.Table, dataset_name: str, partition: str = None) -> str:
        _ = partition  # Not used with Iceberg, kept for compatibility
        if len(data) == 0:
            logger.warning(f"No data to upload for {dataset_name}")
            return ""
        
        # Use tuple format to separate namespace and table name
        table_identifier = ("subsets", dataset_name)
             
        # Check if table exists, create if not
        connector = os.environ['CONNECTOR_NAME']
        try:
            table = self.catalog.load_table(table_identifier)
            logger.info(f"Found existing table: subsets.{dataset_name}")
        except NoSuchTableError:
            logger.info(f"Creating new table: subsets.{dataset_name}")
            table = self.catalog.create_table(
                identifier=table_identifier,
                schema=data.schema,
                properties={'connector': connector}
            )
        
        # Append data to the table
        try:
            table.append(data)
            logger.info(f"Appended {len(data)} rows to subsets.{dataset_name}")
        except CommitFailedException as e:
            if "DataInvalid" in str(e):
                # Default to true - continue on snapshot mismatch errors
                if os.environ.get('CONTINUE_ON_SNAPSHOT_ERROR', 'false').lower() == 'true':
                    logger.debug(f"Snapshot mismatch for {dataset_name}, continuing (data likely already exists)")
                else:
                    raise
            else:
                raise
        
        return f"subsets.{dataset_name}"
    
    def save_snapshot(self, snapshot: dict, dataset_name: str, partition: str = None) -> str:
        _ = partition  # Not used with Iceberg, kept for compatibility
        # For Iceberg, save snapshots locally for debugging
        connector = os.environ['CONNECTOR_NAME']
        run_id = os.environ['RUN_ID']
        
        base_path = Path(get_data_dir())
        snapshot_path = base_path / connector / "snapshots"
        snapshot_path.mkdir(parents=True, exist_ok=True)
        
        snapshot_file = snapshot_path / f"{dataset_name}_{run_id}.json"
        with open(snapshot_file, 'w') as f:
            json.dump(snapshot, f, indent=2)
        
        logger.info(f"Wrote snapshot to {snapshot_file}")
        return str(snapshot_file)
    
    def load_asset(self, connector: str, asset_name: str, run_id: str = None) -> pa.Table:
        _ = run_id  # Not used with Iceberg
        table_name = f"{connector}_{asset_name}"
        table_identifier = ("subsets", table_name)
        
        try:
            table = self.catalog.load_table(table_identifier)
            # Read all data from the table
            df = table.scan().to_pandas()
            return pa.Table.from_pandas(df)
        except NoSuchTableError:
            raise FileNotFoundError(f"No table found: subsets.{table_name}")


def _get_storage():
    """Get or create the storage backend based on STORAGE_BACKEND env var"""
    global _storage_backend
    
    if _storage_backend is None:
        storage_backend = os.environ['STORAGE_BACKEND']
        
        if storage_backend == 'local':
            _storage_backend = LocalStorage()
        elif storage_backend == 'iceberg':
            _storage_backend = IcebergStorage()
        else:
            raise ValueError(f"Unknown STORAGE_BACKEND: {storage_backend}")
    
    return _storage_backend


def _generate_snapshot(data: pa.Table, dataset_name: str) -> dict:
    """Generate detailed snapshot/profile of the dataset"""
    snapshot = {
        "dataset": dataset_name,
        "timestamp": datetime.now().isoformat(),
        "row_count": len(data),
        "column_count": len(data.schema),
        "memory_usage_mb": round(data.nbytes / 1024 / 1024, 2),
        "columns": []
    }
    
    for field in data.schema:
        col = data[field.name]
        col_info = {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
            "null_count": pc.count(pc.is_null(col)).as_py()
        }
        
        # Add cardinality for non-numeric types
        if not (pa.types.is_integer(field.type) or pa.types.is_floating(field.type)) and not pa.types.is_temporal(field.type):
            col_info["cardinality"] = pc.count_distinct(col).as_py()
            # Sample values for string types
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                unique_vals = pc.unique(col)
                col_info["sample_values"] = unique_vals.slice(0, min(5, len(unique_vals))).to_pylist()
        
        # Add statistics for numeric types
        elif pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
            non_null = pc.drop_null(col)
            if len(non_null) > 0:
                col_info["min"] = pc.min(non_null).as_py()
                col_info["max"] = pc.max(non_null).as_py()
                col_info["mean"] = round(pc.mean(non_null).as_py(), 4)
                col_info["stddev"] = round(pc.stddev(non_null).as_py(), 4) if len(non_null) > 1 else 0
        
        # Add range for temporal types
        elif pa.types.is_temporal(field.type):
            non_null = pc.drop_null(col)
            if len(non_null) > 0:
                col_info["min"] = str(pc.min(non_null).as_py())
                col_info["max"] = str(pc.max(non_null).as_py())
        
        snapshot["columns"].append(col_info)
    
    # Add sample rows
    sample_size = min(10, len(data))
    if sample_size > 0:
        snapshot["sample_rows"] = data.slice(0, sample_size).to_pylist()
    
    return snapshot


# Public API functions - thin wrappers around storage backend
def upload_data(data: pa.Table, dataset_name: str, partition: str = None) -> str:
    """Upload data to configured storage backend
    
    Args:
        data: The data to upload as a PyArrow table
        dataset_name: Logical dataset name (e.g., "page_views")
        partition: Optional partition path (e.g., "2024/01/15")
    
    Returns:
        str: The storage path where data was saved
    """
    # Print upload info
    size_mb = round(data.nbytes / 1024 / 1024, 2)
    columns = ', '.join([f.name for f in data.schema])
    print(f"Uploading {dataset_name}: {len(data)} rows, {len(data.schema)} cols ({columns}), {size_mb} MB")
    
    # Upload data
    storage = _get_storage()
    key = storage.upload_data(data, dataset_name, partition)
    
    # Log data output
    schema_info = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in data.schema
    ]
    
    metrics = {}
    if partition:
        metrics['partition'] = partition
    
    debug.log_data_output(
        dataset_name=dataset_name,
        row_count=len(data),
        column_count=len(data.schema),
        size_bytes=data.nbytes,
        storage_path=key,
        schema=schema_info,
        metrics=metrics
    )
    
    # Generate and save snapshot if enabled
    if os.environ.get('WRITE_SNAPSHOT', '').lower() == 'true':
        snapshot = _generate_snapshot(data, dataset_name)
        if partition:
            snapshot['partition'] = partition
        storage.save_snapshot(snapshot, dataset_name, partition)
    
    return key


def load_state(asset: str) -> dict:
    """Load state for an asset from local filesystem.
    
    State is always stored locally to enable version control via Git commits
    in GitHub Actions, providing transparency and audit trail.
    """
    environment = os.environ.get('STORAGE_BACKEND', 'local')
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
        'run_id': os.environ.get('RUN_ID', 'unknown'),
        'connector': os.environ.get('CONNECTOR_NAME', 'unknown')
    }
    
    # Save state
    environment = os.environ.get('STORAGE_BACKEND', 'local')
    state_dir = Path(".state") / environment
    state_dir.mkdir(parents=True, exist_ok=True)
    
    state_file = state_dir / f"{asset}.json"
    with open(state_file, 'w') as f:
        json.dump(state_data, f, indent=2)
    
    # Log state change
    debug.log_state_change(asset, old_state, state_data)
    
    return str(state_file)


def load_asset(connector: str, asset_name: str, run_id: str = None) -> pa.Table:
    """Load a previously saved asset directly from storage for debugging/development
    
    This allows loading assets from previous runs to avoid re-fetching data
    during development. Useful when debugging later stages of a pipeline.
    
    Args:
        connector: The connector name (e.g., 'world-development-indicators')
        asset_name: The dataset/asset name (e.g., 'indicators', 'series')
        run_id: Optional specific run ID to load. If None, loads the most recent.
    
    Returns:
        pa.Table: The loaded PyArrow table
    
    Raises:
        FileNotFoundError: If no asset data found
    """
    return _get_storage().load_asset(connector, asset_name, run_id)


def publish_to_subsets(dataset_name: str = None, metadata: dict = None) -> None:
    """Publish dataset metadata to Subsets platform
    
    After data has been uploaded to Iceberg via upload_data(), this function
    publishes the dataset metadata to make it discoverable on the Subsets platform.
    
    Args:
        dataset_name: Dataset name 
        metadata: Dictionary with dataset metadata:
            - title: Human-readable title
            - description: Dataset description
            - columns: Dict of column_name -> description mappings
    
    Example:
        metadata = {
            "title": "ECB Foreign Exchange Rates",
            "description": "Daily exchange rates from ECB",
            "columns": {
                "date": "The date of observation",
                "USD": "US Dollar to Euro exchange rate",
                "GBP": "British Pound to Euro exchange rate"
            }
        }
        publish_to_subsets(dataset_name="forex_rates", metadata=metadata)
    """
    if not metadata:
        logger.warning("No metadata provided for publishing")
        return
    
    api_key = os.environ.get("SUBSETS_API_KEY")
    if not api_key:
        logger.warning("SUBSETS_API_KEY not set. Skipping publish.")
        return
    
    api_url = os.environ.get("SUBSETS_API_URL", "https://api.subsets.com")
    
    # Determine table name
    if not dataset_name:
        raise ValueError("dataset_name must be provided")
    
    connector = os.environ['CONNECTOR_NAME']
    table_name = f"subsets.{connector}_{dataset_name}"
    
    # Build publish request
    publish_request = {
        "id": table_name,
        "title": metadata.get("title", table_name),
        "description": metadata.get("description", ""),
        "columns": metadata.get("columns", {})
    }
    
    # Make API request to publish
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(
            f"{api_url}/datasets/publish",
            headers=headers,
            json=publish_request,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Dataset published successfully: {table_name}")
            logger.info(f"  Status: {result.get('status')}")
            logger.info(f"  Published at: {result.get('published_at')}")
        elif response.status_code == 409:
            logger.info(f"Dataset {table_name} is already published")
        elif response.status_code == 404:
            logger.error(f"Dataset {table_name} not found in catalog. Ensure data is uploaded first.")
        else:
            logger.error(f"Failed to publish dataset: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error publishing dataset: {e}")