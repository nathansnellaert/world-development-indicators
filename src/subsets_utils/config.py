"""Configuration and environment utilities.

Single source of truth for paths, environment detection, and storage options.
The same code runs both local and cloud (R2) modes — the only difference is
which URI a path-builder returns.
"""

import os
from pathlib import Path


# =============================================================================
# Environment Detection
# =============================================================================

def is_cloud() -> bool:
    """Check if running in cloud mode (CI environment)."""
    return os.environ.get('CI', '').lower() == 'true'


def get_connector_name() -> str:
    """Get current connector name. Auto-detects from cwd if not set."""
    return os.environ.get('CONNECTOR_NAME') or Path.cwd().name


def get_run_id() -> str:
    """Get current run ID."""
    return os.environ.get('RUN_ID', 'unknown')


# =============================================================================
# Directory Configuration
# =============================================================================

def get_data_dir() -> str:
    """Get data directory for local (dev) mode. Raises in cloud mode.

    Dev writes go to `data/dev/` by default — a wegwerp scratch space,
    separate from the read-only SSD mirror of R2 production data.
    Override with DATA_DIR env var.
    """
    if is_cloud():
        raise RuntimeError("get_data_dir() should not be called in cloud mode. Use R2 URIs instead.")
    return os.environ.get('DATA_DIR', 'data/dev')


# =============================================================================
# SSD Mirror — read-only reflection of R2 production state
#
# The R2 → SSD sync daemon (meta/services/r2_sync.py) keeps this in sync
# with cloud writes. Dev runs read from here as a fallback when a file
# isn't yet in the local dev dir, so you don't have to re-download.
# =============================================================================

_MIRROR_ROOT_DEFAULT = "/Volumes/ExtremeSSD/data-integrations/integrations"


def get_mirror_root() -> Path | None:
    """Root of the SSD mirror (read-only). Returns None if unavailable.

    Override with SUBSETS_MIRROR_ROOT env var. Falls back to None if the
    path doesn't exist (e.g. SSD not mounted) — callers should handle that
    gracefully by skipping the fallback.
    """
    root = Path(os.environ.get('SUBSETS_MIRROR_ROOT', _MIRROR_ROOT_DEFAULT))
    return root if root.exists() else None


def mirror_raw_path(asset_id: str, ext: str = "parquet") -> Path | None:
    """Path to a raw asset in the SSD mirror. Returns None if mirror unavailable."""
    root = get_mirror_root()
    if root is None:
        return None
    return root / get_connector_name() / "data" / "raw" / f"{asset_id}.{ext}"


def mirror_state_path(asset: str) -> Path | None:
    """Path to a state file in the SSD mirror. Returns None if mirror unavailable."""
    root = get_mirror_root()
    if root is None:
        return None
    return root / get_connector_name() / "data" / "state" / f"{asset}.json"


# =============================================================================
# Environment Validation
# =============================================================================

def validate_environment(additional_required: list[str] = None):
    """Validate required environment variables based on execution mode.

    Local mode: requires nothing (DATA_DIR defaults to "data").
    Cloud mode: requires R2 credentials.
    """
    if is_cloud():
        required = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"]
    else:
        required = []

    if additional_required:
        required.extend(additional_required)

    missing = [var for var in required if var not in os.environ]
    if missing:
        mode = "cloud" if is_cloud() else "local"
        raise ValueError(f"Missing required environment variables for {mode} mode: {missing}")


# =============================================================================
# R2/S3 Storage Options (DeltaLake)
# =============================================================================

def get_storage_options() -> dict | None:
    """Get storage options for DeltaLake S3 writes. Returns None for local mode."""
    if not is_cloud():
        return None
    return {
        'AWS_ENDPOINT_URL': f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        'AWS_ACCESS_KEY_ID': os.environ['R2_ACCESS_KEY_ID'],
        'AWS_SECRET_ACCESS_KEY': os.environ['R2_SECRET_ACCESS_KEY'],
        'AWS_REGION': 'auto',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
    }


def get_bucket_name() -> str:
    """Get R2 bucket name."""
    return os.environ['R2_BUCKET_NAME']


# =============================================================================
# Path / URI Builders
#
# All save/load functions in io.py call these to get a uri (s3:// in cloud,
# local path otherwise). Dispatch on uri prefix is in io.py's _read_bytes /
# _write_bytes helpers.
# =============================================================================

def get_r2_base() -> str:
    """Get R2 base path for current connector: <connector>/data"""
    return f"{get_connector_name()}/data"


def raw_key(asset_id: str, ext: str = "parquet") -> str:
    """R2 key for a raw asset."""
    return f"{get_r2_base()}/raw/{asset_id}.{ext}"


def raw_uri(asset_id: str, ext: str = "parquet") -> str:
    """URI for a raw asset (s3:// in cloud, local path otherwise)."""
    if is_cloud():
        return f"s3://{get_bucket_name()}/{raw_key(asset_id, ext)}"
    return raw_path(asset_id, ext)


def state_key(asset: str) -> str:
    """R2 key for a state file."""
    return f"{get_r2_base()}/state/{asset}.json"


def state_uri(asset: str) -> str:
    """URI for a state file (s3:// in cloud, local path otherwise)."""
    if is_cloud():
        return f"s3://{get_bucket_name()}/{state_key(asset)}"
    return state_path(asset)


def subsets_uri(dataset_name: str) -> str:
    """URI for a subsets Delta table (s3:// in cloud, local path otherwise).

    Cloud writes live under the connector's own prefix
    (<connector>/datasets/<dataset_name>) — the Subsets server poller
    walks connector roots from the repo, not a global namespace.
    """
    if is_cloud():
        return f"s3://{get_bucket_name()}/{get_r2_base()}/subsets/{dataset_name}"
    return str(Path(get_data_dir()) / "subsets" / dataset_name)


def raw_path(asset_id: str, ext: str = "parquet") -> str:
    """Local path for a raw asset. Creates parent dirs."""
    path = Path(get_data_dir()) / "raw" / f"{asset_id}.{ext}"
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def state_path(asset: str) -> str:
    """Local path for a state file. Creates parent dirs."""
    path = Path(get_data_dir()) / "state" / f"{asset}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)
