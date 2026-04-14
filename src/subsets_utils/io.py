"""Data I/O for raw assets, state files, and Delta tables.

Single code path for both local and cloud storage. The save/load functions
build a URI via config.py's path-builders, then dispatch on uri prefix
(s3:// → boto3 via r2.py, else → local file). The same code runs both modes.

Memory note: parquet reads/writes round-trip via in-memory bytes buffer. For
oversized files (> RAM), use pyarrow.fs S3FileSystem with iter_batches() in
your node directly — this module is the simple-case helper.
"""

import io
import json
import gzip
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable

from . import debug
from .config import (
    is_cloud, get_data_dir, get_storage_options, get_bucket_name,
    raw_uri, state_uri, subsets_uri, raw_key, state_key,
    mirror_raw_path, mirror_state_path,
)
from .r2 import upload_bytes, download_bytes, head_object, list_keys


# =============================================================================
# Mirror fallback — dev mode reads from SSD mirror when local dev file missing
# =============================================================================

def _read_with_mirror_fallback(uri: str, mirror: Path | None) -> Optional[bytes]:
    """Read a URI, falling back to the SSD mirror path if local read misses.

    Only applies in local (dev) mode: s3:// URIs go straight through, and
    writes never touch the mirror. This is the single read-time hook that
    makes dev iterate without re-downloading data already in R2.
    """
    data = _read_bytes(uri)
    if data is not None:
        return data
    if uri.startswith("s3://") or mirror is None:
        return None
    return mirror.read_bytes() if mirror.exists() else None


# =============================================================================
# URI dispatch — the only place that branches on s3:// vs local
# =============================================================================

def _s3_key_from_uri(uri: str) -> str:
    """Extract the key portion from an s3://bucket/key URI."""
    _, _, rest = uri.partition("s3://")
    _, _, key = rest.partition("/")
    return key


def _write_bytes(uri: str, data: bytes) -> None:
    """Write bytes to a URI (s3:// or local path)."""
    if uri.startswith("s3://"):
        upload_bytes(data, _s3_key_from_uri(uri))
    else:
        p = Path(uri)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)


def _read_bytes(uri: str) -> Optional[bytes]:
    """Read bytes from a URI. Returns None if not found."""
    if uri.startswith("s3://"):
        return download_bytes(_s3_key_from_uri(uri))
    p = Path(uri)
    return p.read_bytes() if p.exists() else None


def _exists(uri: str) -> bool:
    """Check if a URI exists, lightweight (head_object for s3, stat for local)."""
    if uri.startswith("s3://"):
        return head_object(_s3_key_from_uri(uri)) is not None
    return Path(uri).exists()


def _delete(uri: str) -> None:
    """Delete a URI (s3:// or local path). No-op if already absent."""
    if uri.startswith("s3://"):
        from .r2 import get_s3_client
        import os
        client = get_s3_client()
        client.delete_object(Bucket=os.environ["R2_BUCKET_NAME"], Key=_s3_key_from_uri(uri))
    else:
        p = Path(uri)
        if p.exists():
            p.unlink()


# =============================================================================
# Hashing
# =============================================================================

def data_hash(table: pa.Table) -> str:
    """Fast hash based on row count + schema. Use with state to detect changes."""
    h = hashlib.md5()
    h.update(f"{len(table)}".encode())
    h.update(str(table.schema).encode())
    return h.hexdigest()[:16]


# =============================================================================
# Subsets (published Delta tables)
# =============================================================================

def load_asset(asset_name: str) -> pa.Table:
    """Load a published Delta table by name."""
    from .tracking import record_read
    uri = subsets_uri(asset_name)
    opts = get_storage_options() if uri.startswith("s3://") else None
    try:
        table = DeltaTable(uri, storage_options=opts).to_pyarrow_table()
    except Exception as e:
        raise FileNotFoundError(f"No Delta table found at {uri}") from e
    record_read(f"subsets/{asset_name}")
    return table


# =============================================================================
# State files (small JSON, per-asset)
# =============================================================================

def load_state(asset: str) -> dict:
    """Load state for an asset. Returns empty dict if not found."""
    uri = state_uri(asset)
    data = _read_with_mirror_fallback(uri, mirror_state_path(asset))
    if not data:
        return {}
    return json.loads(data.decode("utf-8"))


def save_state(asset: str, state_data: dict) -> str:
    """Save state for an asset. Returns the URI."""
    import os
    old_state = load_state(asset)
    state_data = {
        **state_data,
        "_metadata": {
            "updated_at": datetime.now().isoformat(),
            "run_id": os.environ.get("RUN_ID", "unknown"),
        },
    }
    uri = state_uri(asset)
    _write_bytes(uri, json.dumps(state_data, indent=2).encode("utf-8"))
    debug.log_state_change(asset, old_state, state_data)
    return uri


# =============================================================================
# Raw files (text/binary blobs — CSV, XML, ZIP, etc.)
# =============================================================================

def save_raw_file(content: str | bytes, asset_id: str, extension: str = "txt") -> str:
    """Save a raw file. Accepts str or bytes."""
    from .tracking import record_write
    data = content.encode("utf-8") if isinstance(content, str) else content
    uri = raw_uri(asset_id, extension)
    _write_bytes(uri, data)
    print(f"  -> Saved {asset_id}.{extension}")
    record_write(f"raw/{asset_id}.{extension}")
    return uri


def load_raw_file(asset_id: str, extension: str = "txt") -> str | bytes:
    """Load a raw file. Returns str if utf-8 decodable, else bytes."""
    from .tracking import record_read
    uri = raw_uri(asset_id, extension)
    data = _read_with_mirror_fallback(uri, mirror_raw_path(asset_id, extension))
    if data is None:
        raise FileNotFoundError(f"Raw asset '{asset_id}.{extension}' not found at {uri}")
    record_read(f"raw/{asset_id}.{extension}")
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data


# =============================================================================
# Raw JSON (with optional gzip compression)
# =============================================================================

def save_raw_json(data, asset_id: str, compress: bool = False) -> str:
    """Save raw JSON data, optionally gzip-compressed."""
    from .tracking import record_write
    if compress:
        ext = "json.gz"
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(json.dumps(data).encode("utf-8"))
        content = buf.getvalue()
    else:
        ext = "json"
        content = json.dumps(data, indent=2).encode("utf-8")
    uri = raw_uri(asset_id, ext)
    _write_bytes(uri, content)
    print(f"  -> Saved {asset_id}.{ext}")
    record_write(f"raw/{asset_id}.{ext}")
    return uri


def load_raw_json(asset_id: str):
    """Load raw JSON. Auto-detects compression (tries .json then .json.gz)."""
    from .tracking import record_read
    for ext in ("json", "json.gz"):
        uri = raw_uri(asset_id, ext)
        data = _read_with_mirror_fallback(uri, mirror_raw_path(asset_id, ext))
        if data is None:
            continue
        record_read(f"raw/{asset_id}.{ext}")
        if ext == "json.gz":
            with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as gz:
                return json.load(gz)
        return json.loads(data.decode("utf-8"))
    raise FileNotFoundError(f"Raw JSON asset '{asset_id}' not found.")


def delete_raw_file(asset_id: str, extension: str = "parquet") -> None:
    """Delete a raw asset by (asset_id, extension). No-op if absent.

    Symmetric with `save_raw_*` — addresses by id, works in cloud + dev.
    """
    _delete(raw_uri(asset_id, extension))


# =============================================================================
# Raw Parquet (PyArrow tables)
# =============================================================================

def save_raw_parquet(data: pa.Table, asset_id: str) -> str:
    """Save a PyArrow table as Parquet."""
    from .tracking import record_write
    if hasattr(data, "read_all"):
        data = data.read_all()
    buf = io.BytesIO()
    pq.write_table(data, buf, compression="snappy")
    uri = raw_uri(asset_id, "parquet")
    _write_bytes(uri, buf.getvalue())
    print(f"  -> Saved {asset_id}.parquet ({data.num_rows:,} rows)")
    record_write(f"raw/{asset_id}.parquet")
    return uri


def load_raw_parquet(asset_id: str) -> pa.Table:
    """Load a Parquet file as PyArrow table."""
    from .tracking import record_read
    uri = raw_uri(asset_id, "parquet")
    data = _read_with_mirror_fallback(uri, mirror_raw_path(asset_id, "parquet"))
    if data is None:
        raise FileNotFoundError(f"Raw parquet '{asset_id}' not found at {uri}")
    record_read(f"raw/{asset_id}.parquet")
    return pq.read_table(io.BytesIO(data))


# =============================================================================
# Listing & existence checks
# =============================================================================

def list_raw_files(pattern: str) -> list[str]:
    """List raw files matching a glob pattern (relative to the raw dir).

    Args:
        pattern: Glob like "items/*.json.gz" — relative to <connector>/data/raw/

    Returns:
        Sorted list of relative paths.
    """
    raw_dir = Path(get_data_dir()) / "raw"
    if not raw_dir.exists():
        return []
    return sorted([str(p.relative_to(raw_dir)) for p in raw_dir.glob(pattern)])


def raw_asset_exists(asset_id: str, ext: str = "parquet", max_age_days: int | None = None) -> bool:
    """Check if a raw asset exists. Optionally check it is fresh enough.

    In dev mode, checks both the local dev dir AND the SSD mirror — either
    one counts as "exists". This keeps download-if-missing nodes from
    re-downloading data already present in the mirror.

    Args:
        max_age_days: If set, returns False if the asset is older than this many days.
    """
    uri = raw_uri(asset_id, ext)

    def _check(p: Path) -> bool:
        if not p.exists():
            return False
        if max_age_days is not None:
            age = datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)
            return age < timedelta(days=max_age_days)
        return True

    if _check(Path(uri)):
        return True
    mirror = mirror_raw_path(asset_id, ext)
    return mirror is not None and _check(mirror)


def get_raw_path(asset_id: str, ext: str = "parquet") -> str:
    """Get the URI for a raw asset (s3:// in cloud, local path otherwise)."""
    return raw_uri(asset_id, ext)
