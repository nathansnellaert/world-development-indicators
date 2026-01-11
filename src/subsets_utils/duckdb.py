"""DuckDB utilities for querying raw data."""
import duckdb
from .io import get_raw_path
from .r2 import _is_cloud_mode, _get_r2_config

_configured = False


def _configure():
    """Auto-configure DuckDB for S3 if in cloud mode."""
    global _configured
    if _configured:
        return
    if _is_cloud_mode():
        cfg = _get_r2_config()
        duckdb.sql(f"""
            SET s3_endpoint='{cfg['account_id']}.r2.cloudflarestorage.com';
            SET s3_access_key_id='{cfg['access_key_id']}';
            SET s3_secret_access_key='{cfg['secret_access_key']}';
            SET s3_region='auto';
        """)
    _configured = True


def raw(assets: list[str] | str) -> str:
    """Returns read_parquet clause for DuckDB query.

    Usage:
        from subsets_utils.duckdb import raw

        table = duckdb.sql(f"SELECT * FROM {raw('my_asset')}").arrow()
        table = duckdb.sql(f"SELECT * FROM {raw(['asset1', 'asset2'])}").arrow()
    """
    _configure()
    if isinstance(assets, str):
        assets = [assets]
    paths = [get_raw_path(a) for a in assets]
    return f"read_parquet({paths})"
