import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import urlparse
import duckdb
import logging
from .environment import get_data_dir

logger = logging.getLogger(__name__)

# Singleton connections
_runs_connection = None
_logs_connection = None
_runs_path = None
_logs_path = None


def _get_runs_db_path() -> Path:
    """Get centralized runs database path"""
    base_path = Path(get_data_dir())
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path / "runs.db"


def _get_logs_db_path() -> Path:
    """Get connector-specific logs database path"""
    connector = os.environ.get('CONNECTOR_NAME', 'unknown')
    base_path = Path(get_data_dir()) / connector / "debug"
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path / "logs.db"


def _get_runs_connection() -> duckdb.DuckDBPyConnection:
    """Get or create centralized runs database connection"""
    global _runs_connection, _runs_path
    
    db_path = _get_runs_db_path()
    
    if _runs_connection is None or _runs_path != db_path:
        _runs_path = db_path
        _runs_connection = duckdb.connect(str(db_path))
        _init_runs_schema(_runs_connection)
        logger.info(f"Runs database initialized at {db_path}")
    
    return _runs_connection


def _get_logs_connection() -> duckdb.DuckDBPyConnection:
    """Get or create connector-specific logs database connection"""
    global _logs_connection, _logs_path
    
    db_path = _get_logs_db_path()
    
    if _logs_connection is None or _logs_path != db_path:
        _logs_path = db_path
        _logs_connection = duckdb.connect(str(db_path))
        _init_logs_schema(_logs_connection)
        logger.info(f"Logs database initialized at {db_path}")
    
    return _logs_connection


def _init_runs_schema(conn: duckdb.DuckDBPyConnection):
    """Initialize runs database schema"""
    # Runs table (centralized)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id VARCHAR PRIMARY KEY,
            connector VARCHAR,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            status VARCHAR,
            environment JSON,
            error_message VARCHAR,
            error_traceback TEXT,
            total_requests INTEGER DEFAULT 0,
            failed_requests INTEGER DEFAULT 0,
            data_rows_output BIGINT DEFAULT 0
        )
    """)
    
    conn.execute("CREATE INDEX IF NOT EXISTS idx_connector ON runs(connector)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_started ON runs(started_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON runs(status)")


def _init_logs_schema(conn: duckdb.DuckDBPyConnection):
    """Initialize logs database schema"""
    # Create sequences first
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_http_requests START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_data_outputs START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_state_changes START 1")
    
    # HTTP requests table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS http_requests (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_http_requests'),
            run_id VARCHAR,
            timestamp TIMESTAMP,
            method VARCHAR,
            url VARCHAR,
            url_host VARCHAR,
            url_path VARCHAR,
            params JSON,
            headers JSON,
            request_body JSON,
            status_code INTEGER,
            response_headers JSON,
            response_size_bytes BIGINT,
            duration_ms INTEGER,
            cached BOOLEAN,
            cache_key VARCHAR,
            error_message VARCHAR
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run ON http_requests(run_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON http_requests(status_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON http_requests(timestamp)")
    
    # Data outputs table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_outputs (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_data_outputs'),
            run_id VARCHAR,
            dataset_name VARCHAR,
            timestamp TIMESTAMP,
            row_count BIGINT,
            column_count INTEGER,
            size_bytes BIGINT,
            storage_path VARCHAR,
            schema JSON,
            metrics JSON
        )
    """)
    
    # Create index for data_outputs
    conn.execute("CREATE INDEX IF NOT EXISTS idx_output_run ON data_outputs(run_id, dataset_name)")
    
    # State changes table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS state_changes (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_state_changes'),
            run_id VARCHAR,
            asset VARCHAR,
            timestamp TIMESTAMP,
            old_state JSON,
            new_state JSON,
            changed_keys JSON
        )
    """)
    
    # Useful views
    conn.execute("""
        CREATE OR REPLACE VIEW failed_requests AS
        SELECT * FROM http_requests 
        WHERE status_code >= 400 OR error_message IS NOT NULL
    """)
    
    conn.execute("""
        CREATE OR REPLACE VIEW api_performance AS
        SELECT 
            url_host,
            url_path,
            COUNT(*) as request_count,
            AVG(duration_ms) as avg_duration_ms,
            MAX(duration_ms) as max_duration_ms,
            SUM(CASE WHEN cached THEN 1 ELSE 0 END) as cache_hits
        FROM http_requests
        GROUP BY url_host, url_path
    """)


def log_run_start():
    """Log the start of a run"""
    if not os.environ.get('CACHE_REQUESTS', '').lower() == 'true':
        return
        
    conn = _get_runs_connection()
    run_id = os.environ.get('RUN_ID', 'unknown')
    connector = os.environ.get('CONNECTOR_NAME', 'unknown')
    
    # Capture environment
    env_vars = {k: v for k, v in os.environ.items() 
                if k.startswith(('STORAGE_', 'ENABLE_', 'CACHE_', 'WRITE_', 'CONNECTOR_', 'RUN_'))}
    
    conn.execute("""
        INSERT INTO runs (run_id, connector, started_at, status, environment)
        VALUES (?, ?, ?, 'started', ?)
        ON CONFLICT (run_id) DO UPDATE SET
            started_at = EXCLUDED.started_at,
            status = EXCLUDED.status
    """, [run_id, connector, datetime.now(), json.dumps(env_vars)])


def log_run_end(status: str = 'completed', error: Optional[Exception] = None):
    """Log the end of a run"""
    if not os.environ.get('CACHE_REQUESTS', '').lower() == 'true':
        return
        
    runs_conn = _get_runs_connection()
    logs_conn = _get_logs_connection()
    run_id = os.environ.get('RUN_ID', 'unknown')
    
    # Calculate summary stats from logs
    try:
        stats = logs_conn.execute("""
            SELECT 
                COUNT(*) as total_requests,
                SUM(CASE WHEN status_code >= 400 OR error_message IS NOT NULL THEN 1 ELSE 0 END) as failed_requests,
                SUM(row_count) as total_rows
            FROM (
                SELECT status_code, error_message, 0 as row_count FROM http_requests WHERE run_id = ?
                UNION ALL
                SELECT NULL, NULL, row_count FROM data_outputs WHERE run_id = ?
            )
        """, [run_id, run_id]).fetchone()
    except:
        # Tables might not exist if no logs were written
        stats = (0, 0, 0)
    
    if error:
        import traceback
        error_msg = str(error)
        error_tb = traceback.format_exc()
        runs_conn.execute("""
            UPDATE runs 
            SET ended_at = ?, status = ?, error_message = ?, error_traceback = ?,
                total_requests = ?, failed_requests = ?, data_rows_output = ?
            WHERE run_id = ?
        """, [datetime.now(), status, error_msg, error_tb, 
              stats[0] or 0, stats[1] or 0, stats[2] or 0, run_id])
    else:
        runs_conn.execute("""
            UPDATE runs 
            SET ended_at = ?, status = ?,
                total_requests = ?, failed_requests = ?, data_rows_output = ?
            WHERE run_id = ?
        """, [datetime.now(), status, 
              stats[0] or 0, stats[1] or 0, stats[2] or 0, run_id])


def log_http_request(
    method: str,
    url: str,
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None,
    request_body: Optional[Any] = None,
    response_status: Optional[int] = None,
    response_headers: Optional[Dict] = None,
    response_size: Optional[int] = None,
    duration_ms: Optional[int] = None,
    cached: bool = False,
    cache_key: Optional[str] = None,
    error: Optional[str] = None
):
    """Log an HTTP request/response"""
    if not os.environ.get('CACHE_REQUESTS', '').lower() == 'true':
        return
        
    conn = _get_logs_connection()
    run_id = os.environ.get('RUN_ID', 'unknown')
    
    # Parse URL
    parsed = urlparse(url)
    url_host = parsed.netloc
    url_path = parsed.path
    
    # Sanitize headers
    if headers:
        headers = _sanitize_headers(headers)
    if response_headers:
        response_headers = _sanitize_headers(response_headers)
    
    conn.execute("""
        INSERT INTO http_requests (
            run_id, timestamp, method, url, url_host, url_path,
            params, headers, request_body, status_code, response_headers,
            response_size_bytes, duration_ms, cached, cache_key, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        run_id, datetime.now(), method, url, url_host, url_path,
        json.dumps(params) if params else None,
        json.dumps(headers) if headers else None,
        json.dumps(request_body) if request_body else None,
        response_status,
        json.dumps(response_headers) if response_headers else None,
        response_size,
        duration_ms,
        cached,
        cache_key,
        error
    ])


def log_data_output(
    dataset_name: str,
    row_count: int,
    column_count: int,
    size_bytes: int,
    storage_path: str,
    schema: Optional[Dict] = None,
    metrics: Optional[Dict] = None
):
    """Log a data output"""
    if not os.environ.get('CACHE_REQUESTS', '').lower() == 'true':
        return
        
    conn = _get_logs_connection()
    run_id = os.environ.get('RUN_ID', 'unknown')
    
    conn.execute("""
        INSERT INTO data_outputs (
            run_id, dataset_name, timestamp, row_count, column_count,
            size_bytes, storage_path, schema, metrics
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        run_id, dataset_name, datetime.now(), row_count, column_count,
        size_bytes, storage_path,
        json.dumps(schema) if schema else None,
        json.dumps(metrics) if metrics else None
    ])


def log_state_change(
    asset: str,
    old_state: Dict[str, Any],
    new_state: Dict[str, Any]
):
    """Log a state change"""
    if not os.environ.get('CACHE_REQUESTS', '').lower() == 'true':
        return
        
    conn = _get_logs_connection()
    run_id = os.environ.get('RUN_ID', 'unknown')
    
    # Calculate what changed
    changed_keys = []
    all_keys = set(old_state.keys()) | set(new_state.keys())
    for key in all_keys:
        if old_state.get(key) != new_state.get(key):
            changed_keys.append(key)
    
    conn.execute("""
        INSERT INTO state_changes (
            run_id, asset, timestamp, old_state, new_state, changed_keys
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, [
        run_id, asset, datetime.now(),
        json.dumps(old_state),
        json.dumps(new_state),
        json.dumps(changed_keys)
    ])


def _sanitize_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Remove sensitive headers from logs"""
    sanitized = headers.copy()
    sensitive_keys = ['authorization', 'api-key', 'x-api-key', 'token', 'secret', 'password']
    
    for key in list(sanitized.keys()):
        if any(sensitive in key.lower() for sensitive in sensitive_keys):
            sanitized[key] = '[REDACTED]'
    
    return sanitized


def close():
    """Close database connections"""
    global _runs_connection, _logs_connection
    if _runs_connection:
        _runs_connection.close()
        _runs_connection = None
    if _logs_connection:
        _logs_connection.close()
        _logs_connection = None