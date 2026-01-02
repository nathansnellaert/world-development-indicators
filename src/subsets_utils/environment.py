import os


def is_cloud_mode() -> bool:
    """Check if running in cloud mode (CI environment)."""
    return os.environ.get('CI', '').lower() == 'true'


def validate_environment():
    """Validate required environment variables based on execution mode.

    Local mode: requires DATA_DIR
    Cloud mode: requires R2 credentials
    """
    if is_cloud_mode():
        required = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"]
    else:
        required = ["DATA_DIR"]

    missing = [var for var in required if var not in os.environ]
    if missing:
        mode = "cloud" if is_cloud_mode() else "local"
        raise ValueError(f"Missing required environment variables for {mode} mode: {missing}")


def get_data_dir():
    """Get data directory. Only valid in local mode."""
    if is_cloud_mode():
        # In cloud mode, return a temp directory (data goes to R2, not disk)
        return "/tmp/data"
    return os.environ['DATA_DIR']


def get_run_id():
    return os.environ.get('RUN_ID', 'unknown')