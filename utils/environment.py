import os
from typing import List, Optional, Dict

CORE_ENV_VARS = [
    "RUN_ID",
    "STORAGE_BACKEND"
]

def validate_environment(required: Optional[List[str]] = None) -> Dict[str, str]:
    if required is None:
        required = CORE_ENV_VARS.copy()
    
    # Add R2 vars if using R2 storage
    if os.environ.get('STORAGE_BACKEND') == 'r2':
        required.extend([
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY", 
            "R2_ENDPOINT_URL",
            "R2_BUCKET_NAME"
        ])
    
    missing = [var for var in required if var not in os.environ]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")
    
    return {var: os.environ[var] for var in required}

def get_connector_name() -> str:
    if 'CONNECTOR_NAME' not in os.environ:
        raise ValueError("CONNECTOR_NAME must be set before using utils")
    return os.environ['CONNECTOR_NAME']

def is_github_actions() -> bool:
    return os.environ.get('GITHUB_ACTIONS') == 'true'

def is_dev_mode() -> bool:
    return os.environ.get('DEV_MODE', '').lower() == 'true'

def should_write_snapshot() -> bool:
    return os.environ.get('WRITE_SNAPSHOT', '').lower() == 'true'

def get_run_id() -> str:
    return os.environ['RUN_ID']

def get_data_dir() -> str:
    return os.environ.get('DATA_DIR', 'data')