import os

def validate_environment(required=None):
    if required is None:
        required = ["RUN_ID", "DATA_DIR"]
    missing = [var for var in required if var not in os.environ]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")
    return {var: os.environ[var] for var in required}

def get_data_dir():
    return os.environ['DATA_DIR']

def get_run_id():
    return os.environ['RUN_ID']