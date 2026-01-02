import json
from pathlib import Path
from deltalake import DeltaTable
from .environment import get_data_dir

def publish(dataset_name: str, metadata: dict):
    if 'id' not in metadata:
        raise ValueError("Missing required field: 'id'")
    if 'title' not in metadata:
        raise ValueError("Missing required field: 'title'")

    table_path = Path(get_data_dir()) / dataset_name
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
    print(f"Published metadata for {dataset_name}")
