"""World Development Indicators connector utilities.

Provides mapping loaders and shared constants for transforming WDI data
into domain-specific tables.
"""

import csv
import json
from pathlib import Path

import pyarrow as pa
from subsets_utils import validate

MAPPINGS_DIR = Path(__file__).parent.parent / "mappings"

LICENSE = "CC-BY-4.0 (World Bank Open Data)"

COMMON_COLUMN_DESCRIPTIONS = {
    "country_name": "Country name from World Bank database",
    "country_code2": "ISO 3166-1 alpha-2 country code",
    "year": "Observation year",
}


def load_indicator_mapping() -> dict:
    """Load the indicator to table mapping from CSV file.

    Returns:
        Dict mapping indicator_name -> table_name.
    """
    mapping_path = MAPPINGS_DIR / "indicator_table_mapping.csv"
    indicator_to_table = {}
    with open(mapping_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            indicator_to_table[row["indicator_name"]] = row["table_name"]
    return indicator_to_table


def load_indicator_to_column_mapping() -> dict:
    """Load the indicator name to column name mapping from CSV file.

    Returns:
        Dict mapping indicator_name -> column_name (snake_case).
    """
    mapping_path = MAPPINGS_DIR / "indicator_to_col_name.csv"
    indicator_to_column = {}
    with open(mapping_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            indicator_to_column[row["indicator_name"]] = row["column_name"]
    return indicator_to_column


def load_table_metadata() -> dict:
    """Load table metadata (titles, descriptions) from JSON file.

    Returns:
        Dict keyed by table_name with title/description values.
    """
    metadata_path = MAPPINGS_DIR / "table_metadata.json"
    with open(metadata_path, "r") as f:
        return json.load(f)


def load_indicator_summaries() -> dict:
    """Load indicator summaries and convert to {indicator_code: description} format.

    Returns:
        Dict mapping indicator_code -> human-readable description.
    """
    summaries_path = MAPPINGS_DIR / "indicator_summaries.json"
    with open(summaries_path, "r") as f:
        summaries_list = json.load(f)
    return {item["indicator_code"]: item["description"] for item in summaries_list}


def validate_wdi_table(table: pa.Table, table_name: str) -> None:
    """Validate a transformed WDI table before upload.

    Args:
        table: The PyArrow table to validate.
        table_name: Name of the table (for error messages).
    """
    validate(table, {
        "columns": {
            "country_name": "string",
            "country_code2": "string",
            "year": "int",
        },
        "not_null": ["country_name", "country_code2", "year"],
        "min_rows": 1,
    })
