"""
Testing utilities for validating transform outputs.

Usage in asset test.py:
    from utils.testing import validate

    def test(table):
        validate(table, {
            "columns": {
                "date": "date",
                "value": "float64",
                "country": "string",
            },
            "not_null": ["date", "country"],
            "unique": ["date", "country"],  # Composite key
            "min_rows": 100,
        })

        # Custom assertions for business logic
        assert all(v >= 0 for v in table.column("value").to_pylist() if v), "Values must be non-negative"
"""

import pyarrow as pa


def validate(table: pa.Table, schema: dict) -> None:
    """Validate table against schema. Raises AssertionError on failure.

    Args:
        table: PyArrow table to validate
        schema: Validation schema with optional keys:
            - columns: dict of {column_name: expected_type_substring}
            - not_null: list of column names that must not have nulls
            - unique: list of column names that form a unique key (composite if multiple)
            - min_rows: minimum expected row count
            - max_rows: maximum expected row count

    Raises:
        AssertionError: If any validation fails
    """
    # Check min/max rows
    if min_rows := schema.get("min_rows"):
        assert len(table) >= min_rows, f"Expected >= {min_rows} rows, got {len(table)}"

    if max_rows := schema.get("max_rows"):
        assert len(table) <= max_rows, f"Expected <= {max_rows} rows, got {len(table)}"

    # Check columns exist and have correct types
    if columns := schema.get("columns"):
        table_columns = set(table.column_names)

        for col, expected_type in columns.items():
            assert col in table_columns, f"Missing column: {col}"
            actual_type = str(table.schema.field(col).type)
            assert expected_type in actual_type, (
                f"Column '{col}': expected type containing '{expected_type}', got '{actual_type}'"
            )

    # Check not-null columns
    if not_null := schema.get("not_null"):
        for col in not_null:
            null_count = table.column(col).null_count
            assert null_count == 0, f"Column '{col}' has {null_count} null values"

    # Check unique constraint (composite key support)
    if unique := schema.get("unique"):
        if isinstance(unique, str):
            unique = [unique]

        if len(unique) == 1:
            values = table.column(unique[0]).to_pylist()
            duplicates = len(values) - len(set(values))
            assert duplicates == 0, f"Column '{unique[0]}' has {duplicates} duplicate values"
        else:
            # Composite key
            rows = [
                tuple(table.column(col).to_pylist()[i] for col in unique)
                for i in range(len(table))
            ]
            duplicates = len(rows) - len(set(rows))
            assert duplicates == 0, f"Columns {unique} have {duplicates} duplicate combinations"
