"""Transform World Development Indicators data into domain-specific tables.

- Reads raw parquet files via DuckDB
- Splits data by indicator → table mapping
- Transforms to wide format (indicators become columns)
- Uploads each domain table with metadata
"""
import csv
import json
from pathlib import Path
from collections import defaultdict
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from subsets_utils import load_state, save_state, sync_data, sync_metadata, validate
from subsets_utils.duckdb import raw

MAPPINGS_DIR = Path(__file__).parent.parent.parent / 'mappings'

COMMON_COLUMN_DESCRIPTIONS = {
    "country_name": "Country name from World Bank database",
    "country_code2": "ISO 3166-1 alpha-2 country code",
    "year": "Observation year",
}


def load_indicator_mapping() -> dict:
    """Load the indicator to table mapping from CSV file."""
    mapping_path = MAPPINGS_DIR / 'indicator_table_mapping.csv'
    indicator_to_table = {}
    with open(mapping_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            indicator_to_table[row['indicator_name']] = row['table_name']
    return indicator_to_table


def load_indicator_to_column_mapping() -> dict:
    """Load the indicator name to column name mapping from CSV file."""
    mapping_path = MAPPINGS_DIR / 'indicator_to_col_name.csv'
    indicator_to_column = {}
    with open(mapping_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            indicator_to_column[row['indicator_name']] = row['column_name']
    return indicator_to_column


def load_table_metadata() -> dict:
    """Load table metadata from JSON file."""
    metadata_path = MAPPINGS_DIR / 'table_metadata.json'
    with open(metadata_path, 'r') as f:
        return json.load(f)


def load_indicator_summaries() -> dict:
    """Load indicator summaries and convert to {indicator_code: description} format."""
    summaries_path = MAPPINGS_DIR / 'indicator_summaries.json'
    with open(summaries_path, 'r') as f:
        summaries_list = json.load(f)
    return {item['indicator_code']: item['description'] for item in summaries_list}


def test(table: pa.Table, table_name: str) -> None:
    """Validate transformed table before upload."""
    validate(table, {
        "columns": {
            "country_name": "string",
            "country_code2": "string",
            "year": "int",
        },
        "not_null": ["country_name", "country_code2", "year"],
        "min_rows": 1,
    })


def run():
    """Transform WDI data to domain-specific tables."""
    print("Transforming World Development Indicators...")

    ingest_state = load_state("ingest")
    if not ingest_state.get("fetched_assets"):
        print("  No ingested data found, skipping transform")
        return

    # Load mappings
    indicator_mapping = load_indicator_mapping()
    indicator_to_column = load_indicator_to_column_mapping()
    table_metadata = load_table_metadata()
    indicator_summaries = load_indicator_summaries()

    print(f"  Loaded mapping for {len(indicator_mapping)} indicators")

    # Query main data and country mapping via DuckDB
    print("  Loading data via DuckDB...")

    # Get country code mapping (3-letter to 2-letter)
    country_df = duckdb.sql(f"""
        SELECT
            "Country Code" as country_code3,
            "2-alpha code" as country_code2
        FROM {raw('wdicountry')}
        WHERE "2-alpha code" IS NOT NULL
    """).arrow()

    # Get main WDI data, unpivot from wide to long format
    # Detect year columns dynamically
    schema_query = duckdb.sql(f"SELECT * FROM {raw('wdicsv')} LIMIT 0")
    columns = schema_query.columns

    # Year columns are those that look like years (4-digit numbers or YRxxxx)
    year_cols = [c for c in columns if c not in ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code']
                 and not c.startswith('Unnamed')]

    # Build UNPIVOT query
    year_cols_quoted = ', '.join([f'"{c}"' for c in year_cols])

    wdi_long = duckdb.sql(f"""
        WITH unpivoted AS (
            UNPIVOT {raw('wdicsv')}
            ON {year_cols_quoted}
            INTO
                NAME year_str
                VALUE value
        )
        SELECT
            "Country Name" as country_name,
            "Country Code" as country_code3,
            "Indicator Name" as indicator_name,
            "Indicator Code" as indicator_code,
            CAST(REGEXP_REPLACE(year_str, '[^0-9]', '', 'g') AS INTEGER) as year,
            CAST(value AS DOUBLE) as value
        FROM unpivoted
        WHERE value IS NOT NULL
    """).arrow()

    print(f"  Processed {len(wdi_long):,} WDI data records")

    # Join with country codes
    wdi_long_df = duckdb.sql("""
        SELECT
            w.country_name,
            c.country_code2,
            w.indicator_name,
            w.indicator_code,
            w.year,
            w.value
        FROM wdi_long w
        LEFT JOIN country_df c ON w.country_code3 = c.country_code3
    """).arrow()

    # Split by table and transform to wide format
    table_names = set(indicator_mapping.values())
    print(f"  Splitting data into {len(table_names)} domain tables...")

    transformed_tables = []

    for table_name in sorted(table_names):
        # Get indicators for this table
        indicators_for_table = [ind for ind, tbl in indicator_mapping.items() if tbl == table_name]

        if not indicators_for_table:
            continue

        # Filter data for this table
        indicators_set = set(indicators_for_table)
        indicator_mask = pc.is_in(wdi_long_df.column('indicator_name'), value_set=pa.array(list(indicators_set)))
        table_data = wdi_long_df.filter(indicator_mask)

        if len(table_data) == 0:
            continue

        print(f"    {table_name}: {len(table_data):,} records")

        # Transform to wide format using DuckDB PIVOT
        # Create column name mapping
        col_mapping = {}
        for ind in indicators_for_table:
            col_name = indicator_to_column.get(ind, ind)
            col_mapping[ind] = col_name

        # Register table for DuckDB query
        duckdb.register('table_data', table_data)

        # Get unique indicators in this table
        unique_indicators = duckdb.sql("SELECT DISTINCT indicator_name FROM table_data").fetchall()
        indicator_list = [row[0] for row in unique_indicators]

        # Build dynamic PIVOT query
        pivot_cols = []
        for ind in indicator_list:
            col_name = col_mapping.get(ind, ind.replace(' ', '_').replace(',', '').replace('(', '').replace(')', '').replace('%', 'pct').lower()[:60])
            pivot_cols.append(f"first(CASE WHEN indicator_name = '{ind.replace(chr(39), chr(39)+chr(39))}' THEN value END) as \"{col_name}\"")

        pivot_query = f"""
            SELECT
                country_name,
                country_code2,
                year,
                {', '.join(pivot_cols)}
            FROM table_data
            WHERE country_code2 IS NOT NULL
            GROUP BY country_name, country_code2, year
            ORDER BY country_name, year
        """

        try:
            wide_table = duckdb.sql(pivot_query).arrow()
        except Exception as e:
            print(f"      Error pivoting {table_name}: {e}")
            continue

        if len(wide_table) == 0:
            continue

        print(f"      -> {len(wide_table):,} rows x {len(wide_table.schema)} columns")

        # Build column descriptions
        column_descriptions = dict(COMMON_COLUMN_DESCRIPTIONS)

        # Map indicator codes to descriptions
        code_to_desc = {}
        for ind in indicator_list:
            # Find indicator code for this indicator name
            indicator_codes = duckdb.sql(f"""
                SELECT DISTINCT indicator_code
                FROM table_data
                WHERE indicator_name = '{ind.replace(chr(39), chr(39)+chr(39))}'
            """).fetchall()
            if indicator_codes:
                code = indicator_codes[0][0]
                if code in indicator_summaries:
                    col_name = col_mapping.get(ind)
                    if col_name:
                        column_descriptions[col_name] = indicator_summaries[code]

        # Validate before upload
        test(wide_table, table_name)

        # Upload data
        sync_data(wide_table, table_name)

        # Upload metadata
        if table_name in table_metadata:
            sync_metadata(table_name, {
                "id": table_name,
                "title": table_metadata[table_name]["title"],
                "description": table_metadata[table_name]["description"],
                "column_descriptions": column_descriptions
            })
            print(f"      Published metadata for {table_name}")

        transformed_tables.append(table_name)

        duckdb.unregister('table_data')

    # Handle unmapped indicators
    mapped_indicators = set(indicator_mapping.keys())
    all_indicators_query = duckdb.sql("""
        SELECT DISTINCT indicator_name
        FROM wdi_long_df
    """).fetchall()
    all_indicators = {row[0] for row in all_indicators_query}
    unmapped = all_indicators - mapped_indicators

    if unmapped:
        print(f"\n  Found {len(unmapped)} unmapped indicators")
        unmapped_mask = pc.is_in(wdi_long_df.column('indicator_name'), value_set=pa.array(list(unmapped)))
        unmapped_table = wdi_long_df.filter(unmapped_mask)

        if len(unmapped_table) > 0:
            # Filter to only rows with valid country codes
            valid_mask = pc.is_valid(unmapped_table.column('country_code2'))
            unmapped_table = unmapped_table.filter(valid_mask)

            if len(unmapped_table) > 0:
                sync_data(unmapped_table, "wdi_unmapped")
                print(f"    Saved {len(unmapped_table):,} unmapped records")
                transformed_tables.append("wdi_unmapped")

    save_state("wdi_tables", {"transformed_tables": sorted(transformed_tables)})
    print(f"\n  Complete! Transformed {len(transformed_tables)} tables")
