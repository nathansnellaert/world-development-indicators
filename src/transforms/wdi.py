"""Transform World Development Indicators data."""

import csv
import json
from io import StringIO
from pathlib import Path
from collections import defaultdict
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from subsets_utils import load_raw_file, sync_data, sync_metadata


def clean_year_str(year_str: str) -> int:
    """Clean year string by removing YR prefix"""
    return int(year_str.replace("YR", "").replace("yr", "").strip())


def load_indicator_mapping() -> dict:
    """Load the indicator to table mapping from CSV file."""
    mapping_path = Path(__file__).parent.parent.parent / 'mappings' / 'indicator_table_mapping.csv'
    indicator_to_table = {}
    with open(mapping_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            indicator_to_table[row['indicator_name']] = row['table_name']
    return indicator_to_table


def load_indicator_to_column_mapping() -> dict:
    """Load the indicator name to column name mapping from CSV file."""
    mapping_path = Path(__file__).parent.parent.parent / 'mappings' / 'indicator_to_col_name.csv'
    indicator_to_column = {}
    with open(mapping_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            indicator_to_column[row['indicator_name']] = row['column_name']
    return indicator_to_column


def load_table_metadata() -> dict:
    """Load table metadata from JSON file"""
    metadata_path = Path(__file__).parent.parent.parent / 'mappings' / 'table_metadata.json'
    with open(metadata_path, 'r') as f:
        return json.load(f)


def load_indicator_summaries() -> dict:
    """Load indicator summaries and convert to {indicator_code: description} format"""
    summaries_path = Path(__file__).parent.parent.parent / 'mappings' / 'indicator_summaries.json'
    with open(summaries_path, 'r') as f:
        summaries_list = json.load(f)
    return {item['indicator_code']: item['description'] for item in summaries_list}


def process_countries(df: pa.Table) -> pa.Table:
    """Extract country code mapping from WDICountry.csv"""
    if df is None:
        return pa.table({
            'country_code2': pa.array([], type=pa.string()),
            'country_code3': pa.array([], type=pa.string())
        })

    country_code2 = df.column('2-alpha code')
    country_code3 = df.column('Country Code')

    return pa.table({
        'country_code2': country_code2,
        'country_code3': country_code3
    })


def process_wdi_series(df: pa.Table, country_mapping: pa.Table) -> pa.Table:
    """Process main WDI data series"""
    if df is None:
        return pa.table({
            'country_name': pa.array([], type=pa.string()),
            'country_code2': pa.array([], type=pa.string()),
            'indicator_name': pa.array([], type=pa.string()),
            'indicator_code': pa.array([], type=pa.string()),
            'year': pa.array([], type=pa.int32()),
            'value': pa.array([], type=pa.float64())
        })

    country_name = df.column('Country Name')
    country_code3 = df.column('Country Code')
    indicator_name = df.column('Indicator Name')
    indicator_code = df.column('Indicator Code')

    year_columns = []
    year_names = []
    for i, field in enumerate(df.schema):
        if field.name not in ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code']:
            if not field.name.startswith('Unnamed'):
                year_columns.append(df.column(i))
                year_names.append(field.name)

    melted_data = []
    num_rows = len(df)

    for year_col, year_name in zip(year_columns, year_names):
        year_int = clean_year_str(year_name)
        year_array = pa.array([year_int] * num_rows, type=pa.int32())

        year_table = pa.table({
            'country_name': country_name,
            'country_code3': country_code3,
            'indicator_name': indicator_name,
            'indicator_code': indicator_code,
            'year': year_array,
            'value': year_col
        })

        mask = pc.is_valid(year_table.column('value'))
        year_table = year_table.filter(mask)
        melted_data.append(year_table)

    if melted_data:
        result = pa.concat_tables(melted_data)
    else:
        result = pa.table({
            'country_name': pa.array([], type=pa.string()),
            'country_code3': pa.array([], type=pa.string()),
            'indicator_name': pa.array([], type=pa.string()),
            'indicator_code': pa.array([], type=pa.string()),
            'year': pa.array([], type=pa.int32()),
            'value': pa.array([], type=pa.float64())
        })

    result = result.join(country_mapping, keys='country_code3', right_keys='country_code3')

    final_table = pa.table({
        'country_name': result.column('country_name'),
        'country_code2': result.column('country_code2'),
        'indicator_name': result.column('indicator_name'),
        'indicator_code': result.column('indicator_code'),
        'year': result.column('year'),
        'value': result.column('value')
    })

    print(f"Processed {len(final_table)} WDI data records")
    return final_table


# Common column descriptions used across all WDI datasets
COMMON_COLUMN_DESCRIPTIONS = {
    "country_name": "Country name from World Bank database",
    "country_code2": "ISO 3166-1 alpha-2 country code",
    "year": "Observation year",
}


def transform_to_wide_format(table: pa.Table, indicator_summaries: dict) -> tuple[pa.Table, dict]:
    """Transform long format table to wide format where each indicator becomes a column.

    Returns:
        tuple: (wide_table, column_descriptions) where column_descriptions is a dict
               mapping column names to their descriptions for metadata.
    """
    indicator_to_column = load_indicator_to_column_mapping()

    df = table.to_pandas()
    df['column_name'] = df['indicator_name'].map(indicator_to_column)
    df['column_name'] = df['column_name'].fillna(df['indicator_code'])

    wide_df = df.pivot_table(
        index=['country_name', 'country_code2', 'year'],
        columns='column_name',
        values='value',
        aggfunc='first'
    ).reset_index()

    wide_df.columns.name = None
    wide_table = pa.Table.from_pandas(wide_df, preserve_index=False)

    column_to_code = {}
    for idx, row in df[['column_name', 'indicator_code']].drop_duplicates().iterrows():
        column_to_code[row['column_name']] = row['indicator_code']

    # Build column descriptions dict for metadata
    column_descriptions = {}

    schema_fields = []
    for field in wide_table.schema:
        if field.name in COMMON_COLUMN_DESCRIPTIONS:
            column_descriptions[field.name] = COMMON_COLUMN_DESCRIPTIONS[field.name]
            schema_fields.append(field)
        elif field.name in column_to_code:
            indicator_code = column_to_code[field.name]
            if indicator_code in indicator_summaries:
                column_descriptions[field.name] = indicator_summaries[indicator_code]
                new_field = pa.field(
                    field.name,
                    field.type,
                    metadata={'description': indicator_summaries[indicator_code]}
                )
                schema_fields.append(new_field)
            else:
                schema_fields.append(field)
        else:
            schema_fields.append(field)

    new_schema = pa.schema(schema_fields)
    wide_table = wide_table.cast(new_schema)

    return wide_table, column_descriptions


def split_wdi_by_tables(wdi_data: pa.Table) -> dict[str, pa.Table]:
    """Split WDI data into separate tables based on indicator mapping."""
    indicator_mapping = load_indicator_mapping()
    print(f"Loaded mapping for {len(indicator_mapping)} indicators")

    tables_dict = {}
    table_names = set(indicator_mapping.values())
    print(f"Splitting data into {len(table_names)} domain tables...")

    for table_name in table_names:
        indicators_for_table = [ind for ind, tbl in indicator_mapping.items() if tbl == table_name]

        if indicators_for_table:
            indicator_column = wdi_data.column('indicator_name')
            mask = None

            for indicator in indicators_for_table:
                indicator_mask = pc.equal(indicator_column, pa.scalar(indicator))
                if mask is None:
                    mask = indicator_mask
                else:
                    mask = pc.or_(mask, indicator_mask)

            table_data = wdi_data.filter(mask)

            if len(table_data) > 0:
                tables_dict[table_name] = table_data
                print(f"  {table_name}: {len(table_data):,} records")

    all_indicators = set(pc.unique(wdi_data.column('indicator_name')).to_pylist())
    mapped_indicators = set(indicator_mapping.keys())
    unmapped_indicators = all_indicators - mapped_indicators

    if unmapped_indicators:
        print(f"\nFound {len(unmapped_indicators)} unmapped indicators")
        mask = None
        indicator_column = wdi_data.column('indicator_name')

        for indicator in unmapped_indicators:
            indicator_mask = pc.equal(indicator_column, pa.scalar(indicator))
            if mask is None:
                mask = indicator_mask
            else:
                mask = pc.or_(mask, indicator_mask)

        unmapped_table = wdi_data.filter(mask)
        if len(unmapped_table) > 0:
            tables_dict['wdi_unmapped'] = unmapped_table
            print(f"\n  unmapped table: {len(unmapped_table):,} records")

    return tables_dict


def run():
    """Transform WDI data to domain-specific tables."""
    print("  Loading raw CSV data...")

    # Load CSVs from raw cache
    wdicountry_csv = load_raw_file("wdicountry", extension="csv")
    wdicsv_csv = load_raw_file("wdicsv", extension="csv")

    # Parse CSVs to PyArrow tables
    df_country = pd.read_csv(StringIO(wdicountry_csv), on_bad_lines='skip', encoding='utf-8')
    df_main = pd.read_csv(StringIO(wdicsv_csv), on_bad_lines='skip', encoding='utf-8')

    countries_table = pa.Table.from_pandas(df_country)
    main_table = pa.Table.from_pandas(df_main)

    # Get country mapping
    country_mapping = process_countries(countries_table)

    # Process main WDI data
    wdi_data = process_wdi_series(main_table, country_mapping)

    # Load metadata
    table_metadata = load_table_metadata()
    indicator_summaries = load_indicator_summaries()

    # Split the data into domain-specific tables
    tables_long = split_wdi_by_tables(wdi_data)

    # Transform each domain table to wide format and upload
    for table_name, table_data in tables_long.items():
        if table_name.startswith('wdi_'):
            print(f"Transforming {table_name} to wide format...")
            wide_table, column_descriptions = transform_to_wide_format(table_data, indicator_summaries)
            print(f"  {table_name}: {len(wide_table):,} rows x {len(wide_table.schema)} columns")

            if len(wide_table) > 0:
                sync_data(wide_table, table_name)

                if table_name in table_metadata:
                    sync_metadata(table_name, {
                        "id": table_name,
                        "title": table_metadata[table_name]["title"],
                        "description": table_metadata[table_name]["description"],
                        "column_descriptions": column_descriptions
                    })
                    print(f"Published metadata for {table_name}")
        else:
            if len(table_data) > 0:
                sync_data(table_data, table_name)
