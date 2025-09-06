"""World Development Indicators Asset - Downloads and processes WDI CSV data"""
import io
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from utils.http_client import get
from utils.io import save_state
from datetime import datetime


def clean_year_str(year_str: str) -> int:
    """Clean year string by removing YR prefix"""
    return int(year_str.replace("YR", "").replace("yr", "").strip())


def process_wdi() -> tuple[pa.Table, pa.Table, pa.Table, pa.Table, pa.Table]:
    """
    Download and process World Development Indicators CSV data.
    Returns multiple tables for different aspects of the data.
    """
    print("📊 Downloading World Development Indicators CSV data...")
    
    # Download the ZIP file
    download_url = "https://databank.worldbank.org/data/download/WDI_CSV.zip"
    response = get(download_url, timeout=300)
    response.raise_for_status()
    
    print(f"Downloaded {len(response.content) / 1024 / 1024:.1f} MB")
    
    # Extract CSV files from ZIP
    zip_bytes_io = io.BytesIO(response.content)
    csv_contents = {}
    
    with zipfile.ZipFile(zip_bytes_io) as zip_ref:
        for file_info in zip_ref.filelist:
            if file_info.filename.endswith('.csv'):
                print(f"Extracting {file_info.filename}")
                with zip_ref.open(file_info) as file:
                    # Read CSV content as bytes then parse with pandas (more robust)
                    csv_bytes = file.read()
                    # Use pandas for flexible CSV parsing
                    df = pd.read_csv(io.BytesIO(csv_bytes), on_bad_lines='skip', encoding='utf-8')
                    csv_contents[file_info.filename] = pa.Table.from_pandas(df)
    
    print("Processing data files...")
    
    # Get countries for mapping
    countries_table = process_countries(csv_contents.get('WDICountry.csv'))
    
    # Process each dataset
    wdi_data = process_wdi_series(csv_contents.get('WDICSV.csv'), countries_table)
    series_metadata = process_wdi_series_metadata(csv_contents.get('WDISeries.csv'))
    country_series_metadata = process_wdi_country_series_metadata(
        csv_contents.get('WDIcountry-series.csv'), countries_table
    )
    observation_metadata = process_wdi_observation_metadata(
        csv_contents.get('WDIfootnote.csv'), countries_table
    )
    series_time_metadata = process_wdi_series_time_metadata(csv_contents.get('WDIseries-time.csv'))
    
    # Update state
    save_state("wdi", {"last_update": datetime.now().isoformat()})
    
    return (
        wdi_data,
        series_metadata,
        country_series_metadata,
        observation_metadata,
        series_time_metadata
    )


def process_countries(df: pa.Table) -> pa.Table:
    """Extract country code mapping from WDICountry.csv"""
    if df is None:
        # Return empty table with schema if no data
        return pa.table({
            'country_code2': pa.array([], type=pa.string()),
            'country_code3': pa.array([], type=pa.string())
        })
    
    # Get relevant columns for mapping
    country_code2 = df.column('2-alpha code')
    country_code3 = df.column('Country Code')
    
    # Create mapping table
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
    
    # Get id columns
    country_name = df.column('Country Name')
    country_code3 = df.column('Country Code')
    indicator_name = df.column('Indicator Name')
    indicator_code = df.column('Indicator Code')
    
    # Collect year columns (all columns that are year values)
    year_columns = []
    year_names = []
    for i, field in enumerate(df.schema):
        if field.name not in ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code']:
            # Skip unnamed columns
            if not field.name.startswith('Unnamed'):
                year_columns.append(df.column(i))
                year_names.append(field.name)
    
    # Melt the data - create long format
    melted_data = []
    num_rows = len(df)
    
    for year_col, year_name in zip(year_columns, year_names):
        year_int = clean_year_str(year_name)
        
        # Create repeated columns for this year
        year_array = pa.array([year_int] * num_rows, type=pa.int32())
        
        # Build table for this year
        year_table = pa.table({
            'country_name': country_name,
            'country_code3': country_code3,
            'indicator_name': indicator_name,
            'indicator_code': indicator_code,
            'year': year_array,
            'value': year_col
        })
        
        # Filter out null values
        mask = pc.is_valid(year_table.column('value'))
        year_table = year_table.filter(mask)
        
        melted_data.append(year_table)
    
    # Combine all years
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
    
    # Join with country mapping to get 2-letter codes
    result = result.join(country_mapping, keys='country_code3', right_keys='country_code3')
    
    # Select and reorder columns
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


def process_wdi_series_metadata(df: pa.Table) -> pa.Table:
    """Process WDI series metadata"""
    if df is None:
        return pa.table({
            'indicator_code': pa.array([], type=pa.string()),
            'indicator_name': pa.array([], type=pa.string()),
            'short_definition': pa.array([], type=pa.string()),
            'long_definition': pa.array([], type=pa.string()),
            'unit_of_measure': pa.array([], type=pa.string()),
            'periodicity': pa.array([], type=pa.string()),
            'base_period': pa.array([], type=pa.string()),
            'other_notes': pa.array([], type=pa.string()),
            'aggregation_method': pa.array([], type=pa.string()),
            'limitations_and_exceptions': pa.array([], type=pa.string()),
            'notes_from_original_source': pa.array([], type=pa.string()),
            'general_comments': pa.array([], type=pa.string()),
            'source': pa.array([], type=pa.string()),
            'statistical_concept_and_methodology': pa.array([], type=pa.string()),
            'development_relevance': pa.array([], type=pa.string()),
            'license_type': pa.array([], type=pa.string())
        })
    
    # Map column names
    column_mapping = {
        'Series Code': 'indicator_code',
        'Indicator Name': 'indicator_name',
        'Short definition': 'short_definition',
        'Long definition': 'long_definition',
        'Unit of measure': 'unit_of_measure',
        'Periodicity': 'periodicity',
        'Base Period': 'base_period',
        'Other notes': 'other_notes',
        'Aggregation method': 'aggregation_method',
        'Limitations and exceptions': 'limitations_and_exceptions',
        'Notes from original source': 'notes_from_original_source',
        'General comments': 'general_comments',
        'Source': 'source',
        'Statistical concept and methodology': 'statistical_concept_and_methodology',
        'Development relevance': 'development_relevance',
        'License Type': 'license_type'
    }
    
    # Create result table with renamed columns
    result_dict = {}
    for old_name, new_name in column_mapping.items():
        if old_name in df.column_names:
            result_dict[new_name] = df.column(old_name)
        else:
            # Add empty column if not present
            result_dict[new_name] = pa.array([None] * len(df), type=pa.string())
    
    result = pa.table(result_dict)
    print(f"Processed {len(result)} series metadata records")
    return result


def process_wdi_country_series_metadata(df: pa.Table, country_mapping: pa.Table) -> pa.Table:
    """Process WDI country-series metadata"""
    if df is None:
        return pa.table({
            'country_code2': pa.array([], type=pa.string()),
            'indicator_code': pa.array([], type=pa.string()),
            'description': pa.array([], type=pa.string())
        })
    
    # Rename columns
    result = pa.table({
        'country_code3': df.column('CountryCode'),
        'indicator_code': df.column('SeriesCode'),
        'description': df.column('DESCRIPTION')
    })
    
    # Join with country mapping
    result = result.join(country_mapping, keys='country_code3', right_keys='country_code3')
    
    # Select final columns
    final_table = pa.table({
        'country_code2': result.column('country_code2'),
        'indicator_code': result.column('indicator_code'),
        'description': result.column('description')
    })
    
    print(f"Processed {len(final_table)} country-series metadata records")
    return final_table


def process_wdi_observation_metadata(df: pa.Table, country_mapping: pa.Table) -> pa.Table:
    """Process WDI observation/footnote metadata"""
    if df is None:
        return pa.table({
            'country_code2': pa.array([], type=pa.string()),
            'series_code': pa.array([], type=pa.string()),
            'year': pa.array([], type=pa.int32()),
            'description': pa.array([], type=pa.string())
        })
    
    # Process year column
    year_strings = df.column('Year').to_pylist()
    years = [clean_year_str(y) if y else None for y in year_strings]
    year_array = pa.array(years, type=pa.int32())
    
    # Create intermediate table
    result = pa.table({
        'country_code3': df.column('CountryCode'),
        'series_code': df.column('SeriesCode'),
        'year': year_array,
        'description': df.column('DESCRIPTION')
    })
    
    # Join with country mapping
    result = result.join(country_mapping, keys='country_code3', right_keys='country_code3')
    
    # Select final columns
    final_table = pa.table({
        'country_code2': result.column('country_code2'),
        'series_code': result.column('series_code'),
        'year': result.column('year'),
        'description': result.column('description')
    })
    
    print(f"Processed {len(final_table)} observation metadata records")
    return final_table


def process_wdi_series_time_metadata(df: pa.Table) -> pa.Table:
    """Process WDI series-time metadata"""
    if df is None:
        return pa.table({
            'indicator_code': pa.array([], type=pa.string()),
            'year': pa.array([], type=pa.int32()),
            'description': pa.array([], type=pa.string())
        })
    
    # Process year column
    year_strings = df.column('Year').to_pylist()
    years = [clean_year_str(y) if y else None for y in year_strings]
    year_array = pa.array(years, type=pa.int32())
    
    # Create result table
    result = pa.table({
        'indicator_code': df.column('SeriesCode'),
        'year': year_array,
        'description': df.column('DESCRIPTION')
    })
    
    print(f"Processed {len(result)} series-time metadata records")
    return result