"""Fetch raw WDI data from World Bank.

Downloads the full WDI dataset ZIP and extracts CSVs, converting them to parquet
for efficient DuckDB querying. This is a full-refresh connector - runs download
the complete current dataset.
"""
import io
import zipfile
import pyarrow as pa
import pyarrow.csv as pv
from subsets_utils import get, save_raw_parquet, load_state, save_state

WDI_URL = "https://databank.worldbank.org/data/download/WDI_CSV.zip"


def run():
    """Download and extract World Development Indicators ZIP."""
    print("Downloading World Development Indicators ZIP...")
    response = get(WDI_URL, timeout=300)
    response.raise_for_status()
    print(f"  Downloaded {len(response.content) / 1024 / 1024:.1f} MB")

    zip_bytes_io = io.BytesIO(response.content)
    fetched_assets = []

    with zipfile.ZipFile(zip_bytes_io) as zip_ref:
        for file_info in zip_ref.filelist:
            if file_info.filename.endswith('.csv'):
                print(f"  Extracting {file_info.filename}")
                with zip_ref.open(file_info) as file:
                    csv_bytes = file.read()
                    # Parse CSV to PyArrow table
                    csv_stream = io.BytesIO(csv_bytes)
                    table = pv.read_csv(
                        csv_stream,
                        parse_options=pv.ParseOptions(
                            invalid_row_handler=lambda _: 'skip',
                            newlines_in_values=True
                        )
                    )
                    asset_id = file_info.filename.replace('.csv', '').lower()
                    save_raw_parquet(table, asset_id)
                    fetched_assets.append(asset_id)
                    print(f"    -> Saved {asset_id}.parquet ({len(table):,} rows)")

    save_state("ingest", {"fetched_assets": fetched_assets})
    print(f"  Complete! Extracted {len(fetched_assets)} files")
