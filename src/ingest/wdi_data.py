"""Fetch raw WDI data from World Bank."""

import io
import zipfile
from subsets_utils import get, save_raw_file

WDI_URL = "https://databank.worldbank.org/data/download/WDI_CSV.zip"


def run():
    """Download and extract World Development Indicators ZIP."""
    print("  Downloading World Development Indicators ZIP...")
    response = get(WDI_URL, timeout=300)
    response.raise_for_status()
    print(f"  Downloaded {len(response.content) / 1024 / 1024:.1f} MB")

    zip_bytes_io = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_bytes_io) as zip_ref:
        for file_info in zip_ref.filelist:
            if file_info.filename.endswith('.csv'):
                print(f"  Extracting {file_info.filename}")
                with zip_ref.open(file_info) as file:
                    csv_text = file.read().decode('utf-8')
                    asset_id = file_info.filename.replace('.csv', '').lower()
                    save_raw_file(csv_text, asset_id, extension="csv")
