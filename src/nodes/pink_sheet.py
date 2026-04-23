"""World Bank Pink Sheet - monthly commodity prices and indices.

Downloads the World Bank's commodity price dataset ("Pink Sheet") which provides
monthly prices for ~70 commodities and ~17 composite price indices from 1960.
"""
import re
import io

import pandas as pd
import pyarrow as pa
from subsets_utils import (
    get, save_raw_file, load_raw_file,
    merge, publish, validate,
    load_state, save_state, data_hash,
)
from connector_utils import LICENSE

MONTHLY_URL = "https://thedocs.worldbank.org/en/doc/74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Historical-Data-Monthly.xlsx"

PRICES_ID = "wdi_commodity_prices_monthly"
INDICES_ID = "wdi_commodity_price_indices_monthly"

DATE_PATTERN = re.compile(r"(\d{4})M(\d{2})")


def _clean_name(name: str) -> str:
    return re.sub(r"\s*\*+\s*$", "", name).strip()


def _to_snake_case(name: str) -> str:
    name = _clean_name(name)
    name = re.sub(r"\s*\(.*?\)\s*", " ", name)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name


def _parse_sheet(content: bytes, sheet_name: str) -> tuple[pa.Table, dict]:
    """Parse a Pink Sheet data sheet into a wide PyArrow table.

    Handles both single-row headers (prices) and multi-row hierarchical
    headers (indices). Detects units rows by checking for $/unit patterns.
    """
    df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=None)

    data_start = None
    for i in range(len(df)):
        val = str(df.iloc[i, 0]).strip()
        if DATE_PATTERN.match(val):
            data_start = i
            break
    if data_start is None:
        raise ValueError(f"No date pattern found in sheet '{sheet_name}'")

    # Find where headers begin (first row with >=2 non-empty cells beyond col 0)
    header_start = data_start
    for i in range(data_start):
        non_empty = sum(
            1 for j in range(1, len(df.columns))
            if pd.notna(df.iloc[i, j]) and str(df.iloc[i, j]).strip()
        )
        if non_empty >= 2:
            header_start = i
            break

    # Detect if the last header row is a units row (values contain $ or /)
    units_row = None
    last_header = data_start - 1
    if last_header >= header_start:
        sample = [
            str(df.iloc[last_header, j]).strip()
            for j in range(1, min(len(df.columns), 20))
            if pd.notna(df.iloc[last_header, j]) and str(df.iloc[last_header, j]).strip()
        ]
        if sample:
            unit_like = sum(1 for v in sample if "$" in v or "/" in v)
            if unit_like > len(sample) * 0.5:
                units_row = last_header

    name_end = units_row if units_row is not None else data_start

    commodities = []
    col_descriptions = {
        "year": "Observation year",
        "month": "Observation month (1-12)",
    }
    seen_names = {}

    for col_idx in range(1, len(df.columns)):
        # Take the deepest (last) non-empty name across all header rows
        name = None
        for i in range(header_start, name_end):
            val = df.iloc[i, col_idx]
            if pd.notna(val) and str(val).strip():
                name = str(val).strip()
        if not name:
            continue
        name = _clean_name(name)

        snake = _to_snake_case(name)
        if not snake:
            continue

        if snake in seen_names:
            seen_names[snake] += 1
            snake = f"{snake}_{seen_names[snake]}"
        else:
            seen_names[snake] = 0

        unit_str = ""
        if units_row is not None:
            unit = df.iloc[units_row, col_idx]
            if pd.notna(unit):
                unit_str = str(unit).strip()

        commodities.append((col_idx, name, snake))
        if unit_str:
            desc = f"{name} {unit_str}" if unit_str.startswith("(") else f"{name} ({unit_str})"
        else:
            desc = name
        col_descriptions[snake] = desc

    records = []
    for i in range(data_start, len(df)):
        date_val = str(df.iloc[i, 0]).strip()
        m = DATE_PATTERN.match(date_val)
        if not m:
            continue

        row = {"year": int(m.group(1)), "month": int(m.group(2))}
        has_value = False

        for col_idx, _, snake in commodities:
            val = df.iloc[i, col_idx]
            if pd.isna(val):
                row[snake] = None
                continue
            val_str = str(val).strip()
            if val_str in ("...", "…", "", "-", ".."):
                row[snake] = None
                continue
            try:
                row[snake] = float(val)
                has_value = True
            except (ValueError, TypeError):
                row[snake] = None

        if has_value:
            records.append(row)

    return pa.Table.from_pylist(records), col_descriptions


def download():
    """Download the Pink Sheet Excel file."""
    print("Downloading World Bank Pink Sheet (monthly)...")
    response = get(MONTHLY_URL, timeout=120)
    response.raise_for_status()
    save_raw_file(response.content, "pink_sheet_monthly", extension="xlsx")


def transform():
    """Parse Pink Sheet Excel and publish commodity price tables."""
    content = load_raw_file("pink_sheet_monthly", extension="xlsx", binary=True)

    print("Parsing Monthly Prices...")
    prices_table, price_descs = _parse_sheet(content, "Monthly Prices")
    print(f"  {len(prices_table):,} rows x {len(prices_table.schema)} columns")

    validate(prices_table, {
        "columns": {"year": "int", "month": "int"},
        "not_null": ["year", "month"],
        "min_rows": 100,
    })

    h = data_hash(prices_table)
    if load_state(PRICES_ID).get("hash") != h:
        merge(prices_table, PRICES_ID, key=["year", "month"])
        publish(PRICES_ID, {
            "id": PRICES_ID,
            "title": "World Bank Commodity Prices (Monthly)",
            "description": "Monthly commodity prices from the World Bank Pink Sheet. Covers ~70 commodities including energy, metals, agriculture, and fertilizers from 1960 to present. Prices in nominal US dollars.",
            "license": LICENSE,
            "column_descriptions": price_descs,
        })
        save_state(PRICES_ID, {"hash": h})
        print(f"  -> Published {PRICES_ID}")
    else:
        print(f"  Skipping {PRICES_ID} - unchanged")

    print("Parsing Monthly Indices...")
    indices_table, index_descs = _parse_sheet(content, "Monthly Indices")
    print(f"  {len(indices_table):,} rows x {len(indices_table.schema)} columns")

    validate(indices_table, {
        "columns": {"year": "int", "month": "int"},
        "not_null": ["year", "month"],
        "min_rows": 100,
    })

    h = data_hash(indices_table)
    if load_state(INDICES_ID).get("hash") != h:
        merge(indices_table, INDICES_ID, key=["year", "month"])
        publish(INDICES_ID, {
            "id": INDICES_ID,
            "title": "World Bank Commodity Price Indices (Monthly)",
            "description": "Monthly commodity price indices from the World Bank Pink Sheet. Composite indices (2010=100) covering energy, non-energy, agriculture, metals, and other commodity groups.",
            "license": LICENSE,
            "column_descriptions": index_descs,
        })
        save_state(INDICES_ID, {"hash": h})
        print(f"  -> Published {INDICES_ID}")
    else:
        print(f"  Skipping {INDICES_ID} - unchanged")


NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
