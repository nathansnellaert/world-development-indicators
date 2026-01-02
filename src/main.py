"""World Development Indicators Connector."""
import os
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

import argparse
from subsets_utils import validate_environment
from ingest import wdi_data as ingest_wdi
from transforms import wdi as transform_wdi


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_wdi.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        transform_wdi.run()


if __name__ == "__main__":
    main()
