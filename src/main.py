"""World Development Indicators Connector.

DAG-based orchestration for fetching and transforming WDI data from World Bank.
"""
from subsets_utils import DAG, validate_environment
from nodes import ingest, wdi_tables


workflow = DAG({
    ingest.run: [],
    wdi_tables.run: [ingest.run],
})


def main():
    validate_environment()
    workflow.run()


if __name__ == "__main__":
    main()
