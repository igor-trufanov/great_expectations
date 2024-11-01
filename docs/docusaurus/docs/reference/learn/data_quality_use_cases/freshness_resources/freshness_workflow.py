"""
To run this test locally, use the postgresql database docker container.

1. From the repo root dir, run:
cd assets/docker/postgresql
docker compose up

2. Run the following command from the repo root dir in a second terminal:
pytest --postgresql --docs-tests -k "data_quality_use_case_integration_workflow" tests/integration/test_script_runner.py
"""

# <snippet name="docs/docusaurus/docs/reference/learn/data_quality_use_cases/freshness_resources/freshness_workflow.py full workflow">
import great_expectations as gx
import great_expectations.expectations as gxe
from datetime import datetime, timedelta

# Create Data Context.
context = gx.get_context()

# Connect to sample data, create Data Source and Data Asset.
CONNECTION_STRING = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_learn_data_quality"

data_source = context.data_sources.add_postgres(
    "postgres database", connection_string=CONNECTION_STRING
)
data_asset = data_source.add_table_asset(
    name="financial transfers table", table_name="freshness_financial_transfers"
)


# Define freshness thresholds
expected_min_timestamp = datetime(2024, 4, 30, 0, 0)
expected_max_timestamp = datetime(2024, 5, 2, 0, 0)

# Create Expectations testing the min and max values of the transfer_ts column
min_timestamp_expectation = gxe.ExpectColumnMinToBeBetween(
    column="transfer_ts",
    min_value=expected_min_timestamp,
    max_value=expected_max_timestamp - timedelta(days=1),
)
max_timestamp_expectation = gxe.ExpectColumnMaxToBeBetween(
    column="transfer_ts",
    min_value=expected_min_timestamp + timedelta(days=1),
    max_value=expected_max_timestamp,
)

results = data_asset.validate([min_timestamp_expectation, max_timestamp_expectation])
print(results)
# </snippet>
