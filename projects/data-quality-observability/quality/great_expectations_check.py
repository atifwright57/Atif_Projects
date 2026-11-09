"""Validate the orders CSV against a Great Expectations data contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd


REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "order_ts",
    "status",
    "amount",
}
SUITE_NAME = "orders_contract"


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=str(project_root / "dbt" / "seeds" / "orders.csv"),
        help="CSV file to validate",
    )
    return parser.parse_args()


def validate_orders(input_path: str) -> bool:
    dataframe = pd.read_csv(input_path)
    missing_columns = REQUIRED_COLUMNS.difference(dataframe.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Input is missing required columns: {missing}")

    context = gx.get_context(mode="ephemeral")
    data_source = context.sources.add_pandas(name="orders_source")
    data_asset = data_source.add_dataframe_asset(name="orders")
    batch_request = data_asset.build_batch_request(dataframe=dataframe)
    context.add_or_update_expectation_suite(
        expectation_suite_name=SUITE_NAME,
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=SUITE_NAME,
    )
    validator.expect_column_values_to_not_be_null(column="order_id")
    validator.expect_column_values_to_be_unique(column="order_id")
    validator.expect_column_values_to_not_be_null(column="customer_id")
    validator.expect_column_values_to_not_be_null(column="order_ts")
    validator.expect_column_values_to_match_strftime_format(
        column="order_ts",
        strftime_format="%Y-%m-%d %H:%M:%S",
    )
    validator.expect_column_values_to_not_be_null(column="status")
    validator.expect_column_values_to_be_in_set(
        column="status",
        value_set=["created", "updated", "completed"],
    )
    validator.expect_column_values_to_not_be_null(column="amount")
    validator.expect_column_values_to_be_between(
        column="amount",
        min_value=0,
    )

    result = validator.validate()
    print(
        json.dumps(
            {
                "suite": SUITE_NAME,
                "success": result.success,
                "statistics": result.statistics,
            },
            indent=2,
            default=str,
        )
    )
    return bool(result.success)


if __name__ == "__main__":
    arguments = parse_args()
    sys.exit(0 if validate_orders(arguments.input) else 1)
