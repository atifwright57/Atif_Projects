"""Run a small, replayable Bronze/Silver/Gold Delta Lake pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


REQUIRED_COLUMNS = {
    "event_id",
    "customer_id",
    "region",
    "amount",
    "event_ts",
    "status",
}


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("enterprise-lakehouse-medallion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def merge_by_event_id(source: DataFrame, target_path: str) -> None:
    if not DeltaTable.isDeltaTable(source.sparkSession, target_path):
        source.write.format("delta").mode("overwrite").save(target_path)
        return

    target = DeltaTable.forPath(source.sparkSession, target_path)
    target.alias("target").merge(
        source.alias("source"), "target.event_id = source.event_id"
    ).whenMatchedUpdateAll(
        condition=(
            "source.event_ts > target.event_ts OR "
            "(source.event_ts = target.event_ts AND "
            "source._ingested_at > target._ingested_at)"
        )
    ).whenNotMatchedInsertAll().execute()


def run_pipeline(input_path: str, output_root: str) -> None:
    spark = build_spark()
    try:
        source = (
            spark.read.option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .json(input_path)
        )
        missing_columns = REQUIRED_COLUMNS.difference(source.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Input is missing required columns: {missing}")

        bronze_path = f"{output_root.rstrip('/')}/bronze"
        silver_path = f"{output_root.rstrip('/')}/silver"
        gold_path = f"{output_root.rstrip('/')}/gold"

        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
        )
        bronze.write.format("delta").mode("append").save(bronze_path)

        typed = (
            spark.read.format("delta")
            .load(bronze_path)
            .withColumn("event_id", F.trim(F.col("event_id").cast("string")))
            .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
            .withColumn("region", F.lower(F.trim(F.col("region").cast("string"))))
            .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
            .withColumn("event_ts", F.to_timestamp("event_ts"))
            .withColumn("status", F.lower(F.trim(F.col("status").cast("string"))))
            .filter(
                F.col("event_id").isNotNull()
                & (F.col("event_id") != "")
                & F.col("customer_id").isNotNull()
                & (F.col("customer_id") != "")
                & F.col("region").isNotNull()
                & (F.col("region") != "")
                & F.col("amount").isNotNull()
                & (F.col("amount") >= 0)
                & F.col("event_ts").isNotNull()
                & F.col("status").isNotNull()
                & (F.col("status") != "")
            )
        )
        latest_event = Window.partitionBy("event_id").orderBy(
            F.col("event_ts").desc(),
            F.col("_ingested_at").desc(),
            F.col("_source_file").desc(),
            F.sha2(
                F.to_json(
                    F.struct(
                        "customer_id",
                        "region",
                        "amount",
                        "event_ts",
                        "status",
                    )
                ),
                256,
            ).desc(),
        )
        silver = (
            typed.withColumn("_row_number", F.row_number().over(latest_event))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )
        merge_by_event_id(silver, silver_path)

        current_silver = spark.read.format("delta").load(silver_path)
        gold = (
            current_silver.withColumn("order_date", F.to_date("event_ts"))
            .groupBy("order_date", "region", "status")
            .agg(
                F.countDistinct("event_id").alias("order_count"),
                F.sum("amount").alias("gross_amount"),
            )
            .withColumn("_refreshed_at", F.current_timestamp())
        )
        gold.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(gold_path)

        print(
            "Pipeline complete: "
            f"Bronze={bronze_path}, Silver={silver_path}, Gold={gold_path}"
        )
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=str(project_root / "data" / "orders.json"),
        help="Path to newline-delimited JSON input",
    )
    parser.add_argument(
        "--output-root",
        default=str(project_root / "data" / "delta"),
        help="Directory under which Bronze, Silver, and Gold Delta tables are saved",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_pipeline(arguments.input, arguments.output_root)
