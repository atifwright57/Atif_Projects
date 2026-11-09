"""Consume Kafka events and idempotently merge them into Delta Lake."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", DecimalType(18, 2), True),
    ]
)


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("kafka-to-delta-stream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"],
    ).getOrCreate()


def merge_insert_only(batch: DataFrame, path: str, key: str) -> None:
    if not batch.take(1):
        return
    if not DeltaTable.isDeltaTable(batch.sparkSession, path):
        batch.write.format("delta").mode("append").save(path)
        return

    target = DeltaTable.forPath(batch.sparkSession, path)
    condition = f"target.{key} = source.{key}"
    merge = target.alias("target").merge(batch.alias("source"), condition)
    if key == "event_id":
        merge.whenMatchedUpdateAll(
            condition="source.event_ts > target.event_ts"
        ).whenNotMatchedInsertAll().execute()
    else:
        merge.whenNotMatchedInsertAll().execute()


def make_batch_writer(events_path: str, quarantine_path: str):
    def write_batch(batch: DataFrame, batch_id: int) -> None:
        kafka_id = F.concat_ws(
            ":",
            F.col("topic"),
            F.col("partition").cast("string"),
            F.col("offset").cast("string"),
        )
        invalid = (
            batch.filter(F.col("_invalid_reason") != "")
            .select(
                kafka_id.alias("_kafka_id"),
                "raw_value",
                F.col("_invalid_reason").alias("reason"),
                F.col("kafka_timestamp").alias("received_at"),
            )
        )
        merge_insert_only(invalid, quarantine_path, "_kafka_id")

        valid = (
            batch.filter(F.col("_invalid_reason") == "")
            .select(
                "event_id",
                "customer_id",
                "event_ts",
                "event_type",
                "amount",
                "topic",
                "partition",
                "offset",
                F.current_timestamp().alias("_ingested_at"),
            )
            .dropDuplicates(["event_id"])
        )
        merge_insert_only(valid, events_path, "event_id")

        print(f"Processed streaming micro-batch {batch_id}")

    return write_batch


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "business-events"))
    parser.add_argument(
        "--events-path",
        default=str(project_root / "data" / "delta" / "events"),
    )
    parser.add_argument(
        "--quarantine-path",
        default=str(project_root / "data" / "delta" / "quarantine"),
    )
    parser.add_argument(
        "--checkpoint",
        default=str(project_root / "data" / "checkpoints" / "kafka-events"),
    )
    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default="earliest",
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=1000,
    )
    parser.add_argument("--watermark-delay", default="10 minutes")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.max_offsets_per_trigger < 1:
        raise ValueError("--max-offsets-per-trigger must be greater than zero")

    spark = build_spark()
    try:
        kafka = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
            .load()
        )
        decoded = kafka.select(
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("raw_value"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        )
        event = (
            decoded.select(
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
                "raw_value",
                F.trim(F.col("event.event_id")).alias("event_id"),
                F.trim(F.col("event.customer_id")).alias("customer_id"),
                F.col("event.event_ts").alias("event_ts"),
                F.trim(F.col("event.event_type")).alias("event_type"),
                F.col("event.amount").alias("amount"),
            )
            .withColumn(
                "_invalid_reason",
                F.concat_ws(
                    "; ",
                    F.when(
                        F.col("event_id").isNull() | (F.col("event_id") == ""),
                        "missing event_id",
                    ),
                    F.when(
                        F.col("customer_id").isNull() | (F.col("customer_id") == ""),
                        "missing customer_id",
                    ),
                    F.when(
                        F.col("event_ts").isNull(),
                        "missing or invalid event_ts",
                    ),
                    F.when(
                        F.col("event_type").isNull() | (F.col("event_type") == ""),
                        "missing event_type",
                    ),
                    F.when(
                        F.col("amount").isNull(),
                        "missing or invalid amount",
                    ),
                    F.when(
                        F.col("amount") < 0,
                        "negative amount",
                    ),
                ),
            )
            .withColumn("_event_time", F.coalesce("event_ts", "kafka_timestamp"))
            .withColumn(
                "_dedupe_key",
                F.when(
                    F.col("_invalid_reason") == "",
                    F.col("event_id"),
                ).otherwise(
                    F.concat_ws(
                        ":",
                        F.lit("invalid"),
                        F.col("topic"),
                        F.col("partition").cast("string"),
                        F.col("offset").cast("string"),
                    )
                ),
            )
        )

        stream = (
            event.withWatermark("_event_time", args.watermark_delay)
            .dropDuplicatesWithinWatermark(["_dedupe_key"])
        )
        query = (
            stream.writeStream.foreachBatch(
                make_batch_writer(args.events_path, args.quarantine_path)
            )
            .option("checkpointLocation", args.checkpoint)
            .queryName("kafka_business_events_to_delta")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
