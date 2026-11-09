# Atif_Projects — Data Engineering Portfolio

This repository presents production-oriented data engineering patterns using
Databricks, Delta Lake, PySpark, Terraform, Kafka, dbt, and data-quality tools.

## Projects

1. [Enterprise Lakehouse Platform](./projects/enterprise-lakehouse-platform/README.md) —
   AWS S3, Databricks, Unity Catalog, and a runnable Bronze/Silver/Gold pipeline.
2. [Streaming Ingestion Platform](./projects/streaming-ingestion-platform/README.md) —
   Kafka event simulation and checkpointed, deduplicated Delta ingestion.
3. [Data Quality & Observability Framework](./projects/data-quality-observability/README.md) —
   Great Expectations, dbt tests, and simulated SLA/volume alerts.
4. [DataOps & CI/CD Framework](./projects/dataops-cicd/README.md) —
   GitHub Actions checks and a containerized local data pipeline.

## Getting started

Each project documents its own prerequisites, configuration, and run commands.
Cloud infrastructure is not created until you explicitly run Terraform.
