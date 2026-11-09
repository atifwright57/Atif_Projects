# Enterprise Lakehouse Platform

An AWS and Databricks lakehouse reference implementation aligned to the
portfolio's platform, governance, and medallion-architecture themes. It
provisions S3 storage, a Databricks workspace, a Unity Catalog metastore, and
the catalog's S3 data access configuration. A local PySpark job demonstrates
append-only Bronze ingestion, deduplicated Silver records, and a rebuilt Gold
aggregate.

## Architecture

```text
JSON source
    |
    v
Bronze Delta (raw records + ingestion timestamp + source file)
    |
    v
Silver Delta (typed, validated, one latest record per event_id)
    |
    v
Gold Delta (daily order metrics by region and status)
```

Unity Catalog governs the tables and uses a separate S3 location for metastore
storage. The SQL script creates the catalog and schemas, grants least-privilege
schema access, and attaches a row filter to the Silver orders table.

## Prerequisites

- Terraform 1.5 or later, AWS CLI credentials, and an AWS account.
- A Databricks account with permission to create workspaces and Unity Catalog
  metastores.
- A Databricks account-level cross-account IAM role for workspace deployment.
  Supply its ARN as `databricks_cross_account_role_arn`. Create it using the
  current Databricks AWS account instructions for your region and account; its
  trust relationship and deployment permissions are account-specific.
- A separate IAM role for Unity Catalog S3 access. Its trust policy must allow
  Databricks to assume it using the external ID described in the Databricks
  Unity Catalog AWS setup guide. The Terraform applies the bucket-scoped S3
  access policy to this existing role.
- Account groups named `data_engineers`, `data_analysts`, `sales-east`,
  `sales-west`, and `platform-admins` before executing the SQL script.
- Python 3.10-3.12 and Java 8/11/17 for the local Spark example.

Do not grant Terraform's deployment identity broad access casually. Review the
Databricks-generated AWS role policy and the IAM role trust policies for your
account before applying. Configure a remote, encrypted Terraform state backend
with locking for shared or production deployments; this example leaves state
backend configuration to the operator.

## Configure and provision

From this directory:

```powershell
Copy-Item terraform\terraform.tfvars.example terraform\terraform.tfvars
# Edit terraform\terraform.tfvars with your account, region, and IAM role values.
$env:DATABRICKS_HOST = "https://accounts.cloud.databricks.com"
$env:DATABRICKS_ACCOUNT_ID = "<your-databricks-account-id>"
$env:DATABRICKS_CLIENT_ID = "<service-principal-client-id>"
$env:DATABRICKS_CLIENT_SECRET = "<service-principal-secret>"
```

Terraform's Databricks provider uses OAuth M2M environment variables and the
account host. AWS authentication should be configured using the AWS CLI profile
or standard AWS environment variables.

```powershell
terraform -chdir=terraform init
terraform -chdir=terraform validate
terraform -chdir=terraform plan -out=lakehouse.tfplan
terraform -chdir=terraform apply lakehouse.tfplan
terraform -chdir=terraform output workspace_url
```

Inspect the plan before applying. Applying creates billable AWS/Databricks
resources. Once the workspace and metastore exist, configure workspace groups
and execute `sql/unity_catalog_setup.sql` in a Databricks SQL editor. Replace
the example catalog name or group names first if they differ in your account.
The row-filter statement assumes that the Silver pipeline has already created
`enterprise_lakehouse.silver.orders`.

To destroy the resources after use, review the impact and then run
`terraform -chdir=terraform destroy`. The S3 buckets have `force_destroy`
disabled so they cannot be removed while they contain objects.

## Run the local Delta pipeline

From this project directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python src\medallion_pipeline.py
```

The sample source is `data/orders.json`. Delta output is written to
`data/delta/{bronze,silver,gold}`. Override the paths for another dataset:

```powershell
python src\medallion_pipeline.py --input path\to\orders.json --output-root path\to\delta
```

The input must be newline-delimited JSON with `event_id`, `customer_id`,
`region`, `amount`, `event_ts`, and `status` fields. Silver rejects rows with
missing required values, negative amounts, or unparseable timestamps. Re-running
the job appends a raw Bronze audit trail, merges Silver records by `event_id`,
and recomputes Gold from the current Silver table, so downstream output remains
stable when the same input is replayed.

## Key design choices

- **Bronze:** preserves source fields and records ingestion time and source
  filename; append-only to retain the raw ingestion history.
- **Silver:** casts and validates contract fields, then selects the latest
  event per business key and uses a Delta `MERGE` for repeatable updates.
- **Gold:** aggregates the complete current Silver snapshot and overwrites the
  aggregate Delta table atomically, avoiding stale aggregates after corrections.
- **Governance:** Unity Catalog owns the catalog, schemas, storage credential,
  and fine-grained row filter; analysts receive read access without write
  privileges.

## Repository layout

```text
terraform/   AWS S3 and Databricks account/workspace resources
src/         PySpark medallion pipeline
sql/         Unity Catalog setup, grants, and row filter
data/        Small newline-delimited JSON demo input
```
