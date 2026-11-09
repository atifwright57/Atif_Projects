# Data Quality & Observability Framework

This project defines an order data contract at both the file-ingestion and
transformation boundaries. Great Expectations validates a sample CSV, dbt
builds and tests a staging model locally with DuckDB, and a standard-library
monitor simulates freshness SLA and daily-volume anomaly alerts.

## Data contract

| Field | Contract |
|---|---|
| `order_id` | Required and unique |
| `customer_id` | Required |
| `order_ts` | Required, parseable timestamp |
| `status` | Required; `created`, `updated`, or `completed` |
| `amount` | Required and non-negative |

The sample data is in `dbt/seeds/orders.csv`. The same contract is checked by
the Great Expectations script before transformation and by dbt tests on the
`stg_orders` model.

## Install

Use Python 3.10-3.12 for the pinned Great Expectations and dbt versions.
From this project directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Run the checks

Run the standard-library unit tests first:

```powershell
python -m unittest discover -s tests -v
```

### Great Expectations

```powershell
python quality\great_expectations_check.py
```

The script loads the sample CSV into an ephemeral Great Expectations context,
adds not-null, uniqueness, accepted-value, and non-negative amount
expectations, validates the batch, prints a result summary, and exits non-zero
when validation fails. Check another CSV with `--input path\to\orders.csv`.

### dbt

The included DuckDB profile keeps this example self-contained. Copy the
non-secret example profile and run dbt from the project directory:

```powershell
Copy-Item dbt\profiles.yml.example dbt\profiles.yml
Set-Location dbt
dbt seed --profiles-dir .
dbt build --profiles-dir .
```

`dbt build` materializes `stg_orders` and runs the schema tests. The local
database is written under `dbt/target/portfolio.duckdb`; target artifacts,
logs, and the copied profile are ignored by Git. For other environments,
replace this local profile with a warehouse-specific profile outside source
control.

### SLA and anomaly simulation

```powershell
python monitoring\sla_anomaly_monitor.py
python monitoring\sla_anomaly_monitor.py --simulate-lag-minutes 90 --fail-on-alert
python monitoring\sla_anomaly_monitor.py --volume-override 40 --fail-on-alert
```

The monitor compares current CSV row volume with the median and median absolute
deviation (MAD) of the historical daily counts. A robust z-score over 3.5 is
an anomaly; if the history has zero MAD, any deviation from its median is
flagged. It also checks a simulated freshness lag against the 30-minute default
SLA. Alerts are emitted as JSON. `--fail-on-alert` returns exit code 2 so a
workflow can stop or route the alert to an incident channel.

## Quality operations

- Validate at ingestion and transformation boundaries so invalid data is
  detected before downstream reporting.
- Keep contract tests close to the data (`great_expectations_check.py`) and
  model definitions (`dbt/models/schema.yml`).
- Treat thresholds and historical baselines as versioned inputs; recalibrate
  them with owners when volume seasonality or SLA objectives change.
- A production deployment should send emitted alerts to its standard
  monitoring/incident system and retain validation results with run metadata.
