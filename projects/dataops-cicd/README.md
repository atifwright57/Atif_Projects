# DataOps & CI/CD Framework

This project automates repeatable checks for the portfolio and packages the
Project 1 medallion job as a container.

## GitHub Actions

`.github/workflows/ci.yml` runs on pushes, pull requests, and manual dispatch:

1. Installs the pinned Project 3 dependencies, runs Python unit tests, and
   validates the sample data contract with Great Expectations.
2. Seeds the local DuckDB dbt profile, builds the dbt model, and runs its
   schema tests.
3. Initializes Terraform without a backend, validates the configuration, and
   creates a plan with refresh disabled. The CI plan uses the example variables
   and does not apply or refresh cloud infrastructure.

The workflow uses Python 3.12, matching the pinned Great Expectations/dbt
dependencies. Cloud deployment should be a separate protected workflow using
GitHub Environments, short-lived OIDC credentials, a remote encrypted Terraform
backend with locking, and an approval gate before `terraform apply`. Do not
store cloud credentials or production variable files in this repository.

## Build and run the container

Build from the repository root so the Dockerfile can copy the Project 1 source:

```powershell
docker build -t enterprise-lakehouse:local .
docker run --rm -v lakehouse-output:/app/data/delta enterprise-lakehouse:local
```

The image uses Python 3.12, Java 17, the Project 1 Delta/Spark requirements,
and runs as a non-root user. Spark resolves the Delta Lake package from Maven
when the container starts, so runtime network access is required. The named
Docker volume preserves the generated Bronze, Silver, and Gold Delta tables
between runs. The sample input is baked into the image; pass alternate
`--input` or `--output-root` arguments after the image name to override the
defaults.

## Local checks

Run the Python tests and local data-quality/dbt commands from the Project 3
directory as documented in its [README](../data-quality-observability/README.md).
Terraform configuration and provisioning instructions are documented in the
[Enterprise Lakehouse Platform README](../enterprise-lakehouse-platform/README.md).
