output "workspace_url" {
  description = "URL of the provisioned Databricks workspace."
  value       = databricks_mws_workspaces.this.workspace_url
}

output "workspace_id" {
  description = "ID of the provisioned Databricks workspace."
  value       = databricks_mws_workspaces.this.workspace_id
}

output "workspace_root_bucket" {
  description = "S3 bucket used for Databricks workspace root storage."
  value       = aws_s3_bucket.workspace_root.bucket
}

output "unity_catalog_bucket" {
  description = "S3 bucket used for Unity Catalog managed storage."
  value       = aws_s3_bucket.catalog.bucket
}

output "metastore_id" {
  description = "ID of the assigned Unity Catalog metastore."
  value       = databricks_metastore.this.id
}
