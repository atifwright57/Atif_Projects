variable "name_prefix" {
  description = "Lowercase prefix used to name AWS and Databricks resources."
  type        = string
  default     = "portfolio-lakehouse"

  validation {
    condition     = can(regex("^[a-z0-9-]{3,30}$", var.name_prefix))
    error_message = "name_prefix must contain 3-30 lowercase letters, digits, or hyphens."
  }
}

variable "workspace_name" {
  description = "Name for the Databricks workspace."
  type        = string
  default     = "portfolio-lakehouse"
}

variable "aws_region" {
  description = "AWS region used for the workspace and Unity Catalog metastore."
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account UUID."
  type        = string
}

variable "databricks_cross_account_role_arn" {
  description = "Pre-created Databricks AWS cross-account role ARN for workspace deployment."
  type        = string
}

variable "unity_catalog_role_arn" {
  description = "Pre-created IAM role ARN trusted by Databricks for Unity Catalog S3 access."
  type        = string
}

variable "unity_catalog_role_name" {
  description = "Name of the existing Unity Catalog IAM role to which the bucket policy is attached."
  type        = string
}

variable "pricing_tier" {
  description = "Databricks workspace pricing tier."
  type        = string
  default     = "PREMIUM"

  validation {
    condition     = contains(["STANDARD", "PREMIUM", "ENTERPRISE"], var.pricing_tier)
    error_message = "pricing_tier must be STANDARD, PREMIUM, or ENTERPRISE."
  }
}

variable "tags" {
  description = "Additional tags applied to AWS resources."
  type        = map(string)
  default     = {}
}
