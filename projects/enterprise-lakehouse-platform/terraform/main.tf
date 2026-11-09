resource "aws_s3_bucket" "workspace_root" {
  bucket_prefix = "${var.name_prefix}-workspace-root-"
  force_destroy = false

  tags = merge(var.tags, {
    Name    = "${var.name_prefix}-workspace-root"
    Purpose = "Databricks workspace root storage"
  })
}

resource "aws_s3_bucket_public_access_block" "workspace_root" {
  bucket                  = aws_s3_bucket.workspace_root.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "workspace_root" {
  bucket = aws_s3_bucket.workspace_root.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "workspace_root" {
  bucket = aws_s3_bucket.workspace_root.id
  versioning_configuration {
    status = "Enabled"
  }
}

data "aws_iam_policy_document" "workspace_root" {
  statement {
    sid    = "WorkspaceBucketMetadata"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = [var.databricks_cross_account_role_arn]
    }
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.workspace_root.arn]
  }

  statement {
    sid    = "WorkspaceRootObjects"
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = [var.databricks_cross_account_role_arn]
    }
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = ["${aws_s3_bucket.workspace_root.arn}/*"]
  }
}

resource "aws_s3_bucket_policy" "workspace_root" {
  bucket = aws_s3_bucket.workspace_root.id
  policy = data.aws_iam_policy_document.workspace_root.json
}

resource "aws_s3_bucket" "catalog" {
  bucket_prefix = "${var.name_prefix}-catalog-"
  force_destroy = false

  tags = merge(var.tags, {
    Name    = "${var.name_prefix}-catalog"
    Purpose = "Unity Catalog managed storage"
  })
}

resource "aws_s3_bucket_public_access_block" "catalog" {
  bucket                  = aws_s3_bucket.catalog.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "catalog" {
  bucket = aws_s3_bucket.catalog.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "catalog" {
  bucket = aws_s3_bucket.catalog.id
  versioning_configuration {
    status = "Enabled"
  }
}

data "aws_iam_policy_document" "catalog_access" {
  statement {
    sid    = "BucketMetadata"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.catalog.arn]
  }

  statement {
    sid    = "CatalogObjects"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = ["${aws_s3_bucket.catalog.arn}/unity-catalog/*"]
  }
}

resource "aws_iam_role_policy" "catalog_access" {
  name   = "${var.name_prefix}-unity-catalog-s3"
  role   = var.unity_catalog_role_name
  policy = data.aws_iam_policy_document.catalog_access.json
}

resource "databricks_mws_credentials" "workspace" {
  provider         = databricks.account
  credentials_name = "${var.name_prefix}-credentials"
  role_arn         = var.databricks_cross_account_role_arn
}

resource "databricks_mws_storage_configurations" "workspace" {
  provider                   = databricks.account
  storage_configuration_name = "${var.name_prefix}-root-storage"
  account_id                 = var.databricks_account_id

  root_bucket_info {
    bucket_name = aws_s3_bucket.workspace_root.bucket
  }

  depends_on = [
    aws_s3_bucket_public_access_block.workspace_root,
    aws_s3_bucket_server_side_encryption_configuration.workspace_root,
    aws_s3_bucket_policy.workspace_root,
  ]
}

resource "databricks_mws_workspaces" "this" {
  provider                   = databricks.account
  account_id                 = var.databricks_account_id
  workspace_name             = var.workspace_name
  aws_region                 = var.aws_region
  credentials_id             = databricks_mws_credentials.workspace.credentials_id
  storage_configuration_id   = databricks_mws_storage_configurations.workspace.storage_configuration_id
  pricing_tier               = var.pricing_tier
  depends_on                 = [aws_s3_bucket_public_access_block.workspace_root]
}

resource "databricks_metastore" "this" {
  provider     = databricks.account
  name         = "${var.name_prefix}-metastore"
  region       = var.aws_region
  storage_root = "s3://${aws_s3_bucket.catalog.bucket}/unity-catalog"
}

resource "databricks_metastore_data_access" "this" {
  provider     = databricks.account
  name         = "${var.name_prefix}-catalog-storage"
  metastore_id = databricks_metastore.this.id
  is_default   = true

  aws_iam_role {
    role_arn = var.unity_catalog_role_arn
  }

  depends_on = [
    aws_iam_role_policy.catalog_access,
    aws_s3_bucket_public_access_block.catalog,
    aws_s3_bucket_server_side_encryption_configuration.catalog,
  ]
}

resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  workspace_id = databricks_mws_workspaces.this.workspace_id
  metastore_id = databricks_metastore.this.id
}
