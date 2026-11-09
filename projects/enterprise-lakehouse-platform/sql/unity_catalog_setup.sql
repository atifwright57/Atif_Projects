-- Create the governed namespaces after Terraform has assigned a metastore.
CREATE CATALOG IF NOT EXISTS enterprise_lakehouse;

CREATE SCHEMA IF NOT EXISTS enterprise_lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS enterprise_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS enterprise_lakehouse.gold;

-- Group membership is managed centrally in the Databricks account.
GRANT USE CATALOG ON CATALOG enterprise_lakehouse TO `data_engineers`;
GRANT USE CATALOG ON CATALOG enterprise_lakehouse TO `data_analysts`;
GRANT USE CATALOG ON CATALOG enterprise_lakehouse TO `sales-east`;
GRANT USE CATALOG ON CATALOG enterprise_lakehouse TO `sales-west`;
GRANT USE CATALOG ON CATALOG enterprise_lakehouse TO `platform-admins`;

GRANT USE SCHEMA, CREATE TABLE ON SCHEMA enterprise_lakehouse.bronze
  TO `data_engineers`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA enterprise_lakehouse.silver
  TO `data_engineers`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA enterprise_lakehouse.gold
  TO `data_engineers`;

GRANT USE SCHEMA, SELECT ON SCHEMA enterprise_lakehouse.gold
  TO `data_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA enterprise_lakehouse.silver
  TO `sales-east`;
GRANT USE SCHEMA, SELECT ON SCHEMA enterprise_lakehouse.silver
  TO `sales-west`;
GRANT USE SCHEMA, SELECT ON SCHEMA enterprise_lakehouse.silver
  TO `platform-admins`;

-- Expose east-region rows only to the sales-east group, west rows only to
-- sales-west, and all rows to platform administrators.
CREATE OR REPLACE FUNCTION enterprise_lakehouse.silver.orders_region_filter(
  row_region STRING
)
RETURNS BOOLEAN
RETURN
  (is_account_group_member('sales-east') AND row_region = 'east')
  OR (is_account_group_member('sales-west') AND row_region = 'west')
  OR is_account_group_member('platform-admins');

ALTER TABLE IF EXISTS enterprise_lakehouse.silver.orders
  SET ROW FILTER enterprise_lakehouse.silver.orders_region_filter ON (region);
