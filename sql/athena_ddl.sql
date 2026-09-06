-- =====================================================================
-- DataFlip — AWS Glue Data Catalog DDL & Athena Query Definitions
-- =====================================================================

-- 1. Database Creation
CREATE DATABASE IF NOT EXISTS dataflip_db;

-- 2. Primary External Table for Curated Sales Data (Blue/Green Deployment)
-- Top-level StorageDescriptor.Location pointer is updated atomically via Boto3:
-- s3://${s3_bucket}/curated/blue/  <--->  s3://${s3_bucket}/curated/green/
CREATE EXTERNAL TABLE IF NOT EXISTS dataflip_db.sales_curated (
    order_id BIGINT,
    order_date DATE,
    product STRING,
    category STRING,
    quantity INT,
    unit_price DOUBLE,
    region STRING,
    revenue DOUBLE
)
STORED AS PARQUET
LOCATION 's3://${s3_bucket}/curated/blue/'
TBLPROPERTIES (
    'has_encrypted_data'='true',
    'parquet.compression'='SNAPPY'
);

-- =====================================================================
-- ARCHITECTURE NOTE: PARTITIONING & BLUE/GREEN TRADE-OFFS
-- =====================================================================
-- Option A (Default - Root Location Switch):
--   - Unpartitioned root table allows instant zero-downtime location switching
--     via a single `glue.update_table()` API call (<50ms execution).
--   - Best suited for snapshot analytical datasets under 500GB.
--
-- Option B (Multi-Terabyte Partition Projection Alternative):
--   - For multi-TB tables partitioned by region/date, use Athena Partition Projection
--     to dynamically project locations without executing expensive MSCK REPAIR TABLE commands.
--   - Example DDL Table Property for Partition Projection:
--     'projection.enabled' = 'true',
--     'projection.region.type' = 'enum',
--     'projection.region.values' = 'North,South,East,West'
-- =====================================================================

