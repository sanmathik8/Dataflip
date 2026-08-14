-- Database Creation
CREATE DATABASE IF NOT EXISTS dataflip_db;

-- Glue Catalog External Table for Curated Sales Data
-- Initial Location points to curated/blue/
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
TBLPROPERTIES ('has_encrypted_data'='false');
