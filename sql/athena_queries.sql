-- =====================================================================
-- DataFlip — Amazon Athena Analytical Query Patterns
-- =====================================================================
-- Queries execute against dynamically registered datasets in `dataflip_db`.
-- Replace `<dataset_name>` with any deployed dataset (e.g. telemetry, customers).
-- =====================================================================

-- Query 1: Data Preview and Schema Inspection
SELECT * 
FROM dataflip_db.<dataset_name>
LIMIT 10;

-- Query 2: Dataset Volume & Completeness Audit
SELECT 
    COUNT(*) AS total_records
FROM dataflip_db.<dataset_name>;

-- Query 3: Multi-Dataset Catalog Discovery (Query Glue Information Schema)
SELECT 
    table_name,
    table_type
FROM information_schema.tables 
WHERE table_schema = 'dataflip_db'
ORDER BY table_name;

-- Query 4: Column and Data Type Inspection
SELECT 
    column_name, 
    data_type, 
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'dataflip_db' 
  AND table_name = '<dataset_name>'
ORDER BY ordinal_position;

-- Query 5: Example Analytics Pattern — Telemetry Metrics
-- (Applicable when <dataset_name> = 'telemetry')
-- SELECT 
--     metric_name,
--     COUNT(*) AS sample_count,
--     ROUND(AVG(value), 3) AS avg_value,
--     ROUND(MIN(value), 3) AS min_value,
--     ROUND(MAX(value), 3) AS max_value
-- FROM dataflip_db.telemetry
-- GROUP BY metric_name
-- ORDER BY sample_count DESC;

-- Query 6: Example Analytics Pattern — Customer Accounts
-- (Applicable when <dataset_name> = 'customers')
-- SELECT 
--     country,
--     COUNT(customer_id) AS total_customers,
--     SUM(account_balance) AS total_balance
-- FROM dataflip_db.customers
-- GROUP BY country
-- ORDER BY total_customers DESC;
