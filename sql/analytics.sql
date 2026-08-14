-- Query 1: Total Revenue
-- Calculates the sum of revenue across all orders.
SELECT 
    ROUND(SUM(revenue), 2) AS total_revenue,
    COUNT(order_id) AS total_orders
FROM read_parquet(?);

-- Query 2: Revenue by Category
-- Aggregates total revenue and order count by product category.
SELECT 
    category,
    ROUND(SUM(revenue), 2) AS category_revenue,
    COUNT(order_id) AS total_orders
FROM read_parquet(?)
GROUP BY category
ORDER BY category_revenue DESC;

-- Query 3: Revenue by Region
-- Aggregates total revenue and order count by region.
SELECT 
    region,
    ROUND(SUM(revenue), 2) AS region_revenue,
    COUNT(order_id) AS total_orders
FROM read_parquet(?)
GROUP BY region
ORDER BY region_revenue DESC;
