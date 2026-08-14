-- Athena Analytics Query 1: Total Revenue across Active Dataset
SELECT 
    ROUND(SUM(revenue), 2) AS total_revenue,
    COUNT(order_id) AS total_orders
FROM dataflip_db.sales_curated;

-- Athena Analytics Query 2: Revenue by Product Category
SELECT 
    category,
    ROUND(SUM(revenue), 2) AS category_revenue,
    COUNT(order_id) AS total_orders
FROM dataflip_db.sales_curated
GROUP BY category
ORDER BY category_revenue DESC;

-- Athena Analytics Query 3: Revenue by Sales Region
SELECT 
    region,
    ROUND(SUM(revenue), 2) AS region_revenue,
    COUNT(order_id) AS total_orders
FROM dataflip_db.sales_curated
GROUP BY region
ORDER BY region_revenue DESC;
