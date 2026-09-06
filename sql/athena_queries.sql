-- Athena Analytics Query 1: Total Revenue across Active Dataset
SELECT 
    ROUND(SUM(revenue), 2) AS total_revenue,
    COUNT(order_id) AS total_orders,
    ROUND(AVG(revenue), 2) AS average_order_value
FROM dataflip_db.sales_curated;

-- Athena Analytics Query 2: Revenue by Product Category
SELECT 
    category,
    ROUND(SUM(revenue), 2) AS category_revenue,
    COUNT(order_id) AS total_orders,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM dataflip_db.sales_curated
GROUP BY category
ORDER BY category_revenue DESC;

-- Athena Analytics Query 3: Regional Sales Performance & Market Share
SELECT 
    region,
    ROUND(SUM(revenue), 2) AS region_revenue,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(revenue) * 100.0 / SUM(SUM(revenue)) OVER (), 2) AS revenue_share_pct
FROM dataflip_db.sales_curated
GROUP BY region
ORDER BY region_revenue DESC;

-- Athena Analytics Query 4: Top Performing Products by Total Revenue & Quantity Sold
SELECT 
    product,
    category,
    SUM(quantity) AS total_quantity_sold,
    ROUND(SUM(revenue), 2) AS product_revenue
FROM dataflip_db.sales_curated
GROUP BY product, category
ORDER BY product_revenue DESC
LIMIT 10;

-- Athena Analytics Query 5: Daily Revenue & Order Volume Trends
SELECT 
    order_date,
    COUNT(order_id) AS daily_orders,
    ROUND(SUM(revenue), 2) AS daily_revenue
FROM dataflip_db.sales_curated
GROUP BY order_date
ORDER BY order_date ASC;

-- Athena Analytics Query 6: Data Quality Audit Verification Snapshot
SELECT 
    COUNT(*) AS total_rows,
    COUNT(order_id) AS non_null_orders,
    COUNT(order_date) AS non_null_dates,
    MIN(quantity) AS min_quantity,
    MIN(unit_price) AS min_unit_price,
    MIN(revenue) AS min_revenue
FROM dataflip_db.sales_curated;

