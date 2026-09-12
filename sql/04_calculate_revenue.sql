CREATE OR REPLACE TABLE revenue_by_product AS
SELECT
    product_id,
    id_web,
    post_title,
    price,
    total_sales,
    ROUND(price * total_sales, 2) AS revenue
FROM merged_wines
ORDER BY revenue DESC, product_id;

CREATE OR REPLACE TABLE revenue_summary AS
SELECT
    COUNT(*) AS product_count,
    SUM(total_sales) AS bottles_sold,
    ROUND(SUM(price * total_sales), 2) AS total_revenue
FROM merged_wines;

