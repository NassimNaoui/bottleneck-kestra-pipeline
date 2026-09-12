-- ROW_NUMBER garantit une clé primaire unique et rend la règle de sélection explicite.

CREATE OR REPLACE TABLE erp_clean AS
SELECT product_id, onsale_web, price, stock_quantity, stock_status
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY onsale_web DESC NULLS LAST, stock_quantity DESC NULLS LAST
        ) AS row_rank
    FROM erp_not_null
)
WHERE row_rank = 1;

CREATE OR REPLACE TABLE liaison_clean AS
WITH ranked_product AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY id_web
        ) AS product_rank
    FROM liaison_valid
),
unique_product AS (
    SELECT product_id, id_web
    FROM ranked_product
    WHERE product_rank = 1
),
ranked_web_id AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_web
            ORDER BY product_id
        ) AS web_rank
    FROM unique_product
)
SELECT product_id, id_web
FROM ranked_web_id
WHERE id_web IS NULL OR web_rank = 1;

CREATE OR REPLACE TABLE web_clean AS
SELECT sku, total_sales, post_title, post_type, post_modified, guid
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sku
            ORDER BY
                CASE WHEN post_type = 'product' THEN 0 ELSE 1 END,
                post_modified DESC NULLS LAST,
                post_title
        ) AS row_rank
    FROM web_not_null
)
WHERE row_rank = 1;
