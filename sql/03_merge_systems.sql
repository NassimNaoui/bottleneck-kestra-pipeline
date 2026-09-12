CREATE OR REPLACE TABLE merged_wines AS
SELECT
    erp.product_id,
    liaison.id_web,
    web.sku,
    web.post_title,
    erp.onsale_web,
    erp.price,
    erp.stock_quantity,
    erp.stock_status,
    web.total_sales,
    web.post_modified,
    web.guid
FROM erp_clean AS erp
INNER JOIN liaison_clean AS liaison
    ON erp.product_id = liaison.product_id
INNER JOIN web_clean AS web
    ON liaison.id_web = web.sku;

