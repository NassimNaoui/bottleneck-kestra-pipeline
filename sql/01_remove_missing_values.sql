-- On ne supprime que les lignes inexploitables pour les clés, le CA ou le libellé.
-- Les nombreuses colonnes web optionnelles peuvent légitimement rester NULL.

CREATE OR REPLACE TABLE erp_not_null AS
SELECT
    TRY_CAST(TRIM(product_id) AS BIGINT) AS product_id,
    TRY_CAST(TRIM(onsale_web) AS INTEGER) AS onsale_web,
    TRY_CAST(REPLACE(TRIM(price), ',', '.') AS DOUBLE) AS price,
    TRY_CAST(TRIM(stock_quantity) AS BIGINT) AS stock_quantity,
    NULLIF(TRIM(stock_status), '') AS stock_status
FROM raw_erp
WHERE TRY_CAST(TRIM(product_id) AS BIGINT) IS NOT NULL
  AND TRY_CAST(REPLACE(TRIM(price), ',', '.') AS DOUBLE) IS NOT NULL;

-- product_id est la clé primaire de la liaison. Un id_web vide reste conservé :
-- il sera naturellement exclu par l'INNER JOIN, mais doit rester comptabilisé
-- dans le contrôle du volume après dédoublonnage.
CREATE OR REPLACE TABLE liaison_valid AS
SELECT
    TRY_CAST(TRIM(product_id) AS BIGINT) AS product_id,
    NULLIF(TRIM(id_web), '') AS id_web
FROM raw_liaison
WHERE TRY_CAST(TRIM(product_id) AS BIGINT) IS NOT NULL;

CREATE OR REPLACE TABLE web_not_null AS
SELECT
    TRIM(sku) AS sku,
    TRY_CAST(TRIM(total_sales) AS BIGINT) AS total_sales,
    TRIM(post_title) AS post_title,
    LOWER(TRIM(post_type)) AS post_type,
    TRY_CAST(post_modified AS TIMESTAMP) AS post_modified,
    TRIM(guid) AS guid
FROM raw_web
WHERE NULLIF(TRIM(sku), '') IS NOT NULL
  AND TRY_CAST(TRIM(total_sales) AS BIGINT) IS NOT NULL
  AND NULLIF(TRIM(post_title), '') IS NOT NULL
  AND LOWER(TRIM(post_type)) IN ('product', 'attachment');
