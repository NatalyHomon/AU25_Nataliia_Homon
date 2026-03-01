-- =========================================================
--  LOGGING
-- =========================================================

SELECT run_id, log_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
ORDER BY log_dts desc, log_id DESC;

-- =========================================================
--  STAGING AREA schemas sa_sales_pos, sa_sales_online
-- =========================================================

--logging
SELECT run_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.prc_load_sa_sales_online_src'
 or procedure_name = 'bl_cl.prc_load_sa_sales_pos_src'
ORDER BY log_dts DESC;

--how many rows where inserted
SELECT 'pos' AS source, count(*)
FROM sa_sales_pos.src_sales_pos
WHERE load_dts = (
    SELECT max(load_dts)
    FROM sa_sales_pos.src_sales_pos
)

UNION ALL

SELECT 'online' AS source, count(*)
FROM sa_sales_online.src_sales_online
WHERE load_dts = (
    SELECT max(load_dts)
    FROM sa_sales_online.src_sales_online
);

--dublicates in Staging layer
SELECT ckout, product_sku, COUNT(*)
FROM sa_sales_pos.src_sales_pos
GROUP BY ckout, product_sku
HAVING COUNT(*) > 1;

SELECT web_order_id, product_sku, COUNT(*)
FROM sa_sales_online.src_sales_online
GROUP BY web_order_id, product_sku
HAVING COUNT(*) > 1;

-- =========================================================
-- MAPPING COUNTRIES 
-- =========================================================

SELECT run_id, log_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name='bl_cl.pr_load_map_countries'
ORDER BY log_id DESC;

SELECT* FROM bl_cl.t_map_countries;

--alias table
SELECT * FROM bl_cl.t_country_aliases;

-- =========================================================
-- 3NF layer
-- =========================================================

--spam check CE_BRANDS

SELECT brand, load_dts
FROM sa_sales_online.src_sales_online
WHERE brand IS NULL
   OR btrim(brand) = ''
   OR NOT bl_cl.fn_is_brand_name_valid(btrim(brand))
ORDER BY load_dts DESC
LIMIT 50;

SELECT brand, load_dts
FROM sa_sales_pos.src_sales_pos
WHERE brand IS NULL
   OR btrim(brand) = ''
   OR NOT bl_cl.fn_is_brand_name_valid(btrim(brand))
ORDER BY load_dts DESC
LIMIT 50;

SELECT* FROM bl_3nf.ce_brands;

SELECT run_id, log_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name='pr_load_ce_brands'
ORDER BY log_id DESC;


Select * FROM bl_cl.mta_load_control;


--how many inserted

SELECT 'bl_3nf.ce_products'  AS table_name,
       COUNT(*) FILTER (WHERE ta_insert_dt >= date_trunc('day', now())) AS inserted_today,
       COUNT(*) FILTER (WHERE ta_update_dt >= date_trunc('day', now())) AS updated_today
FROM bl_3nf.ce_products

UNION ALL
SELECT 'bl_3nf.ce_stores',
       COUNT(*) FILTER (WHERE ta_insert_dt >= date_trunc('day', now())),
       COUNT(*) FILTER (WHERE ta_update_dt >= date_trunc('day', now()))
FROM bl_3nf.ce_stores

UNION ALL
SELECT 'bl_3nf.ce_employees',
       COUNT(*) FILTER (WHERE ta_insert_dt >= date_trunc('day', now())),
       COUNT(*) FILTER (WHERE ta_update_dt >= date_trunc('day', now()))
FROM bl_3nf.ce_employees
ORDER BY table_name;

--dublicates
SELECT product_sku_src_id, source_system, source_entity, COUNT(*) cnt
FROM bl_3nf.ce_products
GROUP BY 1,2,3
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

SELECT store_src_id, source_system, source_entity, COUNT(*) cnt
FROM bl_3nf.ce_stores
GROUP BY 1,2,3
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

SELECT employee_src_id, source_system, source_entity, COUNT(*) cnt
FROM bl_3nf.ce_employees
GROUP BY 1,2,3
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

--default row
SELECT COUNT(*) AS unknown_rows
FROM bl_3nf.ce_stores
WHERE store_id = -1;

SELECT COUNT(*) AS unknown_rows
FROM bl_3nf.ce_products
WHERE product_id = -1;

SELECT COUNT(*) AS unknown_rows
FROM bl_3nf.ce_employees
WHERE employee_id = -1;

-- =========================================================
-- CE_TRANSACTIONS
-- =========================================================
SELECT run_id, log_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name='bl_cl.pr_load_ce_transactions'
ORDER BY log_id DESC;

SELECT txn_src_id, source_system, source_entity, COUNT(*) cnt
FROM bl_3nf.ce_transactions
GROUP BY 1,2,3
HAVING COUNT(*) > 1;

SELECT* FROM bl_3nf.ce_transactions;
SELECT COUNT(*) FROM bl_3nf.ce_transactions;

--To verify completeness of the ETL load from the Source Area (SA) to the 3NF layer by identifying business keys (transactions) that exist in SA but are missing in the target ce_transactions table.
WITH sa_bk AS (
  SELECT COALESCE(web_order_id,'n. a.') AS txn_src_id,
         'sa_sales_online' AS source_system,
         'src_sales_online' AS source_entity
  FROM sa_sales_online.src_sales_online
  WHERE web_order_id IS NOT NULL

  UNION

  SELECT COALESCE(ckout,'n. a.') AS txn_src_id,
         'sa_sales_pos' AS source_system,
         'src_sales_pos' AS source_entity
  FROM sa_sales_pos.src_sales_pos
  WHERE ckout IS NOT NULL
)
SELECT s.*
FROM sa_bk s
LEFT JOIN bl_3nf.ce_transactions t
  ON t.txn_src_id = s.txn_src_id
 AND t.source_system = s.source_system
 AND t.source_entity = s.source_entity
WHERE t.txn_id IS NULL;

-- =========================================================
-- CE_CUSTOMERS_SCD
-- =========================================================
CALL bl_cl.pr_load_ce_customers_scd();

SELECT run_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_customers_scd'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_customers_scd';

SELECT COUNT(*) FROM bl_3nf.ce_customers_scd;
SELECT * FROM bl_3nf.ce_customers_scd;


--inserted/updated
SELECT
  COUNT(*) FILTER (WHERE ta_insert_dt >= date_trunc('day', now())) AS inserted_today,
  COUNT(*) FILTER (WHERE ta_update_dt >= date_trunc('day', now())
                   AND ta_insert_dt < date_trunc('day', now()))     AS updated_today_only,
  COUNT(*) AS total_rows
FROM bl_3nf.ce_customers_scd;

--one active row per customer_src_id, source_system, source_entity --should be 0
SELECT customer_src_id, source_system, source_entity, COUNT(*) AS active_cnt
FROM bl_3nf.ce_customers_scd
WHERE is_active = TRUE
  AND end_dt = DATE '9999-12-31'
  AND customer_id <> -1
GROUP BY 1,2,3
HAVING COUNT(*) <> 1;


--Consistency active/end_dt (must be 0 violations)
SELECT COUNT(*) AS bad_rows
FROM bl_3nf.ce_customers_scd
WHERE customer_id <> -1
  AND (
    (is_active = TRUE  AND end_dt <> DATE '9999-12-31')
    OR
    (is_active = FALSE AND end_dt =  DATE '9999-12-31')
  );

--“Closing” today: each closed row must have a new active version (must be 0)
WITH closed_today AS (
  SELECT customer_src_id, source_system, source_entity
  FROM bl_3nf.ce_customers_scd
  WHERE customer_id <> -1
    AND is_active = FALSE
    AND end_dt = CURRENT_DATE - 1
    AND ta_update_dt >= date_trunc('day', now())
)
SELECT c.*
FROM closed_today c
LEFT JOIN bl_3nf.ce_customers_scd a
  ON a.customer_src_id = c.customer_src_id
 AND a.source_system   = c.source_system
 AND a.source_entity   = c.source_entity
 AND a.is_active       = TRUE
 AND a.end_dt          = DATE '9999-12-31'
WHERE a.customer_id IS NULL;

--How many active users have been inserted today after the second run. If the data did not change between the first and second runs, then this count should not increase after the second run.
SELECT COUNT(*) AS active_inserted_today
FROM bl_3nf.ce_customers_scd
WHERE customer_id <> -1
  AND is_active = TRUE
  AND end_dt = DATE '9999-12-31'
  AND ta_insert_dt >= date_trunc('day', now());

SELECT cus.customer_src_id, cus.email, cus.phone, cus.start_dt, cus.end_dt, cus.is_active, cus.source_system, cus.source_entity, cus.source_id
FROM bl_3nf.ce_customers_scd cus
WHERE cus.customer_src_id IN(
SELECT cus.customer_src_id
FROM bl_3nf.ce_customers_scd cus
GROUP BY cus.customer_src_id
HAVING count(cus.customer_src_id) >=3)
ORDER BY cus.customer_src_id;



-- =========================================================
-- DIMENSIONS
-- =========================================================

--dim_products
SELECT 
    run_id,
    log_dts,
    procedure_name,
    status,
    rows_affected,
    message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_products_dm_simple'
ORDER BY log_dts DESC;

SELECT count(*) AS na_product_name_cnt
FROM bl_dm.dim_products
WHERE lower(btrim(product_name::text)) = 'n. a.'
  AND source_id <> '-1';

SELECT* FROM bl_dm.dim_products;
SELECT COUNT (*) FROM bl_dm.dim_products;

--dim_employees
SELECT run_id,
		log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_employees_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_employees;



SELECT count(*) AS na_first_name_cnt
FROM bl_dm.dim_employees
WHERE lower(btrim(first_name::text)) = 'n. a.'
  AND source_id <> '-1';


--OTHER DIMS
--stores
SELECT run_id,
		log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_stores_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_stores;

--terminals
SELECT run_id,
		log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_terminals_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_terminals;

--promotion
SELECT run_id,
       log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_promotions_dm_simple'
ORDER BY log_dts DESC; 


--delivery_providers
SELECT run_id,
       log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_delivery_providers_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_delivery_providers;

--junk
SELECT run_id,
       log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_junk_context_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_junk_context;


--customer
SELECT run_id,
       log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_customers_scd_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_customers_scd;

SELECT cus.customer_src_id, cus.email, cus.phone, cus.start_dt, cus.end_dt, cus.is_active, cus.source_system, cus.source_entity, cus.source_id
FROM bl_dm.dim_customers_scd cus
WHERE cus.customer_src_id IN(
SELECT cus.customer_src_id
FROM bl_dm.dim_customers_scd cus
GROUP BY cus.customer_src_id
HAVING count(cus.customer_src_id) >=3)
ORDER BY cus.customer_src_id;


-- =========================================================
-- FCT TABLE
-- =========================================================
SELECT log_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name='bl_cl.pr_load_fct_sales_daily_dm'
ORDER BY log_id DESC;

SELECT date_id, txn_src_id, COUNT(*) AS cnt
FROM bl_dm.fct_sales_daily
GROUP BY date_id, txn_src_id
HAVING COUNT(*) > 1;

SELECT* FROM bl_dm.fct_sales_daily;
SELECT COUNT(*) FROM bl_dm.fct_sales_daily;



WITH bounds AS (
  SELECT
    (date_trunc('month', current_date)::date - (3 - 1) * interval '1 month')::date AS from_d,
    (date_trunc('month', current_date)::date + interval '1 month')::date           AS to_d
),
w AS (
  SELECT
    to_char(txn_ts::date,'YYYYMMDD')::int AS date_id,
    txn_src_id
  FROM bl_3nf.ce_transactions, bounds
  WHERE txn_ts >= bounds.from_d
    AND txn_ts <  bounds.to_d
)
SELECT w.date_id, w.txn_src_id
FROM w
LEFT JOIN bl_dm.fct_sales_daily f
  ON f.date_id = w.date_id
 AND f.txn_src_id = w.txn_src_id
WHERE f.txn_src_id IS NULL;

