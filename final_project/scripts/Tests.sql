select * FROM sa_sales_online.ext_sales_online eso ;
SELECT* FROM sa_sales_pos.ext_sales_pos esp ;

SELECT* FROM sa_sales_online.src_sales_online sso ;
SELECT* FROM sa_sales_pos.src_sales_pos ssp ;

-- =========================================================

SELECT * FROM bl_cl.mta_etl_log;
SELECT * FROM bl_cl.mta_load_control;
SELECT * FROM bl_cl.t_country_aliases;
SELECT * FROM bl_cl.t_map_countries;

-- =========================================================

SELECT * FROM bl_3nf.ce_brands;
SELECT * FROM bl_3nf.ce_card_types;
SELECT * FROM bl_3nf.ce_cities;
SELECT * FROM bl_3nf.ce_countries;
SELECT * FROM bl_3nf.ce_customers_scd;
SELECT * FROM bl_3nf.ce_delivery_addresses;
SELECT * FROM bl_3nf.ce_delivery_providers;
SELECT * FROM bl_3nf.ce_delivery_types;
SELECT * FROM bl_3nf.ce_device_types;
SELECT * FROM bl_3nf.ce_employees;
SELECT * FROM bl_3nf.ce_fulfillment_centers;
SELECT * FROM bl_3nf.ce_order_statuses;
SELECT * FROM bl_3nf.ce_payment_gateways;
SELECT * FROM bl_3nf.ce_payment_methods;
SELECT * FROM bl_3nf.ce_product_departments;
SELECT * FROM bl_3nf.ce_product_subcategories;
SELECT * FROM bl_3nf.ce_products;
SELECT * FROM bl_3nf.ce_promotions;
SELECT * FROM bl_3nf.ce_receipt_types;
SELECT * FROM bl_3nf.ce_regions;
SELECT * FROM bl_3nf.ce_sales_channels;
SELECT * FROM bl_3nf.ce_shifts;
SELECT * FROM bl_3nf.ce_store_formats;
SELECT * FROM bl_3nf.ce_stores;
SELECT * FROM bl_3nf.ce_suppliers;
SELECT * FROM bl_3nf.ce_terminal_types;
SELECT * FROM bl_3nf.ce_terminals;
SELECT * FROM bl_3nf.ce_transactions;
SELECT * FROM bl_3nf.ce_unit_of_measures;
-- =========================================================

SELECT * FROM bl_dm.dim_customers_scd;
SELECT * FROM bl_dm.dim_dates_day;
SELECT * FROM bl_dm.dim_delivery_providers;
SELECT * FROM bl_dm.dim_employees;
SELECT * FROM bl_dm.dim_junk_context;
SELECT * FROM bl_dm.dim_products;
SELECT * FROM bl_dm.dim_promotions;
SELECT * FROM bl_dm.dim_stores;
SELECT * FROM bl_dm.dim_terminals;
SELECT * FROM bl_dm.fct_sales_daily;
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

SELECT* FROM bl_3nf.ce_transactions LIMIT 1000;
SELECT COUNT(*) FROM bl_3nf.ce_transactions;
SELECT count (*) from sa_sales_online.src_sales_online sso ;
SELECT count (*) FROM sa_sales_pos.src_sales_pos ssp ;
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

--check chain of joins for addres
WITH t AS (
  SELECT
      txn_src_id,
      source_system,
      source_entity,
      delivery_address_id,
      txn_ts
  FROM bl_3nf.ce_transactions
  WHERE source_system = 'sa_sales_online'
    AND source_entity = 'src_sales_online'
)
SELECT
    count(*)                                                AS cnt_txn,
    count(*) FILTER (WHERE t.delivery_address_id IS NULL OR t.delivery_address_id = -1) AS cnt_no_delivery_address,

    count(*) FILTER (WHERE adr.delivery_address_id IS NOT NULL OR adr.delivery_address_id =-1)                          AS cnt_join_adr,
    count(*) FILTER (WHERE cty.city_id IS NOT NULL and cty.city_id>-1)                                      AS cnt_join_city,
    count(*) FILTER (WHERE reg.region_id IS NOT NULL AND reg.region_id >-1)                                    AS cnt_join_region,
    count(*) FILTER (WHERE ctr.country_id IS NOT NULL AND ctr.country_id >-1)                                   AS cnt_join_country

FROM t
LEFT JOIN bl_3nf.ce_delivery_addresses adr
  ON adr.delivery_address_id = t.delivery_address_id
LEFT JOIN bl_3nf.ce_cities cty
  ON cty.city_id = adr.city_id
LEFT JOIN bl_3nf.ce_regions reg
  ON reg.region_id = cty.region_id
LEFT JOIN bl_3nf.ce_countries ctr
  ON ctr.country_id = reg.country_id;


WITH t AS (
  SELECT
      txn_src_id,
      source_system,
      source_entity,
      store_id,
      txn_ts
  FROM bl_3nf.ce_transactions
  WHERE source_system = 'sa_sales_pos'
    AND source_entity = 'src_sales_pos'
)
SELECT
    count(*) AS cnt_txn,

    count(*) FILTER (WHERE t.store_id IS NULL OR t.store_id = -1) AS cnt_no_store,
    count(*) FILTER (WHERE str.store_id IS NOT NULL)              AS cnt_join_store,  
    count(*) FILTER (WHERE cty.city_id IS NOT NULL)               AS cnt_join_city,
    count(*) FILTER (WHERE reg.region_id IS NOT NULL)             AS cnt_join_region,
    count(*) FILTER (WHERE ctr.country_id IS NOT NULL)            AS cnt_join_country

FROM t
LEFT JOIN bl_3nf.ce_stores str
  ON str.store_id = t.store_id
LEFT JOIN bl_3nf.ce_cities cty
  ON cty.city_id = str.city_id
LEFT JOIN bl_3nf.ce_regions reg
  ON reg.region_id = cty.region_id
LEFT JOIN bl_3nf.ce_countries ctr
  ON ctr.country_id = reg.country_id;
 
--addresschain to src pos

SELECT * FROM sa_sales_pos.src_sales_pos ssp WHERE ssp.country = 'Ukraine' ;
SELECT
      tr.txn_src_id,      
      str.store_src_id,
      cty.city_name,
      reg.region_name,
      ctr.country_name,
      tmc.country_src_name,
      ssp.country      
FROM bl_3nf.ce_transactions tr 
LEFT JOIN sa_sales_pos.src_sales_pos ssp 
   ON ssp.ckout  = tr.txn_src_id
LEFT JOIN bl_3nf.ce_stores str
  ON str.store_id = tr.store_id
LEFT JOIN bl_3nf.ce_cities cty
  ON cty.city_id = str.city_id
LEFT JOIN bl_3nf.ce_regions reg
  ON reg.region_id = cty.region_id
LEFT JOIN bl_3nf.ce_countries ctr
  ON ctr.country_id = reg.country_id
 LEFT JOIN bl_cl.t_map_countries tmc 
  ON tmc.country_id ::varchar = ctr.source_id 
  AND tmc.country_src_name = ssp.country
  AND tr.source_system  = tmc.source_system 
WHERE tr.txn_src_id = 'pos_000000287';

SELECT * FROM sa_sales_pos.src_sales_pos ssp WHERE ssp.country = 'Ukr' ;

--address chain to src online
SELECT*from sa_sales_online.src_sales_online sso WHERE sso.country = 'Ukraine' ;

SELECT
      tr.txn_src_id,      
      adr.delivery_postal_code, 
      adr.delivery_address_line1,
      cty.city_name,
      reg.region_name,
      ctr.country_name,
      tmc.country_src_name,
      ssp.country,
      ssp.delivery_address_line1,
      ssp.delivery_postal_code
FROM bl_3nf.ce_transactions tr 
LEFT JOIN sa_sales_online.src_sales_online ssp 
   ON ssp.web_order_id  = tr.txn_src_id
LEFT JOIN bl_3nf.ce_delivery_addresses adr
  ON adr.delivery_address_id = tr.delivery_address_id
LEFT JOIN bl_3nf.ce_cities cty
  ON cty.city_id = adr.city_id
LEFT JOIN bl_3nf.ce_regions reg
  ON reg.region_id = cty.region_id
LEFT JOIN bl_3nf.ce_countries ctr
  ON ctr.country_id = reg.country_id
 LEFT JOIN bl_cl.t_map_countries tmc 
  ON tmc.country_id ::varchar = ctr.source_id 
  AND tmc.country_src_name = ssp.country 
  AND tr.source_system  = tmc.source_system 
WHERE tr.txn_src_id = 'onl_000000273';


-- =========================================================
-- CE_CUSTOMERS_SCD
-- =========================================================
CALL bl_cl.pr_load_ce_customers_scd();

SELECT  *
FROM bl_3nf.ce_customers_scd
WHERE customer_src_id = 'c0857672';

SELECT
  customer_src_id,
  source_system,
  source_entity,
  COUNT(*) AS versions_cnt,
  MIN(start_ts) AS first_start_ts,
  MAX(start_ts) AS last_start_ts
FROM bl_3nf.ce_customers_scd
WHERE customer_id <> -1
GROUP BY customer_src_id, source_system, source_entity
HAVING COUNT(*) > 1
ORDER BY versions_cnt DESC, last_start_ts DESC;




SELECT run_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_customers_scd'
ORDER BY log_dts DESC;


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
  AND end_ts = 'infinity'::timestamp
  AND customer_id <> -1
GROUP BY 1,2,3
HAVING COUNT(*) <> 1;


--Consistency active/end_dt (must be 0 violations)
SELECT COUNT(*) AS bad_rows
FROM bl_3nf.ce_customers_scd
WHERE customer_id <> -1
  AND (
    (is_active = TRUE  AND end_ts <> 'infinity'::timestamp)
    OR
    (is_active = FALSE AND end_ts =  'infinity'::timestamp)
  );


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

SELECT  *
FROM bl_dm.dim_customers_scd
WHERE customer_src_id = 'c0857672';

SELECT
  customer_src_id,
  source_system,
  source_entity,
  COUNT(*) AS versions_cnt,
  MIN(start_ts) AS first_start_ts,
  MAX(start_ts) AS last_start_ts
FROM bl_dm.dim_customers_scd
WHERE customer_id <> -1
GROUP BY customer_src_id, source_system, source_entity
HAVING COUNT(*) > 1
ORDER BY versions_cnt DESC, last_start_ts DESC;

--one active row per customer_src_id, source_system, source_entity --should be 0
SELECT customer_src_id, source_system, source_entity, COUNT(*) AS active_cnt
FROM bl_dm.dim_customers_scd
WHERE is_active = TRUE
  AND end_ts = 'infinity'::timestamp
  AND customer_id <> -1
GROUP BY 1,2,3
HAVING COUNT(*) <> 1;


--Consistency active/end_dt (must be 0 violations)
SELECT COUNT(*) AS bad_rows
FROM bl_dm.dim_customers_scd
WHERE customer_id <> -1
  AND (
    (is_active = TRUE  AND end_ts <> 'infinity'::timestamp)
    OR
    (is_active = FALSE AND end_ts =  'infinity'::timestamp)
  );
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

