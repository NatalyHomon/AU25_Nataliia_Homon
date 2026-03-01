-- =========================================================
--  CE_TRANSACTIONS 
-- =========================================================

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_transactions(p_full_reload boolean DEFAULT FALSE, p_run_id uuid DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        text := 'bl_cl.pr_load_ce_transactions';
    v_run_id      uuid := COALESCE(p_run_id, gen_random_uuid());

    v_last_online timestamptz;
    v_last_pos    timestamptz;

    v_new_online  timestamptz;
    v_new_pos     timestamptz;

    v_rows_main   bigint := 0;
BEGIN
    -- 1) ensure load_control rows exist
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    -- 2) read watermarks
    SELECT last_success_load_dts INTO v_last_online
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
        v_last_pos    := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(
        v_proc, 'START', 0,
        CASE WHEN p_full_reload
             THEN 'Start loading CE_TRANSACTIONS (FULL reload mode by reset load_dts watermark).'
             ELSE 'Start loading CE_TRANSACTIONS (incremental by SA.load_dts watermark).'
        END,
        NULL, NULL, NULL, v_run_id
    );

    -- 3) main upsert (incremental source by load_dts)
   WITH src_raw AS (
  /* ONLINE */
  SELECT
      COALESCE(son.web_order_id, 'n. a.')                   AS txn_src_id,
      COALESCE(son.txn_ts, TIMESTAMP '1900-01-01')          AS txn_ts,
      COALESCE(son.product_sku, 'n. a.')                    AS product_sku_src_id,
      COALESCE(son.promo_code, 'n. a.')                     AS promo_code,
      'online'                                              AS sales_channel_name,
      COALESCE(son.customer_src_id, 'n. a.')                AS customer_src_id,
      'n. a.'                                               AS payment_method_name,
      'n. a.'                                               AS card_type_name,
      'n. a.'                                               AS receipt_type_name,
      'n. a.'                                               AS store_src_id,
      'n. a.'                                               AS terminal_src_id,
      'n. a.'                                               AS employee_src_id,
      'n. a.'                                               AS shift_src_id,
      COALESCE(son.order_status, 'n. a.')                   AS order_status_name,
      COALESCE(son.carrier_name, 'n. a.')                   AS carrier_name,
      COALESCE(son.delivery_type, 'n. a.')                  AS delivery_type_name,

      COALESCE(son.delivery_postal_code, 'n. a.')           AS delivery_postal_code,
      COALESCE(son.delivery_address_line1, 'n. a.')         AS delivery_address_line1,
      COALESCE(son.city, 'n. a.')                           AS city_name,
      COALESCE(son.region, 'n. a.')                         AS region_name,
      COALESCE(son.country, 'n. a.')                        AS country_name,

      COALESCE(son.fulfillment_center_id, 'n. a.')          AS fulfillment_center_src_id,
      COALESCE(son.fulfillment_city, 'n. a.')               AS fulfillment_city_name,

      COALESCE(son.device_type, 'n. a.')                    AS device_type_name,
      COALESCE(son.payment_gateway, 'n. a.')                AS payment_gateway_name,

      COALESCE(son.tracking_id, 'n. a.')                    AS tracking_id,
      COALESCE(son.promised_delivery_dt, DATE '1900-01-01') AS promised_delivery_dt,

      COALESCE(son.qty, -1)                                 AS qty,
      COALESCE(son.unit_price_amt, -1)                      AS unit_price_amt,
      COALESCE(son.tax_amt, -1)                             AS tax_amt,
      COALESCE(son.shipping_fee_amt, -1)                    AS shipping_fee_amt,
      COALESCE(son.discount_amt, -1)                        AS discount_amt,
      COALESCE(son.sales_amt, -1)                           AS sales_amt,
      COALESCE(son.cost_amt, -1)                            AS cost_amt,
      COALESCE(son.gross_profit_amt, -1)                    AS gross_profit_amt,
      -1::INT                                               AS loyalty_points_earned,
      COALESCE(son.customer_rating, -1)                     AS customer_rating,

      son.load_dts                                          AS src_load_dts,     -- IMPORTANT
      'sa_sales_online'                                     AS source_system,
      'src_sales_online'                                    AS source_entity,
      COALESCE(son.web_order_id, 'n. a.')                   AS source_id
  FROM sa_sales_online.src_sales_online son
  WHERE son.web_order_id IS NOT NULL
    AND son.load_dts > v_last_online

  UNION ALL

  /* POS */
  SELECT
      COALESCE(spo.ckout, 'n. a.')                          AS txn_src_id,
      COALESCE(spo.txn_ts, TIMESTAMP '1900-01-01')          AS txn_ts,
      COALESCE(spo.product_sku, 'n. a.')                    AS product_sku_src_id,
      COALESCE(spo.promo_code, 'n. a.')                     AS promo_code,
      'pos'                                                 AS sales_channel_name,
      COALESCE(spo.customer_src_id, 'n. a.')                AS customer_src_id,
      COALESCE(spo.payment_method, 'n. a.')                 AS payment_method_name,
      COALESCE(spo.card_type, 'n. a.')                      AS card_type_name,
      COALESCE(spo.receipt_type, 'n. a.')                   AS receipt_type_name,
      COALESCE(spo.store_id, 'n. a.')                       AS store_src_id,
      COALESCE(spo.terminal_id, 'n. a.')                    AS terminal_src_id,
      COALESCE(spo.cashier_id, 'n. a.')                     AS employee_src_id,
      COALESCE(spo.shift_id, 'n. a.')                       AS shift_src_id,
      'n. a.'                                               AS order_status_name,
      'n. a.'                                               AS carrier_name,
      'n. a.'                                               AS delivery_type_name,

      'n. a.'                                               AS delivery_postal_code,
      'n. a.'                                               AS delivery_address_line1,
      'n. a.'                                               AS city_name,
      'n. a.'                                               AS region_name,
      'n. a.'                                               AS country_name,

      'n. a.'                                               AS fulfillment_center_src_id,
      'n. a.'                                               AS fulfillment_city_name,

      'n. a.'                                               AS device_type_name,
      'n. a.'                                               AS payment_gateway_name,

      'n. a.'                                               AS tracking_id,
      DATE '1900-01-01'                                     AS promised_delivery_dt,

      COALESCE(spo.qty, -1)                                 AS qty,
      COALESCE(spo.unit_price_amt, -1)                      AS unit_price_amt,
      COALESCE(spo.tax_amt, -1)                             AS tax_amt,
      -1                                                    AS shipping_fee_amt,
      COALESCE(spo.discount_amt, -1)                        AS discount_amt,
      COALESCE(spo.sales_amt, -1)                           AS sales_amt,
      COALESCE(spo.cost_amt, -1)                            AS cost_amt,
      COALESCE(spo.gross_profit_amt, -1)                    AS gross_profit_amt,
      COALESCE(spo.loyalty_points_earned, -1)::INT          AS loyalty_points_earned,
      COALESCE(spo.customer_rating, -1)                     AS customer_rating,

      spo.load_dts                                          AS src_load_dts,     -- IMPORTANT
      'sa_sales_pos'                                        AS source_system,
      'src_sales_pos'                                       AS source_entity,
      COALESCE(spo.ckout, 'n. a.')                          AS source_id
  FROM sa_sales_pos.src_sales_pos spo
  WHERE spo.ckout IS NOT NULL
    AND spo.load_dts > v_last_pos
),
src AS (
  -- keep latest record per business key based on load timestamp
  SELECT DISTINCT ON (srr.txn_src_id, srr.source_system, srr.source_entity)
      srr.*
  FROM src_raw srr
  ORDER BY srr.txn_src_id, srr.source_system, srr.source_entity, srr.src_load_dts DESC
),
map AS (
  SELECT
      src.txn_src_id,
      src.txn_ts,

      COALESCE(prd.product_id, -1)            AS product_id,
      COALESCE(pro.promotion_id, -1)          AS promotion_id,
      COALESCE(sch.sales_channel_id, -1)      AS sales_channel_id,
      COALESCE(cus.customer_id, -1)           AS customer_id,
      COALESCE(pmt.payment_method_id, -1)     AS payment_method_id,
      COALESCE(crt.card_type_id, -1)          AS card_type_id,
      COALESCE(rct.receipt_type_id, -1)       AS receipt_type_id,
      COALESCE(str.store_id, -1)              AS store_id,
      COALESCE(ter.terminal_id, -1)           AS terminal_id,
      COALESCE(emp.employee_id, -1)           AS employee_id,
      COALESCE(sft.shift_id, -1)              AS shift_id,
      COALESCE(ord.order_status_id, -1)       AS order_status_id,
      COALESCE(dpr.delivery_provider_id, -1)  AS delivery_provider_id,
      COALESCE(dty.delivery_type_id, -1)      AS delivery_type_id,
      COALESCE(adr.delivery_address_id, -1)   AS delivery_address_id,
      COALESCE(ful.fulfillment_center_id, -1) AS fulfillment_center_id,
      COALESCE(dvc.device_type_id, -1)        AS device_type_id,
      COALESCE(pgw.payment_gateway_id, -1)    AS payment_gateway_id,

      src.tracking_id,
      src.promised_delivery_dt,

      src.qty,
      src.unit_price_amt,
      src.tax_amt,
      src.shipping_fee_amt,
      src.discount_amt,
      src.sales_amt,
      src.cost_amt,
      src.gross_profit_amt,
      src.loyalty_points_earned,
      src.customer_rating,

      src.source_system,
      src.source_entity,
      src.source_id,
      src.src_load_dts                         -- IMPORTANT for final dedup
  FROM src
  LEFT JOIN bl_3nf.ce_products prd
    ON prd.product_sku_src_id = src.product_sku_src_id
   AND prd.source_system      = src.source_system
   AND prd.source_entity      = src.source_entity
  LEFT JOIN bl_3nf.ce_promotions pro
    ON pro.promo_code    = src.promo_code
   AND pro.source_system = src.source_system
   AND pro.source_entity = src.source_entity
  LEFT JOIN bl_3nf.ce_sales_channels sch
    ON sch.sales_channel_name = src.sales_channel_name
   AND sch.source_system      = src.source_system
   AND sch.source_entity      = src.source_entity
  LEFT JOIN bl_3nf.ce_customers_scd cus
    ON cus.customer_src_id = src.customer_src_id
   AND cus.source_system   = src.source_system
   AND cus.source_entity   = src.source_entity
   AND cus.is_active       = TRUE
   AND cus.end_dt          = DATE '9999-12-31'
  LEFT JOIN bl_3nf.ce_payment_methods pmt
    ON pmt.payment_method_name = src.payment_method_name
   AND pmt.source_system       = src.source_system
   AND pmt.source_entity       = src.source_entity
  LEFT JOIN bl_3nf.ce_card_types crt
    ON crt.card_type_name = src.card_type_name
   AND crt.source_system  = src.source_system
   AND crt.source_entity  = src.source_entity
  LEFT JOIN bl_3nf.ce_receipt_types rct
    ON rct.receipt_type_name = src.receipt_type_name
   AND rct.source_system     = src.source_system
   AND rct.source_entity     = src.source_entity
  LEFT JOIN bl_3nf.ce_stores str
    ON str.store_src_id  = src.store_src_id
   AND str.source_system = src.source_system
   AND str.source_entity = src.source_entity
  LEFT JOIN bl_3nf.ce_terminals ter
    ON ter.terminal_src_id = src.terminal_src_id
   AND ter.source_system   = src.source_system
   AND ter.source_entity   = src.source_entity
  LEFT JOIN bl_3nf.ce_employees emp
    ON emp.employee_src_id = src.employee_src_id
   AND emp.source_system   = src.source_system
   AND emp.source_entity   = src.source_entity
  LEFT JOIN bl_3nf.ce_shifts sft
    ON sft.shift_src_id  = src.shift_src_id
   AND sft.source_system = src.source_system
   AND sft.source_entity = src.source_entity
  LEFT JOIN bl_3nf.ce_order_statuses ord
    ON ord.order_status_name = src.order_status_name
   AND ord.source_system     = src.source_system
   AND ord.source_entity     = src.source_entity
  LEFT JOIN bl_3nf.ce_delivery_providers dpr
    ON dpr.carrier_name  = src.carrier_name
   AND dpr.source_system = src.source_system
   AND dpr.source_entity = src.source_entity
  LEFT JOIN bl_3nf.ce_delivery_types dty
    ON dty.delivery_type_name = src.delivery_type_name
   AND dty.source_system      = src.source_system
   AND dty.source_entity      = src.source_entity

  -- countries/regions/cities + address
  LEFT JOIN bl_3nf.ce_countries ctr
    ON ctr.country_name  = src.country_name
   AND ctr.source_system = src.source_system
   AND ctr.source_entity = src.source_entity
  LEFT JOIN bl_3nf.ce_regions reg
    ON reg.region_name   = src.region_name
   AND reg.country_id    = ctr.country_id
   AND reg.source_system = src.source_system
   AND reg.source_entity = src.source_entity
  LEFT JOIN bl_3nf.ce_cities cty
    ON cty.city_name     = src.city_name
   AND cty.region_id     = reg.region_id
   AND cty.source_system = src.source_system
   AND cty.source_entity = src.source_entity
  LEFT JOIN bl_3nf.ce_delivery_addresses adr
    ON adr.delivery_postal_code   = src.delivery_postal_code
   AND adr.delivery_address_line1 = src.delivery_address_line1
   AND adr.city_id                = COALESCE(cty.city_id, -1)
   AND adr.source_system          = src.source_system
   AND adr.source_entity          = src.source_entity

  LEFT JOIN bl_3nf.ce_fulfillment_centers ful
    ON ful.fulfillment_center_src_id = src.fulfillment_center_src_id
   AND ful.source_system             = src.source_system
   AND ful.source_entity             = src.source_entity
  LEFT JOIN bl_3nf.ce_device_types dvc
    ON dvc.device_type_name = src.device_type_name
   AND dvc.source_system    = src.source_system
   AND dvc.source_entity    = src.source_entity
  LEFT JOIN bl_3nf.ce_payment_gateways pgw
    ON pgw.payment_gateway_name = src.payment_gateway_name
   AND pgw.source_system        = src.source_system
   AND pgw.source_entity        = src.source_entity
),
to_upsert AS (
  -- FINAL safeguard: after joins, keep only 1 row per conflict key
  SELECT DISTINCT ON (m.txn_src_id, m.source_system, m.source_entity)
      m.*
  FROM map m
  ORDER BY m.txn_src_id, m.source_system, m.source_entity, m.src_load_dts DESC
)
INSERT INTO bl_3nf.ce_transactions (
    txn_src_id, txn_ts,
    product_id, promotion_id, sales_channel_id, customer_id,
    payment_method_id, card_type_id, receipt_type_id,
    store_id, terminal_id, employee_id, shift_id,
    order_status_id, delivery_provider_id, delivery_type_id,
    delivery_address_id, fulfillment_center_id, device_type_id, payment_gateway_id,
    tracking_id, promised_delivery_dt,
    qty, unit_price_amt, tax_amt, shipping_fee_amt, discount_amt,
    sales_amt, cost_amt, gross_profit_amt,
    loyalty_points_earned, customer_rating,
    source_system, source_entity, source_id,
    ta_insert_dt, ta_update_dt
)
SELECT
    m.txn_src_id, m.txn_ts,
    m.product_id, m.promotion_id, m.sales_channel_id, m.customer_id,
    m.payment_method_id, m.card_type_id, m.receipt_type_id,
    m.store_id, m.terminal_id, m.employee_id, m.shift_id,
    m.order_status_id, m.delivery_provider_id, m.delivery_type_id,
    m.delivery_address_id, m.fulfillment_center_id, m.device_type_id, m.payment_gateway_id,
    m.tracking_id, m.promised_delivery_dt,
    m.qty, m.unit_price_amt, m.tax_amt, m.shipping_fee_amt, m.discount_amt,
    m.sales_amt, m.cost_amt, m.gross_profit_amt,
    m.loyalty_points_earned, m.customer_rating,
    m.source_system, m.source_entity, m.source_id,
    now(), now()
FROM to_upsert m
ON CONFLICT (txn_src_id, source_system, source_entity)
DO UPDATE SET
    txn_ts               = EXCLUDED.txn_ts,
    product_id           = EXCLUDED.product_id,
    promotion_id         = EXCLUDED.promotion_id,
    sales_channel_id     = EXCLUDED.sales_channel_id,
    customer_id          = EXCLUDED.customer_id,
    payment_method_id    = EXCLUDED.payment_method_id,
    card_type_id         = EXCLUDED.card_type_id,
    receipt_type_id      = EXCLUDED.receipt_type_id,
    store_id             = EXCLUDED.store_id,
    terminal_id          = EXCLUDED.terminal_id,
    employee_id          = EXCLUDED.employee_id,
    shift_id             = EXCLUDED.shift_id,
    order_status_id      = EXCLUDED.order_status_id,
    delivery_provider_id = EXCLUDED.delivery_provider_id,
    delivery_type_id     = EXCLUDED.delivery_type_id,
    delivery_address_id  = EXCLUDED.delivery_address_id,
    fulfillment_center_id= EXCLUDED.fulfillment_center_id,
    device_type_id       = EXCLUDED.device_type_id,
    payment_gateway_id   = EXCLUDED.payment_gateway_id,
    tracking_id          = EXCLUDED.tracking_id,
    promised_delivery_dt = EXCLUDED.promised_delivery_dt,
    qty                  = EXCLUDED.qty,
    unit_price_amt       = EXCLUDED.unit_price_amt,
    tax_amt              = EXCLUDED.tax_amt,
    shipping_fee_amt     = EXCLUDED.shipping_fee_amt,
    discount_amt         = EXCLUDED.discount_amt,
    sales_amt            = EXCLUDED.sales_amt,
    cost_amt             = EXCLUDED.cost_amt,
    gross_profit_amt     = EXCLUDED.gross_profit_amt,
    loyalty_points_earned= EXCLUDED.loyalty_points_earned,
    customer_rating      = EXCLUDED.customer_rating,
    source_id            = EXCLUDED.source_id,
    ta_update_dt         = now();

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;

    -- 4) new watermarks based on SA.load_dts
    SELECT max(load_dts) INTO v_new_online
    FROM sa_sales_online.src_sales_online
    WHERE web_order_id IS NOT NULL
      AND load_dts > v_last_online;

    SELECT max(load_dts) INTO v_new_pos
    FROM sa_sales_pos.src_sales_pos
    WHERE ckout IS NOT NULL
      AND load_dts > v_last_pos;

    IF v_new_online IS NOT NULL THEN
        UPDATE bl_cl.mta_load_control
        SET last_success_load_dts = v_new_online, ta_update_dt = now()
        WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';
    END IF;

    IF v_new_pos IS NOT NULL THEN
        UPDATE bl_cl.mta_load_control
        SET last_success_load_dts = v_new_pos, ta_update_dt = now()
        WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_main,'Loaded CE_TRANSACTIONS successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RETURN;
END;
$$;
-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_transactions();



SELECT log_id, log_dts, procedure_name, status, rows_affected, message
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
--  FCT_SALES_DAILY
-- =========================================================
DROP TABLE IF EXISTS bl_dm.fct_sales_daily;

CREATE TABLE IF NOT EXISTS bl_dm.fct_sales_daily (
    date_id                   INT,
    customer_id               BIGINT,
    product_id                BIGINT,
    store_id                  BIGINT,
    employee_id               BIGINT,
    terminal_id               BIGINT,
    promotion_id              BIGINT,
    delivery_provider_id      BIGINT,
    junk_context_id           BIGINT,
    promised_delivery_date_id INT,

    txn_src_id                VARCHAR(100),
    txn_ts_src                TIMESTAMP,
    tracking_id_src_id        VARCHAR(100),

    qty                       INT,
    unit_price_amt            DECIMAL(12,2),
    discount_amt              DECIMAL(12,2),
    tax_amt                   DECIMAL(12,2),
    shipping_fee_amt          DECIMAL(12,2),
    sales_amt                 DECIMAL(12,2),
    cost_amt                  DECIMAL(12,2),
    gross_profit_amt          DECIMAL(12,2),
    loyalty_points_earned     INT,
    customer_rating           DECIMAL(4,1),
    calculated_gross_margin_pct DECIMAL(5,2),
    calculated_net_sales_amt  DECIMAL(12,2)
)
PARTITION BY RANGE (date_id);



CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_sales_daily_bk ON bl_dm.fct_sales_daily (date_id, txn_src_id);

CREATE TABLE IF NOT EXISTS bl_dm.fct_sales_daily_default
PARTITION OF bl_dm.fct_sales_daily DEFAULT;

CREATE OR REPLACE PROCEDURE bl_cl.pr_manage_fct_sales_daily_partitions(p_months_back int DEFAULT 3, p_run_id uuid DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_manage_fct_sales_daily_partitions';
  v_run_id uuid := COALESCE(p_run_id, gen_random_uuid());

  v_start_month date := date_trunc('month', current_date)::date - (p_months_back - 1) * interval '1 month';
  v_end_month   date := date_trunc('month', current_date)::date + interval '1 month';
  v_m date;

  v_part_name text;
  v_from int;
  v_to   int;

  v_rows_moved bigint;
BEGIN
  CALL bl_cl.pr_log_write(v_proc,'START',0,'Manage partitions for fct_sales_daily (archive old to DEFAULT)',NULL,'bl_dm','fct_sales_daily',v_run_id);

--0) Ensure DEFAULT partition exists (catch-all for dates outside created partitions)
  EXECUTE 'CREATE TABLE IF NOT EXISTS bl_dm.fct_sales_daily_default
           PARTITION OF bl_dm.fct_sales_daily DEFAULT';

  -- 1) Create rolling window partitions (+ next month)
  v_m := v_start_month;
  WHILE v_m <= v_end_month LOOP
    v_part_name := format('fct_sales_daily_%s', to_char(v_m,'YYYYMM'));
    v_from := to_char(v_m,'YYYYMMDD')::int;
    v_to   := to_char((v_m + interval '1 month')::date,'YYYYMMDD')::int;

    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS bl_dm.%I PARTITION OF bl_dm.fct_sales_daily FOR VALUES FROM (%s) TO (%s);',
      v_part_name, v_from, v_to
    );

    v_m := (v_m + interval '1 month')::date;
  END LOOP;

  -- 2) Detach+archive partitions older than rolling window start
  FOR v_part_name IN
    SELECT c.relname
    FROM pg_class c
    JOIN pg_inherits i ON i.inhrelid = c.oid
    JOIN pg_class p ON p.oid = i.inhparent
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname='bl_dm'
      AND p.relname='fct_sales_daily'
      AND c.relname <> 'fct_sales_daily_default'
  LOOP
    IF v_part_name ~ '^fct_sales_daily_\d{6}$'
       AND to_date(substring(v_part_name from '\d{6}$'),'YYYYMM') < v_start_month
    THEN
      -- detach old partition
      EXECUTE format('ALTER TABLE bl_dm.fct_sales_daily DETACH PARTITION bl_dm.%I;', v_part_name);

      CALL bl_cl.pr_log_write(
        v_proc,'SUCCESS',v_rows_moved,
        format('Archived partition %s into fct_sales_daily_default and dropped it.', v_part_name),
        NULL,'bl_dm','fct_sales_daily',v_run_id
      );
    END IF;
  END LOOP;

  CALL bl_cl.pr_log_write(v_proc,'SUCCESS',0,'Partitions managed (rolling + archived old to DEFAULT).',NULL,'bl_dm','fct_sales_daily',v_run_id);

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,'bl_dm','fct_sales_daily',v_run_id);
  RETURN;
END;
$$;

SELECT
  sales_channel, payment_method, card_type, receipt_type,
  payment_gateway, order_status, shift_name, device_type_id,
  COUNT(*) cnt
FROM bl_dm.dim_junk_context
GROUP BY 1,2,3,4,5,6,7,8
HAVING COUNT(*) > 1;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_fct_sales_daily_dm(p_months_back int DEFAULT 3, p_run_id uuid DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_fct_sales_daily_dm';
  v_run_id uuid := COALESCE(p_run_id, gen_random_uuid());
  v_rows   bigint := 0;
  v_dup_cnt bigint := 0;


  v_from_date date := (date_trunc('month', current_date)::date - (p_months_back - 1) * interval '1 month')::date;
  v_to_date   date := (date_trunc('month', current_date)::date + interval '1 month')::date; -- до кінця поточного місяця
BEGIN
  CALL bl_cl.pr_log_write(v_proc,'START',0,'Start load fct_sales_daily (rolling window)',NULL,'bl_dm','fct_sales_daily',v_run_id);

  -- 1) ensure partitions exist + DETACH old (DDL, dynamic inside manage proc)
  CALL bl_cl.pr_manage_fct_sales_daily_partitions(p_months_back);

  -- 2) ensure unknown date exists
  INSERT INTO bl_dm.dim_dates_day(date_id, day, day_of_week, day_name, week, week_of_year, month, month_name, quarter, year, is_weekend)
  VALUES (-1, -1, -1, 'n. a.', -1, -1, -1, 'n. a.', -1, -1, false)
  ON CONFLICT (date_id) DO NOTHING;

  -- 2.5) PRE-CHECK: duplicates by (date_id, txn_src_id) AFTER JOINS
  SELECT COUNT(*) INTO v_dup_cnt
  FROM (
    SELECT
      to_char(trx.txn_ts::date,'YYYYMMDD')::int AS date_id,
      trx.txn_src_id
    FROM bl_3nf.ce_transactions trx

    LEFT JOIN bl_3nf.ce_products p3 ON p3.product_id = trx.product_id
    LEFT JOIN bl_dm.dim_products dprd ON dprd.source_id = p3.product_id::varchar

    LEFT JOIN bl_3nf.ce_promotions pr3 ON pr3.promotion_id = trx.promotion_id
    LEFT JOIN bl_dm.dim_promotions dpro ON dpro.source_id = pr3.promotion_id::varchar

    LEFT JOIN bl_3nf.ce_stores s3 ON s3.store_id = trx.store_id
    LEFT JOIN bl_dm.dim_stores dstr ON dstr.source_id = s3.store_id::varchar

    LEFT JOIN bl_3nf.ce_terminals t3 ON t3.terminal_id = trx.terminal_id
    LEFT JOIN bl_dm.dim_terminals dter ON dter.source_id = t3.terminal_id::varchar

    LEFT JOIN bl_3nf.ce_employees e3 ON e3.employee_id = trx.employee_id
    LEFT JOIN bl_dm.dim_employees demp ON demp.source_id = e3.employee_id::varchar

    LEFT JOIN bl_3nf.ce_customers_scd c3
      ON c3.customer_id = trx.customer_id
     AND c3.is_active = true
     AND c3.end_dt = date '9999-12-31'
    LEFT JOIN bl_dm.dim_customers_scd dcus
      ON dcus.source_id = c3.customer_id::varchar
     AND dcus.is_active = true
     AND dcus.end_dt = date '9999-12-31'

    LEFT JOIN bl_3nf.ce_delivery_providers dp3 ON dp3.delivery_provider_id = trx.delivery_provider_id
    LEFT JOIN bl_dm.dim_delivery_providers ddpr ON ddpr.source_id = dp3.delivery_provider_id::varchar

    -- JUNK
    LEFT JOIN bl_3nf.ce_sales_channels sch3 ON sch3.sales_channel_id = trx.sales_channel_id
    LEFT JOIN bl_3nf.ce_payment_methods pm3 ON pm3.payment_method_id = trx.payment_method_id
    LEFT JOIN bl_3nf.ce_card_types ct3 ON ct3.card_type_id = trx.card_type_id
    LEFT JOIN bl_3nf.ce_receipt_types rt3 ON rt3.receipt_type_id = trx.receipt_type_id
    LEFT JOIN bl_3nf.ce_payment_gateways pg3 ON pg3.payment_gateway_id = trx.payment_gateway_id
    LEFT JOIN bl_3nf.ce_order_statuses os3 ON os3.order_status_id = trx.order_status_id
    LEFT JOIN bl_3nf.ce_shifts sh3 ON sh3.shift_id = trx.shift_id

    LEFT JOIN bl_dm.dim_junk_context djnk
      ON djnk.sales_channel    = sch3.sales_channel_name
     AND djnk.payment_method  = pm3.payment_method_name
     AND djnk.card_type       = ct3.card_type_name
     AND djnk.receipt_type    = rt3.receipt_type_name
     AND djnk.payment_gateway = pg3.payment_gateway_name
     AND djnk.order_status    = os3.order_status_name
     AND djnk.shift_name      = sh3.shift_src_id
     AND djnk.device_type_id  = trx.device_type_id

    WHERE trx.txn_ts >= v_from_date
      AND trx.txn_ts <  v_to_date

    GROUP BY 1,2
    HAVING COUNT(*) > 1
  ) d;

    IF v_dup_cnt > 0 THEN
      CALL bl_cl.pr_log_write(
        v_proc,'ERROR',0,
        format('Duplicate keys found in ce_transactions for (date_id, txn_src_id) in window [%s, %s). Count=%s',
               v_from_date, v_to_date, v_dup_cnt),
        '21000',
        'bl_dm','fct_sales_daily',v_run_id
      );

      -- зупиняємося, щоб не падати всередині INSERT
      RAISE EXCEPTION 'Duplicate keys for (date_id, txn_src_id) in source window. Count=%', v_dup_cnt
        USING ERRCODE = '21000';
    END IF;

  -- 3) UPSERT into parent table (Postgres routes rows into partitions automatically)
  INSERT INTO bl_dm.fct_sales_daily AS tgt (
    date_id, customer_id, product_id, store_id, employee_id, terminal_id, promotion_id,
    delivery_provider_id, junk_context_id, promised_delivery_date_id,
    txn_src_id, txn_ts_src, tracking_id_src_id,
    qty, unit_price_amt, discount_amt, tax_amt, shipping_fee_amt, sales_amt, cost_amt, gross_profit_amt,
    loyalty_points_earned, customer_rating, calculated_gross_margin_pct, calculated_net_sales_amt
  )
  SELECT
    to_char(trx.txn_ts::date,'YYYYMMDD')::int AS date_id,

    COALESCE(dcus.customer_id, -1)  AS customer_id,
    COALESCE(dprd.product_id,  -1)  AS product_id,
    COALESCE(dstr.store_id,    -1)  AS store_id,
    COALESCE(demp.employee_id, -1)  AS employee_id,
    COALESCE(dter.terminal_id, -1)  AS terminal_id,
    COALESCE(dpro.promotion_id,-1)  AS promotion_id,

    COALESCE(ddpr.delivery_provider_id, -1) AS delivery_provider_id,
    COALESCE(djnk.junk_context_id, -1)      AS junk_context_id,

    COALESCE(to_char(trx.promised_delivery_dt,'YYYYMMDD')::int, -1) AS promised_delivery_date_id,

    trx.txn_src_id,
    trx.txn_ts      AS txn_ts_src,
    trx.tracking_id AS tracking_id_src_id,

    trx.qty,
    trx.unit_price_amt,
    trx.discount_amt,
    trx.tax_amt,
    trx.shipping_fee_amt,
    trx.sales_amt,
    trx.cost_amt,
    trx.gross_profit_amt,
    trx.loyalty_points_earned,
    trx.customer_rating,

    CASE
      WHEN trx.sales_amt IS NULL OR trx.sales_amt = 0 THEN NULL
      ELSE ROUND((trx.gross_profit_amt / trx.sales_amt) * 100.0, 2)
    END AS calculated_gross_margin_pct,

    (trx.sales_amt - trx.discount_amt) AS calculated_net_sales_amt

  FROM bl_3nf.ce_transactions trx

  -- ===== 3NF -> DM key mapping =====
  LEFT JOIN bl_3nf.ce_products p3 ON p3.product_id = trx.product_id
  LEFT JOIN bl_dm.dim_products dprd
    ON dprd.source_id = p3.product_id::varchar  

  LEFT JOIN bl_3nf.ce_promotions pr3 ON pr3.promotion_id = trx.promotion_id
  LEFT JOIN bl_dm.dim_promotions dpro
    ON dpro.source_id = pr3.promotion_id::varchar

  LEFT JOIN bl_3nf.ce_stores s3 ON s3.store_id = trx.store_id
  LEFT JOIN bl_dm.dim_stores dstr
    ON dstr.source_id = s3.store_id::varchar

  LEFT JOIN bl_3nf.ce_terminals t3 ON t3.terminal_id = trx.terminal_id
  LEFT JOIN bl_dm.dim_terminals dter
    ON dter.source_id = t3.terminal_id::varchar

  LEFT JOIN bl_3nf.ce_employees e3 ON e3.employee_id = trx.employee_id
  LEFT JOIN bl_dm.dim_employees demp
    ON demp.source_id = e3.employee_id::varchar

  LEFT JOIN bl_3nf.ce_customers_scd c3
    ON c3.customer_id = trx.customer_id
   AND c3.is_active = true
   AND c3.end_dt = date '9999-12-31'
  LEFT JOIN bl_dm.dim_customers_scd dcus
    ON dcus.source_id = c3.customer_id::varchar
   AND dcus.is_active = true
   AND dcus.end_dt = date '9999-12-31'

  LEFT JOIN bl_3nf.ce_delivery_providers dp3 ON dp3.delivery_provider_id = trx.delivery_provider_id
  LEFT JOIN bl_dm.dim_delivery_providers ddpr
    ON ddpr.source_id = dp3.delivery_provider_id::varchar

  -- JUNK
  LEFT JOIN bl_3nf.ce_sales_channels sch3 ON sch3.sales_channel_id = trx.sales_channel_id
  LEFT JOIN bl_3nf.ce_payment_methods pm3 ON pm3.payment_method_id = trx.payment_method_id
  LEFT JOIN bl_3nf.ce_card_types ct3 ON ct3.card_type_id = trx.card_type_id
  LEFT JOIN bl_3nf.ce_receipt_types rt3 ON rt3.receipt_type_id = trx.receipt_type_id
  LEFT JOIN bl_3nf.ce_payment_gateways pg3 ON pg3.payment_gateway_id = trx.payment_gateway_id
  LEFT JOIN bl_3nf.ce_order_statuses os3 ON os3.order_status_id = trx.order_status_id
  LEFT JOIN bl_3nf.ce_shifts sh3 ON sh3.shift_id = trx.shift_id

  LEFT JOIN bl_dm.dim_junk_context djnk
    ON djnk.sales_channel    = sch3.sales_channel_name
   AND djnk.payment_method  = pm3.payment_method_name
   AND djnk.card_type       = ct3.card_type_name
   AND djnk.receipt_type    = rt3.receipt_type_name
   AND djnk.payment_gateway = pg3.payment_gateway_name
   AND djnk.order_status    = os3.order_status_name
   AND djnk.shift_name      = sh3.shift_src_id  
   AND djnk.device_type_id  = trx.device_type_id

  WHERE trx.txn_ts >= v_from_date
    AND trx.txn_ts <  v_to_date

  ON CONFLICT (date_id, txn_src_id)
  DO UPDATE SET
    customer_id               = EXCLUDED.customer_id,
    product_id                = EXCLUDED.product_id,
    store_id                  = EXCLUDED.store_id,
    employee_id               = EXCLUDED.employee_id,
    terminal_id               = EXCLUDED.terminal_id,
    promotion_id              = EXCLUDED.promotion_id,
    delivery_provider_id      = EXCLUDED.delivery_provider_id,
    junk_context_id           = EXCLUDED.junk_context_id,
    promised_delivery_date_id = EXCLUDED.promised_delivery_date_id,
    txn_ts_src                = EXCLUDED.txn_ts_src,
    tracking_id_src_id        = EXCLUDED.tracking_id_src_id,
    qty                       = EXCLUDED.qty,
    unit_price_amt            = EXCLUDED.unit_price_amt,
    discount_amt              = EXCLUDED.discount_amt,
    tax_amt                   = EXCLUDED.tax_amt,
    shipping_fee_amt          = EXCLUDED.shipping_fee_amt,
    sales_amt                 = EXCLUDED.sales_amt,
    cost_amt                  = EXCLUDED.cost_amt,
    gross_profit_amt          = EXCLUDED.gross_profit_amt,
    loyalty_points_earned     = EXCLUDED.loyalty_points_earned,
    customer_rating           = EXCLUDED.customer_rating,
    calculated_gross_margin_pct = EXCLUDED.calculated_gross_margin_pct,
    calculated_net_sales_amt  = EXCLUDED.calculated_net_sales_amt
  WHERE
    (tgt.customer_id, tgt.product_id, tgt.store_id, tgt.employee_id, tgt.terminal_id,
     tgt.promotion_id, tgt.delivery_provider_id, tgt.junk_context_id, tgt.promised_delivery_date_id,
     tgt.txn_ts_src, tgt.tracking_id_src_id,
     tgt.qty, tgt.unit_price_amt, tgt.discount_amt, tgt.tax_amt, tgt.shipping_fee_amt,
     tgt.sales_amt, tgt.cost_amt, tgt.gross_profit_amt,
     tgt.loyalty_points_earned, tgt.customer_rating,
     tgt.calculated_gross_margin_pct, tgt.calculated_net_sales_amt)
    IS DISTINCT FROM
    (EXCLUDED.customer_id, EXCLUDED.product_id, EXCLUDED.store_id, EXCLUDED.employee_id, EXCLUDED.terminal_id,
     EXCLUDED.promotion_id, EXCLUDED.delivery_provider_id, EXCLUDED.junk_context_id, EXCLUDED.promised_delivery_date_id,
     EXCLUDED.txn_ts_src, EXCLUDED.tracking_id_src_id,
     EXCLUDED.qty, EXCLUDED.unit_price_amt, EXCLUDED.discount_amt, EXCLUDED.tax_amt, EXCLUDED.shipping_fee_amt,
     EXCLUDED.sales_amt, EXCLUDED.cost_amt, EXCLUDED.gross_profit_amt,
     EXCLUDED.loyalty_points_earned, EXCLUDED.customer_rating,
     EXCLUDED.calculated_gross_margin_pct, EXCLUDED.calculated_net_sales_amt);

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows,'DM fct_sales_daily loaded (rolling window)',NULL,'bl_dm','fct_sales_daily',v_run_id);

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,'bl_dm','fct_sales_daily',v_run_id);
  RAISE;
END;
$$;
-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_fct_sales_daily_dm(3);



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


