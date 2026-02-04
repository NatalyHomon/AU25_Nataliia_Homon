-- 0) safe to re-run
BEGIN;

CREATE EXTENSION IF NOT EXISTS file_fdw;


CREATE SERVER IF NOT EXISTS sa_sales_file_srv
  FOREIGN DATA WRAPPER file_fdw;

COMMIT;

BEGIN;

CREATE SCHEMA IF NOT EXISTS sa_sales_online;

DROP FOREIGN TABLE IF EXISTS sa_sales_online.ext_sales_online;
--1) External (foreign) table (ext_...)
CREATE FOREIGN TABLE sa_sales_online.ext_sales_online (
    web_order_id              text,
    txn_ts                    timestamp,
    order_status              text,
    customer_src_id           text,
    customer_first_name       text,
    customer_last_name        text,
    customer_email            text,
    customer_phone            text,
    gender                    text,
    customer_age              integer,
    customer_age_group        text,
    customer_segment          text,
    country                   text,
    region                    text,
    city                      text,
    delivery_postal_code      text,
    delivery_address_line1    text,
    fulfillment_center_id     text,
    fulfillment_city          text,
    delivery_type             text,
    carrier_name              text,
    tracking_id               text,
    promised_delivery_dt      date,
    device_type               text,
    payment_gateway           text,
    promo_code                text,
    discount_pct              numeric(5,2),
    discount_amt              numeric(12,2),
    shipping_fee_amt          numeric(12,2),
    product_dept              text,
    product_subcategory       text,
    product_sku               text,
    product_name              text,
    brand                     text,
    unit_of_measure           text,
    supplier_id               text,
    unit_price_amt            numeric(12,2),
    qty                       integer,
    tax_amt                   numeric(12,2),
    sales_amt                 numeric(14,2),
    cost_amt                  numeric(14,2),
    gross_profit_amt          numeric(14,2),
    customer_rating           numeric(4,1)
)
SERVER sa_sales_file_srv
OPTIONS (
    filename 'C:/src_data/src_sales_online.csv',
    format 'csv',
    header 'true',
    delimiter ',',
    null '',
    encoding 'UTF8'
);
COMMIT;

--2) Source table (src_...)
BEGIN;

DROP TABLE IF EXISTS sa_sales.src_sales_online;

CREATE TABLE sa_sales_online.src_sales_online (
    web_order_id              text NOT NULL,
    txn_ts                    timestamp,
    order_status              text,
    customer_src_id           text,
    customer_first_name       text,
    customer_last_name        text,
    customer_email            text,
    customer_phone            text,
    gender                    text,
    customer_age              integer,
    customer_age_group        text,
    customer_segment          text,
    country                   text,
    region                    text,
    city                      text,
    delivery_postal_code      text,
    delivery_address_line1    text,
    fulfillment_center_id     text,
    fulfillment_city          text,
    delivery_type             text,
    carrier_name              text,
    tracking_id               text,
    promised_delivery_dt      date,
    device_type               text,
    payment_gateway           text,
    promo_code                text,
    discount_pct              numeric(5,2),
    discount_amt              numeric(12,2),
    shipping_fee_amt          numeric(12,2),
    product_dept              text,
    product_subcategory       text,
    product_sku               text NOT NULL,
    product_name              text,
    brand                     text,
    unit_of_measure           text,
    supplier_id               text,
    unit_price_amt            numeric(12,2),
    qty                       integer,
    tax_amt                   numeric(12,2),
    sales_amt                 numeric(14,2),
    cost_amt                  numeric(14,2),
    gross_profit_amt          numeric(14,2),
    customer_rating           numeric(4,1),

    -- technical fields
    load_dts                  timestamptz NOT NULL DEFAULT now(),
    source_file               text,
    
   CONSTRAINT pk_src_sales_online PRIMARY KEY (web_order_id, product_sku)
);
COMMIT;

BEGIN;

INSERT INTO sa_sales_online.src_sales_online (
    web_order_id,
    txn_ts,
    order_status,
    customer_src_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_phone,
    gender,
    customer_age,
    customer_age_group,
    customer_segment,
    country,
    region,
    city,
    delivery_postal_code,
    delivery_address_line1,
    fulfillment_center_id,
    fulfillment_city,
    delivery_type,
    carrier_name,
    tracking_id,
    promised_delivery_dt,
    device_type,
    payment_gateway,
    promo_code,
    discount_pct,
    discount_amt,
    shipping_fee_amt,
    product_dept,
    product_subcategory,
    product_sku,
    product_name,
    brand,
    unit_of_measure,
    supplier_id,
    unit_price_amt,
    qty,
    tax_amt,
    sales_amt,
    cost_amt,
    gross_profit_amt,
    customer_rating,
    source_file
)
SELECT
    web_order_id,
    txn_ts,
    order_status,
    customer_src_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_phone,
    gender,
    customer_age,
    customer_age_group,
    customer_segment,
    country,
    region,
    city,
    delivery_postal_code,
    delivery_address_line1,
    fulfillment_center_id,
    fulfillment_city,
    delivery_type,
    carrier_name,
    tracking_id,
    promised_delivery_dt,
    device_type,
    payment_gateway,
    promo_code,
    discount_pct,
    discount_amt,
    shipping_fee_amt,
    product_dept,
    product_subcategory,
    product_sku,
    product_name,
    brand,
    unit_of_measure,
    supplier_id,
    unit_price_amt,
    qty,
    tax_amt,
    sales_amt,
    cost_amt,
    gross_profit_amt,
    customer_rating,
    'src_sales_online.csv'::text
FROM sa_sales_online.ext_sales_online
ON CONFLICT (web_order_id, product_sku)
DO UPDATE
SET
    txn_ts             = EXCLUDED.txn_ts,
    order_status       = EXCLUDED.order_status,
    customer_src_id    = EXCLUDED.customer_src_id,
    customer_first_name= EXCLUDED.customer_first_name,
    customer_last_name = EXCLUDED.customer_last_name,
    customer_email     = EXCLUDED.customer_email,
    customer_phone     = EXCLUDED.customer_phone,
    gender             = EXCLUDED.gender,
    customer_age       = EXCLUDED.customer_age,
    customer_age_group = EXCLUDED.customer_age_group,
    customer_segment   = EXCLUDED.customer_segment,
    country            = EXCLUDED.country,
    region             = EXCLUDED.region,
    city               = EXCLUDED.city,
    delivery_postal_code   = EXCLUDED.delivery_postal_code,
    delivery_address_line1 = EXCLUDED.delivery_address_line1,
    fulfillment_center_id  = EXCLUDED.fulfillment_center_id,
    fulfillment_city       = EXCLUDED.fulfillment_city,
    delivery_type      = EXCLUDED.delivery_type,
    carrier_name       = EXCLUDED.carrier_name,
    tracking_id        = EXCLUDED.tracking_id,
    promised_delivery_dt = EXCLUDED.promised_delivery_dt,
    device_type        = EXCLUDED.device_type,
    payment_gateway    = EXCLUDED.payment_gateway,
    promo_code         = EXCLUDED.promo_code,
    discount_pct       = EXCLUDED.discount_pct,
    discount_amt       = EXCLUDED.discount_amt,
    shipping_fee_amt   = EXCLUDED.shipping_fee_amt,
    product_dept       = EXCLUDED.product_dept,
    product_subcategory= EXCLUDED.product_subcategory,
    product_name       = EXCLUDED.product_name,
    brand              = EXCLUDED.brand,
    unit_of_measure    = EXCLUDED.unit_of_measure,
    supplier_id        = EXCLUDED.supplier_id,
    unit_price_amt     = EXCLUDED.unit_price_amt,
    qty                = EXCLUDED.qty,
    tax_amt            = EXCLUDED.tax_amt,
    sales_amt          = EXCLUDED.sales_amt,
    cost_amt           = EXCLUDED.cost_amt,
    gross_profit_amt   = EXCLUDED.gross_profit_amt,
    customer_rating    = EXCLUDED.customer_rating,
    load_dts           = now(),
    source_file        = EXCLUDED.source_file;

COMMIT;
-- 4) validation queries


-- preview external data
SELECT * FROM sa_sales_online.ext_sales_online LIMIT 5;

-- preview loaded source data
SELECT * FROM sa_sales_online.src_sales_online ORDER BY load_dts DESC LIMIT 5;

-- duplicates check (should return 0 rows)
SELECT web_order_id, product_sku, COUNT(*)
FROM sa_sales_online.src_sales_online
GROUP BY web_order_id, product_sku
HAVING COUNT(*) > 1;


BEGIN;

CREATE SCHEMA IF NOT EXISTS sa_sales_pos;
-- 1) EXTERNAL TABLE: ext_sales_pos

DROP FOREIGN TABLE IF EXISTS sa_sales_pos.ext_sales_pos;

CREATE FOREIGN TABLE sa_sales_pos.ext_sales_pos (
    ckout                  text,
    txn_ts                 timestamp,
    customer_src_id        text,
    customer_phone         text,
    customer_age_group     text,
    customer_segment       text,
    product_dept           text,
    product_subcategory    text,
    product_sku            text,
    product_name           text,
    brand                  text,
    unit_of_measure        text,
    supplier_id            text,
    store_id               text,
    store_format           text,
    store_open_dt          date,
    store_open_time        time,
    store_close_time       time,
    country                text,
    region                 text,
    city                   text,
    terminal_id            text,
    terminal_type          text,
    cashier_id             text,
    cashier_first_name     text,
    cashier_last_name      text,
    cashier_dept           text,
    cashier_position       text,
    cashier_hire_dt        date,
    shift_id               text,
    payment_method         text,
    card_type              text,
    receipt_type           text,
    promo_code             text,
    promo_type             text,
    discount_pct           numeric(5,2),
    discount_amt           numeric(12,2),
    loyalty_points_earned  numeric(14,2),
    unit_price_amt         numeric(12,2),
    qty                    integer,
    tax_amt                numeric(12,2),
    sales_amt              numeric(14,2),
    cost_amt               numeric(14,2),
    gross_profit_amt       numeric(14,2),
    customer_rating        numeric(4,1)   
)
SERVER sa_sales_file_srv
OPTIONS (
    filename 'C:/src_data/src_sales_pos.csv',
    format 'csv',
    header 'true',
    delimiter ',',
    null '',
    encoding 'UTF8'
);

COMMIT;
-- 2) SOURCE TABLE: src_sales_pos (physical)
--    PK -> no duplicates + enables UPSERT
BEGIN;
DROP TABLE IF EXISTS sa_sales_pos.src_sales_pos;

CREATE TABLE sa_sales_pos.src_sales_pos (
    ckout                  text NOT NULL,
    txn_ts                 timestamp,
    customer_src_id        text,
    customer_phone         text,
    customer_age_group     text,
    customer_segment       text,
    product_dept           text,
    product_subcategory    text,
    product_sku            text NOT NULL,
    product_name           text,
    brand                  text,
    unit_of_measure        text,
    supplier_id            text,
    store_id               text,
    store_format           text,
    store_open_dt          date,
    store_open_time        time,
    store_close_time       time,
    country                text,
    region                 text,
    city                   text,
    terminal_id            text,
    terminal_type          text,
    cashier_id             text,
    cashier_first_name     text,
    cashier_last_name      text,
    cashier_dept           text,
    cashier_position       text,
    cashier_hire_dt        date,
    shift_id               text,
    payment_method         text,
    card_type              text,
    receipt_type           text,
    promo_code             text,
    promo_type             text,
    discount_pct           numeric(5,2),
    discount_amt           numeric(12,2),
    loyalty_points_earned  numeric(14,2),
    unit_price_amt         numeric(12,2),
    qty                    integer,
    tax_amt                numeric(12,2),
    sales_amt              numeric(14,2),
    cost_amt               numeric(14,2),
    gross_profit_amt       numeric(14,2),
    customer_rating        numeric(4,1),

    load_dts               timestamptz NOT NULL DEFAULT now(),
    source_file            text,

    CONSTRAINT pk_src_sales_pos
        PRIMARY KEY (ckout, product_sku)
);

COMMIT;


-- 3) REUSABLE LOAD 
BEGIN;

INSERT INTO sa_sales_pos.src_sales_pos (
    ckout,
    txn_ts,
    customer_src_id,
    customer_phone,
    customer_age_group,
    customer_segment,
    product_dept,
    product_subcategory,
    product_sku,
    product_name,
    brand,
    unit_of_measure,
    supplier_id,
    store_id,
    store_format,
    store_open_dt,
    store_open_time,
    store_close_time,
    country,
    region,
    city,
    terminal_id,
    terminal_type,
    cashier_id,
    cashier_first_name,
    cashier_last_name,
    cashier_dept,
    cashier_position,
    cashier_hire_dt,
    shift_id,
    payment_method,
    card_type,
    receipt_type,
    promo_code,
    promo_type,
    discount_pct,
    discount_amt,
    loyalty_points_earned,
    unit_price_amt,
    qty,
    tax_amt,
    sales_amt,
    cost_amt,
    gross_profit_amt,
    customer_rating,
    source_file
)
SELECT
    ckout,
    txn_ts,
    customer_src_id,
    customer_phone,
    customer_age_group,
    customer_segment,
    product_dept,
    product_subcategory,
    product_sku,
    product_name,
    brand,
    unit_of_measure,
    supplier_id,
    store_id,
    store_format,
    store_open_dt,
    store_open_time,
    store_close_time,
    country,
    region,
    city,
    terminal_id,
    terminal_type,
    cashier_id,
    cashier_first_name,
    cashier_last_name,
    cashier_dept,
    cashier_position,
    cashier_hire_dt,
    shift_id,
    payment_method,
    card_type,
    receipt_type,
    promo_code,
    promo_type,
    discount_pct,
    discount_amt,
    loyalty_points_earned,
    unit_price_amt,
    qty,
    tax_amt,
    sales_amt,
    cost_amt,
    gross_profit_amt,
    customer_rating,
    'src_sales_pos.csv'::text
FROM sa_sales_pos.ext_sales_pos
ON CONFLICT (ckout, product_sku)
DO UPDATE
SET
    txn_ts                = EXCLUDED.txn_ts,
    customer_src_id       = EXCLUDED.customer_src_id,
    customer_phone        = EXCLUDED.customer_phone,
    customer_age_group    = EXCLUDED.customer_age_group,
    customer_segment      = EXCLUDED.customer_segment,
    product_dept          = EXCLUDED.product_dept,
    product_subcategory   = EXCLUDED.product_subcategory,
    product_name          = EXCLUDED.product_name,
    brand                 = EXCLUDED.brand,
    unit_of_measure       = EXCLUDED.unit_of_measure,
    supplier_id           = EXCLUDED.supplier_id,
    store_id              = EXCLUDED.store_id,
    store_format          = EXCLUDED.store_format,
    store_open_dt         = EXCLUDED.store_open_dt,
    store_open_time       = EXCLUDED.store_open_time,
    store_close_time      = EXCLUDED.store_close_time,
    country               = EXCLUDED.country,
    region                = EXCLUDED.region,
    city                  = EXCLUDED.city,
    terminal_id           = EXCLUDED.terminal_id,
    terminal_type         = EXCLUDED.terminal_type,
    cashier_id            = EXCLUDED.cashier_id,
    cashier_first_name    = EXCLUDED.cashier_first_name,
    cashier_last_name     = EXCLUDED.cashier_last_name,
    cashier_dept          = EXCLUDED.cashier_dept,
    cashier_position      = EXCLUDED.cashier_position,
    cashier_hire_dt       = EXCLUDED.cashier_hire_dt,
    shift_id              = EXCLUDED.shift_id,
    payment_method        = EXCLUDED.payment_method,
    card_type             = EXCLUDED.card_type,
    receipt_type          = EXCLUDED.receipt_type,
    promo_code            = EXCLUDED.promo_code,
    promo_type            = EXCLUDED.promo_type,
    discount_pct          = EXCLUDED.discount_pct,
    discount_amt          = EXCLUDED.discount_amt,
    loyalty_points_earned = EXCLUDED.loyalty_points_earned,
    unit_price_amt        = EXCLUDED.unit_price_amt,
    qty                   = EXCLUDED.qty,
    tax_amt               = EXCLUDED.tax_amt,
    sales_amt             = EXCLUDED.sales_amt,
    cost_amt              = EXCLUDED.cost_amt,
    gross_profit_amt      = EXCLUDED.gross_profit_amt,
    customer_rating       = EXCLUDED.customer_rating,
    load_dts              = now(),
    source_file           = EXCLUDED.source_file;

COMMIT;
-- Quick checks 
SELECT * FROM sa_sales_pos.ext_sales_pos LIMIT 5;
SELECT * FROM sa_sales_pos.src_sales_pos ORDER BY load_dts DESC LIMIT 5;

SELECT ckout, product_sku, COUNT(*)
FROM sa_sales_pos.src_sales_pos
GROUP BY ckout, product_sku
HAVING COUNT(*) > 1;