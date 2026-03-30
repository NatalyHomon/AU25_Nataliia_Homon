    
 CREATE TABLE IF NOT EXISTS dilab_student73.dim_customers_scd (
    customer_id        BIGINT,
    customer_src_id    VARCHAR(100),
    age_group          VARCHAR(50),
    email              VARCHAR(255),
    phone              VARCHAR(50),
    customer_segment   VARCHAR(100),
    gender             VARCHAR(20),
    start_ts           TIMESTAMP,
    end_ts             TIMESTAMP,
    is_active          BOOLEAN,
    source_system      VARCHAR(100),
    source_entity      VARCHAR(100),
    source_id          VARCHAR(100)
);

COPY dilab_student73.dim_customers_scd
(
    customer_id,
    customer_src_id,
    age_group,
    email,
    phone,
    customer_segment,
    gender,
    start_ts,
    end_ts,
    is_active,
    source_system,
    source_entity,
    source_id
)
FROM 's3://nataliia-bl-dm-260586643565/di_dwh_database/bl_dm/dim_customers_scd/dim_customers_scd.csv'
CREDENTIALS 'aws_iam_role=arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ','
CSV
IGNOREHEADER 1;

SELECT * FROM dilab_student73.dim_customers_scd;

CREATE TABLE IF NOT EXISTS dilab_student73.dim_products (
    product_id               BIGINT ,
    product_sku_src_id       VARCHAR(100),
    product_name             VARCHAR(150),
    product_subcategory_name VARCHAR(60),
    product_department_name  VARCHAR(60),
    brand_name               VARCHAR(60),
    unit_of_measure          VARCHAR(20),
    supplier_src_id          VARCHAR(100),

    source_system            VARCHAR(30),
    source_entity            VARCHAR(60),
    source_id                VARCHAR(100)
);

COPY dilab_student73.dim_products
(   product_id,
    product_sku_src_id,
    product_name,
    product_subcategory_name,
    product_department_name,
    brand_name,
    unit_of_measure,
    supplier_src_id,
    source_system,
    source_entity,
    source_id
)
FROM 's3://nataliia-bl-dm-260586643565/di_dwh_database/bl_dm/dim_products/dim_products.csv'
CREDENTIALS 'aws_iam_role=arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ','
CSV
IGNOREHEADER 1;

SELECT * FROM dilab_student73.dim_products;

CREATE TABLE IF NOT EXISTS dilab_student73.dim_dates_day (
    date_id        INT,
    day            SMALLINT,
    day_of_week    SMALLINT,
    day_name       VARCHAR(50),
    week           SMALLINT,
    week_of_year   SMALLINT,
    month          SMALLINT,
    month_name     VARCHAR(50),
    quarter        SMALLINT,
    year           SMALLINT,
    is_weekend     BOOLEAN
);

COPY dilab_student73.dim_dates_day
(
    date_id,
    "day",
    day_of_week,
    day_name,
    "week",
    week_of_year,
    "month",
    month_name,
    "quarter",
    "year",
    is_weekend
)
FROM 's3://nataliia-bl-dm-260586643565/di_dwh_database/bl_dm/dim_dates_day/dim_dates_day.csv'
CREDENTIALS 'aws_iam_role=arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ','
CSV
IGNOREHEADER 1;

SELECT* FROM dilab_student73.dim_dates_day;

CREATE TABLE IF NOT EXISTS dilab_student73.fct_sales_daily (
    date_id                  INT,
    customer_id              BIGINT,
    product_id               BIGINT,
    store_id                 BIGINT,
    employee_id              BIGINT,
    terminal_id              BIGINT,
    promotion_id             BIGINT,
    delivery_provider_id     BIGINT,
    junk_context_id          BIGINT,
    promised_delivery_date_id INT,

    txn_src_id               VARCHAR(100),
    txn_ts_src               TIMESTAMP,
    tracking_id_src_id       VARCHAR(100),

    qty                      INT,
    unit_price_amt           DECIMAL(12,2),
    discount_amt             DECIMAL(12,2),
    tax_amt                  DECIMAL(12,2),
    shipping_fee_amt         DECIMAL(12,2),
    sales_amt                DECIMAL(12,2),
    cost_amt                 DECIMAL(12,2),
    gross_profit_amt         DECIMAL(12,2),
    loyalty_points_earned    INT,
    customer_rating          DECIMAL(4,1),
    calculated_gross_margin_pct DECIMAL(5,2),
    calculated_net_sales_amt DECIMAL(12,2)
);

COPY dilab_student73.fct_sales_daily
(
    date_id,
    customer_id,
    product_id,
    store_id,
    employee_id,
    terminal_id,
    promotion_id,
    delivery_provider_id,
    junk_context_id,
    promised_delivery_date_id,

    txn_src_id,
    txn_ts_src,
    tracking_id_src_id,

    qty,
    unit_price_amt,
    discount_amt,
    tax_amt,
    shipping_fee_amt,
    sales_amt,
    cost_amt,
    gross_profit_amt,
    loyalty_points_earned,
    customer_rating,
    calculated_gross_margin_pct,
    calculated_net_sales_amt
)
FROM 's3://nataliia-bl-dm-260586643565/di_dwh_database/bl_dm/fct_sales_daily/fct_sales_daily.csv'
CREDENTIALS 'aws_iam_role=arn:aws:iam::260586643565:role/dilab-redshift-role'
REGION 'eu-central-1'
DELIMITER ','
CSV
IGNOREHEADER 1;

SELECT * FROM dilab_student73.fct_sales_daily
LIMIT 100;

----check -compression, dist style, sort keys
SELECT 
    "table",
    diststyle,
    sortkey1,
    sortkey_num,
    encoded
FROM svv_table_info
WHERE schema = 'dilab_student73';

SELECT
    schemaname,
    tablename,
    "column",
    type,
    encoding,
    distkey,
    sortkey
FROM pg_table_def
WHERE schemaname = 'dilab_student73'
ORDER BY tablename, sortkey DESC, "column";

--fct_sales_daily current compression
SELECT
    schemaname,
    tablename,
    "column",
    type,
    encoding,
    distkey,
    sortkey
FROM pg_table_def
WHERE schemaname = 'dilab_student73'
  AND tablename = 'fct_sales_daily'
ORDER BY sortkey DESC, "column";

--fct_sales_daily without compression
DROP TABLE IF EXISTS dilab_student73.fct_sales_daily_withoutcomp;


CREATE TABLE dilab_student73.fct_sales_daily_withoutcomp (
    date_id                    INT ENCODE RAW,
    customer_id                BIGINT ENCODE RAW,
    product_id                 BIGINT ENCODE RAW,
    store_id                   BIGINT ENCODE RAW,
    employee_id                BIGINT ENCODE RAW,
    terminal_id                BIGINT ENCODE RAW,
    promotion_id               BIGINT ENCODE RAW,
    delivery_provider_id       BIGINT ENCODE RAW,
    junk_context_id            BIGINT ENCODE RAW,
    promised_delivery_date_id  INT ENCODE RAW,

    txn_src_id                 VARCHAR(100) ENCODE RAW,
    txn_ts_src                 TIMESTAMP ENCODE RAW,
    tracking_id_src_id         VARCHAR(100) ENCODE RAW,

    qty                        INT ENCODE RAW,
    unit_price_amt             DECIMAL(12,2) ENCODE RAW,
    discount_amt               DECIMAL(12,2) ENCODE RAW,
    tax_amt                    DECIMAL(12,2) ENCODE RAW,
    shipping_fee_amt           DECIMAL(12,2) ENCODE RAW,
    sales_amt                  DECIMAL(12,2) ENCODE RAW,
    cost_amt                   DECIMAL(12,2) ENCODE RAW,
    gross_profit_amt           DECIMAL(12,2) ENCODE RAW,
    loyalty_points_earned      INT ENCODE RAW,
    customer_rating            DECIMAL(4,1) ENCODE RAW,
    calculated_gross_margin_pct DECIMAL(5,2) ENCODE RAW,
    calculated_net_sales_amt   DECIMAL(12,2) ENCODE RAW
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (txn_ts_src);

INSERT INTO dilab_student73.fct_sales_daily_withoutcomp
SELECT *
FROM dilab_student73.fct_sales_daily;

select* FROM dilab_student73.fct_sales_daily_withoutcomp
LIMIT 100;


--analyze compression
ANALYZE COMPRESSION dilab_student73.fct_sales_daily;

--a new table with recommended compresiion
DROP TABLE IF EXISTS dilab_student73.fct_sales_daily_analyzedcomp;

CREATE TABLE dilab_student73.fct_sales_daily_analyzedcomp (
    date_id                    INT ENCODE AZ64,
    customer_id                BIGINT ENCODE AZ64,
    product_id                 BIGINT ENCODE AZ64,
    store_id                   BIGINT ENCODE AZ64,
    employee_id                BIGINT ENCODE AZ64,
    terminal_id                BIGINT ENCODE AZ64,
    promotion_id               BIGINT ENCODE AZ64,
    delivery_provider_id       BIGINT ENCODE AZ64,
    junk_context_id            BIGINT ENCODE AZ64,
    promised_delivery_date_id  INT ENCODE AZ64,

    txn_src_id                 VARCHAR(100) ENCODE LZO,
    txn_ts_src                 TIMESTAMP ENCODE AZ64,
    tracking_id_src_id         VARCHAR(100) ENCODE LZO,

    qty                        INT ENCODE AZ64,
    unit_price_amt             DECIMAL(12,2) ENCODE AZ64,
    discount_amt               DECIMAL(12,2) ENCODE AZ64,
    tax_amt                    DECIMAL(12,2) ENCODE AZ64,
    shipping_fee_amt           DECIMAL(12,2) ENCODE AZ64,
    sales_amt                  DECIMAL(12,2) ENCODE AZ64,
    cost_amt                   DECIMAL(12,2) ENCODE AZ64,
    gross_profit_amt           DECIMAL(12,2) ENCODE AZ64,
    loyalty_points_earned      INT ENCODE AZ64,
    customer_rating            DECIMAL(4,1) ENCODE AZ64,
    calculated_gross_margin_pct DECIMAL(5,2) ENCODE AZ64,
    calculated_net_sales_amt   DECIMAL(12,2) ENCODE AZ64
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (txn_ts_src);

INSERT INTO dilab_student73.fct_sales_daily_analyzedcomp
SELECT *
FROM dilab_student73.fct_sales_daily;

SELECT * FROM dilab_student73.fct_sales_daily_analyzedcomp
LIMIT 100;

--Compare table size
SELECT
    "table",
    encoded,
    diststyle,
    sortkey1,
    size,
    tbl_rows
FROM svv_table_info
WHERE schema = 'dilab_student73'
  AND "table" IN (
      'fct_sales_daily',
      'fct_sales_daily_withoutcomp',
      'fct_sales_daily_analyzedcomp'
  )
ORDER BY "table";

--Compare table size by columns
SELECT
    t.name AS table_name,
    a.attname AS column_name,
    COUNT(*) AS blocks_1mb
FROM stv_blocklist b
JOIN stv_tbl_perm t
  ON b.tbl = t.id
JOIN pg_attribute a
  ON a.attrelid = t.id
 AND a.attnum = b.col
WHERE t.name IN (
    'fct_sales_daily',
    'fct_sales_daily_withoutcomp',
    'fct_sales_daily_analyzedcomp'
)
GROUP BY t.name, a.attname
ORDER BY a.attname, t.name;

SELECT 
    a.attname AS column_name,
    SUM(CASE WHEN t.name = 'fct_sales_daily' THEN 1 ELSE 0 END) AS sizemb_default,
    SUM(CASE WHEN t.name = 'fct_sales_daily_withoutcomp' THEN 1 ELSE 0 END) AS sizemb_raw,
    SUM(CASE WHEN t.name = 'fct_sales_daily_analyzedcomp' THEN 1 ELSE 0 END) AS sizemb_analyzed
FROM stv_blocklist b
JOIN stv_tbl_perm t
    ON b.tbl = t.id
JOIN pg_attribute a
    ON a.attrelid = t.id
   AND a.attnum = b.col
WHERE t.name IN (
    'fct_sales_daily',
    'fct_sales_daily_withoutcomp',
    'fct_sales_daily_analyzedcomp'
)
GROUP BY a.attname
ORDER BY a.attname;


--task 4distribution
set enable_result_cache_for_session = off;

 
 EXPLAIN WITH sales_base AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        c.customer_segment,
        c.age_group,
        p.product_department_name,
        p.brand_name,
        f.txn_ts_src,
        f.txn_src_id,
        f.qty,
        f.sales_amt,
        f.discount_amt,
        f.gross_profit_amt,
        f.customer_rating,
        f.calculated_net_sales_amt
    FROM dilab_student73.fct_sales_daily f
    JOIN dilab_student73.dim_dates_day d
      ON f.date_id = d.date_id
    JOIN dilab_student73.dim_customers_scd c
      ON f.customer_id = c.customer_id
    JOIN dilab_student73.dim_products p
      ON f.product_id = p.product_id
    WHERE d.year IN (2025, 2026)
      AND (
            (d.year = 2025 AND d.month IN (11, 12))
         OR (d.year = 2026 AND d.month = 1)
      )
      AND c.customer_segment IN ('loyalty', 'walk_in')
),
agg AS (
    SELECT
        year,
        month,
        month_name,
        customer_segment,
        age_group,
        product_department_name,
        brand_name,
        COUNT(DISTINCT txn_src_id) AS orders_cnt,
        SUM(qty) AS total_qty,
        SUM(calculated_net_sales_amt) AS total_net_sales_amt,
        SUM(gross_profit_amt) AS total_gross_profit_amt,
        SUM(discount_amt) AS total_discount_amt,
        AVG(customer_rating) AS avg_customer_rating
    FROM sales_base
    GROUP BY
        year,
        month,
        month_name,
        customer_segment,
        age_group,
        product_department_name,
        brand_name
)
SELECT
    year,
    month,
    month_name,
    customer_segment,
    age_group,
    product_department_name,
    brand_name,
    orders_cnt,
    total_qty,
    total_net_sales_amt,
    total_gross_profit_amt,
    total_discount_amt,
    avg_customer_rating,
    RANK() OVER (
        PARTITION BY year, month, customer_segment
        ORDER BY total_net_sales_amt DESC
    ) AS sales_rank_in_segment_month
FROM agg
ORDER BY
    year,
    month,
    customer_segment,
    sales_rank_in_segment_month,
    total_net_sales_amt DESC;
 
 create or replace procedure dilab_student73.sp_load_sales_report()
language plpgsql
as $$
begin
    drop table if exists dilab_student73.sales_report;

    create table dilab_student73.sales_report as
WITH sales_base AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        c.customer_segment,
        c.age_group,
        p.product_department_name,
        p.brand_name,
        f.txn_ts_src,
        f.txn_src_id,
        f.qty,
        f.sales_amt,
        f.discount_amt,
        f.gross_profit_amt,
        f.customer_rating,
        f.calculated_net_sales_amt
    FROM dilab_student73.fct_sales_daily f
    JOIN dilab_student73.dim_dates_day d
      ON f.date_id = d.date_id
    JOIN dilab_student73.dim_customers_scd c
      ON f.customer_id = c.customer_id
    JOIN dilab_student73.dim_products p
      ON f.product_id = p.product_id
    WHERE d.year IN (2025, 2026)
      AND (
            (d.year = 2025 AND d.month IN (11, 12))
         OR (d.year = 2026 AND d.month = 1)
      )
      AND c.customer_segment IN ('loyalty', 'walk_in')
),
agg AS (
    SELECT
        year,
        month,
        month_name,
        customer_segment,
        age_group,
        product_department_name,
        brand_name,
        COUNT(DISTINCT txn_src_id) AS orders_cnt,
        SUM(qty) AS total_qty,
        SUM(calculated_net_sales_amt) AS total_net_sales_amt,
        SUM(gross_profit_amt) AS total_gross_profit_amt,
        SUM(discount_amt) AS total_discount_amt,
        AVG(customer_rating) AS avg_customer_rating
    FROM sales_base
    GROUP BY
        year,
        month,
        month_name,
        customer_segment,
        age_group,
        product_department_name,
        brand_name
)
SELECT
    year,
    month,
    month_name,
    customer_segment,
    age_group,
    product_department_name,
    brand_name,
    orders_cnt,
    total_qty,
    total_net_sales_amt,
    total_gross_profit_amt,
    total_discount_amt,
    avg_customer_rating,
    RANK() OVER (
        PARTITION BY year, month, customer_segment
        ORDER BY total_net_sales_amt DESC
    ) AS sales_rank_in_segment_month
FROM agg
ORDER BY
    year,
    month,
    customer_segment,
    sales_rank_in_segment_month,
    total_net_sales_amt DESC;
    
end;
$$;

CALL dilab_student73.sp_load_sales_report();

SELECT * FROM dilab_student73.sales_report;

-- 5 optimazid tables
DROP TABLE IF EXISTS dilab_student73.fct_sales_daily_opt;

CREATE TABLE dilab_student73.fct_sales_daily_opt (
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
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (date_id, product_id);

DROP TABLE IF EXISTS dilab_student73.dim_customers_scd_opt;

CREATE TABLE dilab_student73.dim_customers_scd_opt (
    customer_id        BIGINT,
    customer_src_id    VARCHAR(100),
    age_group          VARCHAR(50),
    email              VARCHAR(255),
    phone              VARCHAR(50),
    customer_segment   VARCHAR(100),
    gender             VARCHAR(20),
    start_ts           TIMESTAMP,
    end_ts             TIMESTAMP,
    is_active          BOOLEAN,
    source_system      VARCHAR(100),
    source_entity      VARCHAR(100),
    source_id          VARCHAR(100)
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (customer_id, end_ts);

DROP TABLE IF EXISTS dilab_student73.dim_products_opt;

CREATE TABLE dilab_student73.dim_products_opt (
    product_id               BIGINT,
    product_sku_src_id       VARCHAR(100),
    product_name             VARCHAR(150),
    product_subcategory_name VARCHAR(60),
    product_department_name  VARCHAR(60),
    brand_name               VARCHAR(60),
    unit_of_measure          VARCHAR(20),
    supplier_src_id          VARCHAR(100),
    source_system            VARCHAR(30),
    source_entity            VARCHAR(60),
    source_id                VARCHAR(100)
)
DISTSTYLE ALL
SORTKEY (product_id);

DROP TABLE IF EXISTS dilab_student73.dim_dates_day_opt;

CREATE TABLE dilab_student73.dim_dates_day_opt (
    date_id        INT,
    day            SMALLINT,
    day_of_week    SMALLINT,
    day_name       VARCHAR(50),
    week           SMALLINT,
    week_of_year   SMALLINT,
    month          SMALLINT,
    month_name     VARCHAR(50),
    quarter        SMALLINT,
    year           SMALLINT,
    is_weekend     BOOLEAN
)
DISTSTYLE ALL
SORTKEY (date_id);

INSERT INTO dilab_student73.fct_sales_daily_opt
SELECT * FROM dilab_student73.fct_sales_daily;

INSERT INTO dilab_student73.dim_customers_scd_opt
SELECT * FROM dilab_student73.dim_customers_scd;

INSERT INTO dilab_student73.dim_products_opt
SELECT * FROM dilab_student73.dim_products;

INSERT INTO dilab_student73.dim_dates_day_opt
SELECT * FROM dilab_student73.dim_dates_day;

SELECT
    "table",
    diststyle,
    sortkey1,
    sortkey_num,
    encoded
FROM svv_table_info
WHERE schema = 'dilab_student73'
  AND "table" IN (
      'fct_sales_daily_opt',
      'dim_customers_scd_opt',
      'dim_products_opt',
      'dim_dates_day_opt'
  )
ORDER BY "table";

EXPLAIN WITH sales_base AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        c.customer_segment,
        c.age_group,
        p.product_department_name,
        p.brand_name,
        f.txn_ts_src,
        f.txn_src_id,
        f.qty,
        f.sales_amt,
        f.discount_amt,
        f.gross_profit_amt,
        f.customer_rating,
        f.calculated_net_sales_amt
    FROM dilab_student73.fct_sales_daily_opt f
    JOIN dilab_student73.dim_dates_day_opt d
      ON f.date_id = d.date_id
    JOIN dilab_student73.dim_customers_scd_opt c
      ON f.customer_id = c.customer_id
    JOIN dilab_student73.dim_products_opt p
      ON f.product_id = p.product_id
    WHERE d.year IN (2025, 2026)
      AND (
            (d.year = 2025 AND d.month IN (11, 12))
         OR (d.year = 2026 AND d.month = 1)
      )
      AND c.customer_segment IN ('loyalty', 'walk_in')
),
agg AS (
    SELECT
        year,
        month,
        month_name,
        customer_segment,
        age_group,
        product_department_name,
        brand_name,
        COUNT(DISTINCT txn_src_id) AS orders_cnt,
        SUM(qty) AS total_qty,
        SUM(calculated_net_sales_amt) AS total_net_sales_amt,
        SUM(gross_profit_amt) AS total_gross_profit_amt,
        SUM(discount_amt) AS total_discount_amt,
        AVG(customer_rating) AS avg_customer_rating
    FROM sales_base
    GROUP BY
        year,
        month,
        month_name,
        customer_segment,
        age_group,
        product_department_name,
        brand_name
)
SELECT
    year,
    month,
    month_name,
    customer_segment,
    age_group,
    product_department_name,
    brand_name,
    orders_cnt,
    total_qty,
    total_net_sales_amt,
    total_gross_profit_amt,
    total_discount_amt,
    avg_customer_rating,
    RANK() OVER (
        PARTITION BY year, month, customer_segment
        ORDER BY total_net_sales_amt DESC
    ) AS sales_rank_in_segment_month
FROM agg
ORDER BY
    year,
    month,
    customer_segment,
    sales_rank_in_segment_month,
    total_net_sales_amt DESC;


--procedure for opt tables

CREATE OR REPLACE PROCEDURE dilab_student73.sp_load_sales_report_opt()
LANGUAGE plpgsql
AS $$
BEGIN   
    DROP TABLE IF EXISTS dilab_student73.sales_report_opt;   
    CREATE TABLE dilab_student73.sales_report_opt AS
    WITH sales_base AS (
        SELECT
            d.year,
            d.month,
            d.month_name,
            c.customer_segment,
            c.age_group,
            p.product_department_name,
            p.brand_name,
            f.txn_ts_src,
            f.txn_src_id,
            f.qty,
            f.sales_amt,
            f.discount_amt,
            f.gross_profit_amt,
            f.customer_rating,
            f.calculated_net_sales_amt
        FROM dilab_student73.fct_sales_daily_opt f
        JOIN dilab_student73.dim_dates_day_opt d
            ON f.date_id = d.date_id
        JOIN dilab_student73.dim_customers_scd_opt c
            ON f.customer_id = c.customer_id
        JOIN dilab_student73.dim_products_opt p
            ON f.product_id = p.product_id
        WHERE d.year IN (2025, 2026)
          AND (
                (d.year = 2025 AND d.month IN (11, 12))
             OR (d.year = 2026 AND d.month = 1)
          )
          AND c.customer_segment IN ('loyalty', 'walk_in')
    ),

    agg AS (
        SELECT
            year,
            month,
            month_name,
            customer_segment,
            age_group,
            product_department_name,
            brand_name,
            COUNT(DISTINCT txn_src_id) AS orders_cnt,
            SUM(qty) AS total_qty,
            SUM(calculated_net_sales_amt) AS total_net_sales_amt,
            SUM(gross_profit_amt) AS total_gross_profit_amt,
            SUM(discount_amt) AS total_discount_amt,
            AVG(customer_rating) AS avg_customer_rating
        FROM sales_base
        GROUP BY
            year,
            month,
            month_name,
            customer_segment,
            age_group,
            product_department_name,
            brand_name
    )

    SELECT
        year,
        month,
        month_name,
        customer_segment,
        age_group,
        product_department_name,
        brand_name,
        orders_cnt,
        total_qty,
        total_net_sales_amt,
        total_gross_profit_amt,
        total_discount_amt,
        avg_customer_rating,
        RANK() OVER (
            PARTITION BY year, month, customer_segment
            ORDER BY total_net_sales_amt DESC
        ) AS sales_rank_in_segment_month
    FROM agg;

END;
$$;


CALL dilab_student73.sp_load_sales_report_opt();
SELECT * FROM dilab_student73.sales_report_opt;

--external tables
CREATE EXTERNAL SCHEMA if not exists dilab_student73_ext
FROM DATA catalog
DATABASE 'di_dwh_database_nataliia_homon'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role';

SELECT *
FROM svv_external_tables
WHERE schemaname = 'dilab_student73_ext';

SELECT *
FROM dilab_student73_ext.dim_products
LIMIT 10;

--unload data by months

UNLOAD ('
    SELECT
        date_id,
        customer_id,
        product_id,
        store_id,
        employee_id,
        terminal_id,
        promotion_id,
        delivery_provider_id,
        junk_context_id,
        promised_delivery_date_id,
        txn_src_id,
        txn_ts_src,
        tracking_id_src_id,
        qty,
        unit_price_amt,
        discount_amt,
        tax_amt,
        shipping_fee_amt,
        sales_amt,
        cost_amt,
        gross_profit_amt,
        loyalty_points_earned,
        customer_rating,
        calculated_gross_margin_pct,
        calculated_net_sales_amt,
        date_trunc(''month'', txn_ts_src)::date AS txn_month
    FROM dilab_student73.fct_sales_daily
')
TO 's3://nataliia-bl-dm-260586643565/spectrum/fct_sales_daily_partitioned/'
IAM_ROLE 'arn:aws:iam::260586643565:role/dilab-redshift-role'
FORMAT PARQUET
PARTITION BY (txn_month);

CREATE EXTERNAL TABLE dilab_student73_ext.ext_student73_partitioned (
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
PARTITIONED BY (txn_month DATE)
STORED AS PARQUET
LOCATION 's3://nataliia-bl-dm-260586643565/spectrum/fct_sales_daily_partitioned/';

ALTER TABLE dilab_student73_ext.ext_student73_partitioned
ADD IF NOT EXISTS
PARTITION (txn_month='2025-10-01')
LOCATION 's3://nataliia-bl-dm-260586643565/spectrum/fct_sales_daily_partitioned/txn_month=2025-10-01/';

ALTER TABLE dilab_student73_ext.ext_student73_partitioned
ADD IF NOT EXISTS
PARTITION (txn_month='2025-11-01')
LOCATION 's3://nataliia-bl-dm-260586643565/spectrum/fct_sales_daily_partitioned/txn_month=2025-11-01/';

ALTER TABLE dilab_student73_ext.ext_student73_partitioned
ADD IF NOT EXISTS
PARTITION (txn_month='2025-12-01')
LOCATION 's3://nataliia-bl-dm-260586643565/spectrum/fct_sales_daily_partitioned/txn_month=2025-12-01/';

ALTER TABLE dilab_student73_ext.ext_student73_partitioned
ADD IF NOT EXISTS
PARTITION (txn_month='2026-01-01')
LOCATION 's3://nataliia-bl-dm-260586643565/spectrum/fct_sales_daily_partitioned/txn_month=2026-01-01/';

SELECT COUNT(*)
FROM dilab_student73_ext.ext_student73_partitioned;

WITH src AS (
    SELECT
        date_trunc('month', txn_ts_src)::date AS txn_month,
        COUNT(*) AS cnt
    FROM dilab_student73.fct_sales_daily
    GROUP BY 1
),
ext AS (
    SELECT
        txn_month,
        COUNT(*) AS cnt
    FROM dilab_student73_ext.ext_student73_partitioned
    GROUP BY 1
)
SELECT
    COALESCE(src.txn_month, ext.txn_month) AS txn_month,
    COALESCE(src.cnt, 0) AS source_cnt,
    COALESCE(ext.cnt, 0) AS external_cnt,
    COALESCE(src.cnt, 0) - COALESCE(ext.cnt, 0) AS diff
FROM src
FULL OUTER JOIN ext
    ON src.txn_month = ext.txn_month
ORDER BY 1;

EXPLAIN
SELECT COUNT(*)
FROM dilab_student73_ext.ext_student73_partitioned
WHERE txn_month = DATE '2025-12-01';


SELECT *
FROM svl_s3partition
ORDER BY query DESC
LIMIT 20;



