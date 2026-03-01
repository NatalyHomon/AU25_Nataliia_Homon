/* BL_DM – Dimensional Model (Sales)
   DB: PostgreSQL
    */


-- 1) Schema
BEGIN;
CREATE SCHEMA IF NOT EXISTS bl_dm;

-- 2) Sequences (Surrogate keys)(in )
/*In this diagram, I decided to show a self-documented version of creating a sequence.
  However, I will note again that in Postgres SQL, such creation is by default.*/
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_products      AS BIGINT START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_stores        AS BIGINT START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_terminals     AS BIGINT START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_employees     AS BIGINT START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_promotions    AS BIGINT START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_delivery_prv  AS BIGINT START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_junk_context  AS BIGINT START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bl_dm.seq_dim_customers_scd AS BIGINT START WITH 1 INCREMENT BY 1;



-- 3) Dimensions

-- 3.1 DIM_PRODUCTS (SCD1/0)
CREATE TABLE IF NOT EXISTS bl_dm.dim_products (
    product_id               BIGINT DEFAULT nextval('bl_dm.seq_dim_products'),
    product_sku_src_id        VARCHAR(100) NOT NULL,
    product_name             VARCHAR(150) NOT NULL,
    product_subcategory_name  VARCHAR(60)  NOT NULL,
    product_department_name   VARCHAR(60)  NOT NULL,
    brand_name               VARCHAR(60)  NOT NULL,
    unit_of_measure          VARCHAR(20)  NOT NULL,
    supplier_src_id          VARCHAR(100) NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

        
    CONSTRAINT pk_dim_products PRIMARY KEY (product_id)
);

-- 3.2 DIM_STORES (SCD1/0)
CREATE TABLE IF NOT EXISTS bl_dm.dim_stores (
    store_id                 BIGINT DEFAULT nextval('bl_dm.seq_dim_stores'),
    store_src_id             VARCHAR(100) NOT NULL,
    store_format             VARCHAR(40)  NOT NULL,
    store_open_dt            DATE         NOT NULL,
    store_open_time          TIME         NOT NULL,
    store_close_time         TIME         NOT NULL,
    city_name                VARCHAR(60)  NOT NULL,
    region_name              VARCHAR(60)  NOT NULL,
    country_name             VARCHAR(60)  NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

        
    CONSTRAINT pk_dim_stores PRIMARY KEY (store_id)
);

-- 3.3 DIM_TERMINALS (SCD1/0)
CREATE TABLE IF NOT EXISTS bl_dm.dim_terminals (
    terminal_id              BIGINT DEFAULT nextval('bl_dm.seq_dim_terminals'),
    terminal_src_id          VARCHAR(100) NOT NULL,
    terminal_type_name       VARCHAR(40)  NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

       
    CONSTRAINT pk_dim_terminals PRIMARY KEY (terminal_id)
);

-- 3.4 DIM_EMPLOYEES (SCD1/0)
CREATE TABLE IF NOT EXISTS bl_dm.dim_employees (
    employee_id              BIGINT  DEFAULT nextval('bl_dm.seq_dim_employees'),
    employee_src_id          VARCHAR(100) NOT NULL,
    first_name               VARCHAR(100) NOT NULL,
    last_name                VARCHAR(100) NOT NULL,
    department               VARCHAR(60)  NOT NULL,
    position                 VARCHAR(60)  NOT NULL,
    hire_dt                  DATE         NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

       
    CONSTRAINT pk_dim_employees PRIMARY KEY (employee_id)
);

-- 3.5 DIM_PROMOTIONS (SCD1/0)
CREATE TABLE IF NOT EXISTS bl_dm.dim_promotions (
    promotion_id             BIGINT DEFAULT nextval('bl_dm.seq_dim_promotions'),
    promo_code               VARCHAR(60)  NOT NULL,
    discount_pct             DECIMAL(5,2) NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

    
    CONSTRAINT pk_dim_promotions PRIMARY KEY (promotion_id)
);

-- 3.6 DIM_DELIVERY_PROVIDERS (SCD1/0)
CREATE TABLE IF NOT EXISTS bl_dm.dim_delivery_providers (
    delivery_provider_id     BIGINT DEFAULT nextval('bl_dm.seq_dim_delivery_prv'),
    carrier_name             VARCHAR(60)  NOT NULL,
    delivery_type_name       VARCHAR(40)  NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

       
    CONSTRAINT pk_dim_delivery_providers PRIMARY KEY (delivery_provider_id)
);

-- 3.7 DIM_JUNK_CONTEXT (SCD0/1)
CREATE TABLE IF NOT EXISTS bl_dm.dim_junk_context (
    junk_context_id          BIGINT  DEFAULT nextval('bl_dm.seq_dim_junk_context'),
    sales_channel            VARCHAR(20)  NOT NULL,
    payment_method           VARCHAR(40)  NOT NULL,
    card_type                VARCHAR(40)  NOT NULL,
    receipt_type             VARCHAR(40)  NOT NULL,
    payment_gateway          VARCHAR(40)  NOT NULL,
    order_status             VARCHAR(40)  NOT NULL,
    shift_name               VARCHAR(60)  NOT NULL,
    device_type_id           BIGINT       NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

        
    CONSTRAINT pk_dim_junk_context PRIMARY KEY (junk_context_id)
);

-- 3.8 DIM_DATES_DAY (Calendar dimension)
CREATE TABLE IF NOT EXISTS bl_dm.dim_dates_day (
    date_id                  INT,
    day                      SMALLINT     NOT NULL,
    day_of_week              SMALLINT     NOT NULL,
    day_name                 VARCHAR(20)  NOT NULL,
    week                     SMALLINT     NOT NULL,
    week_of_year             SMALLINT     NOT NULL,
    month                    SMALLINT     NOT NULL,
    month_name               VARCHAR(20)  NOT NULL,
    quarter                  SMALLINT     NOT NULL,
    year                     SMALLINT     NOT NULL,
    is_weekend               BOOLEAN      NOT NULL,

    CONSTRAINT pk_dim_dates_day PRIMARY KEY (date_id)
    );

-- 3.9 DIM_CUSTOMERS_SCD (SCD2)
CREATE TABLE IF NOT EXISTS bl_dm.dim_customers_scd (
    customer_id              BIGINT DEFAULT nextval('bl_dm.seq_dim_customers_scd'),
    customer_src_id          VARCHAR(100) NOT NULL,
    age_group                VARCHAR(20)  NOT NULL,
    email                    VARCHAR(255) NOT NULL,
    phone                    VARCHAR(30)  NOT NULL,
    customer_segment         VARCHAR(30)  NOT NULL,
    gender                   VARCHAR(20)  NOT NULL,

    start_dt                 DATE         NOT NULL,
    end_dt                   DATE         NOT NULL,
    is_active                BOOLEAN      NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

    CONSTRAINT pk_dim_customers_scd PRIMARY KEY (customer_id)
);
COMMIT;


-- 4) Fact: FCT_SALES_DD (daily grain / transaction line)
BEGIN;
CREATE TABLE IF NOT EXISTS bl_dm.fct_sales_daily (
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

    -- Degenerate dimensions (DD) / source identifiers
    txn_src_id               VARCHAR(100),
    txn_ts_src               TIMESTAMP,
    tracking_id_src_id       VARCHAR(100),

    -- Metrics 
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
    calculated_net_sales_amt DECIMAL(12,2),

   
    CONSTRAINT fk_fct_sales_dd_date
        FOREIGN KEY (date_id)
        REFERENCES bl_dm.dim_dates_day (date_id),

    CONSTRAINT fk_fct_sales_dd_promised_date
        FOREIGN KEY (promised_delivery_date_id)
        REFERENCES bl_dm.dim_dates_day (date_id),

    CONSTRAINT fk_fct_sales_dd_customer
        FOREIGN KEY (customer_id)
        REFERENCES bl_dm.dim_customers_scd (customer_id),

    CONSTRAINT fk_fct_sales_dd_product
        FOREIGN KEY (product_id)
        REFERENCES bl_dm.dim_products (product_id),

    CONSTRAINT fk_fct_sales_dd_store
        FOREIGN KEY (store_id)
        REFERENCES bl_dm.dim_stores (store_id),

    CONSTRAINT fk_fct_sales_dd_employee
        FOREIGN KEY (employee_id)
        REFERENCES bl_dm.dim_employees (employee_id),

    CONSTRAINT fk_fct_sales_dd_terminal
        FOREIGN KEY (terminal_id)
        REFERENCES bl_dm.dim_terminals (terminal_id),

    CONSTRAINT fk_fct_sales_dd_promotion
        FOREIGN KEY (promotion_id)
        REFERENCES bl_dm.dim_promotions (promotion_id),

    CONSTRAINT fk_fct_sales_dd_delivery_provider
        FOREIGN KEY (delivery_provider_id)
        REFERENCES bl_dm.dim_delivery_providers (delivery_provider_id),

    CONSTRAINT fk_fct_sales_dd_junk_context
        FOREIGN KEY (junk_context_id)
        REFERENCES bl_dm.dim_junk_context (junk_context_id)
);
COMMIT;


-- 6) Indexes on fact tables are created on foreign keys and time attributes to optimize filtering and aggregations
BEGIN;
CREATE INDEX IF NOT EXISTS ix_fct_sales_daily_date         ON bl_dm.fct_sales_daily(date_id);
CREATE INDEX IF NOT EXISTS ix_fct_sales_daily_customer     ON bl_dm.fct_sales_daily(customer_id);
CREATE INDEX IF NOT EXISTS ix_fct_sales_daily_store        ON bl_dm.fct_sales_daily(store_id);
CREATE INDEX IF NOT EXISTS ix_fct_sales_daily_product      ON bl_dm.fct_sales_daily(product_id);
COMMIT;
