/*In PostgreSQL we didn’t explicitly specify START WITH 1 for sequences because it is the default behavior.
  When you create a sequence without parameters, PostgreSQL automatically uses START WITH 1 and INCREMENT BY 1.
 Therefore, CREATE SEQUENCE IF NOT EXISTS ...; already generates surrogate keys starting from 1, and adding START WITH 1 would be redundant.
 We still keep the default row with ID = -1 inserted manually,
 which does not conflict with the sequence because the sequence generates only positive values by default.
 
 */
--3NF SCHEMA
BEGIN;

CREATE SCHEMA IF NOT EXISTS BL_3NF;

COMMIT;

BEGIN;

-- SEQUENCE
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_brand_id;

-- TABLE
CREATE TABLE IF NOT EXISTS bl_3nf.ce_brands (
    brand_id        BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_brand_id'),
    brand_name      VARCHAR(60) NOT NULL,

    source_system   VARCHAR(30) NOT NULL,
    source_entity   VARCHAR(60) NOT NULL,
    source_id       VARCHAR(100) NOT NULL,

    ta_insert_dt    TIMESTAMP NOT NULL,
    ta_update_dt    TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_brands
        PRIMARY KEY (brand_id),

    -- uniqueness INSIDE source system + entity
    CONSTRAINT uk_ce_brands
        UNIQUE (brand_name, source_system, source_entity)
);

COMMIT;

BEGIN;

--ce_unit_of_measures
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_uom_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_unit_of_measures (
    uom_id        BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_uom_id'),
    uom_name      VARCHAR(20) NOT NULL,

    source_system VARCHAR(30)  NOT NULL,
    source_entity VARCHAR(60)  NOT NULL,
    source_id     VARCHAR(100) NOT NULL,

    ta_insert_dt  TIMESTAMP NOT NULL,
    ta_update_dt  TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_unit_of_measures
        PRIMARY KEY (uom_id),

    CONSTRAINT uk_ce_unit_of_measures
        UNIQUE (uom_name, source_system, source_entity)
);

COMMIT;

--ce_suppliers
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_supplier_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_suppliers (
    supplier_id      BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_supplier_id'),
    supplier_src_id  VARCHAR(100) NOT NULL,

    source_system    VARCHAR(30)  NOT NULL,
    source_entity    VARCHAR(60)  NOT NULL,
    source_id        VARCHAR(100) NOT NULL,

    ta_insert_dt     TIMESTAMP NOT NULL,
    ta_update_dt     TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_suppliers
        PRIMARY KEY (supplier_id),

    CONSTRAINT uk_ce_suppliers
        UNIQUE (supplier_src_id, source_system, source_entity)
);

COMMIT;

--ce_product_departments
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_product_department_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_product_departments (
    product_department_id   BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_product_department_id'),
    product_department_name VARCHAR(40) NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

    ta_insert_dt             TIMESTAMP NOT NULL,
    ta_update_dt             TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_product_departments
        PRIMARY KEY (product_department_id),

    CONSTRAINT uk_ce_product_departments
        UNIQUE (product_department_name, source_system, source_entity)
);

COMMIT;

--ce_product_subcategories
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_product_subcategory_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_product_subcategories (
    product_subcategory_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_product_subcategory_id'),
    product_subcategory_name  VARCHAR(60) NOT NULL,
    product_department_id     BIGINT NOT NULL,

    source_system             VARCHAR(30)  NOT NULL,
    source_entity             VARCHAR(60)  NOT NULL,
    source_id                 VARCHAR(100) NOT NULL,

    ta_insert_dt              TIMESTAMP NOT NULL,
    ta_update_dt              TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_product_subcategories
        PRIMARY KEY (product_subcategory_id),

    CONSTRAINT uk_ce_product_subcategories
        UNIQUE (product_subcategory_name, source_system, source_entity),

    CONSTRAINT fk_ce_psc_department
        FOREIGN KEY (product_department_id)
        REFERENCES bl_3nf.ce_product_departments(product_department_id)
);

COMMIT;

--ce_products
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_product_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_products (
    product_id              BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_product_id'),
    product_sku_src_id      VARCHAR(100) NOT NULL,
    product_name            VARCHAR(150) NOT NULL,

    product_subcategory_id  BIGINT NOT NULL,
    brand_id                BIGINT NOT NULL,
    uom_id                  BIGINT NOT NULL,
    supplier_id             BIGINT NOT NULL,

    source_system           VARCHAR(30)  NOT NULL,
    source_entity           VARCHAR(60)  NOT NULL,
    source_id               VARCHAR(100) NOT NULL,

    ta_insert_dt            TIMESTAMP NOT NULL,
    ta_update_dt            TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_products
        PRIMARY KEY (product_id),

    CONSTRAINT uk_ce_products
        UNIQUE (product_sku_src_id, source_system, source_entity),

    CONSTRAINT fk_ce_products_subcat
        FOREIGN KEY (product_subcategory_id)
        REFERENCES bl_3nf.ce_product_subcategories(product_subcategory_id),

    CONSTRAINT fk_ce_products_brand
        FOREIGN KEY (brand_id)
        REFERENCES bl_3nf.ce_brands(brand_id),

    CONSTRAINT fk_ce_products_uom
        FOREIGN KEY (uom_id)
        REFERENCES bl_3nf.ce_unit_of_measures(uom_id),

    CONSTRAINT fk_ce_products_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES bl_3nf.ce_suppliers(supplier_id)
);

COMMIT;

--ce_promotions
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_promotion_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_promotions (
    promotion_id   BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_promotion_id'),
    promo_code     VARCHAR(60) NOT NULL,
    discount_pct   DECIMAL(5,2) NOT NULL,

    source_system  VARCHAR(30)  NOT NULL,
    source_entity  VARCHAR(60)  NOT NULL,
    source_id      VARCHAR(100) NOT NULL,

    ta_insert_dt   TIMESTAMP NOT NULL,
    ta_update_dt   TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_promotions
        PRIMARY KEY (promotion_id),

    CONSTRAINT uk_ce_promotions
        UNIQUE (promo_code, source_system, source_entity, discount_pct)
);

COMMIT;

--ce_countries
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_country_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_countries (
    country_id      BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_country_id'),
    country_name    VARCHAR(60) NOT NULL,

    source_system   VARCHAR(30)  NOT NULL,
    source_entity   VARCHAR(60)  NOT NULL,
    source_id       VARCHAR(100) NOT NULL,

    ta_insert_dt    TIMESTAMP NOT NULL,
    ta_update_dt    TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_countries PRIMARY KEY (country_id),
    CONSTRAINT uk_ce_countries UNIQUE (country_name, source_system, source_entity, source_id)
);

COMMIT;

--ce_regions
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_region_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_regions (
    region_id      BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_region_id'),
    region_name    VARCHAR(60) NOT NULL,
    country_id     BIGINT NOT NULL,

    source_system  VARCHAR(30)  NOT NULL,
    source_entity  VARCHAR(60)  NOT NULL,
    source_id      VARCHAR(100) NOT NULL,

    ta_insert_dt   TIMESTAMP NOT NULL,
    ta_update_dt   TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_regions PRIMARY KEY (region_id),
    CONSTRAINT uk_ce_regions UNIQUE (region_name, country_id, source_system, source_entity),
    CONSTRAINT fk_ce_regions_country FOREIGN KEY (country_id)
        REFERENCES bl_3nf.ce_countries(country_id)
);

COMMIT;

--ce_cities
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_city_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_cities (
    city_id       BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_city_id'),
    city_name     VARCHAR(60) NOT NULL,
    region_id     BIGINT NOT NULL,

    source_system VARCHAR(30)  NOT NULL,
    source_entity VARCHAR(60)  NOT NULL,
    source_id     VARCHAR(100) NOT NULL,

    ta_insert_dt  TIMESTAMP NOT NULL,
    ta_update_dt  TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_cities PRIMARY KEY (city_id),
    CONSTRAINT uk_ce_cities UNIQUE (city_name, region_id, source_system, source_entity),
    CONSTRAINT fk_ce_cities_region FOREIGN KEY (region_id)
        REFERENCES bl_3nf.ce_regions(region_id)
);

COMMIT;

--ce_store_formats
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_store_format_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_store_formats (
    store_format_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_store_format_id'),
    store_format_name  VARCHAR(40) NOT NULL,

    source_system      VARCHAR(30)  NOT NULL,
    source_entity      VARCHAR(60)  NOT NULL,
    source_id          VARCHAR(100) NOT NULL,

    ta_insert_dt       TIMESTAMP NOT NULL,
    ta_update_dt       TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_store_formats PRIMARY KEY (store_format_id),
    CONSTRAINT uk_ce_store_formats UNIQUE (store_format_name, source_system, source_entity)
);

COMMIT;

--ce_stores
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_store_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_stores (
    store_id           BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_store_id'),
    store_src_id       VARCHAR(100) NOT NULL,

    store_format_id    BIGINT NOT NULL,
    store_open_dt      DATE NOT NULL,
    store_open_time    TIME NOT NULL,
    store_close_time   TIME NOT NULL,
    city_id            BIGINT NOT NULL,

    source_system      VARCHAR(30)  NOT NULL,
    source_entity      VARCHAR(60)  NOT NULL,
    source_id          VARCHAR(100) NOT NULL,

    ta_insert_dt       TIMESTAMP NOT NULL,
    ta_update_dt       TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_stores PRIMARY KEY (store_id),
    CONSTRAINT uk_ce_stores UNIQUE (store_src_id, source_system, source_entity),

    CONSTRAINT fk_ce_stores_format FOREIGN KEY (store_format_id)
        REFERENCES bl_3nf.ce_store_formats(store_format_id),

    CONSTRAINT fk_ce_stores_city FOREIGN KEY (city_id)
        REFERENCES bl_3nf.ce_cities(city_id)
);


SELECT c.conname, pg_get_constraintdef(c.oid) AS def
FROM pg_constraint c
WHERE c.conrelid = 'bl_3nf.ce_stores'::regclass
ORDER BY c.conname;

COMMIT;

--ce_delivery_addresses
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_delivery_address_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_delivery_addresses (
    delivery_address_id      BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_delivery_address_id'),
    delivery_postal_code     VARCHAR(20) NOT NULL,
    delivery_address_line1   VARCHAR(255) NOT NULL,
    city_id                  BIGINT NOT NULL,

    source_system            VARCHAR(30)  NOT NULL,
    source_entity            VARCHAR(60)  NOT NULL,
    source_id                VARCHAR(100) NOT NULL,

    ta_insert_dt             TIMESTAMP NOT NULL,
    ta_update_dt             TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_delivery_addresses PRIMARY KEY (delivery_address_id),
    

    CONSTRAINT fk_ce_delivery_addresses_city FOREIGN KEY (city_id)
        REFERENCES bl_3nf.ce_cities(city_id)
);

COMMIT;

--bl_3nf.seq_ce_fulfillment_center_id
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_fulfillment_center_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_fulfillment_centers (
    fulfillment_center_id      BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_fulfillment_center_id'),
    fulfillment_center_src_id  VARCHAR(100) NOT NULL,
    city_id                    BIGINT NOT NULL,

    source_system              VARCHAR(30)  NOT NULL,
    source_entity              VARCHAR(60)  NOT NULL,
    source_id                  VARCHAR(100) NOT NULL,

    ta_insert_dt               TIMESTAMP NOT NULL,
    ta_update_dt               TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_fulfillment_centers PRIMARY KEY (fulfillment_center_id),
    CONSTRAINT uk_ce_fulfillment_centers UNIQUE (fulfillment_center_src_id, source_system, source_entity, city_id),

    CONSTRAINT fk_ce_fulfillment_centers_city FOREIGN KEY (city_id)
        REFERENCES bl_3nf.ce_cities(city_id)
);

COMMIT;

--bl_3nf.seq_ce_delivery_type_id
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_delivery_type_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_delivery_types (
    delivery_type_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_delivery_type_id'),
    delivery_type_name  VARCHAR(40) NOT NULL,

    source_system       VARCHAR(30)  NOT NULL,
    source_entity       VARCHAR(60)  NOT NULL,
    source_id           VARCHAR(100) NOT NULL,

    ta_insert_dt        TIMESTAMP NOT NULL,
    ta_update_dt        TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_delivery_types
        PRIMARY KEY (delivery_type_id),

    CONSTRAINT uk_ce_delivery_types
        UNIQUE (delivery_type_name, source_system, source_entity)
);

COMMIT;

--bl_3nf.seq_ce_delivery_provider_id
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_delivery_provider_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_delivery_providers (
    delivery_provider_id  BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_delivery_provider_id'),
    carrier_name          VARCHAR(60) NOT NULL,

    source_system         VARCHAR(30)  NOT NULL,
    source_entity         VARCHAR(60)  NOT NULL,
    source_id             VARCHAR(100) NOT NULL,

    ta_insert_dt          TIMESTAMP NOT NULL,
    ta_update_dt          TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_delivery_providers
        PRIMARY KEY (delivery_provider_id),

    CONSTRAINT uk_ce_delivery_providers
        UNIQUE (carrier_name, source_system, source_entity)
);

COMMIT;

--ce_terminal_types
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_terminal_type_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_terminal_types (
    terminal_type_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_terminal_type_id'),
    terminal_type_name  VARCHAR(40) NOT NULL,

    source_system       VARCHAR(30)  NOT NULL,
    source_entity       VARCHAR(60)  NOT NULL,
    source_id           VARCHAR(100) NOT NULL,

    ta_insert_dt        TIMESTAMP NOT NULL,
    ta_update_dt        TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_terminal_types
        PRIMARY KEY (terminal_type_id),

    CONSTRAINT uk_ce_terminal_types
        UNIQUE (terminal_type_name, source_system, source_entity)
);

COMMIT;

--ce_terminals
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_terminal_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_terminals (
    terminal_id        BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_terminal_id'),
    terminal_src_id    VARCHAR(100) NOT NULL,

    terminal_type_id   BIGINT NOT NULL,
    store_id           BIGINT NOT NULL,

    source_system      VARCHAR(30)  NOT NULL,
    source_entity      VARCHAR(60)  NOT NULL,
    source_id          VARCHAR(100) NOT NULL,

    ta_insert_dt       TIMESTAMP NOT NULL,
    ta_update_dt       TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_terminals
        PRIMARY KEY (terminal_id),

    CONSTRAINT uk_ce_terminals
        UNIQUE (terminal_src_id, source_system, source_entity),

    CONSTRAINT fk_ce_terminals_type
        FOREIGN KEY (terminal_type_id)
        REFERENCES bl_3nf.ce_terminal_types(terminal_type_id),

    CONSTRAINT fk_ce_terminals_store
        FOREIGN KEY (store_id)
        REFERENCES bl_3nf.ce_stores(store_id)
);

COMMIT;

--ce_shifts
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_shift_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_shifts (
    shift_id        BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_shift_id'),
    shift_src_id    VARCHAR(60) NOT NULL,

    source_system   VARCHAR(30)  NOT NULL,
    source_entity   VARCHAR(60)  NOT NULL,
    source_id       VARCHAR(100) NOT NULL,

    ta_insert_dt    TIMESTAMP NOT NULL,
    ta_update_dt    TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_shifts
        PRIMARY KEY (shift_id),

    CONSTRAINT uk_ce_shifts
        UNIQUE (shift_src_id, source_system, source_entity)
);

COMMIT;

--ce_payment_gateways
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_payment_gateway_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_payment_gateways (
    payment_gateway_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_payment_gateway_id'),
    payment_gateway_name  VARCHAR(40) NOT NULL,

    source_system         VARCHAR(30)  NOT NULL,
    source_entity         VARCHAR(60)  NOT NULL,
    source_id             VARCHAR(100) NOT NULL,

    ta_insert_dt          TIMESTAMP NOT NULL,
    ta_update_dt          TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_payment_gateways
        PRIMARY KEY (payment_gateway_id),

    CONSTRAINT uk_ce_payment_gateways
        UNIQUE (payment_gateway_name, source_system, source_entity)
);

COMMIT;
--ce_employees
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_employee_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_employees (
    employee_id        BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_employee_id'),
    employee_src_id    VARCHAR(100) NOT NULL,

    first_name         VARCHAR(100) NOT NULL,
    last_name          VARCHAR(100) NOT NULL,
    department         VARCHAR(60)  NOT NULL,
    position           VARCHAR(60)  NOT NULL,
    hire_dt            DATE NOT NULL,

    source_system      VARCHAR(30)  NOT NULL,
    source_entity      VARCHAR(60)  NOT NULL,
    source_id          VARCHAR(100) NOT NULL,

    ta_insert_dt       TIMESTAMP NOT NULL,
    ta_update_dt       TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_employees
        PRIMARY KEY (employee_id),

    CONSTRAINT uk_ce_employees
        UNIQUE (employee_src_id, source_system, source_entity)
);

COMMIT;

--ce_device_types
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_device_type_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_device_types (
    device_type_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_device_type_id'),
    device_type_name  VARCHAR(40) NOT NULL,

    source_system     VARCHAR(30)  NOT NULL,
    source_entity     VARCHAR(60)  NOT NULL,
    source_id         VARCHAR(100) NOT NULL,

    ta_insert_dt      TIMESTAMP NOT NULL,
    ta_update_dt      TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_device_types PRIMARY KEY (device_type_id),
    CONSTRAINT uk_ce_device_types UNIQUE (device_type_name, source_system, source_entity)
);

COMMIT;

--
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_order_status_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_order_statuses (
    order_status_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_order_status_id'),
    order_status_name  VARCHAR(40) NOT NULL,

    source_system      VARCHAR(30)  NOT NULL,
    source_entity      VARCHAR(60)  NOT NULL,
    source_id          VARCHAR(100) NOT NULL,

    ta_insert_dt       TIMESTAMP NOT NULL,
    ta_update_dt       TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_order_statuses PRIMARY KEY (order_status_id),
    CONSTRAINT uk_ce_order_statuses UNIQUE (order_status_name, source_system, source_entity)
);
COMMIT;

--ce_sales_channels
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_sales_channel_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_sales_channels (
    sales_channel_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_sales_channel_id'),
    sales_channel_name  VARCHAR(20) NOT NULL,

    source_system       VARCHAR(30)  NOT NULL,
    source_entity       VARCHAR(60)  NOT NULL,
    source_id           VARCHAR(100) NOT NULL,

    ta_insert_dt        TIMESTAMP NOT NULL,
    ta_update_dt        TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_sales_channels PRIMARY KEY (sales_channel_id),
    CONSTRAINT uk_ce_sales_channels UNIQUE (sales_channel_name, source_system, source_entity)
);
COMMIT;

BEGIN;

/* SEQUENCES */
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_card_type_id;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_payment_method_id;
CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_receipt_type_id;

/* CE_CARD_TYPES */
CREATE TABLE IF NOT EXISTS bl_3nf.ce_card_types (
    card_type_id     BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_card_type_id'),
    card_type_name   VARCHAR(40) NOT NULL,

    source_system    VARCHAR(30)  NOT NULL,
    source_entity    VARCHAR(60)  NOT NULL,
    source_id        VARCHAR(100) NOT NULL,

    ta_insert_dt     TIMESTAMP NOT NULL,
    ta_update_dt     TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_card_types PRIMARY KEY (card_type_id),
    CONSTRAINT uk_ce_card_types UNIQUE (card_type_name, source_system, source_entity)
);

/* CE_PAYMENT_METHODS */
CREATE TABLE IF NOT EXISTS bl_3nf.ce_payment_methods (
    payment_method_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_payment_method_id'),
    payment_method_name  VARCHAR(40) NOT NULL,

    source_system        VARCHAR(30)  NOT NULL,
    source_entity        VARCHAR(60)  NOT NULL,
    source_id            VARCHAR(100) NOT NULL,

    ta_insert_dt         TIMESTAMP NOT NULL,
    ta_update_dt         TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_payment_methods PRIMARY KEY (payment_method_id),
    CONSTRAINT uk_ce_payment_methods UNIQUE (payment_method_name, source_system, source_entity)
);

/* CE_RECEIPT_TYPES */
CREATE TABLE IF NOT EXISTS bl_3nf.ce_receipt_types (
    receipt_type_id    BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_receipt_type_id'),
    receipt_type_name  VARCHAR(40) NOT NULL,

    source_system      VARCHAR(30)  NOT NULL,
    source_entity      VARCHAR(60)  NOT NULL,
    source_id          VARCHAR(100) NOT NULL,

    ta_insert_dt       TIMESTAMP NOT NULL,
    ta_update_dt       TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_receipt_types PRIMARY KEY (receipt_type_id),
    CONSTRAINT uk_ce_receipt_types UNIQUE (receipt_type_name, source_system, source_entity)
);

COMMIT;

--ce_customers_scd 
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_customer_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_customers_scd (
    customer_id         BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_customer_id'),
    customer_src_id     VARCHAR(100) NOT NULL,

    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    email               VARCHAR(255) NOT NULL,
    phone               VARCHAR(30)  NOT NULL,
    age_grp             VARCHAR(20)  NOT NULL,
    customer_segment    VARCHAR(30)  NOT NULL,
    gender              VARCHAR(20)  NOT NULL,

    start_dt            DATE NOT NULL,
    end_dt              DATE NOT NULL,
    is_active           BOOLEAN NOT NULL,

    source_system       VARCHAR(30)  NOT NULL,
    source_entity       VARCHAR(60)  NOT NULL,
    source_id           VARCHAR(100) NOT NULL,

    ta_insert_dt        TIMESTAMP NOT NULL,
    ta_update_dt        TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_customers_scd
        PRIMARY KEY (customer_id, start_dt),
        
    CONSTRAINT uk_ce_customers_scd_customer_id
        UNIQUE (customer_id),

    CONSTRAINT uk_ce_customers_scd_nk
        UNIQUE (customer_src_id, source_system, source_entity, start_dt),

    CONSTRAINT ck_ce_customers_scd_dates
        CHECK (end_dt >= start_dt)
);

COMMIT;

--ce_transactions
BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_txn_id;

CREATE TABLE IF NOT EXISTS bl_3nf.ce_transactions (
    txn_id                 BIGINT NOT NULL DEFAULT nextval('bl_3nf.seq_ce_txn_id'),
    txn_src_id             VARCHAR(100) NOT NULL,
    txn_ts                 TIMESTAMP NOT NULL,

    product_id             BIGINT NOT NULL,
    promotion_id           BIGINT NOT NULL,
    sales_channel_id       BIGINT NOT NULL,
    customer_id            BIGINT NOT NULL,
    payment_method_id      BIGINT NOT NULL,
    card_type_id           BIGINT NOT NULL,
    receipt_type_id        BIGINT NOT NULL,
    store_id               BIGINT NOT NULL,
    terminal_id            BIGINT NOT NULL,
    employee_id            BIGINT NOT NULL,
    shift_id               BIGINT NOT NULL,
    order_status_id        BIGINT NOT NULL,
    delivery_provider_id   BIGINT NOT NULL,
    delivery_type_id       BIGINT NOT NULL,
    delivery_address_id    BIGINT NOT NULL,
    fulfillment_center_id  BIGINT NOT NULL,
    device_type_id         BIGINT NOT NULL,
    payment_gateway_id     BIGINT NOT NULL,

    tracking_id            VARCHAR(100) NOT NULL,
    promised_delivery_dt   DATE NOT NULL,

    qty                    INT NOT NULL,
    unit_price_amt         DECIMAL(12,2) NOT NULL,
    tax_amt                DECIMAL(12,2) NOT NULL,
    shipping_fee_amt       DECIMAL(12,2) NOT NULL,
    discount_amt           DECIMAL(12,2) NOT NULL,
    sales_amt              DECIMAL(12,2) NOT NULL,
    cost_amt               DECIMAL(12,2) NOT NULL,
    gross_profit_amt       DECIMAL(12,2) NOT NULL,

    loyalty_points_earned  INT NOT NULL,
    customer_rating        DECIMAL(4,1) NOT NULL,

    source_system          VARCHAR(30)  NOT NULL,
    source_entity          VARCHAR(60)  NOT NULL,
    source_id              VARCHAR(100) NOT NULL,

    ta_insert_dt           TIMESTAMP NOT NULL,
    ta_update_dt           TIMESTAMP NOT NULL,

    CONSTRAINT pk_ce_transactions
        PRIMARY KEY (txn_id),

    CONSTRAINT uk_ce_transactions
        UNIQUE (txn_src_id, source_system, source_entity),

    CONSTRAINT fk_trx_product            FOREIGN KEY (product_id)            REFERENCES bl_3nf.ce_products(product_id),
    CONSTRAINT fk_trx_promotion          FOREIGN KEY (promotion_id)          REFERENCES bl_3nf.ce_promotions(promotion_id),
    CONSTRAINT fk_trx_sales_channel      FOREIGN KEY (sales_channel_id)      REFERENCES bl_3nf.ce_sales_channels(sales_channel_id),
    CONSTRAINT fk_trx_customer           FOREIGN KEY (customer_id)           REFERENCES bl_3nf.ce_customers_scd(customer_id),
    CONSTRAINT fk_trx_payment_method     FOREIGN KEY (payment_method_id)     REFERENCES bl_3nf.ce_payment_methods(payment_method_id),
    CONSTRAINT fk_trx_card_type          FOREIGN KEY (card_type_id)          REFERENCES bl_3nf.ce_card_types(card_type_id),
    CONSTRAINT fk_trx_receipt_type       FOREIGN KEY (receipt_type_id)       REFERENCES bl_3nf.ce_receipt_types(receipt_type_id),
    CONSTRAINT fk_trx_store              FOREIGN KEY (store_id)              REFERENCES bl_3nf.ce_stores(store_id),
    CONSTRAINT fk_trx_terminal           FOREIGN KEY (terminal_id)           REFERENCES bl_3nf.ce_terminals(terminal_id),
    CONSTRAINT fk_trx_employee           FOREIGN KEY (employee_id)           REFERENCES bl_3nf.ce_employees(employee_id),
    CONSTRAINT fk_trx_shift              FOREIGN KEY (shift_id)              REFERENCES bl_3nf.ce_shifts(shift_id),
    CONSTRAINT fk_trx_order_status       FOREIGN KEY (order_status_id)       REFERENCES bl_3nf.ce_order_statuses(order_status_id),
    CONSTRAINT fk_trx_delivery_provider  FOREIGN KEY (delivery_provider_id)  REFERENCES bl_3nf.ce_delivery_providers(delivery_provider_id),
    CONSTRAINT fk_trx_delivery_type      FOREIGN KEY (delivery_type_id)      REFERENCES bl_3nf.ce_delivery_types(delivery_type_id),
    CONSTRAINT fk_trx_delivery_address   FOREIGN KEY (delivery_address_id)   REFERENCES bl_3nf.ce_delivery_addresses(delivery_address_id),
    CONSTRAINT fk_trx_fulfillment_center FOREIGN KEY (fulfillment_center_id) REFERENCES bl_3nf.ce_fulfillment_centers(fulfillment_center_id),
    CONSTRAINT fk_trx_device_type        FOREIGN KEY (device_type_id)        REFERENCES bl_3nf.ce_device_types(device_type_id),
    CONSTRAINT fk_trx_payment_gateway    FOREIGN KEY (payment_gateway_id)    REFERENCES bl_3nf.ce_payment_gateways(payment_gateway_id)
);

COMMIT;

--had iisues with this table => was loading more then 30min, found that adding indexes and additional function could spead this process

BEGIN;

CREATE INDEX IF NOT EXISTS ix_ce_transactions_bk
ON bl_3nf.ce_transactions (txn_src_id, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_products_bk
ON bl_3nf.ce_products (product_sku_src_id, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_promotions_bk
ON bl_3nf.ce_promotions (promo_code, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_sales_channels_bk
ON bl_3nf.ce_sales_channels (sales_channel_name, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_payment_methods_bk
ON bl_3nf.ce_payment_methods (payment_method_name, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_card_types_bk
ON bl_3nf.ce_card_types (card_type_name, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_receipt_types_bk
ON bl_3nf.ce_receipt_types (receipt_type_name, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_stores_bk
ON bl_3nf.ce_stores (store_src_id, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_terminals_bk
ON bl_3nf.ce_terminals (terminal_src_id, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_employees_bk
ON bl_3nf.ce_employees (employee_src_id, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_shifts_bk
ON bl_3nf.ce_shifts (shift_src_id, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_order_statuses_bk
ON bl_3nf.ce_order_statuses (order_status_name, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_delivery_providers_bk
ON bl_3nf.ce_delivery_providers (carrier_name, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_delivery_types_bk
ON bl_3nf.ce_delivery_types (delivery_type_name, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_device_types_bk
ON bl_3nf.ce_device_types (device_type_name, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_payment_gateways_bk
ON bl_3nf.ce_payment_gateways (payment_gateway_name, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_fulfillment_centers_bk
ON bl_3nf.ce_fulfillment_centers (fulfillment_center_src_id, source_system, source_entity);

CREATE INDEX IF NOT EXISTS ix_ce_delivery_addresses_lkp
ON bl_3nf.ce_delivery_addresses (delivery_postal_code, delivery_address_line1, city_id, source_system, source_entity);

-- Active customers lookup 
CREATE INDEX IF NOT EXISTS ix_ce_customers_active
ON bl_3nf.ce_customers_scd (customer_src_id, source_system, source_entity)
WHERE is_active = TRUE AND end_dt = DATE '9999-12-31';

COMMIT;

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname = 'bl_3nf'
          AND rel.relname = 'ce_delivery_addresses'
          AND con.conname = 'uk_ce_delivery_addresses_bk'
    ) THEN
        ALTER TABLE bl_3nf.ce_delivery_addresses
        ADD CONSTRAINT uk_ce_delivery_addresses_bk
        UNIQUE (delivery_postal_code, delivery_address_line1, city_id, source_system, source_entity);
    END IF;
END $$;

COMMIT;



