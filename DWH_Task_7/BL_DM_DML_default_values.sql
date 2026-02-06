 -- Default rows for BL_DM 
  
-- DIM_PRODUCTS
 BEGIN;
INSERT INTO bl_dm.dim_products (
    product_id,
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
SELECT
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'MANUAL',
    'MANUAL',
    'n.a.'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_products WHERE product_id = -1
)
RETURNING*;

-- DIM_STORES
INSERT INTO bl_dm.dim_stores (
    store_id,
    store_src_id,
    store_format,
    store_open_dt,
    store_open_time,
    store_close_time,
    city_name,
    region_name,
    country_name,
    source_system,
    source_entity,
    source_id
)
SELECT
    -1,
    'n.a.',
    'n.a.',
    DATE '1900-01-01',
    TIME '00:00:00',
    TIME '00:00:00',
    'n.a.',
    'n.a.',
    'n.a.',
    'MANUAL',
    'MANUAL',
    'n.a.'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_stores WHERE store_id = -1
)
RETURNING*;

-- DIM_TERMINALS
INSERT INTO bl_dm.dim_terminals (
    terminal_id,
    terminal_src_id,
    terminal_type_name,
    source_system,
    source_entity,
    source_id
)
SELECT
    -1,
    'n.a.',
    'n.a.',
    'MANUAL',
    'MANUAL',
    'n.a.'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_terminals WHERE terminal_id = -1
)
RETURNING*;

-- DIM_EMPLOYEES
INSERT INTO bl_dm.dim_employees (
    employee_id,
    employee_src_id,
    first_name,
    last_name,
    department,
    position,
    hire_dt,
    source_system,
    source_entity,
    source_id
)
SELECT
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    DATE '1900-01-01',
    'MANUAL',
    'MANUAL',
    'n.a.'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_employees WHERE employee_id = -1
)
RETURNING*;

-- DIM_PROMOTIONS
INSERT INTO bl_dm.dim_promotions (
    promotion_id,
    promo_code,
    discount_pct,
    source_system,
    source_entity,
    source_id
)
SELECT
    -1,
    'n.a.',
    -1.00,
    'MANUAL',
    'MANUAL',
    'n.a.'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_promotions WHERE promotion_id = -1
)
RETURNING*;

-- DIM_DELIVERY_PROVIDERS
INSERT INTO bl_dm.dim_delivery_providers (
    delivery_provider_id,
    carrier_name,
    delivery_type_name,
    source_system,
    source_entity,
    source_id
)
SELECT
    -1,
    'n.a.',
    'n.a.',
    'MANUAL',
    'MANUAL',
    'n.a.'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_delivery_providers WHERE delivery_provider_id = -1
)
RETURNING*;

-- DIM_JUNK_CONTEXT
INSERT INTO bl_dm.dim_junk_context (
    junk_context_id,
    sales_channel,
    payment_method,
    card_type,
    receipt_type,
    payment_gateway,
    order_status,
    shift_name,
    device_type_id,
    source_system,
    source_entity,
    source_id
)
SELECT
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    -1,
    'MANUAL',
    'MANUAL',
    'n.a.'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_junk_context WHERE junk_context_id = -1
)
RETURNING*;

-- DIM_CUSTOMERS_SCD (SCD2 default row)
INSERT INTO bl_dm.dim_customers_scd (
    customer_id,
    customer_src_id,
    age_group,
    email,
    phone,
    customer_segment,
    gender,
    start_dt,
    end_dt,
    is_active,
    source_system,
    source_entity,
    source_id
)
SELECT
    -1,
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    'n.a.',
    DATE '1900-01-01',
    DATE '9999-12-31',
    TRUE,
    'MANUAL',
    'MANUAL',
    'n.a.'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_customers_scd WHERE customer_id = -1
)
RETURNING*;

-- DIM_DATES_DAY 
INSERT INTO bl_dm.dim_dates_day (
    date_id,
    day,
    day_of_week,
    day_name,
    week,
    week_of_year,
    month,
    month_name,
    quarter,
    year,
    is_weekend)
SELECT
    -1,
    -1,
    -1,
    'n.a.',
    -1,
    -1,
    -1,
    'n.a.',
    -1,
    -1,
    FALSE   
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_dates_day WHERE date_id = -1
)
RETURNING*;

COMMIT;