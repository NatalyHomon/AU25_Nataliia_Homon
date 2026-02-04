--ce_brands SCD1
BEGIN;

INSERT INTO bl_3nf.ce_brands (
    brand_id,
    brand_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    'MANUAL',
    'MANUAL',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_brands
    WHERE brand_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(s.brand, 'n. a.') AS brand_name,
        'sa_sales_online'          AS source_system,
        'src_sales_online'         AS source_entity,
        COALESCE(s.brand, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online s
    WHERE s.brand IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        COALESCE(p.brand, 'n. a.') AS brand_name,
        'sa_sales_pos'             AS source_system,
        'src_sales_pos'            AS source_entity,
        COALESCE(p.brand, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos p
    WHERE p.brand IS NOT NULL
)
INSERT INTO bl_3nf.ce_brands (
    brand_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    s.brand_name,
    s.source_system,
    s.source_entity,
    s.source_id,
    now(),
    now()
FROM src s
ON CONFLICT (brand_name, source_system, source_entity)
DO UPDATE
SET
    source_id    = EXCLUDED.source_id,
    ta_update_dt = now();

COMMIT;

SELECT * FROM bl_3nf.ce_brands;

--ce_unit_of_measures SCD0
BEGIN;

INSERT INTO bl_3nf.ce_unit_of_measures (
    uom_id,
    uom_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    'MANUAL',
    'MANUAL',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_unit_of_measures
    WHERE uom_id = -1
);

COMMIT;
BEGIN;

WITH src AS (
    /* ONLINE */
    SELECT DISTINCT
        COALESCE(s.unit_of_measure, 'n. a.') AS uom_name,
        'sa_sales_online'                    AS source_system,
        'src_sales_online'                   AS source_entity,
        COALESCE(s.unit_of_measure, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online s
    WHERE s.unit_of_measure IS NOT NULL

    UNION ALL

    /* POS */
    SELECT DISTINCT
        COALESCE(p.unit_of_measure, 'n. a.') AS uom_name,
        'sa_sales_pos'                        AS source_system,
        'src_sales_pos'                       AS source_entity,
        COALESCE(p.unit_of_measure, 'n. a.')  AS source_id
    FROM sa_sales_pos.src_sales_pos p
    WHERE p.unit_of_measure IS NOT NULL
)
INSERT INTO bl_3nf.ce_unit_of_measures (
    uom_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    s.uom_name,
    s.source_system,
    s.source_entity,
    s.source_id,
    now(),
    now()
FROM src s
ON CONFLICT (uom_name, source_system, source_entity)
DO NOTHING;

COMMIT;

SELECT * FROM bl_3nf.ce_unit_of_measures;

--ce_suppliers SCD1
BEGIN;

INSERT INTO bl_3nf.ce_suppliers (
    supplier_id,
    supplier_src_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    'MANUAL',
    'MANUAL',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_suppliers sup
    WHERE sup.supplier_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    /* ONLINE */
    SELECT DISTINCT
        COALESCE(son.supplier_id, 'n. a.') AS supplier_src_id,
        'sa_sales_online'                  AS source_system,
        'src_sales_online'                 AS source_entity,
        COALESCE(son.supplier_id, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.supplier_id IS NOT NULL

    UNION ALL

    /* POS */
    SELECT DISTINCT
        COALESCE(spo.supplier_id, 'n. a.') AS supplier_src_id,
        'sa_sales_pos'                     AS source_system,
        'src_sales_pos'                    AS source_entity,
        COALESCE(spo.supplier_id, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.supplier_id IS NOT NULL
)
INSERT INTO bl_3nf.ce_suppliers (
    supplier_src_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    src.supplier_src_id,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(),
    now()
FROM src
ON CONFLICT (supplier_src_id, source_system, source_entity)
DO UPDATE
SET
    source_id    = EXCLUDED.source_id,
    ta_update_dt = now();

COMMIT;

SELECT * FROM bl_3nf.ce_suppliers;

--ce_product_departments SCD1
BEGIN;

INSERT INTO bl_3nf.ce_product_departments (
    product_department_id,
    product_department_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_product_departments dep
    WHERE dep.product_department_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    /* ONLINE */
    SELECT DISTINCT
        COALESCE(son.product_dept, 'n. a.') AS product_department_name,
        'sa_sales_online'                   AS source_system,
        'src_sales_online'                  AS source_entity,
        COALESCE(son.product_dept, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.product_dept IS NOT NULL

    UNION ALL

    /* POS */
    SELECT DISTINCT
        COALESCE(spo.product_dept, 'n. a.') AS product_department_name,
        'sa_sales_pos'                      AS source_system,
        'src_sales_pos'                     AS source_entity,
        COALESCE(spo.product_dept, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.product_dept IS NOT NULL
)
INSERT INTO bl_3nf.ce_product_departments (
    product_department_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    src.product_department_name,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(),
    now()
FROM src
ON CONFLICT (product_department_name, source_system, source_entity)
DO UPDATE
SET
    source_id    = EXCLUDED.source_id,
    ta_update_dt = now();

COMMIT;

SELECT * FROM bl_3nf.ce_product_departments;

--ce_product_subcategories SCD1
BEGIN;

INSERT INTO bl_3nf.ce_product_subcategories (
    product_subcategory_id,
    product_subcategory_name,
    product_department_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    -1,
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_product_subcategories sub
    WHERE sub.product_subcategory_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    /* ONLINE */
    SELECT DISTINCT
        COALESCE(son.product_subcategory, 'n. a.') AS product_subcategory_name,
        COALESCE(son.product_dept, 'n. a.')        AS product_department_name,
        'sa_sales_online'                          AS source_system,
        'src_sales_online'                         AS source_entity,
        COALESCE(son.product_subcategory, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.product_subcategory IS NOT NULL

    UNION ALL

    /* POS */
    SELECT DISTINCT
        COALESCE(spo.product_subcategory, 'n. a.') AS product_subcategory_name,
        COALESCE(spo.product_dept, 'n. a.')        AS product_department_name,
        'sa_sales_pos'                             AS source_system,
        'src_sales_pos'                            AS source_entity,
        COALESCE(spo.product_subcategory, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.product_subcategory IS NOT NULL
),
map AS (
    SELECT
        src.product_subcategory_name,
        COALESCE(dep.product_department_id, -1) AS product_department_id,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_product_departments dep
      ON dep.product_department_name = src.product_department_name
     AND dep.source_system = src.source_system
     AND dep.source_entity = src.source_entity
)
INSERT INTO bl_3nf.ce_product_subcategories (
    product_subcategory_name,
    product_department_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    map.product_subcategory_name,
    map.product_department_id,
    map.source_system,
    map.source_entity,
    map.source_id,
    now(),
    now()
FROM map
ON CONFLICT (product_subcategory_name, source_system, source_entity)
DO UPDATE
SET
    product_department_id = EXCLUDED.product_department_id,
    source_id             = EXCLUDED.source_id,
    ta_update_dt          = now();

COMMIT;

SELECT * FROM bl_3nf.ce_product_subcategories;

--ce_products SCD1
BEGIN;

INSERT INTO bl_3nf.ce_products (
    product_id,
    product_sku_src_id,
    product_name,
    product_subcategory_id,
    brand_id,
    uom_id,
    supplier_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    'n. a.',
    -1,
    -1,
    -1,
    -1,
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_products prd
    WHERE prd.product_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    /* ONLINE */
    SELECT DISTINCT
        COALESCE(son.product_sku, 'n. a.')          AS product_sku_src_id,
        COALESCE(son.product_name, 'n. a.')         AS product_name,
        COALESCE(son.product_subcategory, 'n. a.')  AS product_subcategory_name,
        COALESCE(son.product_dept, 'n. a.')         AS product_department_name,
        COALESCE(son.brand, 'n. a.')                AS brand_name,
        COALESCE(son.unit_of_measure, 'n. a.')      AS uom_name,
        COALESCE(son.supplier_id, 'n. a.')          AS supplier_src_id,
        'sa_sales_online'                           AS source_system,
        'src_sales_online'                          AS source_entity,
        COALESCE(son.product_sku, 'n. a.')          AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.product_sku IS NOT NULL

    UNION ALL

    /* POS */
    SELECT DISTINCT
        COALESCE(spo.product_sku, 'n. a.')          AS product_sku_src_id,
        COALESCE(spo.product_name, 'n. a.')         AS product_name,
        COALESCE(spo.product_subcategory, 'n. a.')  AS product_subcategory_name,
        COALESCE(spo.product_dept, 'n. a.')         AS product_department_name,
        COALESCE(spo.brand, 'n. a.')                AS brand_name,
        COALESCE(spo.unit_of_measure, 'n. a.')      AS uom_name,
        COALESCE(spo.supplier_id, 'n. a.')          AS supplier_src_id,
        'sa_sales_pos'                              AS source_system,
        'src_sales_pos'                             AS source_entity,
        COALESCE(spo.product_sku, 'n. a.')          AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.product_sku IS NOT NULL
),
map AS (
    SELECT
        src.product_sku_src_id,
        src.product_name,
        COALESCE(sub.product_subcategory_id, -1) AS product_subcategory_id,
        COALESCE(brd.brand_id, -1)               AS brand_id,
        COALESCE(uom.uom_id, -1)                 AS uom_id,
        COALESCE(sup.supplier_id, -1)            AS supplier_id,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    
    LEFT JOIN bl_3nf.ce_product_subcategories sub
      ON sub.product_subcategory_name = src.product_subcategory_name
     AND sub.source_system = src.source_system
     AND sub.source_entity = src.source_entity

    LEFT JOIN bl_3nf.ce_brands brd
      ON brd.brand_name = src.brand_name
     AND brd.source_system = src.source_system
     AND brd.source_entity = src.source_entity

    LEFT JOIN bl_3nf.ce_unit_of_measures uom
      ON uom.uom_name = src.uom_name
     AND uom.source_system = src.source_system
     AND uom.source_entity = src.source_entity

    LEFT JOIN bl_3nf.ce_suppliers sup
      ON sup.supplier_src_id = src.supplier_src_id
     AND sup.source_system = src.source_system
     AND sup.source_entity = src.source_entity
)
INSERT INTO bl_3nf.ce_products (
    product_sku_src_id,
    product_name,
    product_subcategory_id,
    brand_id,
    uom_id,
    supplier_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    map.product_sku_src_id,
    map.product_name,
    map.product_subcategory_id,
    map.brand_id,
    map.uom_id,
    map.supplier_id,
    map.source_system,
    map.source_entity,
    map.source_id,
    now(),
    now()
FROM map
ON CONFLICT (product_sku_src_id, source_system, source_entity)
DO UPDATE
SET
    product_name           = EXCLUDED.product_name,
    product_subcategory_id = EXCLUDED.product_subcategory_id,
    brand_id               = EXCLUDED.brand_id,
    uom_id                 = EXCLUDED.uom_id,
    supplier_id            = EXCLUDED.supplier_id,
    source_id              = EXCLUDED.source_id,
    ta_update_dt           = now();

COMMIT;

SELECT count(*) FROM bl_3nf.ce_products;

--ce_promotions SCD1
BEGIN;

INSERT INTO bl_3nf.ce_promotions (
    promotion_id,
    promo_code,
    discount_pct,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',    
    -1,
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_promotions pro
    WHERE pro.promotion_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    /* ONLINE */
    SELECT DISTINCT
        COALESCE(son.promo_code, 'n. a.')      AS promo_code,
        COALESCE(son.discount_pct, -1)         AS discount_pct,
        'sa_sales_online'                      AS source_system,
        'src_sales_online'                     AS source_entity,
        COALESCE(son.promo_code, 'n. a.')      AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.promo_code IS NOT NULL

    UNION ALL

    /* POS */
    SELECT DISTINCT
        COALESCE(spo.promo_code, 'n. a.')      AS promo_code,       
        COALESCE(spo.discount_pct, -1)         AS discount_pct,
        'sa_sales_pos'                         AS source_system,
        'src_sales_pos'                        AS source_entity,
        COALESCE(spo.promo_code, 'n. a.')      AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.promo_code IS NOT NULL
)
INSERT INTO bl_3nf.ce_promotions (
    promo_code,
    discount_pct,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    src.promo_code,
    src.discount_pct,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(),
    now()
FROM src
ON CONFLICT (promo_code, source_system, source_entity, discount_pct)
DO UPDATE
SET
    
    source_id    = EXCLUDED.source_id,
    ta_update_dt = now();

COMMIT;

SELECT * FROM bl_3nf.ce_promotions;

--ce_countries SCD1
BEGIN;

INSERT INTO bl_3nf.ce_countries (
    country_id, country_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    -1, 'n. a.', 'manual', 'manual', 'n. a.', DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_countries ctr WHERE ctr.country_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.country, 'n. a.') AS country_name,
        'sa_sales_online'              AS source_system,
        'src_sales_online'             AS source_entity,
        COALESCE(son.country, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.country IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        COALESCE(spo.country, 'n. a.') AS country_name,
        'sa_sales_pos'                 AS source_system,
        'src_sales_pos'                AS source_entity,
        COALESCE(spo.country, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.country IS NOT NULL
)
INSERT INTO bl_3nf.ce_countries (
    country_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    src.country_name, src.source_system, src.source_entity, src.source_id, now(), now()
FROM src
ON CONFLICT (country_name, source_system, source_entity)
DO UPDATE
SET
    source_id    = EXCLUDED.source_id,
    ta_update_dt = now();

COMMIT;

SELECT * FROM bl_3nf.ce_countries;

--ce_regions SCD1
BEGIN;

INSERT INTO bl_3nf.ce_regions (
    region_id, region_name, country_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    -1, 'n. a.', -1, 'manual', 'manual', 'n. a.', DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_regions reg WHERE reg.region_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.region, 'n. a.')  AS region_name,
        COALESCE(son.country, 'n. a.') AS country_name,
        'sa_sales_online'              AS source_system,
        'src_sales_online'             AS source_entity,
        COALESCE(son.region, 'n. a.')  AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.region IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        COALESCE(spo.region, 'n. a.')  AS region_name,
        COALESCE(spo.country, 'n. a.') AS country_name,
        'sa_sales_pos'                 AS source_system,
        'src_sales_pos'                AS source_entity,
        COALESCE(spo.region, 'n. a.')  AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.region IS NOT NULL
),
map AS (
    SELECT
        src.region_name,
        COALESCE(ctr.country_id, -1) AS country_id,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_countries ctr
      ON ctr.country_name  = src.country_name
     AND ctr.source_system = src.source_system
     AND ctr.source_entity = src.source_entity
)
INSERT INTO bl_3nf.ce_regions (
    region_name, country_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    map.region_name, map.country_id, map.source_system, map.source_entity, map.source_id, now(), now()
FROM map
ON CONFLICT (region_name, country_id, source_system, source_entity)
DO UPDATE
SET
    source_id    = EXCLUDED.source_id,
    ta_update_dt = now();

COMMIT;

SELECT * FROM bl_3nf.ce_regions;

--ce_cities
BEGIN;

INSERT INTO bl_3nf.ce_cities (
    city_id, city_name, region_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    -1, 'n. a.', -1, 'manual', 'manual', 'n. a.', DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_cities cty WHERE cty.city_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.city, 'n. a.')    AS city_name,
        COALESCE(son.region, 'n. a.')  AS region_name,
        COALESCE(son.country, 'n. a.') AS country_name,
        'sa_sales_online'              AS source_system,
        'src_sales_online'             AS source_entity,
        COALESCE(son.city, 'n. a.')    AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.city IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        COALESCE(spo.city, 'n. a.')    AS city_name,
        COALESCE(spo.region, 'n. a.')  AS region_name,
        COALESCE(spo.country, 'n. a.') AS country_name,
        'sa_sales_pos'                 AS source_system,
        'src_sales_pos'                AS source_entity,
        COALESCE(spo.city, 'n. a.')    AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.city IS NOT NULL
),
map AS (
    SELECT
        src.city_name,
        COALESCE(reg.region_id, -1) AS region_id,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_countries ctr
      ON ctr.country_name  = src.country_name
     AND ctr.source_system = src.source_system
     AND ctr.source_entity = src.source_entity
    LEFT JOIN bl_3nf.ce_regions reg
      ON reg.region_name   = src.region_name
     AND reg.country_id    = ctr.country_id
     AND reg.source_system = src.source_system
     AND reg.source_entity = src.source_entity
)
INSERT INTO bl_3nf.ce_cities (
    city_name, region_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    map.city_name, map.region_id, map.source_system, map.source_entity, map.source_id, now(), now()
FROM map
ON CONFLICT (city_name, region_id, source_system, source_entity)
DO UPDATE
SET
    source_id    = EXCLUDED.source_id,
    ta_update_dt = now();

COMMIT;

SELECT * FROM bl_3nf.ce_cities;

--ce_store_formats SCD1
BEGIN;

INSERT INTO bl_3nf.ce_store_formats (
    store_format_id, store_format_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    -1, 'n. a.', 'manual', 'manual', 'n. a.', DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_store_formats sft WHERE sft.store_format_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(spo.store_format, 'n. a.') AS store_format_name,
        'sa_sales_pos'                      AS source_system,
        'src_sales_pos'                     AS source_entity,
        COALESCE(spo.store_format, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.store_format IS NOT NULL
)
INSERT INTO bl_3nf.ce_store_formats (
    store_format_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    src.store_format_name, src.source_system, src.source_entity, src.source_id, now(), now()
FROM src
ON CONFLICT (store_format_name, source_system, source_entity)
DO UPDATE
SET
    source_id    = EXCLUDED.source_id,
    ta_update_dt = now();

COMMIT;

SELECT * FROM bl_3nf.ce_store_formats;

--ce_stores
BEGIN;

INSERT INTO bl_3nf.ce_stores (
    store_id, store_src_id, store_format_id, store_open_dt, store_open_time, store_close_time, city_id,
    source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    -1, 'n. a.', -1, DATE '1900-01-01', TIME '00:00:00', TIME '00:00:00', -1,
    'manual', 'manual', 'n. a.', DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_stores str WHERE str.store_id = -1
);

COMMIT;

BEGIN;

WITH src_raw AS (
    SELECT
        spo.store_id                      AS store_src_id,
        spo.store_format                  AS store_format_name,
        spo.store_open_dt,
        spo.store_open_time,
        spo.store_close_time,
        spo.city                          AS city_name,
        spo.region                        AS region_name,
        spo.country                       AS country_name,
        'sa_sales_pos'                    AS source_system,
        'src_sales_pos'                   AS source_entity
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.store_id IS NOT NULL
),
src AS (
    SELECT
        store_src_id,
        source_system,
        source_entity,

        MAX(store_format_name) AS store_format_name,
        MIN(store_open_dt)     AS store_open_dt,
        MIN(store_open_time)   AS store_open_time,
        MAX(store_close_time)  AS store_close_time,
        MAX(city_name)         AS city_name,
        MAX(region_name)       AS region_name,
        MAX(country_name)      AS country_name
    FROM src_raw
    GROUP BY
        store_src_id,
        source_system,
        source_entity
),
map AS (
    SELECT
        s.store_src_id,
        COALESCE(sft.store_format_id, -1) AS store_format_id,
        s.store_open_dt,
        s.store_open_time,
        s.store_close_time,
        COALESCE(cty.city_id, -1)         AS city_id,
        s.source_system,
        s.source_entity,
        s.store_src_id                    AS source_id
    FROM src s
    LEFT JOIN bl_3nf.ce_store_formats sft
      ON sft.store_format_name = s.store_format_name
     AND sft.source_system     = s.source_system
     AND sft.source_entity     = s.source_entity
    LEFT JOIN bl_3nf.ce_countries ctr
      ON ctr.country_name      = s.country_name
     AND ctr.source_system     = s.source_system
     AND ctr.source_entity     = s.source_entity
    LEFT JOIN bl_3nf.ce_regions reg
      ON reg.region_name       = s.region_name
     AND reg.country_id        = ctr.country_id
     AND reg.source_system     = s.source_system
     AND reg.source_entity     = s.source_entity
    LEFT JOIN bl_3nf.ce_cities cty
      ON cty.city_name         = s.city_name
     AND cty.region_id         = reg.region_id
     AND cty.source_system     = s.source_system
     AND cty.source_entity     = s.source_entity
)
INSERT INTO bl_3nf.ce_stores (
    store_src_id, store_format_id, store_open_dt, store_open_time, store_close_time, city_id,
    source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    map.store_src_id, map.store_format_id, map.store_open_dt, map.store_open_time, map.store_close_time, map.city_id,
    map.source_system, map.source_entity, map.source_id, now(), now()
FROM map
ON CONFLICT (store_src_id, source_system, source_entity)
DO UPDATE
SET
    store_format_id  = EXCLUDED.store_format_id,
    store_open_dt    = EXCLUDED.store_open_dt,
    store_open_time  = EXCLUDED.store_open_time,
    store_close_time = EXCLUDED.store_close_time,
    city_id          = EXCLUDED.city_id,
    source_id        = EXCLUDED.source_id,
    ta_update_dt     = now();

COMMIT;

SELECT * FROM bl_3nf.ce_stores;

--ce_delivery_addresses
BEGIN;

INSERT INTO bl_3nf.ce_delivery_addresses (
    delivery_address_id,
    delivery_postal_code,
    delivery_address_line1,
    city_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    'n. a.',
    -1,
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_delivery_addresses dad
    WHERE dad.delivery_address_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.delivery_postal_code, 'n. a.')   AS delivery_postal_code,
        COALESCE(son.delivery_address_line1, 'n. a.') AS delivery_address_line1,
        COALESCE(son.city, 'n. a.')                   AS city_name,
        COALESCE(son.region, 'n. a.')                 AS region_name,
        COALESCE(son.country, 'n. a.')                AS country_name,
        'sa_sales_online'                              AS source_system,
        'src_sales_online'                             AS source_entity,
        COALESCE(son.delivery_postal_code, 'n. a.')    AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.delivery_postal_code IS NOT NULL
),
map AS (
    SELECT
        src.delivery_postal_code,
        src.delivery_address_line1,
        COALESCE(cty.city_id, -1) AS city_id,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
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
),
to_insert AS (
    SELECT
        map.delivery_postal_code,
        map.delivery_address_line1,
        map.city_id,
        map.source_system,
        map.source_entity,
        map.source_id
    FROM map
    LEFT JOIN bl_3nf.ce_delivery_addresses dad
      ON dad.delivery_postal_code   = map.delivery_postal_code
     AND dad.delivery_address_line1 = map.delivery_address_line1
     AND dad.city_id                = map.city_id
     AND dad.source_system          = map.source_system
     AND dad.source_entity          = map.source_entity
    WHERE dad.delivery_address_id IS NULL
)
INSERT INTO bl_3nf.ce_delivery_addresses (
    delivery_postal_code,
    delivery_address_line1,
    city_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    tin.delivery_postal_code,
    tin.delivery_address_line1,
    tin.city_id,
    tin.source_system,
    tin.source_entity,
    tin.source_id,
    now(),
    now()
FROM to_insert tin;

COMMIT;

SELECT count(*) FROM bl_3nf.ce_delivery_addresses;
SELECT * FROM bl_3nf.ce_delivery_addresses LIMIT 5;

--ce_fulfillment_centers SCD0
BEGIN;

INSERT INTO bl_3nf.ce_fulfillment_centers (
    fulfillment_center_id, fulfillment_center_src_id, city_id,
    source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT
    -1, 'n. a.', -1,
    'manual', 'manual', 'n. a.', DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_fulfillment_centers ful WHERE ful.fulfillment_center_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.fulfillment_center_id, 'n. a.') AS fulfillment_center_src_id,
        COALESCE(son.fulfillment_city, 'n. a.')      AS city_name,
        'sa_sales_online'                            AS source_system,
        'src_sales_online'                           AS source_entity,
        COALESCE(son.fulfillment_center_id, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.fulfillment_center_id IS NOT NULL
),
map AS (
    SELECT
        src.fulfillment_center_src_id,
        COALESCE(cty.city_id, -1) AS city_id,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_cities cty
      ON cty.city_name     = src.city_name
     AND cty.source_system = src.source_system
     AND cty.source_entity = src.source_entity
),
to_insert AS (
    SELECT
        map.fulfillment_center_src_id,
        map.city_id,
        map.source_system,
        map.source_entity,
        map.source_id
    FROM map
    LEFT JOIN bl_3nf.ce_fulfillment_centers ful
      ON ful.fulfillment_center_src_id = map.fulfillment_center_src_id
     AND ful.city_id                   = map.city_id
     AND ful.source_system             = map.source_system
     AND ful.source_entity             = map.source_entity
    WHERE ful.fulfillment_center_id IS NULL
)
INSERT INTO bl_3nf.ce_fulfillment_centers (
    fulfillment_center_src_id,
    city_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    tin.fulfillment_center_src_id,
    tin.city_id,
    tin.source_system,
    tin.source_entity,
    tin.source_id,
    now(),
    now()
FROM to_insert tin;

COMMIT;

SELECT * FROM bl_3nf.ce_fulfillment_centers;

--ce_delivery_types scd0
BEGIN;

INSERT INTO bl_3nf.ce_delivery_types (
    delivery_type_id,
    delivery_type_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_delivery_types dty
    WHERE dty.delivery_type_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.delivery_type, 'n. a.') AS delivery_type_name,
        'sa_sales_online'                    AS source_system,
        'src_sales_online'                   AS source_entity,
        COALESCE(son.delivery_type, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.delivery_type IS NOT NULL
),
to_insert AS (
    SELECT
        src.delivery_type_name,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_delivery_types dty
      ON dty.delivery_type_name = src.delivery_type_name
     AND dty.source_system      = src.source_system
     AND dty.source_entity      = src.source_entity
    WHERE dty.delivery_type_id IS NULL
)
INSERT INTO bl_3nf.ce_delivery_types (
    delivery_type_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    tin.delivery_type_name,
    tin.source_system,
    tin.source_entity,
    tin.source_id,
    now(),
    now()
FROM to_insert tin;

COMMIT;

SELECT * FROM bl_3nf.ce_delivery_types;

--ce_delivery_providers
BEGIN;

INSERT INTO bl_3nf.ce_delivery_providers (
    delivery_provider_id,
    carrier_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_delivery_providers prv
    WHERE prv.delivery_provider_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.carrier_name, 'n. a.') AS carrier_name,
        'sa_sales_online'                   AS source_system,
        'src_sales_online'                  AS source_entity,
        COALESCE(son.carrier_name, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.carrier_name IS NOT NULL
),
to_insert AS (
    SELECT
        src.carrier_name,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_delivery_providers prv
      ON prv.carrier_name  = src.carrier_name
     AND prv.source_system = src.source_system
     AND prv.source_entity = src.source_entity
    WHERE prv.delivery_provider_id IS NULL
)
INSERT INTO bl_3nf.ce_delivery_providers (
    carrier_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    tin.carrier_name,
    tin.source_system,
    tin.source_entity,
    tin.source_id,
    now(),
    now()
FROM to_insert tin;

COMMIT;

SELECT * FROM bl_3nf.ce_delivery_providers;

--ce_terminal_types  scd0
BEGIN;

INSERT INTO bl_3nf.ce_terminal_types (
    terminal_type_id,
    terminal_type_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_terminal_types tty
    WHERE tty.terminal_type_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(spo.terminal_type, 'n. a.') AS terminal_type_name,
        'sa_sales_pos'                       AS source_system,
        'src_sales_pos'                      AS source_entity,
        COALESCE(spo.terminal_type, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.terminal_type IS NOT NULL
),
to_insert AS (
    SELECT
        src.terminal_type_name,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_terminal_types tty
      ON tty.terminal_type_name = src.terminal_type_name
     AND tty.source_system      = src.source_system
     AND tty.source_entity      = src.source_entity
    WHERE tty.terminal_type_id IS NULL
)
INSERT INTO bl_3nf.ce_terminal_types (
    terminal_type_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    tin.terminal_type_name,
    tin.source_system,
    tin.source_entity,
    tin.source_id,
    now(),
    now()
FROM to_insert tin;

COMMIT;

SELECT * FROM bl_3nf.ce_terminal_types;

--ce_terminals
BEGIN;

INSERT INTO bl_3nf.ce_terminals (
    terminal_id,
    terminal_src_id,
    terminal_type_id,
    store_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    -1,
    'n. a.',
    -1,
    -1,
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_terminals ter
    WHERE ter.terminal_id = -1
);

COMMIT;

BEGIN;

WITH src_raw AS (
    SELECT
        COALESCE(spo.terminal_id, 'n. a.')   AS terminal_src_id,
        COALESCE(spo.terminal_type, 'n. a.') AS terminal_type_name,
        COALESCE(spo.store_id, 'n. a.')      AS store_src_id,
        COALESCE(spo.txn_ts, TIMESTAMP '1900-01-01') AS txn_ts,
        'sa_sales_pos'                       AS source_system,
        'src_sales_pos'                      AS source_entity,
        COALESCE(spo.terminal_id, 'n. a.')   AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.terminal_id IS NOT NULL
),
src AS (
    SELECT DISTINCT ON (srr.terminal_src_id, srr.source_system, srr.source_entity)
        srr.terminal_src_id,
        srr.terminal_type_name,
        srr.store_src_id,
        srr.source_system,
        srr.source_entity,
        srr.source_id
    FROM src_raw srr
    ORDER BY
        srr.terminal_src_id,
        srr.source_system,
        srr.source_entity,
        srr.txn_ts DESC
),
map AS (
    SELECT
        src.terminal_src_id,
        COALESCE(tty.terminal_type_id, -1) AS terminal_type_id,
        COALESCE(str.store_id, -1)         AS store_id,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_terminal_types tty
      ON tty.terminal_type_name = src.terminal_type_name
     AND tty.source_system      = src.source_system
     AND tty.source_entity      = src.source_entity
    LEFT JOIN bl_3nf.ce_stores str
      ON str.store_src_id       = src.store_src_id
     AND str.source_system      = src.source_system
     AND str.source_entity      = src.source_entity
)
INSERT INTO bl_3nf.ce_terminals (
    terminal_src_id,
    terminal_type_id,
    store_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    map.terminal_src_id,
    map.terminal_type_id,
    map.store_id,
    map.source_system,
    map.source_entity,
    map.source_id,
    now(),
    now()
FROM map
ON CONFLICT (terminal_src_id, source_system, source_entity)
DO UPDATE
SET
    terminal_type_id = EXCLUDED.terminal_type_id,
    store_id         = EXCLUDED.store_id,
    source_id        = EXCLUDED.source_id,
    ta_update_dt     = now();

COMMIT;

SELECT * FROM bl_3nf.ce_terminals;

--ce_shifts scd0
BEGIN;

INSERT INTO bl_3nf.ce_shifts (
    shift_id, shift_src_id,
    source_system, source_entity, source_id,
    ta_insert_dt, ta_update_dt
)
SELECT
    -1, 'n. a.',
    'manual', 'manual', 'n. a.',
    DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_shifts sft WHERE sft.shift_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(spo.shift_id, 'n. a.') AS shift_src_id,
        'sa_sales_pos'                  AS source_system,
        'src_sales_pos'                 AS source_entity,
        COALESCE(spo.shift_id, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.shift_id IS NOT NULL
),
to_insert AS (
    SELECT
        src.shift_src_id,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_shifts sft
      ON sft.shift_src_id  = src.shift_src_id
     AND sft.source_system = src.source_system
     AND sft.source_entity = src.source_entity
    WHERE sft.shift_id IS NULL
)
INSERT INTO bl_3nf.ce_shifts (
    shift_src_id,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    tin.shift_src_id,
    tin.source_system,
    tin.source_entity,
    tin.source_id,
    now(),
    now()
FROM to_insert tin;

COMMIT;

SELECT * FROM bl_3nf.ce_shifts;

--ce_payment_gateways
BEGIN;

INSERT INTO bl_3nf.ce_payment_gateways (
    payment_gateway_id, payment_gateway_name,
    source_system, source_entity, source_id,
    ta_insert_dt, ta_update_dt
)
SELECT
    -1, 'n. a.',
    'manual', 'manual', 'n. a.',
    DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_payment_gateways pgt WHERE pgt.payment_gateway_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.payment_gateway, 'n. a.') AS payment_gateway_name,
        'sa_sales_online'                      AS source_system,
        'src_sales_online'                     AS source_entity,
        COALESCE(son.payment_gateway, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.payment_gateway IS NOT NULL
),
to_insert AS (
    SELECT
        src.payment_gateway_name,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN bl_3nf.ce_payment_gateways pgt
      ON pgt.payment_gateway_name = src.payment_gateway_name
     AND pgt.source_system        = src.source_system
     AND pgt.source_entity        = src.source_entity
    WHERE pgt.payment_gateway_id IS NULL
)
INSERT INTO bl_3nf.ce_payment_gateways (
    payment_gateway_name,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    tin.payment_gateway_name,
    tin.source_system,
    tin.source_entity,
    tin.source_id,
    now(),
    now()
FROM to_insert tin;

COMMIT;

SELECT * FROM bl_3nf.ce_payment_gateways;

--ce_employees scd1
BEGIN;

INSERT INTO bl_3nf.ce_employees (
    employee_id, employee_src_id,
    first_name, last_name, department, position, hire_dt,
    source_system, source_entity, source_id,
    ta_insert_dt, ta_update_dt
)
SELECT
    -1, 'n. a.',
    'n. a.', 'n. a.', 'n. a.', 'n. a.', DATE '1900-01-01',
    'manual', 'manual', 'n. a.',
    DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_employees emp WHERE emp.employee_id = -1
);

COMMIT;



