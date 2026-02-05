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

BEGIN;

WITH src_raw AS (
    SELECT
        COALESCE(spo.cashier_id, 'n. a.')         AS employee_src_id,
        COALESCE(spo.cashier_first_name, 'n. a.') AS first_name,
        COALESCE(spo.cashier_last_name, 'n. a.')  AS last_name,
        COALESCE(spo.cashier_dept, 'n. a.')       AS department,
        COALESCE(spo.cashier_position, 'n. a.')   AS position,
        COALESCE(spo.cashier_hire_dt, DATE '1900-01-01') AS hire_dt,
        COALESCE(spo.txn_ts, TIMESTAMP '1900-01-01')     AS txn_ts,
        'sa_sales_pos'                            AS source_system,
        'src_sales_pos'                           AS source_entity,
        COALESCE(spo.cashier_id, 'n. a.')         AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.cashier_id IS NOT NULL
),
src AS (
    SELECT DISTINCT ON (srr.employee_src_id, srr.source_system, srr.source_entity)
        srr.employee_src_id,
        srr.first_name,
        srr.last_name,
        srr.department,
        srr.position,
        srr.hire_dt,
        srr.source_system,
        srr.source_entity,
        srr.source_id
    FROM src_raw srr
    ORDER BY
        srr.employee_src_id,
        srr.source_system,
        srr.source_entity,
        srr.txn_ts DESC
)
INSERT INTO bl_3nf.ce_employees (
    employee_src_id,
    first_name,
    last_name,
    department,
    position,
    hire_dt,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    src.employee_src_id,
    src.first_name,
    src.last_name,
    src.department,
    src.position,
    src.hire_dt,
    src.source_system,
    src.source_entity,
    src.source_id,
    now(),
    now()
FROM src
ON CONFLICT (employee_src_id, source_system, source_entity)
DO UPDATE
SET
    first_name   = EXCLUDED.first_name,
    last_name    = EXCLUDED.last_name,
    department   = EXCLUDED.department,
    position     = EXCLUDED.position,
    hire_dt      = EXCLUDED.hire_dt,
    source_id    = EXCLUDED.source_id,
    ta_update_dt = now();

COMMIT;

SELECT * FROM bl_3nf.ce_employees;

--ce_device_types scd0
BEGIN;

INSERT INTO bl_3nf.ce_device_types
SELECT
    -1, 'n. a.', 'manual', 'manual', 'n. a.',
    DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_device_types WHERE device_type_id = -1
);

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.device_type, 'n. a.') AS device_type_name,
        'sa_sales_online'                  AS source_system,
        'src_sales_online'                 AS source_entity,
        COALESCE(son.device_type, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.device_type IS NOT NULL
),
to_insert AS (
    SELECT src.*
    FROM src
    LEFT JOIN bl_3nf.ce_device_types dvc
      ON dvc.device_type_name = src.device_type_name
     AND dvc.source_system    = src.source_system
     AND dvc.source_entity    = src.source_entity
    WHERE dvc.device_type_id IS NULL
)
INSERT INTO bl_3nf.ce_device_types (
    device_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT device_type_name, source_system, source_entity, source_id, now(), now()
FROM to_insert;

COMMIT;

SELECT * FROM bl_3nf.ce_device_types;

--ce_order_statuses
INSERT INTO bl_3nf.ce_order_statuses
SELECT
    -1, 'n. a.', 'manual', 'manual', 'n. a.',
    DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_order_statuses WHERE order_status_id = -1
);

WITH src AS (
    SELECT DISTINCT
        COALESCE(son.order_status, 'n. a.') AS order_status_name,
        'sa_sales_online'                   AS source_system,
        'src_sales_online'                  AS source_entity,
        COALESCE(son.order_status, 'n. a.') AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.order_status IS NOT NULL
),
to_insert AS (
    SELECT src.*
    FROM src
    LEFT JOIN bl_3nf.ce_order_statuses ord
      ON ord.order_status_name = src.order_status_name
     AND ord.source_system     = src.source_system
     AND ord.source_entity     = src.source_entity
    WHERE ord.order_status_id IS NULL
)
INSERT INTO bl_3nf.ce_order_statuses (
    order_status_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT order_status_name, source_system, source_entity, source_id, now(), now()
FROM to_insert;

COMMIT;

SELECT * FROM bl_3nf.ce_order_statuses;

--ce_sales_channels scd0
BEGIN;

INSERT INTO bl_3nf.ce_sales_channels
SELECT
    -1, 'n. a.', 'manual', 'manual', 'n. a.',
    DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_sales_channels WHERE sales_channel_id = -1
);

WITH src AS (
    SELECT DISTINCT
        'online'              AS sales_channel_name,
        'sa_sales_online'     AS source_system,
        'src_sales_online'    AS source_entity,
        'online'              AS source_id
    UNION ALL
    SELECT DISTINCT
        'pos', 'sa_sales_pos', 'src_sales_pos', 'pos'
),
to_insert AS (
    SELECT src.*
    FROM src
    LEFT JOIN bl_3nf.ce_sales_channels sch
      ON sch.sales_channel_name = src.sales_channel_name
     AND sch.source_system      = src.source_system
     AND sch.source_entity      = src.source_entity
    WHERE sch.sales_channel_id IS NULL
)
INSERT INTO bl_3nf.ce_sales_channels (
    sales_channel_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
)
SELECT sales_channel_name, source_system, source_entity, source_id, now(), now()
FROM to_insert;

COMMIT;

SELECT * FROM bl_3nf.ce_sales_channels;

--3tables from online channel, type scd0
BEGIN;

/* CE_CARD_TYPES */
INSERT INTO bl_3nf.ce_card_types
SELECT
    -1, 'n. a.', 'manual', 'manual', 'n. a.',
    DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_card_types WHERE card_type_id = -1
);

/* CE_PAYMENT_METHODS */
INSERT INTO bl_3nf.ce_payment_methods
SELECT
    -1, 'n. a.', 'manual', 'manual', 'n. a.',
    DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_payment_methods WHERE payment_method_id = -1
);

/* CE_RECEIPT_TYPES */
INSERT INTO bl_3nf.ce_receipt_types
SELECT
    -1, 'n. a.', 'manual', 'manual', 'n. a.',
    DATE '1900-01-01', DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_receipt_types WHERE receipt_type_id = -1
);

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(spo.card_type, 'n. a.') AS card_type_name,
        'sa_sales_pos'                   AS source_system,
        'src_sales_pos'                  AS source_entity,
        COALESCE(spo.card_type, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.card_type IS NOT NULL
),
to_insert AS (
    SELECT src.*
    FROM src
    LEFT JOIN bl_3nf.ce_card_types crt
      ON crt.card_type_name = src.card_type_name
     AND crt.source_system  = src.source_system
     AND crt.source_entity  = src.source_entity
    WHERE crt.card_type_id IS NULL
)
INSERT INTO bl_3nf.ce_card_types (
    card_type_name, source_system, source_entity, source_id,
    ta_insert_dt, ta_update_dt
)
SELECT
    card_type_name, source_system, source_entity, source_id,
    now(), now()
FROM to_insert;

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(spo.payment_method, 'n. a.') AS payment_method_name,
        'sa_sales_pos'                        AS source_system,
        'src_sales_pos'                       AS source_entity,
        COALESCE(spo.payment_method, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.payment_method IS NOT NULL
),
to_insert AS (
    SELECT src.*
    FROM src
    LEFT JOIN bl_3nf.ce_payment_methods pmt
      ON pmt.payment_method_name = src.payment_method_name
     AND pmt.source_system       = src.source_system
     AND pmt.source_entity       = src.source_entity
    WHERE pmt.payment_method_id IS NULL
)
INSERT INTO bl_3nf.ce_payment_methods (
    payment_method_name, source_system, source_entity, source_id,
    ta_insert_dt, ta_update_dt
)
SELECT
    payment_method_name, source_system, source_entity, source_id,
    now(), now()
FROM to_insert;

COMMIT;

BEGIN;

WITH src AS (
    SELECT DISTINCT
        COALESCE(spo.receipt_type, 'n. a.') AS receipt_type_name,
        'sa_sales_pos'                      AS source_system,
        'src_sales_pos'                     AS source_entity,
        COALESCE(spo.receipt_type, 'n. a.') AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.receipt_type IS NOT NULL
),
to_insert AS (
    SELECT src.*
    FROM src
    LEFT JOIN bl_3nf.ce_receipt_types rct
      ON rct.receipt_type_name = src.receipt_type_name
     AND rct.source_system     = src.source_system
     AND rct.source_entity     = src.source_entity
    WHERE rct.receipt_type_id IS NULL
)
INSERT INTO bl_3nf.ce_receipt_types (
    receipt_type_name, source_system, source_entity, source_id,
    ta_insert_dt, ta_update_dt
)
SELECT
    receipt_type_name, source_system, source_entity, source_id,
    now(), now()
FROM to_insert;

COMMIT;
SELECT * FROM bl_3nf.ce_card_types;
SELECT * FROM bl_3nf.ce_payment_methods;
SELECT * FROM bl_3nf.ce_receipt_types;

--ce_customers_scd  type2
BEGIN;

INSERT INTO bl_3nf.ce_customers_scd (
    customer_id,
    customer_src_id,
    first_name,
    last_name,
    email,
    phone,
    age_grp,
    customer_segment,
    gender,
    start_dt,
    end_dt,
    is_active,
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
    'n. a.',
    'n. a.',
    'n. a.',
    'n. a.',
    'n. a.',
    'n. a.',
    DATE '1900-01-01',
    DATE '9999-12-31',
    TRUE,
    'manual',
    'manual',
    'n. a.',
    DATE '1900-01-01',
    DATE '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1
    FROM bl_3nf.ce_customers_scd cus
    WHERE cus.customer_id = -1
);

COMMIT;

BEGIN;

WITH src_raw AS (
    /* ONLINE */
    SELECT
        COALESCE(son.customer_src_id, 'n. a.')       AS customer_src_id,
        COALESCE(son.customer_first_name, 'n. a.')   AS first_name,
        COALESCE(son.customer_last_name, 'n. a.')    AS last_name,
        COALESCE(son.customer_email, 'n. a.')        AS email,
        COALESCE(son.customer_phone, 'n. a.')        AS phone,
        COALESCE(son.customer_age_group, 'n. a.')    AS age_grp,
        COALESCE(son.customer_segment, 'n. a.')      AS customer_segment,
        COALESCE(son.gender, 'n. a.')                AS gender,
        COALESCE(son.txn_ts, TIMESTAMP '1900-01-01') AS txn_ts,
        'sa_sales_online'                             AS source_system,
        'src_sales_online'                            AS source_entity,
        COALESCE(son.customer_src_id, 'n. a.')       AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.customer_src_id IS NOT NULL

    UNION ALL

    /* POS */
    SELECT
        COALESCE(spo.customer_src_id, 'n. a.')       AS customer_src_id,
        'n. a.'                                      AS first_name,
        'n. a.'                                      AS last_name,
        'n. a.'                                      AS email,
        COALESCE(spo.customer_phone, 'n. a.')        AS phone,
        COALESCE(spo.customer_age_group, 'n. a.')    AS age_grp,
        COALESCE(spo.customer_segment, 'n. a.')      AS customer_segment,
        'n. a.'                                      AS gender,
        COALESCE(spo.txn_ts, TIMESTAMP '1900-01-01') AS txn_ts,
        'sa_sales_pos'                                AS source_system,
        'src_sales_pos'                               AS source_entity,
        COALESCE(spo.customer_src_id, 'n. a.')       AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.customer_src_id IS NOT NULL
),
src AS (
    SELECT DISTINCT ON (srr.customer_src_id, srr.source_system, srr.source_entity)
        srr.customer_src_id,
        srr.first_name,
        srr.last_name,
        srr.email,
        srr.phone,
        srr.age_grp,
        srr.customer_segment,
        srr.gender,
        srr.source_system,
        srr.source_entity,
        srr.source_id
    FROM src_raw srr
    ORDER BY
        srr.customer_src_id,
        srr.source_system,
        srr.source_entity,
        srr.txn_ts DESC
),
cur AS (
    SELECT
        cus.customer_id,
        cus.customer_src_id,
        cus.first_name,
        cus.last_name,
        cus.email,
        cus.phone,
        cus.age_grp,
        cus.customer_segment,
        cus.gender,
        cus.source_system,
        cus.source_entity
    FROM bl_3nf.ce_customers_scd cus
    WHERE cus.is_active = TRUE
      AND cus.end_dt = DATE '9999-12-31'
      AND cus.customer_id <> -1
),
chg AS (
    SELECT
        cur.customer_id,
        src.customer_src_id,
        src.first_name,
        src.last_name,
        src.email,
        src.phone,
        src.age_grp,
        src.customer_segment,
        src.gender,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    JOIN cur
      ON cur.customer_src_id = src.customer_src_id
     AND cur.source_system   = src.source_system
     AND cur.source_entity   = src.source_entity
    WHERE
        cur.first_name        IS DISTINCT FROM src.first_name
     OR cur.last_name         IS DISTINCT FROM src.last_name
     OR cur.email             IS DISTINCT FROM src.email
     OR cur.phone             IS DISTINCT FROM src.phone
     OR cur.age_grp           IS DISTINCT FROM src.age_grp
     OR cur.customer_segment  IS DISTINCT FROM src.customer_segment
     OR cur.gender            IS DISTINCT FROM src.gender
),
new_or_changed AS (
    /* NEW */
    SELECT
        src.customer_src_id,
        src.first_name,
        src.last_name,
        src.email,
        src.phone,
        src.age_grp,
        src.customer_segment,
        src.gender,
        src.source_system,
        src.source_entity,
        src.source_id
    FROM src
    LEFT JOIN cur
      ON cur.customer_src_id = src.customer_src_id
     AND cur.source_system   = src.source_system
     AND cur.source_entity   = src.source_entity
    WHERE cur.customer_id IS NULL

    UNION ALL

    /* CHANGED */
    SELECT
        chg.customer_src_id,
        chg.first_name,
        chg.last_name,
        chg.email,
        chg.phone,
        chg.age_grp,
        chg.customer_segment,
        chg.gender,
        chg.source_system,
        chg.source_entity,
        chg.source_id
    FROM chg
),
upd AS (
    UPDATE bl_3nf.ce_customers_scd cus
    SET
        end_dt       = CURRENT_DATE - 1,
        is_active    = FALSE,
        ta_update_dt = now()
    FROM chg
    WHERE cus.customer_id = chg.customer_id
      AND cus.is_active = TRUE
      AND cus.end_dt = DATE '9999-12-31'
    RETURNING cus.customer_id
)
INSERT INTO bl_3nf.ce_customers_scd (
    customer_src_id,
    first_name,
    last_name,
    email,
    phone,
    age_grp,
    customer_segment,
    gender,
    start_dt,
    end_dt,
    is_active,
    source_system,
    source_entity,
    source_id,
    ta_insert_dt,
    ta_update_dt
)
SELECT
    nac.customer_src_id,
    nac.first_name,
    nac.last_name,
    nac.email,
    nac.phone,
    nac.age_grp,
    nac.customer_segment,
    nac.gender,
    CURRENT_DATE,
    DATE '9999-12-31',
    TRUE,
    nac.source_system,
    nac.source_entity,
    nac.source_id,
    now(),
    now()
FROM new_or_changed nac
LEFT JOIN bl_3nf.ce_customers_scd cus
  ON cus.customer_src_id = nac.customer_src_id
 AND cus.source_system   = nac.source_system
 AND cus.source_entity   = nac.source_entity
 AND cus.start_dt        = CURRENT_DATE
WHERE cus.customer_id IS NULL;

COMMIT;

SELECT * FROM bl_3nf.ce_customers_scd;


--ce_transactions
--had iisues with this table => was loading more then 30min, found that adding indexes and additional function could spead this process


BEGIN;

WITH src_raw AS (
    /* ONLINE */
    SELECT
        COALESCE(son.web_order_id, 'n. a.')                 AS txn_src_id,
        COALESCE(son.txn_ts, TIMESTAMP '1900-01-01')        AS txn_ts,
        COALESCE(son.product_sku, 'n. a.')                  AS product_sku_src_id,
        COALESCE(son.promo_code, 'n. a.')                   AS promo_code,
        'online'                                            AS sales_channel_name,
        COALESCE(son.customer_src_id, 'n. a.')              AS customer_src_id,
        'n. a.'                                             AS payment_method_name,
        'n. a.'                                             AS card_type_name,
        'n. a.'                                             AS receipt_type_name,
        'n. a.'                                             AS store_src_id,
        'n. a.'                                             AS terminal_src_id,
        'n. a.'                                             AS employee_src_id,
        'n. a.'                                             AS shift_src_id,
        COALESCE(son.order_status, 'n. a.')                 AS order_status_name,
        COALESCE(son.carrier_name, 'n. a.')                 AS carrier_name,
        COALESCE(son.delivery_type, 'n. a.')                AS delivery_type_name,

        COALESCE(son.delivery_postal_code, 'n. a.')         AS delivery_postal_code,
        COALESCE(son.delivery_address_line1, 'n. a.')       AS delivery_address_line1,
        COALESCE(son.city, 'n. a.')                         AS city_name,
        COALESCE(son.region, 'n. a.')                       AS region_name,
        COALESCE(son.country, 'n. a.')                      AS country_name,

        COALESCE(son.fulfillment_center_id, 'n. a.')        AS fulfillment_center_src_id,
        COALESCE(son.fulfillment_city, 'n. a.')             AS fulfillment_city_name,

        COALESCE(son.device_type, 'n. a.')                  AS device_type_name,
        COALESCE(son.payment_gateway, 'n. a.')              AS payment_gateway_name,

        COALESCE(son.tracking_id, 'n. a.')                  AS tracking_id,
        COALESCE(son.promised_delivery_dt, DATE '1900-01-01') AS promised_delivery_dt,

        COALESCE(son.qty, -1)                               AS qty,
        COALESCE(son.unit_price_amt, -1)                    AS unit_price_amt,
        COALESCE(son.tax_amt, -1)                           AS tax_amt,
        COALESCE(son.shipping_fee_amt, -1)                  AS shipping_fee_amt,
        COALESCE(son.discount_amt, -1)                      AS discount_amt,
        COALESCE(son.sales_amt, -1)                         AS sales_amt,
        COALESCE(son.cost_amt, -1)                          AS cost_amt,
        COALESCE(son.gross_profit_amt, -1)                  AS gross_profit_amt,
        -1::INT 									        AS loyalty_points_earned,
        COALESCE(son.customer_rating, -1)                   AS customer_rating,

        'sa_sales_online'                                   AS source_system,
        'src_sales_online'                                  AS source_entity,
        COALESCE(son.web_order_id, 'n. a.')                 AS source_id
    FROM sa_sales_online.src_sales_online son
    WHERE son.web_order_id IS NOT NULL   
	   


    UNION ALL

    /* POS */
    SELECT
        COALESCE(spo.ckout, 'n. a.')                        AS txn_src_id,
        COALESCE(spo.txn_ts, TIMESTAMP '1900-01-01')        AS txn_ts,
        COALESCE(spo.product_sku, 'n. a.')                  AS product_sku_src_id,
        COALESCE(spo.promo_code, 'n. a.')                   AS promo_code,
        'pos'                                               AS sales_channel_name,
        COALESCE(spo.customer_src_id, 'n. a.')              AS customer_src_id,
        COALESCE(spo.payment_method, 'n. a.')               AS payment_method_name,
        COALESCE(spo.card_type, 'n. a.')                    AS card_type_name,
        COALESCE(spo.receipt_type, 'n. a.')                 AS receipt_type_name,
        COALESCE(spo.store_id, 'n. a.')                     AS store_src_id,
        COALESCE(spo.terminal_id, 'n. a.')                  AS terminal_src_id,
        COALESCE(spo.cashier_id, 'n. a.')                   AS employee_src_id,
        COALESCE(spo.shift_id, 'n. a.')                     AS shift_src_id,
        'n. a.'                                             AS order_status_name,
        'n. a.'                                             AS carrier_name,
        'n. a.'                                             AS delivery_type_name,

        'n. a.'                                             AS delivery_postal_code,
        'n. a.'                                             AS delivery_address_line1,
        'n. a.'                                             AS city_name,
        'n. a.'                                             AS region_name,
        'n. a.'                                             AS country_name,

        'n. a.'                                             AS fulfillment_center_src_id,
        'n. a.'                                             AS fulfillment_city_name,

        'n. a.'                                             AS device_type_name,
        'n. a.'                                             AS payment_gateway_name,

        'n. a.'                                             AS tracking_id,
        DATE '1900-01-01'                                   AS promised_delivery_dt,

        COALESCE(spo.qty, -1)                               AS qty,
        COALESCE(spo.unit_price_amt, -1)                    AS unit_price_amt,
        COALESCE(spo.tax_amt, -1)                           AS tax_amt,
        -1                                                  AS shipping_fee_amt,
        COALESCE(spo.discount_amt, -1)                      AS discount_amt,
        COALESCE(spo.sales_amt, -1)                         AS sales_amt,
        COALESCE(spo.cost_amt, -1)                          AS cost_amt,
        COALESCE(spo.gross_profit_amt, -1)                  AS gross_profit_amt,
        COALESCE(spo.loyalty_points_earned, -1)::INT        AS loyalty_points_earned,
        COALESCE(spo.customer_rating, -1)                   AS customer_rating,

        'sa_sales_pos'                                      AS source_system,
        'src_sales_pos'                                     AS source_entity,
        COALESCE(spo.ckout, 'n. a.')                        AS source_id
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.ckout IS NOT NULL
    --AND spo.txn_ts >= TIMESTAMP '2025-11-01'
	--  AND spo.txn_ts <  TIMESTAMP '2025-11-10'
	),
   
src AS (
   
    SELECT DISTINCT ON (srr.txn_src_id, srr.source_system, srr.source_entity)
        srr.*
    FROM src_raw srr
    ORDER BY
        srr.txn_src_id,
        srr.source_system,
        srr.source_entity,
        srr.txn_ts DESC
),
map AS (
    SELECT
        src.txn_src_id,
        src.txn_ts,

        COALESCE(prd.product_id, -1)           AS product_id,
        COALESCE(pro.promotion_id, -1)         AS promotion_id,
        COALESCE(sch.sales_channel_id, -1)     AS sales_channel_id,
        COALESCE(cus.customer_id, -1)          AS customer_id,
        COALESCE(pmt.payment_method_id, -1)    AS payment_method_id,
        COALESCE(crt.card_type_id, -1)         AS card_type_id,
        COALESCE(rct.receipt_type_id, -1)      AS receipt_type_id,
        COALESCE(str.store_id, -1)             AS store_id,
        COALESCE(ter.terminal_id, -1)          AS terminal_id,
        COALESCE(emp.employee_id, -1)          AS employee_id,
        COALESCE(sft.shift_id, -1)             AS shift_id,
        COALESCE(ord.order_status_id, -1)      AS order_status_id,
        COALESCE(dpr.delivery_provider_id, -1) AS delivery_provider_id,
        COALESCE(dty.delivery_type_id, -1)     AS delivery_type_id,
        COALESCE(adr.delivery_address_id, -1)  AS delivery_address_id,
        COALESCE(ful.fulfillment_center_id, -1) AS fulfillment_center_id,
        COALESCE(dvc.device_type_id, -1)       AS device_type_id,
        COALESCE(pgw.payment_gateway_id, -1)   AS payment_gateway_id,

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
        src.source_id
    FROM src

    LEFT JOIN bl_3nf.ce_products prd
      ON prd.product_sku_src_id = src.product_sku_src_id
     AND prd.source_system      = src.source_system
     AND prd.source_entity      = src.source_entity

    LEFT JOIN bl_3nf.ce_promotions pro
      ON pro.promo_code     = src.promo_code
     AND pro.source_system  = src.source_system
     AND pro.source_entity  = src.source_entity

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
      ON str.store_src_id   = src.store_src_id
     AND str.source_system  = src.source_system
     AND str.source_entity  = src.source_entity

    LEFT JOIN bl_3nf.ce_terminals ter
      ON ter.terminal_src_id = src.terminal_src_id
     AND ter.source_system   = src.source_system
     AND ter.source_entity   = src.source_entity

    LEFT JOIN bl_3nf.ce_employees emp
      ON emp.employee_src_id = src.employee_src_id
     AND emp.source_system   = src.source_system
     AND emp.source_entity   = src.source_entity

    LEFT JOIN bl_3nf.ce_shifts sft
      ON sft.shift_src_id   = src.shift_src_id
     AND sft.source_system  = src.source_system
     AND sft.source_entity  = src.source_entity

    LEFT JOIN bl_3nf.ce_order_statuses ord
      ON ord.order_status_name = src.order_status_name
     AND ord.source_system     = src.source_system
     AND ord.source_entity     = src.source_entity

    LEFT JOIN bl_3nf.ce_delivery_providers dpr
      ON dpr.carrier_name   = src.carrier_name
     AND dpr.source_system  = src.source_system
     AND dpr.source_entity  = src.source_entity

    LEFT JOIN bl_3nf.ce_delivery_types dty
      ON dty.delivery_type_name = src.delivery_type_name
     AND dty.source_system      = src.source_system
     AND dty.source_entity      = src.source_entity

    
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
     
     LEFT JOIN bl_3nf.ce_delivery_addresses adr
	  ON adr.delivery_postal_code   = src.delivery_postal_code
	 AND adr.delivery_address_line1 = src.delivery_address_line1
	 AND adr.city_id                = COALESCE(cty.city_id, -1)
	 AND adr.source_system          = src.source_system
	 AND adr.source_entity          = src.source_entity

),
to_insert AS (
    SELECT DISTINCT ON (map.txn_src_id, map.source_system, map.source_entity)
        map.*
    FROM map
    LEFT JOIN bl_3nf.ce_transactions trx
      ON trx.txn_src_id     = map.txn_src_id
     AND trx.source_system  = map.source_system
     AND trx.source_entity  = map.source_entity
    WHERE trx.txn_id IS NULL
    ORDER BY
        map.txn_src_id,
        map.source_system,
        map.source_entity,
        map.txn_ts DESC
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
    tin.txn_src_id, tin.txn_ts,
    tin.product_id, tin.promotion_id, tin.sales_channel_id, tin.customer_id,
    tin.payment_method_id, tin.card_type_id, tin.receipt_type_id,
    tin.store_id, tin.terminal_id, tin.employee_id, tin.shift_id,
    tin.order_status_id, tin.delivery_provider_id, tin.delivery_type_id,
    tin.delivery_address_id, tin.fulfillment_center_id, tin.device_type_id, tin.payment_gateway_id,
    tin.tracking_id, tin.promised_delivery_dt,
    tin.qty, tin.unit_price_amt, tin.tax_amt, tin.shipping_fee_amt, tin.discount_amt,
    tin.sales_amt, tin.cost_amt, tin.gross_profit_amt,
    tin.loyalty_points_earned, tin.customer_rating,
    tin.source_system, tin.source_entity, tin.source_id,
    now(), now()
FROM to_insert tin;

COMMIT;

SELECT count(*) FROM bl_3nf.ce_transactions;
SELECT * FROM bl_3nf.ce_transactions LIMIT 100;
