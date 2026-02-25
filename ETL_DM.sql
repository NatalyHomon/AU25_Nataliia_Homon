-------------------------------------------------------------------
  -- UNIQUE CONSTRAINTS FOR IMPLEMENTING UPSERT
  -------------------------------------------------------------------

ALTER TABLE bl_dm.dim_products
ADD CONSTRAINT uk_dim_products_nk
UNIQUE (source_system, source_entity, source_id);

ALTER TABLE bl_dm.dim_stores
ADD CONSTRAINT uk_dim_stores_nk
UNIQUE (source_system, source_entity, source_id);

ALTER TABLE bl_dm.dim_terminals          ADD CONSTRAINT uk_dim_terminals_nk          UNIQUE (source_system, source_entity, source_id);
ALTER TABLE bl_dm.dim_employees          ADD CONSTRAINT uk_dim_employees_nk          UNIQUE (source_system, source_entity, source_id);
ALTER TABLE bl_dm.dim_promotions         ADD CONSTRAINT uk_dim_promotions_nk         UNIQUE (source_system, source_entity, source_id);
ALTER TABLE bl_dm.dim_delivery_providers ADD CONSTRAINT uk_dim_delivery_providers_nk UNIQUE (source_system, source_entity, source_id);
ALTER TABLE bl_dm.dim_junk_context       ADD CONSTRAINT uk_dim_junk_context_nk       UNIQUE (source_system, source_entity, source_id);
ALTER TABLE bl_dm.dim_customers_scd      ADD CONSTRAINT uk_dim_customers_scd_nk      UNIQUE (source_system, source_entity, source_id);


-------------------------------------------------------------------
  -- Composite type (t_source_ctx)
  -------------------------------------------------------------------

CREATE TYPE bl_cl.t_source_ctx AS (
  src_layer_3nf   text,  -- for logging: bl_3nf
  src_entity_3nf  text,  -- for logging: ce_products
  dm_layer        text,  -- constant: BL_3NF (lineage in DM)
  dm_entity       text   -- constant: CE_PRODUCTS (lineage in DM)
);

-------------------------------------------------------------------
  --Validation procedure (pr_validate_na_only)
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_validate_na_only(
  p_proc_name         text,
  p_run_id            uuid,
  p_table             regclass,  -- e.g. 'bl_3nf.ce_products'::regclass
  p_column            text,      -- e.g. 'product_name'
  p_pk_column         text,      -- e.g. 'product_id'
  p_limit_examples    int DEFAULT 5,
  p_default_pk_value  text DEFAULT '-1',  -- 👈 ignore default row PK (as text)
  p_src_system_3nf    text DEFAULT NULL,  -- optional filter on 3NF source_system (SA info)
  p_src_entity_3nf    text DEFAULT NULL   -- optional filter on 3NF source_entity (SA info)
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_cnt bigint := 0;

  v_sql_count    text;
  v_sql_examples text;

  c_bad refcursor;
  r_bad record;
BEGIN
  -------------------------------------------------------------------
  -- 1) COUNT only 'n.a.' but IGNORE default row by PK
  -------------------------------------------------------------------
  v_sql_count := format(
    'SELECT count(*) FROM %s
     WHERE lower(btrim(%I::text)) = ''n. a.''
       AND %I::text <> %L',
    p_table, p_column, p_pk_column, p_default_pk_value
  );

  IF p_src_system_3nf IS NOT NULL THEN
    v_sql_count := v_sql_count || format(' AND source_system = %L', p_src_system_3nf);
  END IF;

  IF p_src_entity_3nf IS NOT NULL THEN
    v_sql_count := v_sql_count || format(' AND source_entity = %L', p_src_entity_3nf);
  END IF;

  EXECUTE v_sql_count INTO v_cnt;

  -------------------------------------------------------------------
  -- 2) If found -> log WARN + first N examples (also ignoring default PK)
  -------------------------------------------------------------------
  IF v_cnt > 0 THEN
    CALL bl_cl.pr_log_write(
      p_proc_name,
      'WARN',
      0,
      format('Validation: %s values = ''n. a.'' found in %s.%s (default PK %s ignored).',
             v_cnt, p_table::text, p_column, p_default_pk_value),
      NULL,
      'bl_3nf',
      p_table::text,
      p_run_id
    );

    v_sql_examples := format(
      'SELECT %I::text AS bad_value,
              %I::text AS pk_value
       FROM %s
       WHERE lower(btrim(%I::text)) = ''n. a.''
         AND %I::text <> %L',
      p_column, p_pk_column, p_table, p_column, p_pk_column, p_default_pk_value
    );

    IF p_src_system_3nf IS NOT NULL THEN
      v_sql_examples := v_sql_examples || format(' AND source_system = %L', p_src_system_3nf);
    END IF;

    IF p_src_entity_3nf IS NOT NULL THEN
      v_sql_examples := v_sql_examples || format(' AND source_entity = %L', p_src_entity_3nf);
    END IF;

    v_sql_examples := v_sql_examples || format(' LIMIT %s', p_limit_examples);

    OPEN c_bad FOR EXECUTE v_sql_examples;

    LOOP
      FETCH c_bad INTO r_bad;
      EXIT WHEN NOT FOUND;

      CALL bl_cl.pr_log_write(
        p_proc_name,
        'WARN',
        0,
        format('NA example (default ignored): value=%s, 3NF_PK=%s', r_bad.bad_value, r_bad.pk_value),
        NULL,
        'bl_3nf',
        p_table::text,
        p_run_id
      );
    END LOOP;

    CLOSE c_bad;
  END IF;

END;
$$;


-------------------------------------------------------------------
  -- DIM_PRODUCTS
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_dim_products_dm_simple()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_dim_products_dm_simple';
  v_run_id uuid := gen_random_uuid();
  v_rows   bigint := 0;

  -- ✅ composite type usage (simple)
  v_ctx bl_cl.t_source_ctx;
BEGIN
  -- init composite object once
  v_ctx := ROW('bl_3nf','ce_products','BL_3NF','CE_PRODUCTS')::bl_cl.t_source_ctx;

  IF to_regclass('bl_dm.dim_products') IS NULL THEN
    RAISE EXCEPTION 'Table bl_dm.dim_products does not exist';
  END IF;

  CALL bl_cl.pr_log_write(
    v_proc,'START',0,'Start load DIM_PRODUCTS (NA-only validation).',
    NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id
  );

  -- validation (ignore default PK = -1)
  CALL bl_cl.pr_validate_na_only(
    p_proc_name        => v_proc,
    p_run_id           => v_run_id,
    p_table            => 'bl_3nf.ce_products'::regclass,
    p_column           => 'product_name',
    p_pk_column        => 'product_id',
    p_limit_examples   => 5,
    p_default_pk_value => '-1'
  );

  INSERT INTO bl_dm.dim_products (
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
    p.product_sku_src_id,
    p.product_name,
    sc.product_subcategory_name,
    d.product_department_name,
    b.brand_name,
    u.uom_name,
    s.supplier_src_id,

    -- ✅ lineage in DM from composite type
    v_ctx.dm_layer::varchar(30)  AS source_system,
    v_ctx.dm_entity::varchar(60) AS source_entity,
    p.product_id::varchar(100)   AS source_id

  FROM bl_3nf.ce_products p
  JOIN bl_3nf.ce_product_subcategories sc
    ON sc.product_subcategory_id = p.product_subcategory_id
  JOIN bl_3nf.ce_product_departments d
    ON d.product_department_id = sc.product_department_id
  JOIN bl_3nf.ce_brands b
    ON b.brand_id = p.brand_id
  JOIN bl_3nf.ce_unit_of_measures u
    ON u.uom_id = p.uom_id
  JOIN bl_3nf.ce_suppliers s
    ON s.supplier_id = p.supplier_id
  WHERE p.product_sku_src_id IS NOT NULL
    AND p.product_id <> -1
    AND lower(btrim(p.source_id::text)) <> 'n.a.'   -- якщо так у вас маркується default у 3NF

  ON CONFLICT (source_system, source_entity, source_id)
  DO UPDATE SET
    product_sku_src_id = EXCLUDED.product_sku_src_id,
    product_name = EXCLUDED.product_name,
    product_subcategory_name = EXCLUDED.product_subcategory_name,
    product_department_name = EXCLUDED.product_department_name,
    brand_name = EXCLUDED.brand_name,
    unit_of_measure = EXCLUDED.unit_of_measure,
    supplier_src_id = EXCLUDED.supplier_src_id
  WHERE
    bl_dm.dim_products.product_sku_src_id IS DISTINCT FROM EXCLUDED.product_sku_src_id OR
    bl_dm.dim_products.product_name IS DISTINCT FROM EXCLUDED.product_name OR
    bl_dm.dim_products.product_subcategory_name IS DISTINCT FROM EXCLUDED.product_subcategory_name OR
    bl_dm.dim_products.product_department_name IS DISTINCT FROM EXCLUDED.product_department_name OR
    bl_dm.dim_products.brand_name IS DISTINCT FROM EXCLUDED.brand_name OR
    bl_dm.dim_products.unit_of_measure IS DISTINCT FROM EXCLUDED.unit_of_measure OR
    bl_dm.dim_products.supplier_src_id IS DISTINCT FROM EXCLUDED.supplier_src_id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  CALL bl_cl.pr_log_write(
    v_proc,'SUCCESS',v_rows,'DIM_PRODUCTS loaded (NA-only validation).',
    NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id
  );

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);
  RAISE;
END;
$$;

-------------------------------------------------------------------
  -- CHECK
  -------------------------------------------------------------------


CALL bl_cl.pr_load_dim_products_dm_simple();


SELECT 
    log_dts,
    procedure_name,
    status,
    rows_affected,
    message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_products_dm_simple'
ORDER BY log_dts DESC;

SELECT *
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_products_dm_simple'
  AND status = 'WARN'
ORDER BY log_dts DESC;

SELECT* FROM bl_dm.dim_products;
SELECT COUNT (*) FROM bl_dm.dim_products;

-------------------------------------------------------------------
  -- DIM_STORES
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_dim_stores_dm_simple()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_dim_stores_dm_simple';
  v_run_id uuid := gen_random_uuid();
  v_rows   bigint := 0;

  -- composite type for lineage context
  v_ctx bl_cl.t_source_ctx;
BEGIN
  -- initialize context
  v_ctx := ROW('bl_3nf','ce_stores','BL_3NF','CE_STORES')::bl_cl.t_source_ctx;

  IF to_regclass('bl_dm.dim_stores') IS NULL THEN
    RAISE EXCEPTION 'Table bl_dm.dim_stores does not exist';
  END IF;

  -------------------------------------------------------------------
  -- START LOG
  -------------------------------------------------------------------
  CALL bl_cl.pr_log_write(
    v_proc,'START',0,'Start load DIM_STORES.',
    NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id
  );

  -------------------------------------------------------------------
  -- NA validation (store_src_id)
  -- ignore default row (store_id = -1)
  -------------------------------------------------------------------
  CALL bl_cl.pr_validate_na_only(
    p_proc_name        => v_proc,
    p_run_id           => v_run_id,
    p_table            => 'bl_3nf.ce_stores'::regclass,
    p_column           => 'store_src_id',
    p_pk_column        => 'store_id',
    p_limit_examples   => 5,
    p_default_pk_value => '-1'
  );

  -------------------------------------------------------------------
  -- MAIN LOAD
  -------------------------------------------------------------------
  INSERT INTO bl_dm.dim_stores (
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
    s.store_src_id,
    sf.store_format_name,
    s.store_open_dt,
    s.store_open_time,
    s.store_close_time,
    c.city_name,
    r.region_name,
    co.country_name,

    -- lineage fields
    v_ctx.dm_layer::varchar(30)  AS source_system,
    v_ctx.dm_entity::varchar(60) AS source_entity,
    s.store_id::varchar(100)     AS source_id

  FROM bl_3nf.ce_stores s
  JOIN bl_3nf.ce_store_formats sf
    ON sf.store_format_id = s.store_format_id
  JOIN bl_3nf.ce_cities c
    ON c.city_id = s.city_id
  JOIN bl_3nf.ce_regions r
    ON r.region_id = c.region_id
  JOIN bl_3nf.ce_countries co
    ON co.country_id = r.country_id

  WHERE s.store_id <> -1
    AND lower(btrim(s.source_id::text)) <> 'n. a.'

  ON CONFLICT (source_system, source_entity, source_id)
  DO UPDATE SET
    store_src_id     = EXCLUDED.store_src_id,
    store_format     = EXCLUDED.store_format,
    store_open_dt    = EXCLUDED.store_open_dt,
    store_open_time  = EXCLUDED.store_open_time,
    store_close_time = EXCLUDED.store_close_time,
    city_name        = EXCLUDED.city_name,
    region_name      = EXCLUDED.region_name,
    country_name     = EXCLUDED.country_name
  WHERE
    bl_dm.dim_stores.store_src_id     IS DISTINCT FROM EXCLUDED.store_src_id OR
    bl_dm.dim_stores.store_format     IS DISTINCT FROM EXCLUDED.store_format OR
    bl_dm.dim_stores.store_open_dt    IS DISTINCT FROM EXCLUDED.store_open_dt OR
    bl_dm.dim_stores.store_open_time  IS DISTINCT FROM EXCLUDED.store_open_time OR
    bl_dm.dim_stores.store_close_time IS DISTINCT FROM EXCLUDED.store_close_time OR
    bl_dm.dim_stores.city_name        IS DISTINCT FROM EXCLUDED.city_name OR
    bl_dm.dim_stores.region_name      IS DISTINCT FROM EXCLUDED.region_name OR
    bl_dm.dim_stores.country_name     IS DISTINCT FROM EXCLUDED.country_name;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  -------------------------------------------------------------------
  -- SUCCESS LOG
  -------------------------------------------------------------------
  CALL bl_cl.pr_log_write(
    v_proc,'SUCCESS',v_rows,'DIM_STORES loaded successfully.',
    NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id
  );

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(
    v_proc,'ERROR',0,SQLERRM,SQLSTATE,
    v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id
  );
  RAISE;
END;
$$;

 -------------------------------------------------------------------
  -- CHECK
  -------------------------------------------------------------------

CALL bl_cl.pr_load_dim_stores_dm_simple();


SELECT log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_stores_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_stores;


-------------------------------------------------------------------
  -- DIM_TERMINALS (3NF: ce_terminals + ce_terminal_types)
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_dim_terminals_dm_simple()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_dim_terminals_dm_simple';
  v_run_id uuid := gen_random_uuid();
  v_rows   bigint := 0;
  v_ctx bl_cl.t_source_ctx;
BEGIN
  v_ctx := ROW('bl_3nf','ce_terminals','BL_3NF','CE_TERMINALS')::bl_cl.t_source_ctx;

  CALL bl_cl.pr_log_write(v_proc,'START',0,'Start load DIM_TERMINALS.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

  -- NA validation: terminal_src_id in 3NF (ignore terminal_id=-1)
  CALL bl_cl.pr_validate_na_only(
    p_proc_name        => v_proc,
    p_run_id           => v_run_id,
    p_table            => 'bl_3nf.ce_terminals'::regclass,
    p_column           => 'terminal_src_id',
    p_pk_column        => 'terminal_id',
    p_limit_examples   => 5,
    p_default_pk_value => '-1'
  );

  INSERT INTO bl_dm.dim_terminals (
    terminal_src_id,
    terminal_type_name,
    source_system,
    source_entity,
    source_id
  )
  SELECT
    t.terminal_src_id,
    tt.terminal_type_name,
    v_ctx.dm_layer::varchar(30)  AS source_system,
    v_ctx.dm_entity::varchar(60) AS source_entity,
    t.terminal_id::varchar(100)  AS source_id
  FROM bl_3nf.ce_terminals t
  JOIN bl_3nf.ce_terminal_types tt
    ON tt.terminal_type_id = t.terminal_type_id
  WHERE t.terminal_id <> -1

  ON CONFLICT (source_system, source_entity, source_id)
  DO UPDATE SET
    terminal_src_id      = EXCLUDED.terminal_src_id,
    terminal_type_name   = EXCLUDED.terminal_type_name
  WHERE
    bl_dm.dim_terminals.terminal_src_id    IS DISTINCT FROM EXCLUDED.terminal_src_id OR
    bl_dm.dim_terminals.terminal_type_name IS DISTINCT FROM EXCLUDED.terminal_type_name;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows,'DIM_TERMINALS loaded successfully.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);
  RAISE;
END;
$$;

-------------------------------------------------------------------
  -- CHECK
  -------------------------------------------------------------------

CALL bl_cl.pr_load_dim_terminals_dm_simple();


SELECT log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_terminals_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_terminals;


-------------------------------------------------------------------
  -- DIM_EMPLOYEES (3NF: ce_employees)
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_dim_employees_dm_simple()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_dim_employees_dm_simple';
  v_run_id uuid := gen_random_uuid();
  v_rows   bigint := 0;
  v_ctx bl_cl.t_source_ctx;
BEGIN
  v_ctx := ROW('bl_3nf','ce_employees','BL_3NF','CE_EMPLOYEES')::bl_cl.t_source_ctx;

  CALL bl_cl.pr_log_write(v_proc,'START',0,'Start load DIM_EMPLOYEES.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

  -- NA validation: first_name (ignore employee_id=-1)
  CALL bl_cl.pr_validate_na_only(
    p_proc_name        => v_proc,
    p_run_id           => v_run_id,
    p_table            => 'bl_3nf.ce_employees'::regclass,
    p_column           => 'first_name',
    p_pk_column        => 'employee_id',
    p_limit_examples   => 5,
    p_default_pk_value => '-1'
  );

  INSERT INTO bl_dm.dim_employees (
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
    e.employee_src_id,
    e.first_name,
    e.last_name,
    e.department,
    e.position,
    e.hire_dt,
    v_ctx.dm_layer::varchar(30),
    v_ctx.dm_entity::varchar(60),
    e.employee_id::varchar(100)
  FROM bl_3nf.ce_employees e
  WHERE e.employee_id <> -1

  ON CONFLICT (source_system, source_entity, source_id)
  DO UPDATE SET
    employee_src_id = EXCLUDED.employee_src_id,
    first_name      = EXCLUDED.first_name,
    last_name       = EXCLUDED.last_name,
    department      = EXCLUDED.department,
    position        = EXCLUDED.position,
    hire_dt         = EXCLUDED.hire_dt
  WHERE
    bl_dm.dim_employees.employee_src_id IS DISTINCT FROM EXCLUDED.employee_src_id OR
    bl_dm.dim_employees.first_name      IS DISTINCT FROM EXCLUDED.first_name OR
    bl_dm.dim_employees.last_name       IS DISTINCT FROM EXCLUDED.last_name OR
    bl_dm.dim_employees.department      IS DISTINCT FROM EXCLUDED.department OR
    bl_dm.dim_employees.position        IS DISTINCT FROM EXCLUDED.position OR
    bl_dm.dim_employees.hire_dt         IS DISTINCT FROM EXCLUDED.hire_dt;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows,'DIM_EMPLOYEES loaded successfully.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);
  RAISE;
END;
$$;


-------------------------------------------------------------------
  -- CHECK
  -------------------------------------------------------------------

CALL bl_cl.pr_load_dim_employees_dm_simple();


SELECT log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_employees_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_employees;

-------------------------------------------------------------------
  --DIM_PROMOTIONS (3NF: ce_promotions)
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_dim_promotions_dm_simple()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_dim_promotions_dm_simple';
  v_run_id uuid := gen_random_uuid();
  v_rows   bigint := 0;
  v_ctx bl_cl.t_source_ctx;
BEGIN
  v_ctx := ROW('bl_3nf','ce_promotions','BL_3NF','CE_PROMOTIONS')::bl_cl.t_source_ctx;

  CALL bl_cl.pr_log_write(v_proc,'START',0,'Start load DIM_PROMOTIONS.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

  -- NA validation: promo_code (ignore promotion_id=-1)
  CALL bl_cl.pr_validate_na_only(
    p_proc_name        => v_proc,
    p_run_id           => v_run_id,
    p_table            => 'bl_3nf.ce_promotions'::regclass,
    p_column           => 'promo_code',
    p_pk_column        => 'promotion_id',
    p_limit_examples   => 5,
    p_default_pk_value => '-1'
  );

  INSERT INTO bl_dm.dim_promotions (
    promo_code,
    discount_pct,
    source_system,
    source_entity,
    source_id
  )
  SELECT
    p.promo_code,
    p.discount_pct,
    v_ctx.dm_layer::varchar(30),
    v_ctx.dm_entity::varchar(60),
    p.promotion_id::varchar(100)
  FROM bl_3nf.ce_promotions p
  WHERE p.promotion_id <> -1

  ON CONFLICT (source_system, source_entity, source_id)
  DO UPDATE SET
    promo_code    = EXCLUDED.promo_code,
    discount_pct  = EXCLUDED.discount_pct
  WHERE
    bl_dm.dim_promotions.promo_code   IS DISTINCT FROM EXCLUDED.promo_code OR
    bl_dm.dim_promotions.discount_pct IS DISTINCT FROM EXCLUDED.discount_pct;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows,'DIM_PROMOTIONS loaded successfully.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);
  RAISE;
END;
$$;


-------------------------------------------------------------------
  -- CHECK
  -------------------------------------------------------------------

CALL bl_cl.pr_load_dim_promotions_dm_simple();


SELECT log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_promotions_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_promotions;

-------------------------------------------------------------------
  -- DIM_DELIVERY_PROVIDERS (3NF: ce_transactions + providers + types)
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_dim_delivery_providers_dm_simple()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_dim_delivery_providers_dm_simple';
  v_run_id uuid := gen_random_uuid();
  v_rows   bigint := 0;
  v_ctx bl_cl.t_source_ctx;
BEGIN
  v_ctx := ROW('bl_3nf','ce_transactions','BL_3NF','CE_TRANSACTIONS')::bl_cl.t_source_ctx;

  CALL bl_cl.pr_log_write(v_proc,'START',0,'Start load DIM_DELIVERY_PROVIDERS.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

  INSERT INTO bl_dm.dim_delivery_providers (
    carrier_name,
    delivery_type_name,
    source_system,
    source_entity,
    source_id
  )
  SELECT
    dp.carrier_name,
    dt.delivery_type_name,
    'BL_3NF'::varchar(30)        AS source_system,
    'CE_TRANSACTIONS'::varchar(60) AS source_entity,
    (t.delivery_provider_id::text || '|' || t.delivery_type_id::text)::varchar(100) AS source_id
  FROM (
    SELECT DISTINCT delivery_provider_id, delivery_type_id
    FROM bl_3nf.ce_transactions
    WHERE delivery_provider_id <> -1 AND delivery_type_id <> -1
  ) t
  JOIN bl_3nf.ce_delivery_providers dp ON dp.delivery_provider_id = t.delivery_provider_id
  JOIN bl_3nf.ce_delivery_types dt     ON dt.delivery_type_id     = t.delivery_type_id

  ON CONFLICT (source_system, source_entity, source_id)
  DO UPDATE SET
    carrier_name       = EXCLUDED.carrier_name,
    delivery_type_name = EXCLUDED.delivery_type_name
  WHERE
    bl_dm.dim_delivery_providers.carrier_name       IS DISTINCT FROM EXCLUDED.carrier_name OR
    bl_dm.dim_delivery_providers.delivery_type_name IS DISTINCT FROM EXCLUDED.delivery_type_name;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows,'DIM_DELIVERY_PROVIDERS loaded successfully.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);
  RAISE;
END;
$$;

-------------------------------------------------------------------
  -- CHECK
  -------------------------------------------------------------------

CALL bl_cl.pr_load_dim_delivery_providers_dm_simple();


SELECT log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_delivery_providers_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_delivery_providers;

-------------------------------------------------------------------
 -- DIM_JUNK_CONTEXT (3NF: ce_transactions + lookup tables)
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_dim_junk_context_dm_simple()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_dim_junk_context_dm_simple';
  v_run_id uuid := gen_random_uuid();
  v_rows   bigint := 0;
BEGIN
  CALL bl_cl.pr_log_write(v_proc,'START',0,'Start load DIM_JUNK_CONTEXT.',NULL,'bl_3nf','ce_transactions',v_run_id);

  INSERT INTO bl_dm.dim_junk_context (
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
    sc.sales_channel_name,
    pm.payment_method_name,
    ct.card_type_name,
    rt.receipt_type_name,
    pg.payment_gateway_name,
    os.order_status_name,
    sh.shift_src_id        AS shift_name,
    x.device_type_id,

    'BL_3NF'::varchar(30)          AS source_system,
    'MANUAL'::varchar(60) AS source_entity,

    concat_ws('|',
      x.sales_channel_id,
      x.payment_method_id,
      x.card_type_id,
      x.receipt_type_id,
      x.payment_gateway_id,
      x.order_status_id,
      x.shift_id,
      x.device_type_id
    )::varchar(100) AS source_id

  FROM (
    SELECT DISTINCT
      sales_channel_id,
      payment_method_id,
      card_type_id,
      receipt_type_id,
      payment_gateway_id,
      order_status_id,
      shift_id,
      device_type_id
    FROM bl_3nf.ce_transactions
    
  ) x
  LEFT JOIN bl_3nf.ce_sales_channels sc      ON sc.sales_channel_id     = x.sales_channel_id
  LEFT JOIN bl_3nf.ce_payment_methods pm    ON pm.payment_method_id    = x.payment_method_id
  LEFT JOIN bl_3nf.ce_card_types ct         ON ct.card_type_id         = x.card_type_id
  LEFT JOIN bl_3nf.ce_receipt_types rt      ON rt.receipt_type_id      = x.receipt_type_id
  LEFT JOIN bl_3nf.ce_payment_gateways pg   ON pg.payment_gateway_id   = x.payment_gateway_id
  LEFT JOIN bl_3nf.ce_order_statuses os     ON os.order_status_id      = x.order_status_id
  LEFT JOIN bl_3nf.ce_shifts sh             ON sh.shift_id             = x.shift_id

  ON CONFLICT (source_system, source_entity, source_id)
  DO UPDATE SET
    sales_channel   = EXCLUDED.sales_channel,
    payment_method  = EXCLUDED.payment_method,
    card_type       = EXCLUDED.card_type,
    receipt_type    = EXCLUDED.receipt_type,
    payment_gateway = EXCLUDED.payment_gateway,
    order_status    = EXCLUDED.order_status,
    shift_name      = EXCLUDED.shift_name,
    device_type_id  = EXCLUDED.device_type_id
  WHERE
    bl_dm.dim_junk_context.sales_channel   IS DISTINCT FROM EXCLUDED.sales_channel OR
    bl_dm.dim_junk_context.payment_method  IS DISTINCT FROM EXCLUDED.payment_method OR
    bl_dm.dim_junk_context.card_type       IS DISTINCT FROM EXCLUDED.card_type OR
    bl_dm.dim_junk_context.receipt_type    IS DISTINCT FROM EXCLUDED.receipt_type OR
    bl_dm.dim_junk_context.payment_gateway IS DISTINCT FROM EXCLUDED.payment_gateway OR
    bl_dm.dim_junk_context.order_status    IS DISTINCT FROM EXCLUDED.order_status OR
    bl_dm.dim_junk_context.shift_name      IS DISTINCT FROM EXCLUDED.shift_name OR
    bl_dm.dim_junk_context.device_type_id  IS DISTINCT FROM EXCLUDED.device_type_id;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows,'DIM_JUNK_CONTEXT loaded successfully.',NULL,'bl_3nf','ce_transactions',v_run_id);

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,'bl_3nf','ce_transactions',v_run_id);
  RAISE;
END;
$$;

-------------------------------------------------------------------
  -- CHECK
  -------------------------------------------------------------------

CALL bl_cl.pr_load_dim_junk_context_dm_simple();


SELECT log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_junk_context_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_junk_context;

-------------------------------------------------------------------
  -- DIM_CUSTOMERS_SCD (3NF: ce_customers_scd)
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_dim_customers_scd_dm_simple()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_dim_customers_scd_dm_simple';
  v_run_id uuid := gen_random_uuid();
  v_rows   bigint := 0;
  v_ctx bl_cl.t_source_ctx;
BEGIN
  v_ctx := ROW('bl_3nf','ce_customers_scd','BL_3NF','CE_CUSTOMERS_SCD')::bl_cl.t_source_ctx;

  CALL bl_cl.pr_log_write(v_proc,'START',0,'Start load DIM_CUSTOMERS_SCD.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

  -- NA validation: email (ignore customer_id=-1)
  CALL bl_cl.pr_validate_na_only(
    p_proc_name        => v_proc,
    p_run_id           => v_run_id,
    p_table            => 'bl_3nf.ce_customers_scd'::regclass,
    p_column           => 'email',
    p_pk_column        => 'customer_id',
    p_limit_examples   => 5,
    p_default_pk_value => '-1'
  );

  INSERT INTO bl_dm.dim_customers_scd (
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
    c.customer_src_id,
    c.age_grp           AS age_group,
    c.email,
    c.phone,
    c.customer_segment,
    c.gender,
    c.start_dt,
    c.end_dt,
    c.is_active,
    v_ctx.dm_layer::varchar(30),
    v_ctx.dm_entity::varchar(60),
    c.customer_id::varchar(100)
  FROM bl_3nf.ce_customers_scd c
  WHERE c.customer_id <> -1

  ON CONFLICT (source_system, source_entity, source_id)
  DO UPDATE SET
    customer_src_id    = EXCLUDED.customer_src_id,
    age_group          = EXCLUDED.age_group,
    email              = EXCLUDED.email,
    phone              = EXCLUDED.phone,
    customer_segment   = EXCLUDED.customer_segment,
    gender             = EXCLUDED.gender,
    start_dt           = EXCLUDED.start_dt,
    end_dt             = EXCLUDED.end_dt,
    is_active          = EXCLUDED.is_active
  WHERE
    bl_dm.dim_customers_scd.customer_src_id  IS DISTINCT FROM EXCLUDED.customer_src_id OR
    bl_dm.dim_customers_scd.age_group        IS DISTINCT FROM EXCLUDED.age_group OR
    bl_dm.dim_customers_scd.email            IS DISTINCT FROM EXCLUDED.email OR
    bl_dm.dim_customers_scd.phone            IS DISTINCT FROM EXCLUDED.phone OR
    bl_dm.dim_customers_scd.customer_segment IS DISTINCT FROM EXCLUDED.customer_segment OR
    bl_dm.dim_customers_scd.gender           IS DISTINCT FROM EXCLUDED.gender OR
    bl_dm.dim_customers_scd.start_dt         IS DISTINCT FROM EXCLUDED.start_dt OR
    bl_dm.dim_customers_scd.end_dt           IS DISTINCT FROM EXCLUDED.end_dt OR
    bl_dm.dim_customers_scd.is_active        IS DISTINCT FROM EXCLUDED.is_active;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows,'DIM_CUSTOMERS_SCD loaded successfully.',NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id);
  RAISE;
END;
$$;

-------------------------------------------------------------------
  -- CHECK
  -------------------------------------------------------------------

CALL bl_cl.pr_load_dim_customers_scd_dm_simple();


SELECT log_dts,
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

-------------------------------------------------------------------
  --DIM_DATES_DAY
  -------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_dim_dates_day_dm_simple()
LANGUAGE plpgsql
AS $$
DECLARE
  v_proc   text := 'bl_cl.pr_load_dim_dates_day_dm_simple';
  v_run_id uuid := gen_random_uuid();
  v_rows   bigint := 0;

  v_min_date date;
  v_max_date date;

  -- logging context 
  v_ctx bl_cl.t_source_ctx;
BEGIN
  v_ctx := ROW('bl_3nf','ce_transactions','BL_3NF','CE_TRANSACTIONS')::bl_cl.t_source_ctx;

  IF to_regclass('bl_dm.dim_dates_day') IS NULL THEN
    RAISE EXCEPTION 'Table bl_dm.dim_dates_day does not exist';
  END IF;

  CALL bl_cl.pr_log_write(
    v_proc,'START',0,'Start load DIM_DATES_DAY (calendar generated from 3NF dates).',
    NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id
  );

	SELECT MIN(txn_ts::date), MAX(txn_ts::date)
	INTO v_min_date, v_max_date
	FROM bl_3nf.ce_transactions
	WHERE txn_ts IS NOT NULL;

  -- if there is no transactions in the 3nf
 IF v_min_date IS NULL OR v_max_date IS NULL THEN
    CALL bl_cl.pr_log_write(
      v_proc,'SUCCESS',0,'DIM_DATES_DAY skipped: no source dates found in bl_3nf.ce_transactions.',
      NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id
    );
    RETURN;
  END IF;

  WITH calendar AS (
    SELECT gs::date AS d
    FROM generate_series(v_min_date, v_max_date, interval '1 day') gs
  ),
  src AS (
    SELECT
      to_char(d,'YYYYMMDD')::int                       AS date_id,
      EXTRACT(day FROM d)::smallint                    AS day,
      EXTRACT(isodow FROM d)::smallint                 AS day_of_week,
      to_char(d,'FMDay')                               AS day_name,
      ( (EXTRACT(day FROM d)::int - 1) / 7 + 1 )::smallint AS week,              
      EXTRACT(week FROM d)::smallint                   AS week_of_year,
      EXTRACT(month FROM d)::smallint                  AS month,
      to_char(d,'FMMonth')                             AS month_name,
      EXTRACT(quarter FROM d)::smallint                AS quarter,
      EXTRACT(year FROM d)::smallint                   AS year,
      (EXTRACT(isodow FROM d) IN (6,7))                AS is_weekend
    FROM calendar
  )
  INSERT INTO bl_dm.dim_dates_day (
    date_id, day, day_of_week, day_name,
    week, week_of_year,
    month, month_name, quarter, year,
    is_weekend
  )
  SELECT
    date_id, day, day_of_week, day_name,
    week, week_of_year,
    month, month_name, quarter, year,
    is_weekend
  FROM src
  ON CONFLICT (date_id)
  DO UPDATE SET
    day          = EXCLUDED.day,
    day_of_week  = EXCLUDED.day_of_week,
    day_name     = EXCLUDED.day_name,
    week         = EXCLUDED.week,
    week_of_year = EXCLUDED.week_of_year,
    month        = EXCLUDED.month,
    month_name   = EXCLUDED.month_name,
    quarter      = EXCLUDED.quarter,
    year         = EXCLUDED.year,
    is_weekend   = EXCLUDED.is_weekend
  WHERE
    bl_dm.dim_dates_day.day          IS DISTINCT FROM EXCLUDED.day OR
    bl_dm.dim_dates_day.day_of_week  IS DISTINCT FROM EXCLUDED.day_of_week OR
    bl_dm.dim_dates_day.day_name     IS DISTINCT FROM EXCLUDED.day_name OR
    bl_dm.dim_dates_day.week         IS DISTINCT FROM EXCLUDED.week OR
    bl_dm.dim_dates_day.week_of_year IS DISTINCT FROM EXCLUDED.week_of_year OR
    bl_dm.dim_dates_day.month        IS DISTINCT FROM EXCLUDED.month OR
    bl_dm.dim_dates_day.month_name   IS DISTINCT FROM EXCLUDED.month_name OR
    bl_dm.dim_dates_day.quarter      IS DISTINCT FROM EXCLUDED.quarter OR
    bl_dm.dim_dates_day.year         IS DISTINCT FROM EXCLUDED.year OR
    bl_dm.dim_dates_day.is_weekend   IS DISTINCT FROM EXCLUDED.is_weekend;

  GET DIAGNOSTICS v_rows = ROW_COUNT;

  CALL bl_cl.pr_log_write(
    v_proc,'SUCCESS',v_rows,
    format('DIM_DATES_DAY loaded for range %s..%s.', v_min_date, v_max_date),
    NULL, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id
  );

EXCEPTION WHEN OTHERS THEN
  CALL bl_cl.pr_log_write(
    v_proc,'ERROR',0,SQLERRM,SQLSTATE, v_ctx.src_layer_3nf, v_ctx.src_entity_3nf, v_run_id
  );
  RAISE;
END;
$$;


-------------------------------------------------------------------
  -- CHECK
  -------------------------------------------------------------------

CALL bl_cl.pr_load_dim_dates_day_dm_simple();


SELECT log_dts,
       procedure_name,
       status,
       rows_affected,
       message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'bl_cl.pr_load_dim_dates_day_dm_simple'
ORDER BY log_dts DESC; 

SELECT* FROM bl_dm.dim_dates_day;

