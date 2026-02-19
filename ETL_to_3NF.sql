-- =========================================================
-- PREREQ (optional): for gen_random_uuid()
-- =========================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================================================
-- 1) METADATA: centralized ETL log + load control + log proc
-- =========================================================
CREATE TABLE IF NOT EXISTS bl_cl.mta_etl_log (
    log_id          bigserial PRIMARY KEY, 					-- unique identifier of each log record (auto-increment)
    log_dts         timestamptz NOT NULL DEFAULT now(),  	-- timestamp when the log record was created
    procedure_name  text        NOT NULL,  					-- name of ETL procedure (example: pr_load_ce_sales)
    status          text        NOT NULL,   				-- execution status: START, SUCCESS, ERROR
    rows_affected   bigint      NOT NULL DEFAULT 0, 		-- number of rows inserted/updated by the procedure
    message         text,  									-- informational or error message
    sqlstate        text,									-- PostgreSQL error code (useful for debugging)
    source_system   text,									-- source system name (example: sales_online, sales_pos)
    source_entity   text,									-- source table or entity name
    run_id          uuid        NOT NULL DEFAULT gen_random_uuid()	-- unique identifier of a single ETL run (groups related log records)
);

---- index to quickly retrieve latest logs for a procedure
CREATE INDEX IF NOT EXISTS ix_mta_etl_log_proc_dts
ON bl_cl.mta_etl_log (procedure_name, log_dts DESC);
-- procedure_name → filter by procedure
-- log_dts DESC → quickly get latest executions

-- =========================================================
-- 2) Metadata table: load control (watermark for batch incremental)
-- One row per procedure + source triplet
-- =========================================================
CREATE TABLE IF NOT EXISTS bl_cl.mta_load_control (
    procedure_name          text        NOT NULL,										-- ETL procedure name
    source_system           text        NOT NULL,										-- source system identifier    
    source_entity           text        NOT NULL,										-- source table/entity identifier
    last_success_load_dts   timestamptz NOT NULL DEFAULT '1900-01-01'::timestamptz,		-- watermark timestamp of last successful load --used to extract only new/changed data
    ta_insert_dt            timestamptz NOT NULL DEFAULT now(),							-- record creation timestamp (technical column)
    ta_update_dt            timestamptz NOT NULL DEFAULT now(),							-- record last update timestamp (technical column)
    CONSTRAINT pk_mta_load_control PRIMARY KEY (procedure_name, source_system, source_entity)	-- ensures only one watermark per procedure + source
);


-- =========================================================
-- 3) Procedure: centralized logging
-- =========================================================
CREATE OR REPLACE PROCEDURE bl_cl.pr_log_write(
    p_procedure_name text,
    p_status         text,
    p_rows_affected  bigint DEFAULT 0,
    p_message        text DEFAULT NULL,
    p_sqlstate       text DEFAULT NULL,
    p_source_system  text DEFAULT NULL,
    p_source_entity  text DEFAULT NULL,
    p_run_id         uuid DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO bl_cl.mta_etl_log (
        procedure_name, status, rows_affected, message, sqlstate,
        source_system, source_entity, run_id
    )
    VALUES (
        p_procedure_name, p_status, COALESCE(p_rows_affected,0), p_message, p_sqlstate,
        p_source_system, p_source_entity, COALESCE(p_run_id, gen_random_uuid())
    );
END;
$$;

-- =========================================================
-- 4) PREPARE DATASET: returns table (from SA), incremental by load_dts
-- =========================================================
CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_brands(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    brand_name     varchar(60),
    source_system  varchar(30),
    source_entity  varchar(60),
    source_id      varchar(100),
    max_load_dts   timestamptz
)
LANGUAGE sql
AS $$
    WITH src AS (
        SELECT
            trim(s.brand)::varchar(60)        AS brand_name,
            'sa_sales_online'::varchar(30)    AS source_system,
            'src_sales_online'::varchar(60)   AS source_entity,
            trim(s.brand)::varchar(100)       AS source_id,
            s.load_dts                        AS load_dts
        FROM sa_sales_online.src_sales_online s
        WHERE s.brand IS NOT NULL
          AND s.load_dts > p_last_dts_online

        UNION ALL

        SELECT
            trim(p.brand)::varchar(60)        AS brand_name,
            'sa_sales_pos'::varchar(30)       AS source_system,
            'src_sales_pos'::varchar(60)      AS source_entity,
            trim(p.brand)::varchar(100)       AS source_id,
            p.load_dts                        AS load_dts
        FROM sa_sales_pos.src_sales_pos p
        WHERE p.brand IS NOT NULL
          AND p.load_dts > p_last_dts_pos
    )
    SELECT
        COALESCE(NULLIF(brand_name, ''), 'n. a.')::varchar(60) AS brand_name,
        source_system,
        source_entity,
        COALESCE(NULLIF(source_id, ''), 'n. a.')::varchar(100) AS source_id,
        max(load_dts) AS max_load_dts
    FROM src
    GROUP BY
        COALESCE(NULLIF(brand_name, ''), 'n. a.'),
        source_system,
        source_entity,
        COALESCE(NULLIF(source_id, ''), 'n. a.');
$$;

-- =========================================================
-- 5) BRAND NAME VALIDATION: "not spam-like"
--    Note: allows letters/digits/spaces/dot/hyphen/apostrophes/&
--          (so "H&M" is valid)
-- =========================================================
CREATE OR REPLACE FUNCTION bl_cl.fn_is_brand_name_valid(p_name text)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    v text;
    v_len int;
    v_letters int;
    v_alnum int;
BEGIN
    IF p_name IS NULL THEN
        RETURN false;
    END IF;

    v := btrim(p_name);
    v_len := char_length(v);

    -- length check
    IF v_len < 2 OR v_len > 60 THEN
        RETURN false;
    END IF;

    -- allow only: letters/digits/space/.-'’&
    IF v !~ '^[[:alpha:][:digit:][:space:]\.\-''’&]+$' THEN
        RETURN false;
    END IF;

    -- count letters and alnum
    v_letters := char_length(regexp_replace(v, '[^[:alpha:]]', '', 'g'));
    v_alnum   := char_length(regexp_replace(v, '[^[:alpha:][:digit:]]', '', 'g'));

    -- must contain at least 2 letters
    IF v_letters < 2 THEN
        RETURN false;
    END IF;

    -- too much "noise": if non-alnum > 40%
    IF (v_len - v_alnum) > (v_len * 0.40) THEN
        RETURN false;
    END IF;

    RETURN true;
END;
$$;

-- =========================================================
-- 4) VALIDATED PREPARE: FOR LOOP over query result + returns table
-- =========================================================
CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_brands_validated(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    brand_name     varchar(60),
    source_system  varchar(30),
    source_entity  varchar(60),
    source_id      varchar(100),
    max_load_dts   timestamptz
)
LANGUAGE plpgsql
AS $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT *
        FROM bl_cl.fn_prepare_ce_brands(p_last_dts_online, p_last_dts_pos)
    LOOP
        IF bl_cl.fn_is_brand_name_valid(r.brand_name) THEN
            brand_name    := r.brand_name;
            source_system := r.source_system;
            source_entity := r.source_entity;
            source_id     := r.source_id;
            max_load_dts  := r.max_load_dts;
            RETURN NEXT;
        END IF;
    END LOOP;

    RETURN;
END;
$$;

-- =========================================================
-- 5) LOAD PROCEDURE: incremental by load_dts + logging + exception
-- =========================================================
CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_brands(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc           text := 'pr_load_ce_brands';
    v_run_id         uuid := gen_random_uuid();

    v_last_online    timestamptz;
    v_last_pos       timestamptz;

    v_new_online     timestamptz;
    v_new_pos        timestamptz;

    v_rows_default   bigint := 0;
    v_rows_main      bigint := 0;
    v_rows_total     bigint := 0;
BEGIN
    -- Ensure control rows exist (one per source triplet)
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    -- Read watermarks
    SELECT last_success_load_dts
      INTO v_last_online
      FROM bl_cl.mta_load_control
     WHERE procedure_name = v_proc
       AND source_system = 'sa_sales_online'
       AND source_entity = 'src_sales_online';

    SELECT last_success_load_dts
      INTO v_last_pos
      FROM bl_cl.mta_load_control
     WHERE procedure_name = v_proc
       AND source_system = 'sa_sales_pos'
       AND source_entity = 'src_sales_pos';

    -- NEW: explicit full reload mode
    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
        v_last_pos    := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(
        p_procedure_name => v_proc,
        p_status         => 'START',
        p_rows_affected  => 0,
        p_message        => CASE WHEN p_full_reload
                                 THEN 'Start loading CE_BRANDS (FULL reload mode).'
                                 ELSE 'Start loading CE_BRANDS (incremental by load_dts).'
                            END,
        p_run_id         => v_run_id
    );

    -- Default row (-1)
    INSERT INTO bl_3nf.ce_brands (
        brand_id, brand_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', 'MANUAL', 'MANUAL', 'n. a.',
        timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_brands WHERE brand_id = -1
    );

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    -- Main upsert (validated dataset)
    INSERT INTO bl_3nf.ce_brands (
        brand_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.brand_name,
        p.source_system,
        p.source_entity,
        p.source_id,
        now(),
        now()
    FROM bl_cl.fn_prepare_ce_brands_validated(v_last_online, v_last_pos) p
    ON CONFLICT (brand_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_brands.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    -- New watermarks from processed batch
    SELECT max(max_load_dts)
      INTO v_new_online
      FROM bl_cl.fn_prepare_ce_brands_validated(v_last_online, v_last_pos)
     WHERE source_system = 'sa_sales_online'
       AND source_entity = 'src_sales_online';

    SELECT max(max_load_dts)
      INTO v_new_pos
      FROM bl_cl.fn_prepare_ce_brands_validated(v_last_online, v_last_pos)
     WHERE source_system = 'sa_sales_pos'
       AND source_entity = 'src_sales_pos';

    IF v_new_online IS NOT NULL THEN
        UPDATE bl_cl.mta_load_control
           SET last_success_load_dts = v_new_online,
               ta_update_dt = now()
         WHERE procedure_name = v_proc
           AND source_system = 'sa_sales_online'
           AND source_entity = 'src_sales_online';
    END IF;

    IF v_new_pos IS NOT NULL THEN
        UPDATE bl_cl.mta_load_control
           SET last_success_load_dts = v_new_pos,
               ta_update_dt = now()
         WHERE procedure_name = v_proc
           AND source_system = 'sa_sales_pos'
           AND source_entity = 'src_sales_pos';
    END IF;

    CALL bl_cl.pr_log_write(
        p_procedure_name => v_proc,
        p_status         => 'SUCCESS',
        p_rows_affected  => v_rows_total,
        p_message        => 'Loaded CE_BRANDS successfully.',
        p_run_id         => v_run_id
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.pr_log_write(
            p_procedure_name => v_proc,
            p_status         => 'ERROR',
            p_rows_affected  => 0,
            p_message        => SQLERRM,
            p_sqlstate       => SQLSTATE,
            p_run_id         => v_run_id
        );
        RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_brands(true);
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_brands'
ORDER BY log_dts DESC;

TRUNCATE TABLE bl_3nf.ce_brands cascade

Select * FROM bl_cl.mta_load_control;

SELECT* FROM bl_3nf.ce_brands;

-- =========================================================
-- Prepare function (RETURNS TABLE) for CE_UNIT_OF_MEASURES
--    incremental by load_dts from SA sources
-- =========================================================
CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_unit_of_measures(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    uom_name      varchar(20),
    source_system varchar(30),
    source_entity varchar(60),
    source_id     varchar(100),
    max_load_dts  timestamptz
)
LANGUAGE sql
AS $$
    WITH src AS (
        /* ONLINE */
        SELECT
            COALESCE(NULLIF(trim(s.unit_of_measure), ''), 'n. a.')::varchar(20) AS uom_name,
            'sa_sales_online'::varchar(30)                                     AS source_system,
            'src_sales_online'::varchar(60)                                    AS source_entity,
            COALESCE(NULLIF(trim(s.unit_of_measure), ''), 'n. a.')::varchar(100) AS source_id,
            s.load_dts                                                         AS load_dts
        FROM sa_sales_online.src_sales_online s
        WHERE s.unit_of_measure IS NOT NULL
          AND s.load_dts > p_last_dts_online

        UNION ALL

        /* POS */
        SELECT
            COALESCE(NULLIF(trim(p.unit_of_measure), ''), 'n. a.')::varchar(20) AS uom_name,
            'sa_sales_pos'::varchar(30)                                        AS source_system,
            'src_sales_pos'::varchar(60)                                       AS source_entity,
            COALESCE(NULLIF(trim(p.unit_of_measure), ''), 'n. a.')::varchar(100) AS source_id,
            p.load_dts                                                         AS load_dts
        FROM sa_sales_pos.src_sales_pos p
        WHERE p.unit_of_measure IS NOT NULL
          AND p.load_dts > p_last_dts_pos
    )
    SELECT
        uom_name,
        source_system,
        source_entity,
        source_id,
        max(load_dts) AS max_load_dts
    FROM src
    GROUP BY uom_name, source_system, source_entity, source_id;
$$;

-- =========================================================
--  Load procedure: CE_UNIT_OF_MEASURES (SCD0)
--    repeatable + incremental + logging + exception
-- =========================================================
CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_unit_of_measures(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        text := 'pr_load_ce_unit_of_measures';
    v_run_id      uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_last_pos    timestamptz;

    v_new_online  timestamptz;
    v_new_pos     timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_online
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    -- NEW: explicit full reload mode
    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
        v_last_pos    := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(
        v_proc, 'START', 0,
        CASE WHEN p_full_reload
             THEN 'Start loading CE_UNIT_OF_MEASURES (FULL reload mode).'
             ELSE 'Start loading CE_UNIT_OF_MEASURES (incremental by load_dts).'
        END,
        NULL, NULL, NULL, v_run_id
    );

    INSERT INTO bl_3nf.ce_unit_of_measures (
        uom_id, uom_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', 'MANUAL', 'MANUAL', 'n. a.',
        timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_unit_of_measures WHERE uom_id = -1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_unit_of_measures (
        uom_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.uom_name,
        p.source_system,
        p.source_entity,
        p.source_id,
        now(),
        now()
    FROM bl_cl.fn_prepare_ce_unit_of_measures(v_last_online, v_last_pos) p
    ON CONFLICT (uom_name, source_system, source_entity)
    DO NOTHING;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_unit_of_measures(v_last_online, v_last_pos)
    WHERE source_system='sa_sales_online' AND source_entity='src_sales_online';

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_unit_of_measures(v_last_online, v_last_pos)
    WHERE source_system='sa_sales_pos' AND source_entity='src_sales_pos';

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

    CALL bl_cl.pr_log_write(v_proc, 'SUCCESS', v_rows_total, 'Loaded CE_UNIT_OF_MEASURES successfully.', NULL, NULL, NULL, v_run_id);

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.pr_log_write(v_proc, 'ERROR', 0, SQLERRM, SQLSTATE, NULL, NULL, v_run_id);
        RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_unit_of_measures();

SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_unit_of_measures'
ORDER BY log_dts DESC;

SELECT* FROM bl_3nf.ce_unit_of_measures;
Select * FROM bl_cl.mta_load_control;

-- =========================================================
--Prepare function (RETURNS TABLE) for CE_SUPPLIERS
--    incremental by load_dts from SA sources
-- =========================================================
CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_suppliers(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    supplier_src_id varchar(100),
    source_system   varchar(30),
    source_entity   varchar(60),
    source_id       varchar(100),
    max_load_dts    timestamptz
)
LANGUAGE sql
AS $$
    WITH src AS (
        /* ONLINE */
        SELECT
            COALESCE(NULLIF(trim(s.supplier_id), ''), 'n. a.')::varchar(100) AS supplier_src_id,
            'sa_sales_online'::varchar(30)                                   AS source_system,
            'src_sales_online'::varchar(60)                                  AS source_entity,
            COALESCE(NULLIF(trim(s.supplier_id), ''), 'n. a.')::varchar(100) AS source_id,
            s.load_dts                                                       AS load_dts
        FROM sa_sales_online.src_sales_online s
        WHERE s.supplier_id IS NOT NULL
          AND s.load_dts > p_last_dts_online

        UNION ALL

        /* POS */
        SELECT
            COALESCE(NULLIF(trim(p.supplier_id), ''), 'n. a.')::varchar(100) AS supplier_src_id,
            'sa_sales_pos'::varchar(30)                                      AS source_system,
            'src_sales_pos'::varchar(60)                                     AS source_entity,
            COALESCE(NULLIF(trim(p.supplier_id), ''), 'n. a.')::varchar(100) AS source_id,
            p.load_dts                                                       AS load_dts
        FROM sa_sales_pos.src_sales_pos p
        WHERE p.supplier_id IS NOT NULL
          AND p.load_dts > p_last_dts_pos
    )
    SELECT
        supplier_src_id,
        source_system,
        source_entity,
        source_id,
        max(load_dts) AS max_load_dts
    FROM src
    GROUP BY supplier_src_id, source_system, source_entity, source_id;
$$;
-- =========================================================
--Procedure for CE_SUPPLIERS

-- =========================================================
CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_suppliers(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc         text := 'pr_load_ce_suppliers';
    v_run_id       uuid := gen_random_uuid();

    v_last_online  timestamptz;
    v_last_pos     timestamptz;
    v_new_online   timestamptz;
    v_new_pos      timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

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

    CALL bl_cl.pr_log_write(v_proc,'START',0,
        CASE WHEN p_full_reload THEN 'Start loading CE_SUPPLIERS (FULL).'
             ELSE 'Start loading CE_SUPPLIERS (INCREMENTAL).' END,
        NULL,NULL,NULL,v_run_id);

    -- default row
    INSERT INTO bl_3nf.ce_suppliers (
        supplier_id, supplier_src_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', 'MANUAL', 'MANUAL', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_suppliers WHERE supplier_id = -1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_suppliers (
        supplier_src_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.supplier_src_id,
        p.source_system,
        p.source_entity,
        p.source_id,
        now(), now()
    FROM bl_cl.fn_prepare_ce_suppliers(v_last_online, v_last_pos) p
    ON CONFLICT (supplier_src_id, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_suppliers.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
      FROM bl_cl.fn_prepare_ce_suppliers(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_online';

    SELECT max(max_load_dts) INTO v_new_pos
      FROM bl_cl.fn_prepare_ce_suppliers(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_pos';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_SUPPLIERS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_suppliers();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_suppliers'
ORDER BY log_dts DESC;


SELECT  * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_suppliers';

SELECT* FROM bl_3nf.ce_suppliers;

-- ========== PRODUCT_DEPARTMENTS ==========
CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_product_departments(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    product_department_name varchar(60),
    source_system           varchar(30),
    source_entity           varchar(60),
    source_id               varchar(100),
    max_load_dts            timestamptz
)
LANGUAGE sql
AS $$
    WITH src AS (
        SELECT
            COALESCE(NULLIF(trim(s.product_dept), ''), 'n. a.')::varchar(60) AS product_department_name,
            'sa_sales_online'::varchar(30) AS source_system,
            'src_sales_online'::varchar(60) AS source_entity,
            COALESCE(NULLIF(trim(s.product_dept), ''), 'n. a.')::varchar(100) AS source_id,
            s.load_dts AS load_dts
        FROM sa_sales_online.src_sales_online s
        WHERE s.product_dept IS NOT NULL
          AND s.load_dts > p_last_dts_online

        UNION ALL

        SELECT
            COALESCE(NULLIF(trim(p.product_dept), ''), 'n. a.')::varchar(60) AS product_department_name,
            'sa_sales_pos'::varchar(30) AS source_system,
            'src_sales_pos'::varchar(60) AS source_entity,
            COALESCE(NULLIF(trim(p.product_dept), ''), 'n. a.')::varchar(100) AS source_id,
            p.load_dts AS load_dts
        FROM sa_sales_pos.src_sales_pos p
        WHERE p.product_dept IS NOT NULL
          AND p.load_dts > p_last_dts_pos
    )
    SELECT product_department_name, source_system, source_entity, source_id, max(load_dts) AS max_load_dts
    FROM src
    GROUP BY product_department_name, source_system, source_entity, source_id;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_product_departments(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_product_departments';
    v_run_id uuid := gen_random_uuid();
    v_last_online timestamptz;
    v_last_pos timestamptz;
    v_new_online timestamptz;
    v_new_pos timestamptz;
    v_rows_default bigint := 0;
    v_rows_main bigint := 0;
    v_rows_total bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

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

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_PRODUCT_DEPARTMENTS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_product_departments (
        product_department_id, product_department_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'MANUAL', 'MANUAL', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_departments WHERE product_department_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_product_departments (
        product_department_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.product_department_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_product_departments(v_last_online, v_last_pos) p
    ON CONFLICT (product_department_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_product_departments.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
      FROM bl_cl.fn_prepare_ce_product_departments(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_online';

    SELECT max(max_load_dts) INTO v_new_pos
      FROM bl_cl.fn_prepare_ce_product_departments(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_pos';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_PRODUCT_DEPARTMENTS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_product_departments();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_product_departments'
ORDER BY log_dts DESC;


SELECT  * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_product_departments';

SELECT* FROM bl_3nf.ce_product_departments;


-- ========== PRODUCT_SUBCATEGORIES ==========
CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_product_subcategories(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    product_subcategory_name varchar(60),
    product_department_id    bigint,
    source_system            varchar(30),
    source_entity            varchar(60),
    source_id                varchar(100),
    max_load_dts             timestamptz
)
LANGUAGE sql
AS $$
    WITH base AS (
        SELECT
            COALESCE(NULLIF(trim(s.product_subcategory), ''), 'n. a.')::varchar(60) AS product_subcategory_name,
            COALESCE(NULLIF(trim(s.product_dept), ''), 'n. a.')::varchar(60)        AS dept_name,
            'sa_sales_online'::varchar(30) AS source_system,
            'src_sales_online'::varchar(60) AS source_entity,
            COALESCE(NULLIF(trim(s.product_subcategory), ''), 'n. a.')::varchar(100) AS source_id,
            s.load_dts AS load_dts
        FROM sa_sales_online.src_sales_online s
        WHERE s.product_subcategory IS NOT NULL
          AND s.load_dts > p_last_dts_online

        UNION ALL

        SELECT
            COALESCE(NULLIF(trim(p.product_subcategory), ''), 'n. a.')::varchar(60) AS product_subcategory_name,
            COALESCE(NULLIF(trim(p.product_dept), ''), 'n. a.')::varchar(60)        AS dept_name,
            'sa_sales_pos'::varchar(30) AS source_system,
            'src_sales_pos'::varchar(60) AS source_entity,
            COALESCE(NULLIF(trim(p.product_subcategory), ''), 'n. a.')::varchar(100) AS source_id,
            p.load_dts AS load_dts
        FROM sa_sales_pos.src_sales_pos p
        WHERE p.product_subcategory IS NOT NULL
          AND p.load_dts > p_last_dts_pos
    ),
    enriched AS (
        SELECT
            b.product_subcategory_name,
            COALESCE(d.product_department_id, -1) AS product_department_id,
            b.source_system,
            b.source_entity,
            b.source_id,
            b.load_dts
        FROM base b
        LEFT JOIN bl_3nf.ce_product_departments d
          ON d.product_department_name = b.dept_name
         AND d.source_system = b.source_system
         AND d.source_entity = b.source_entity
    )
    SELECT
        product_subcategory_name,
        product_department_id,
        source_system,
        source_entity,
        source_id,
        max(load_dts) AS max_load_dts
    FROM enriched
    GROUP BY product_subcategory_name, product_department_id, source_system, source_entity, source_id;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_product_subcategories(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_product_subcategories';
    v_run_id uuid := gen_random_uuid();
    v_last_online timestamptz;
    v_last_pos timestamptz;
    v_new_online timestamptz;
    v_new_pos timestamptz;
    v_rows_default bigint := 0;
    v_rows_main bigint := 0;
    v_rows_total bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

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

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_PRODUCT_SUBCATEGORIES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_product_subcategories (
        product_subcategory_id, product_subcategory_name, product_department_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', -1, 'MANUAL', 'MANUAL', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_product_subcategories WHERE product_subcategory_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_product_subcategories (
        product_subcategory_name, product_department_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.product_subcategory_name,
        p.product_department_id,
        p.source_system,
        p.source_entity,
        p.source_id,
        now(), now()
    FROM bl_cl.fn_prepare_ce_product_subcategories(v_last_online, v_last_pos) p
    ON CONFLICT (product_subcategory_name, source_system, source_entity)
    DO UPDATE
       SET product_department_id = EXCLUDED.product_department_id,
           source_id            = EXCLUDED.source_id,
           ta_update_dt         = now()
     WHERE bl_3nf.ce_product_subcategories.product_department_id IS DISTINCT FROM EXCLUDED.product_department_id
        OR bl_3nf.ce_product_subcategories.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
      FROM bl_cl.fn_prepare_ce_product_subcategories(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_online';

    SELECT max(max_load_dts) INTO v_new_pos
      FROM bl_cl.fn_prepare_ce_product_subcategories(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_pos';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_PRODUCT_SUBCATEGORIES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_product_subcategories();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_product_subcategories'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_product_departments';

SELECT COUNT(*) FROM bl_3nf.ce_product_subcategories;
SELECT * FROM bl_3nf.ce_product_subcategories;


-- ========== PRODUCTS ==========
CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_products(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    product_sku_src_id     varchar(100),
    product_name           varchar(150),
    product_subcategory_id bigint,
    brand_id               bigint,
    uom_id                 bigint,
    supplier_id            bigint,
    source_system          varchar(30),
    source_entity          varchar(60),
    source_id              varchar(100),
    max_load_dts           timestamptz
)
LANGUAGE sql
AS $$
    WITH base AS (
        SELECT
            COALESCE(NULLIF(trim(s.product_sku), ''), 'n. a.')::varchar(100) AS product_sku_src_id,
            COALESCE(NULLIF(trim(s.product_name), ''), 'n. a.')::varchar(150) AS product_name,
            COALESCE(NULLIF(trim(s.product_subcategory), ''), 'n. a.')::varchar(60) AS subcat_name,
            COALESCE(NULLIF(trim(s.brand), ''), 'n. a.')::varchar(150) AS brand_name,
            COALESCE(NULLIF(trim(s.unit_of_measure), ''), 'n. a.')::varchar(20) AS uom_name,
            COALESCE(NULLIF(trim(s.supplier_id), ''), 'n. a.')::varchar(100) AS supplier_src_id,
            'sa_sales_online'::varchar(30) AS source_system,
            'src_sales_online'::varchar(60) AS source_entity,
            COALESCE(NULLIF(trim(s.product_sku), ''), 'n. a.')::varchar(100) AS source_id,
            s.load_dts AS load_dts
        FROM sa_sales_online.src_sales_online s
        WHERE s.product_sku IS NOT NULL
          AND s.load_dts > p_last_dts_online

        UNION ALL

        SELECT
            COALESCE(NULLIF(trim(p.product_sku), ''), 'n. a.')::varchar(100) AS product_sku_src_id,
            COALESCE(NULLIF(trim(p.product_name), ''), 'n. a.')::varchar(150) AS product_name,
            COALESCE(NULLIF(trim(p.product_subcategory), ''), 'n. a.')::varchar(60) AS subcat_name,
            COALESCE(NULLIF(trim(p.brand), ''), 'n. a.')::varchar(150) AS brand_name,
            COALESCE(NULLIF(trim(p.unit_of_measure), ''), 'n. a.')::varchar(20) AS uom_name,
            COALESCE(NULLIF(trim(p.supplier_id), ''), 'n. a.')::varchar(100) AS supplier_src_id,
            'sa_sales_pos'::varchar(30) AS source_system,
            'src_sales_pos'::varchar(60) AS source_entity,
            COALESCE(NULLIF(trim(p.product_sku), ''), 'n. a.')::varchar(100) AS source_id,
            p.load_dts AS load_dts
        FROM sa_sales_pos.src_sales_pos p
        WHERE p.product_sku IS NOT NULL
          AND p.load_dts > p_last_dts_pos
    ),
    enriched AS (
        SELECT
            b.product_sku_src_id,
            b.product_name,
            COALESCE(psc.product_subcategory_id, -1) AS product_subcategory_id,
            COALESCE(br.brand_id, -1)                AS brand_id,
            COALESCE(u.uom_id, -1)                   AS uom_id,
            COALESCE(sup.supplier_id, -1)            AS supplier_id,
            b.source_system,
            b.source_entity,
            b.source_id,
            b.load_dts
        FROM base b
        LEFT JOIN bl_3nf.ce_product_subcategories psc
          ON psc.product_subcategory_name = b.subcat_name
         AND psc.source_system = b.source_system
         AND psc.source_entity = b.source_entity
        LEFT JOIN bl_3nf.ce_brands br
          ON br.brand_name = b.brand_name
         AND br.source_system = b.source_system
         AND br.source_entity = b.source_entity
        LEFT JOIN bl_3nf.ce_unit_of_measures u
          ON u.uom_name = b.uom_name
         AND u.source_system = b.source_system
         AND u.source_entity = b.source_entity
        LEFT JOIN bl_3nf.ce_suppliers sup
          ON sup.supplier_src_id = b.supplier_src_id
         AND sup.source_system = b.source_system
         AND sup.source_entity = b.source_entity
    )
    SELECT
        product_sku_src_id, product_name, product_subcategory_id, brand_id, uom_id, supplier_id,
        source_system, source_entity, source_id,
        max(load_dts) AS max_load_dts
    FROM enriched
    GROUP BY product_sku_src_id, product_name, product_subcategory_id, brand_id, uom_id, supplier_id,
             source_system, source_entity, source_id;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_products(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_products';
    v_run_id uuid := gen_random_uuid();
    v_last_online timestamptz;
    v_last_pos timestamptz;
    v_new_online timestamptz;
    v_new_pos timestamptz;
    v_rows_default bigint := 0;
    v_rows_main bigint := 0;
    v_rows_total bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

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

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_PRODUCTS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_products (
        product_id, product_sku_src_id, product_name, product_subcategory_id, brand_id, uom_id, supplier_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', 'n. a.', -1, -1, -1, -1,
        'MANUAL', 'MANUAL', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_products WHERE product_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_products (
        product_sku_src_id, product_name, product_subcategory_id, brand_id, uom_id, supplier_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.product_sku_src_id, p.product_name, p.product_subcategory_id, p.brand_id, p.uom_id, p.supplier_id,
        p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_products(v_last_online, v_last_pos) p
    ON CONFLICT (product_sku_src_id, source_system, source_entity)
    DO UPDATE
       SET product_name          = EXCLUDED.product_name,
           product_subcategory_id= EXCLUDED.product_subcategory_id,
           brand_id              = EXCLUDED.brand_id,
           uom_id                = EXCLUDED.uom_id,
           supplier_id           = EXCLUDED.supplier_id,
           source_id             = EXCLUDED.source_id,
           ta_update_dt          = now()
     WHERE bl_3nf.ce_products.product_name IS DISTINCT FROM EXCLUDED.product_name
        OR bl_3nf.ce_products.product_subcategory_id IS DISTINCT FROM EXCLUDED.product_subcategory_id
        OR bl_3nf.ce_products.brand_id IS DISTINCT FROM EXCLUDED.brand_id
        OR bl_3nf.ce_products.uom_id IS DISTINCT FROM EXCLUDED.uom_id
        OR bl_3nf.ce_products.supplier_id IS DISTINCT FROM EXCLUDED.supplier_id
        OR bl_3nf.ce_products.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
      FROM bl_cl.fn_prepare_ce_products(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_online';

    SELECT max(max_load_dts) INTO v_new_pos
      FROM bl_cl.fn_prepare_ce_products(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_pos';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_PRODUCTS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_products();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_products'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_products';

SELECT COUNT(*) FROM bl_3nf.ce_products;
SELECT * FROM bl_3nf.ce_products;

-- ========== CE_ PROMOTIONS ==========

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_promotions(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    promo_code    varchar(60),
    discount_pct  numeric(5,2),
    source_system varchar(30),
    source_entity varchar(60),
    source_id     varchar(100),
    max_load_dts  timestamptz
)
LANGUAGE sql
AS $$
    WITH src AS (
        SELECT
            COALESCE(NULLIF(trim(s.promo_code), ''), 'n. a.')::varchar(60)     AS promo_code,
            COALESCE(s.discount_pct, 0)::numeric(5,2)                          AS discount_pct,
            'sa_sales_online'::varchar(30) AS source_system,
            'src_sales_online'::varchar(60) AS source_entity,
            COALESCE(NULLIF(trim(s.promo_code), ''), 'n. a.')::varchar(100)    AS source_id,
            s.load_dts AS load_dts
        FROM sa_sales_online.src_sales_online s
        WHERE s.promo_code IS NOT NULL
          AND s.load_dts > p_last_dts_online

        UNION ALL

        SELECT
            COALESCE(NULLIF(trim(p.promo_code), ''), 'n. a.')::varchar(60)     AS promo_code,
            COALESCE(p.discount_pct, 0)::numeric(5,2)                          AS discount_pct,
            'sa_sales_pos'::varchar(30) AS source_system,
            'src_sales_pos'::varchar(60) AS source_entity,
            COALESCE(NULLIF(trim(p.promo_code), ''), 'n. a.')::varchar(100)    AS source_id,
            p.load_dts AS load_dts
        FROM sa_sales_pos.src_sales_pos p
        WHERE p.promo_code IS NOT NULL
          AND p.load_dts > p_last_dts_pos
    )
    SELECT promo_code, discount_pct, source_system, source_entity, source_id, max(load_dts) AS max_load_dts
    FROM src
    GROUP BY promo_code, discount_pct, source_system, source_entity, source_id;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_promotions(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_promotions';
    v_run_id uuid := gen_random_uuid();
    v_last_online timestamptz;
    v_last_pos timestamptz;
    v_new_online timestamptz;
    v_new_pos timestamptz;
    v_rows_default bigint := 0;
    v_rows_main bigint := 0;
    v_rows_total bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

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

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_PROMOTIONS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_promotions (
        promotion_id, promo_code, discount_pct, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 0, 'MANUAL', 'MANUAL', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_promotions WHERE promotion_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_promotions (
        promo_code, discount_pct, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.promo_code, p.discount_pct, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_promotions(v_last_online, v_last_pos) p
    ON CONFLICT (promo_code, source_system, source_entity, discount_pct)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_promotions.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
      FROM bl_cl.fn_prepare_ce_promotions(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_online';

    SELECT max(max_load_dts) INTO v_new_pos
      FROM bl_cl.fn_prepare_ce_promotions(v_last_online, v_last_pos)
     WHERE source_system='sa_sales_pos';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_PROMOTIONS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_promotions();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_promotions'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_promotions';

SELECT COUNT(*) FROM bl_3nf.ce_promotions;
SELECT * FROM bl_3nf.ce_promotions;

-- =========================================
-- CE_COUNTRIES (source: bl_cl.t_map_countries)
-- =========================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_countries_from_map(
    p_last_dts timestamptz
)
RETURNS TABLE (
    country_name  varchar(60),
    source_system varchar(30),
    source_entity varchar(60),
    source_id     varchar(100),
    max_load_dts  timestamptz
)
LANGUAGE sql
AS $$
    SELECT
        m.country_name::varchar(60)            AS country_name,
        'bl_cl'::varchar(30)                   AS source_system,
        't_map_countries'::varchar(60)         AS source_entity,
        m.country_src_name::varchar(100)       AS source_id,
        max(m.update_dts)::timestamptz         AS max_load_dts
    FROM bl_cl.t_map_countries m
    WHERE m.country_name IS NOT NULL
      AND m.update_dts > p_last_dts
    GROUP BY 1,2,3,4;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_countries_from_map(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_countries_from_map';
    v_run_id uuid := gen_random_uuid();

    v_last_map timestamptz;
    v_new_map  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'bl_cl', 't_map_countries', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_map
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='bl_cl' AND source_entity='t_map_countries';

    IF p_full_reload THEN
        v_last_map := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_COUNTRIES from t_map_countries.',NULL,NULL,NULL,v_run_id);

    -- default row
    INSERT INTO bl_3nf.ce_countries (
        country_id, country_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_countries WHERE country_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    -- SCD0: DO NOTHING on conflict (repeatable => 0 rows on rerun)
    INSERT INTO bl_3nf.ce_countries (
        country_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.country_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_countries_from_map(v_last_map) p
    ON CONFLICT (country_name, source_system, source_entity, source_id) DO NOTHING;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_map
    FROM bl_cl.fn_prepare_ce_countries_from_map(v_last_map);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_map, v_last_map),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='bl_cl' AND source_entity='t_map_countries';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_COUNTRIES loaded successfully (from map).',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_countries_from_map();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_countries_from_map'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_countries_from_map';

SELECT COUNT(*) FROM bl_3nf.ce_countries;
SELECT * FROM bl_3nf.ce_countries;

-- =========================================
-- CE_REGIONS (country_id resolved via map -> canonical country -> ce_countries)
-- =========================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_regions_via_map(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    region_name   varchar(60),
    country_id    bigint,
    source_system varchar(30),
    source_entity varchar(60),
    source_id     varchar(100),
    max_load_dts  timestamptz
)
LANGUAGE sql
AS $$
    WITH src AS (
        SELECT DISTINCT
            COALESCE(NULLIF(trim(son.region), ''), 'n. a.')   AS region_name,
            COALESCE(NULLIF(trim(son.country), ''), 'n. a.')  AS country_src_name,
            'sa_sales_online'::text                           AS source_system,
            'src_sales_online'::text                          AS source_entity,
            COALESCE(NULLIF(trim(son.region), ''), 'n. a.')    AS source_id,
            son.load_dts                                       AS load_dts
        FROM sa_sales_online.src_sales_online son
        WHERE son.region IS NOT NULL
          AND son.load_dts > p_last_dts_online

        UNION ALL

        SELECT DISTINCT
            COALESCE(NULLIF(trim(spo.region), ''), 'n. a.')   AS region_name,
            COALESCE(NULLIF(trim(spo.country), ''), 'n. a.')  AS country_src_name,
            'sa_sales_pos'::text                              AS source_system,
            'src_sales_pos'::text                             AS source_entity,
            COALESCE(NULLIF(trim(spo.region), ''), 'n. a.')    AS source_id,
            spo.load_dts                                       AS load_dts
        FROM sa_sales_pos.src_sales_pos spo
        WHERE spo.region IS NOT NULL
          AND spo.load_dts > p_last_dts_pos
    ),
    mapped AS (
        SELECT
            s.region_name,
            COALESCE(ctr.country_id, -1) AS country_id,
            s.source_system,
            s.source_entity,
            s.source_id,
            s.load_dts
        FROM src s
        LEFT JOIN bl_3nf.ce_countries ctr
          ON ctr.source_id = s.country_src_name   -- ТІЛЬКИ ПО SOURCE_ID (як у твоєму DML)
    )
    SELECT
        region_name::varchar(60),
        country_id,
        source_system::varchar(30),
        source_entity::varchar(60),
        source_id::varchar(100),
        max(load_dts)::timestamptz AS max_load_dts
    FROM mapped
    GROUP BY 1,2,3,4,5;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_regions_via_map(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_regions_via_map';
    v_run_id uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_last_pos    timestamptz;
    v_new_online  timestamptz;
    v_new_pos     timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

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

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_REGIONS via map.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_regions (
        region_id, region_name, country_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', -1, 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_regions WHERE region_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_regions (
        region_name, country_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.region_name, p.country_id, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_regions_via_map(v_last_online, v_last_pos) p
    ON CONFLICT (region_name, country_id, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_regions.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_regions_via_map(v_last_online, v_last_pos)
    WHERE source_system='sa_sales_online';

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_regions_via_map(v_last_online, v_last_pos)
    WHERE source_system='sa_sales_pos';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_REGIONS loaded successfully (via map).',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_regions_via_map();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_regions_via_map'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_regions_via_map';

SELECT COUNT(*) FROM bl_3nf.ce_regions;
SELECT * FROM bl_3nf.ce_regions;

-- =========================================
-- CE_CITIES (region_id resolved via ce_regions; country via map -> ce_countries)
-- =========================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_cities_via_map(
    p_last_dts_online timestamptz,
    p_last_dts_pos    timestamptz
)
RETURNS TABLE (
    city_name     varchar(60),
    region_id     bigint,
    source_system varchar(30),
    source_entity varchar(60),
    source_id     varchar(100),
    max_load_dts  timestamptz
)
LANGUAGE sql
AS $$
    WITH src AS (
        SELECT DISTINCT
            COALESCE(NULLIF(trim(son.city), ''), 'n. a.')      AS city_name,
            COALESCE(NULLIF(trim(son.region), ''), 'n. a.')    AS region_name,
            COALESCE(NULLIF(trim(son.country), ''), 'n. a.')   AS country_src_name,
            'sa_sales_online'::text                            AS source_system,
            'src_sales_online'::text                           AS source_entity,
            COALESCE(NULLIF(trim(son.city), ''), 'n. a.')      AS source_id,
            son.load_dts                                        AS load_dts
        FROM sa_sales_online.src_sales_online son
        WHERE son.city IS NOT NULL
          AND son.load_dts > p_last_dts_online

        UNION ALL

        SELECT DISTINCT
            COALESCE(NULLIF(trim(spo.city), ''), 'n. a.')      AS city_name,
            COALESCE(NULLIF(trim(spo.region), ''), 'n. a.')    AS region_name,
            COALESCE(NULLIF(trim(spo.country), ''), 'n. a.')   AS country_src_name,
            'sa_sales_pos'::text                               AS source_system,
            'src_sales_pos'::text                              AS source_entity,
            COALESCE(NULLIF(trim(spo.city), ''), 'n. a.')      AS source_id,
            spo.load_dts                                        AS load_dts
        FROM sa_sales_pos.src_sales_pos spo
        WHERE spo.city IS NOT NULL
          AND spo.load_dts > p_last_dts_pos
    ),
    country_resolved AS (
        SELECT
            s.*,
            COALESCE(ctr.country_id, -1) AS country_id
        FROM src s
        LEFT JOIN bl_3nf.ce_countries ctr
          ON ctr.source_id = s.country_src_name  -- ТІЛЬКИ ПО SOURCE_ID
    ),
    region_resolved AS (
        SELECT
            cr.city_name,
            COALESCE(r.region_id, -1) AS region_id,
            cr.source_system,
            cr.source_entity,
            cr.source_id,
            cr.load_dts
        FROM country_resolved cr
        LEFT JOIN bl_3nf.ce_regions r
          ON r.region_name   = cr.region_name
         AND r.country_id    = cr.country_id
         AND r.source_system = cr.source_system
         AND r.source_entity = cr.source_entity
    )
    SELECT
        city_name::varchar(60),
        region_id,
        source_system::varchar(30),
        source_entity::varchar(60),
        source_id::varchar(100),
        max(load_dts)::timestamptz AS max_load_dts
    FROM region_resolved
    GROUP BY 1,2,3,4,5;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_cities_via_map(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_cities_via_map';
    v_run_id uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_last_pos    timestamptz;
    v_new_online  timestamptz;
    v_new_pos     timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES
        (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz),
        (v_proc, 'sa_sales_pos',    'src_sales_pos',    '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

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

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_CITIES via map.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_cities (
        city_id, city_name, region_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', -1, 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_cities WHERE city_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_cities (
        city_name, region_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.city_name, p.region_id, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_cities_via_map(v_last_online, v_last_pos) p
    ON CONFLICT (city_name, region_id, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_cities.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_cities_via_map(v_last_online, v_last_pos)
    WHERE source_system='sa_sales_online';

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_cities_via_map(v_last_online, v_last_pos)
    WHERE source_system='sa_sales_pos';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_CITIES loaded successfully (via map).',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_cities_via_map();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_cities_via_map'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_cities_via_map';

SELECT COUNT(*) FROM bl_3nf.ce_cities;
SELECT * FROM bl_3nf.ce_cities;

-- =========================================================
--  CE_STORE_FORMATS
-- =========================================================
CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_store_formats(
    p_last_dts_pos timestamptz
)
RETURNS TABLE (
    store_format_name varchar(40),
    source_system     varchar(30),
    source_entity     varchar(60),
    source_id         varchar(100),
    max_load_dts      timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(spo.store_format), ''), 'n. a.') AS store_format_name,
        'sa_sales_pos'::text                                  AS source_system,
        'src_sales_pos'::text                                 AS source_entity,
        COALESCE(NULLIF(trim(spo.store_format), ''), 'n. a.') AS source_id,
        spo.load_dts                                           AS load_dts
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.store_format IS NOT NULL
      AND spo.load_dts > p_last_dts_pos
)
SELECT
    store_format_name::varchar(40),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_store_formats(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_store_formats';
    v_run_id uuid := gen_random_uuid();

    v_last_pos timestamptz;
    v_new_pos  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_pos', 'src_sales_pos', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_pos := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_STORE_FORMATS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_store_formats (
        store_format_id, store_format_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_store_formats WHERE store_format_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_store_formats (
        store_format_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.store_format_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_store_formats(v_last_pos) p
    ON CONFLICT (store_format_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_store_formats.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_store_formats(v_last_pos);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_STORE_FORMATS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_store_formats();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_store_formats'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_store_formats';

SELECT COUNT(*) FROM bl_3nf.ce_store_formats;
SELECT * FROM bl_3nf.ce_store_formats;

-- =========================================================
--  CE_STORES
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_stores(
    p_last_dts_pos timestamptz
)
RETURNS TABLE (
    store_src_id     varchar(100),
    store_format_id  bigint,
    store_open_dt    date,
    store_open_time  time,
    store_close_time time,
    city_id          bigint,
    source_system    varchar(30),
    source_entity    varchar(60),
    source_id        varchar(100),
    max_load_dts     timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(spo.store_id), ''), 'n. a.')        AS store_src_id,
        COALESCE(NULLIF(trim(spo.store_format), ''), 'n. a.')    AS store_format_name,
        COALESCE(spo.store_open_dt, DATE '1900-01-01')           AS store_open_dt,
        COALESCE(spo.store_open_time, TIME '00:00:00')           AS store_open_time,
        COALESCE(spo.store_close_time, TIME '00:00:00')          AS store_close_time,
        COALESCE(NULLIF(trim(spo.city), ''), 'n. a.')            AS city_name,
        COALESCE(NULLIF(trim(spo.region), ''), 'n. a.')          AS region_name,
        COALESCE(NULLIF(trim(spo.country), ''), 'n. a.')         AS country_name,
        'sa_sales_pos'::text                                      AS source_system,
        'src_sales_pos'::text                                     AS source_entity,
        spo.load_dts                                              AS load_dts
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.store_id IS NOT NULL
      AND spo.load_dts > p_last_dts_pos
),
map AS (
    SELECT
        s.store_src_id,
        COALESCE(sft.store_format_id, -1) AS store_format_id,
        s.store_open_dt,
        s.store_open_time,
        s.store_close_time,

        COALESCE(cty.city_id, -1) AS city_id,

        s.source_system,
        s.source_entity,
        s.store_src_id             AS source_id,
        s.load_dts
    FROM src s
    LEFT JOIN bl_3nf.ce_store_formats sft
      ON sft.store_format_name = s.store_format_name
     AND sft.source_system     = s.source_system
     AND sft.source_entity     = s.source_entity

    LEFT JOIN bl_3nf.ce_countries ctr
      ON ctr.source_id = s.country_name   -- ТІЛЬКИ ПО SOURCE_ID

    LEFT JOIN bl_3nf.ce_regions reg
      ON reg.region_name   = s.region_name
     AND reg.country_id    = ctr.country_id
     AND reg.source_system = s.source_system
     AND reg.source_entity = s.source_entity

    LEFT JOIN bl_3nf.ce_cities cty
      ON cty.city_name     = s.city_name
     AND cty.region_id     = reg.region_id
     AND cty.source_system = s.source_system
     AND cty.source_entity = s.source_entity
)
SELECT
    store_src_id::varchar(100),
    store_format_id,
    store_open_dt,
    store_open_time,
    store_close_time,
    city_id,
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM map
GROUP BY 1,2,3,4,5,6,7,8,9;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_stores(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_stores';
    v_run_id uuid := gen_random_uuid();

    v_last_pos timestamptz;
    v_new_pos  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_pos', 'src_sales_pos', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_pos := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_STORES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_stores (
        store_id, store_src_id, store_format_id, store_open_dt, store_open_time, store_close_time, city_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', -1, DATE '1900-01-01', TIME '00:00:00', TIME '00:00:00', -1,
        'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_stores WHERE store_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_stores (
        store_src_id, store_format_id, store_open_dt, store_open_time, store_close_time, city_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.store_src_id, p.store_format_id, p.store_open_dt, p.store_open_time, p.store_close_time, p.city_id,
        p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_stores(v_last_pos) p
    ON CONFLICT (store_src_id, source_system, source_entity)
    DO UPDATE
       SET store_format_id  = EXCLUDED.store_format_id,
           store_open_dt    = EXCLUDED.store_open_dt,
           store_open_time  = EXCLUDED.store_open_time,
           store_close_time = EXCLUDED.store_close_time,
           city_id          = EXCLUDED.city_id,
           source_id        = EXCLUDED.source_id,
           ta_update_dt     = now()
     WHERE (bl_3nf.ce_stores.store_format_id, bl_3nf.ce_stores.store_open_dt, bl_3nf.ce_stores.store_open_time,
            bl_3nf.ce_stores.store_close_time, bl_3nf.ce_stores.city_id, bl_3nf.ce_stores.source_id)
           IS DISTINCT FROM
           (EXCLUDED.store_format_id, EXCLUDED.store_open_dt, EXCLUDED.store_open_time,
            EXCLUDED.store_close_time, EXCLUDED.city_id, EXCLUDED.source_id);

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_stores(v_last_pos);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_STORES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_stores();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_stores'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_stores';

SELECT COUNT(*) FROM bl_3nf.ce_stores;
SELECT * FROM bl_3nf.ce_stores;

-- =========================================================
--  CE_DELIVERY_ADDRESSES
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_delivery_addresses(
    p_last_dts_online timestamptz
)
RETURNS TABLE (
    delivery_postal_code   varchar(20),
    delivery_address_line1 varchar(255),
    city_id                bigint,
    source_system          varchar(30),
    source_entity          varchar(60),
    source_id              varchar(100),
    max_load_dts           timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(son.delivery_postal_code), ''), 'n. a.')   AS delivery_postal_code,
        COALESCE(NULLIF(trim(son.delivery_address_line1), ''), 'n. a.') AS delivery_address_line1,
        COALESCE(NULLIF(trim(son.city), ''), 'n. a.')                  AS city_name,
        COALESCE(NULLIF(trim(son.region), ''), 'n. a.')                AS region_name,
        COALESCE(NULLIF(trim(son.country), ''), 'n. a.')               AS country_name,
        'sa_sales_online'::text                                         AS source_system,
        'src_sales_online'::text                                        AS source_entity,
        son.load_dts                                                    AS load_dts
    FROM sa_sales_online.src_sales_online son
    WHERE son.delivery_postal_code IS NOT NULL
      AND son.load_dts > p_last_dts_online
),
map AS (
    SELECT
        s.delivery_postal_code,
        s.delivery_address_line1,
        COALESCE(cty.city_id, -1) AS city_id,
        s.source_system,
        s.source_entity,
        s.delivery_postal_code     AS source_id,
        s.load_dts
    FROM src s
    LEFT JOIN bl_3nf.ce_countries ctr
      ON ctr.source_id = s.country_name   
    LEFT JOIN bl_3nf.ce_regions reg
      ON reg.region_name   = s.region_name
     AND reg.country_id    = ctr.country_id
     AND reg.source_system = s.source_system
     AND reg.source_entity = s.source_entity
    LEFT JOIN bl_3nf.ce_cities cty
      ON cty.city_name     = s.city_name
     AND cty.region_id     = reg.region_id
     AND cty.source_system = s.source_system
     AND cty.source_entity = s.source_entity
)
SELECT
    delivery_postal_code::varchar(20),
    delivery_address_line1::varchar(255),
    city_id,
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM map
GROUP BY 1,2,3,4,5,6;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_delivery_addresses(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_delivery_addresses';
    v_run_id uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_new_online  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_online
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_DELIVERY_ADDRESSES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_delivery_addresses (
        delivery_address_id, delivery_postal_code, delivery_address_line1, city_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', 'n. a.', -1,
        'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_delivery_addresses WHERE delivery_address_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_delivery_addresses (
        delivery_postal_code, delivery_address_line1, city_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.delivery_postal_code, p.delivery_address_line1, p.city_id,
        p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_delivery_addresses(v_last_online) p
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_delivery_addresses a
        WHERE a.delivery_postal_code   = p.delivery_postal_code
          AND a.delivery_address_line1 = p.delivery_address_line1
          AND a.city_id                = p.city_id
          AND a.source_system          = p.source_system
          AND a.source_entity          = p.source_entity
    );

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_delivery_addresses(v_last_online);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_DELIVERY_ADDRESSES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;
-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_delivery_addresses();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_delivery_addresses'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_delivery_addresses';

SELECT COUNT(*) FROM bl_3nf.ce_delivery_addresses;
SELECT * FROM bl_3nf.ce_delivery_addresses;

-- =========================================================
--  CE_FULFILLMENT_CENTERS
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_fulfillment_centers(
    p_last_dts_online timestamptz
)
RETURNS TABLE (
    fulfillment_center_src_id varchar(100),
    city_id                   bigint,
    source_system             varchar(30),
    source_entity             varchar(60),
    source_id                 varchar(100),
    max_load_dts              timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(son.fulfillment_center_id), ''), 'n. a.') AS fulfillment_center_src_id,
        COALESCE(NULLIF(trim(son.fulfillment_city), ''), 'n. a.')      AS city_name,
        'sa_sales_online'::text                                        AS source_system,
        'src_sales_online'::text                                       AS source_entity,
        son.load_dts                                                   AS load_dts
    FROM sa_sales_online.src_sales_online son
    WHERE son.fulfillment_center_id IS NOT NULL
      AND son.load_dts > p_last_dts_online
),
map AS (
    SELECT
        s.fulfillment_center_src_id,
        COALESCE(cty.city_id, -1) AS city_id,
        s.source_system,
        s.source_entity,
        s.fulfillment_center_src_id AS source_id,
        s.load_dts
    FROM src s
    LEFT JOIN bl_3nf.ce_cities cty
      ON cty.city_name     = s.city_name
     AND cty.source_system = s.source_system
     AND cty.source_entity = s.source_entity
)
SELECT
    fulfillment_center_src_id::varchar(100),
    city_id,
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM map
GROUP BY 1,2,3,4,5;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_fulfillment_centers(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_fulfillment_centers';
    v_run_id uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_new_online  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_online
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_FULFILLMENT_CENTERS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_fulfillment_centers (
        fulfillment_center_id, fulfillment_center_src_id, city_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', -1,
        'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_fulfillment_centers WHERE fulfillment_center_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_fulfillment_centers (
        fulfillment_center_src_id, city_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        p.fulfillment_center_src_id, p.city_id,
        p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_fulfillment_centers(v_last_online) p
    ON CONFLICT (fulfillment_center_src_id, source_system, source_entity, city_id)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_fulfillment_centers.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_fulfillment_centers(v_last_online);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_FULFILLMENT_CENTERS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_fulfillment_centers();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_fulfillment_centers'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_fulfillment_centers';

SELECT COUNT(*) FROM bl_3nf.ce_fulfillment_centers;
SELECT * FROM bl_3nf.ce_fulfillment_centers;

-- =========================================================
--  CE_DELIVERY_TYPES
-- =========================================================
CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_delivery_types(
    p_last_dts_online timestamptz
)
RETURNS TABLE (
    delivery_type_name varchar(40),
    source_system      varchar(30),
    source_entity      varchar(60),
    source_id          varchar(100),
    max_load_dts       timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(son.delivery_type), ''), 'n. a.') AS delivery_type_name,
        'sa_sales_online'::text                                 AS source_system,
        'src_sales_online'::text                                AS source_entity,
        COALESCE(NULLIF(trim(son.delivery_type), ''), 'n. a.') AS source_id,
        son.load_dts                                            AS load_dts
    FROM sa_sales_online.src_sales_online son
    WHERE son.delivery_type IS NOT NULL
      AND son.load_dts > p_last_dts_online
)
SELECT
    delivery_type_name::varchar(40),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_delivery_types(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_delivery_types';
    v_run_id uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_new_online  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_online
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_DELIVERY_TYPES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_delivery_types (
        delivery_type_id, delivery_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_delivery_types WHERE delivery_type_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_delivery_types (
        delivery_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.delivery_type_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_delivery_types(v_last_online) p
    ON CONFLICT (delivery_type_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_delivery_types.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_delivery_types(v_last_online);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_DELIVERY_TYPES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_delivery_types();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_delivery_types'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_delivery_types';

SELECT COUNT(*) FROM bl_3nf.ce_delivery_types;
SELECT * FROM bl_3nf.ce_delivery_types;

-- =========================================================
--  CE_DELIVERY_PROVIDERS
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_delivery_providers(
    p_last_dts_online timestamptz
)
RETURNS TABLE (
    carrier_name  varchar(60),
    source_system varchar(30),
    source_entity varchar(60),
    source_id     varchar(100),
    max_load_dts  timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(son.carrier_name), ''), 'n. a.') AS carrier_name,
        'sa_sales_online'::text                               AS source_system,
        'src_sales_online'::text                              AS source_entity,
        COALESCE(NULLIF(trim(son.carrier_name), ''), 'n. a.') AS source_id,
        son.load_dts                                          AS load_dts
    FROM sa_sales_online.src_sales_online son
    WHERE son.carrier_name IS NOT NULL
      AND son.load_dts > p_last_dts_online
)
SELECT
    carrier_name::varchar(60),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;

CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_delivery_providers(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_delivery_providers';
    v_run_id uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_new_online  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_online
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_DELIVERY_PROVIDERS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_delivery_providers (
        delivery_provider_id, carrier_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_delivery_providers WHERE delivery_provider_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_delivery_providers (
        carrier_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.carrier_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_delivery_providers(v_last_online) p
    ON CONFLICT (carrier_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_delivery_providers.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_delivery_providers(v_last_online);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_DELIVERY_PROVIDERS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_delivery_providers();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_delivery_providers'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_delivery_providers';

SELECT COUNT(*) FROM bl_3nf.ce_delivery_providers;
SELECT * FROM bl_3nf.ce_delivery_providers;

-- =========================================================
--  CE_SALES_CHANNELS
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_sales_channels()
RETURNS TABLE (
    sales_channel_name varchar(20),
    source_system      varchar(30),
    source_entity      varchar(60),
    source_id          varchar(100),
    max_load_dts       timestamptz
)
LANGUAGE sql
AS $$
SELECT 'online'::varchar(20), 'sa_sales_online'::varchar(30), 'src_sales_online'::varchar(60), 'online'::varchar(100), now()::timestamptz
UNION ALL
SELECT 'pos'::varchar(20), 'sa_sales_pos'::varchar(30), 'src_sales_pos'::varchar(60), 'pos'::varchar(100), now()::timestamptz;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_sales_channels(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_sales_channels';
    v_run_id uuid := gen_random_uuid();

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_SALES_CHANNELS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_sales_channels (
        sales_channel_id, sales_channel_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_sales_channels WHERE sales_channel_id = -1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_sales_channels (
        sales_channel_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.sales_channel_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_sales_channels() p
    ON CONFLICT (sales_channel_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_sales_channels.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_SALES_CHANNELS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_sales_channels();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_sales_channels'
ORDER BY log_dts DESC;

SELECT COUNT(*) FROM bl_3nf.ce_sales_channels;
SELECT * FROM bl_3nf.ce_sales_channels;

-- =========================================================
--  CE_PAYMENT_METHODS (POS)
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_payment_methods(
    p_last_dts_pos timestamptz
)
RETURNS TABLE (
    payment_method_name varchar(40),
    source_system       varchar(30),
    source_entity       varchar(60),
    source_id           varchar(100),
    max_load_dts        timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(spo.payment_method), ''), 'n. a.') AS payment_method_name,
        'sa_sales_pos'::text                                     AS source_system,
        'src_sales_pos'::text                                    AS source_entity,
        COALESCE(NULLIF(trim(spo.payment_method), ''), 'n. a.') AS source_id,
        spo.load_dts                                              AS load_dts
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.payment_method IS NOT NULL
      AND spo.load_dts > p_last_dts_pos
)
SELECT
    payment_method_name::varchar(40),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_payment_methods(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_payment_methods';
    v_run_id uuid := gen_random_uuid();

    v_last_pos timestamptz;
    v_new_pos  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_pos', 'src_sales_pos', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_pos := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_PAYMENT_METHODS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_payment_methods (
        payment_method_id, payment_method_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_payment_methods WHERE payment_method_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_payment_methods (
        payment_method_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.payment_method_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_payment_methods(v_last_pos) p
    ON CONFLICT (payment_method_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_payment_methods.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_payment_methods(v_last_pos);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_PAYMENT_METHODS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_payment_methods();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_payment_methods'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_payment_methods';

SELECT COUNT(*) FROM bl_3nf.ce_payment_methods;
SELECT * FROM bl_3nf.ce_payment_methods;

-- =========================================================
--  CE_PAYMENT_GATEWAYS (ONLINE)
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_payment_gateways(
    p_last_dts_online timestamptz
)
RETURNS TABLE (
    payment_gateway_name varchar(40),
    source_system        varchar(30),
    source_entity        varchar(60),
    source_id            varchar(100),
    max_load_dts         timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(son.payment_gateway), ''), 'n. a.') AS payment_gateway_name,
        'sa_sales_online'::text                                   AS source_system,
        'src_sales_online'::text                                  AS source_entity,
        COALESCE(NULLIF(trim(son.payment_gateway), ''), 'n. a.') AS source_id,
        son.load_dts                                              AS load_dts
    FROM sa_sales_online.src_sales_online son
    WHERE son.payment_gateway IS NOT NULL
      AND son.load_dts > p_last_dts_online
)
SELECT
    payment_gateway_name::varchar(40),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_payment_gateways(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_payment_gateways';
    v_run_id uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_new_online  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_online
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_PAYMENT_GATEWAYS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_payment_gateways (
        payment_gateway_id, payment_gateway_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_payment_gateways WHERE payment_gateway_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_payment_gateways (
        payment_gateway_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.payment_gateway_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_payment_gateways(v_last_online) p
    ON CONFLICT (payment_gateway_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_payment_gateways.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_payment_gateways(v_last_online);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_PAYMENT_GATEWAYS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_payment_gateways();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_payment_gateways'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_payment_gateways';

SELECT COUNT(*) FROM bl_3nf.ce_payment_gateways;
SELECT * FROM bl_3nf.ce_payment_gateways;


-- =========================================================
--  CE_ORDER_STATUSES (ONLINE)
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_order_statuses(
    p_last_dts_online timestamptz
)
RETURNS TABLE (
    order_status_name varchar(40),
    source_system     varchar(30),
    source_entity     varchar(60),
    source_id         varchar(100),
    max_load_dts      timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(son.order_status), ''), 'n. a.') AS order_status_name,
        'sa_sales_online'::text                                AS source_system,
        'src_sales_online'::text                               AS source_entity,
        COALESCE(NULLIF(trim(son.order_status), ''), 'n. a.') AS source_id,
        son.load_dts                                           AS load_dts
    FROM sa_sales_online.src_sales_online son
    WHERE son.order_status IS NOT NULL
      AND son.load_dts > p_last_dts_online
)
SELECT
    order_status_name::varchar(40),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_order_statuses(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_order_statuses';
    v_run_id uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_new_online  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_online
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_ORDER_STATUSES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_order_statuses (
        order_status_id, order_status_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_order_statuses WHERE order_status_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_order_statuses (
        order_status_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.order_status_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_order_statuses(v_last_online) p
    ON CONFLICT (order_status_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_order_statuses.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_order_statuses(v_last_online);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_ORDER_STATUSES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_order_statuses();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_order_statuses'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_order_statuses';

SELECT COUNT(*) FROM bl_3nf.ce_order_statuses;
SELECT * FROM bl_3nf.ce_order_statuses;

-- =========================================================
--  CE_RECEIPT_TYPES (POS)
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_receipt_types(
    p_last_dts_pos timestamptz
)
RETURNS TABLE (
    receipt_type_name varchar(40),
    source_system     varchar(30),
    source_entity     varchar(60),
    source_id         varchar(100),
    max_load_dts      timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(spo.receipt_type), ''), 'n. a.') AS receipt_type_name,
        'sa_sales_pos'::text                                   AS source_system,
        'src_sales_pos'::text                                  AS source_entity,
        COALESCE(NULLIF(trim(spo.receipt_type), ''), 'n. a.') AS source_id,
        spo.load_dts                                            AS load_dts
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.receipt_type IS NOT NULL
      AND spo.load_dts > p_last_dts_pos
)
SELECT
    receipt_type_name::varchar(40),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_receipt_types(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_receipt_types';
    v_run_id uuid := gen_random_uuid();

    v_last_pos timestamptz;
    v_new_pos  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_pos', 'src_sales_pos', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_pos := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_RECEIPT_TYPES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_receipt_types (
        receipt_type_id, receipt_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_receipt_types WHERE receipt_type_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_receipt_types (
        receipt_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.receipt_type_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_receipt_types(v_last_pos) p
    ON CONFLICT (receipt_type_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_receipt_types.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_receipt_types(v_last_pos);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_RECEIPT_TYPES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_receipt_types();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_receipt_types'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_receipt_types';

SELECT COUNT(*) FROM bl_3nf.ce_receipt_types;
SELECT * FROM bl_3nf.ce_receipt_types;

-- =========================================================
--  CE_CARD_TYPES (POS)
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_card_types(
    p_last_dts_pos timestamptz
)
RETURNS TABLE (
    card_type_name varchar(40),
    source_system  varchar(30),
    source_entity  varchar(60),
    source_id      varchar(100),
    max_load_dts   timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(spo.card_type), ''), 'n. a.') AS card_type_name,
        'sa_sales_pos'::text                               AS source_system,
        'src_sales_pos'::text                              AS source_entity,
        COALESCE(NULLIF(trim(spo.card_type), ''), 'n. a.') AS source_id,
        spo.load_dts                                       AS load_dts
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.card_type IS NOT NULL
      AND spo.load_dts > p_last_dts_pos
)
SELECT
    card_type_name::varchar(40),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_card_types(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_card_types';
    v_run_id uuid := gen_random_uuid();

    v_last_pos timestamptz;
    v_new_pos  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_pos', 'src_sales_pos', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_pos := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_CARD_TYPES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_card_types (
        card_type_id, card_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_card_types WHERE card_type_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_card_types (
        card_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.card_type_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_card_types(v_last_pos) p
    ON CONFLICT (card_type_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_card_types.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_card_types(v_last_pos);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_CARD_TYPES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_card_types();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_card_types'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_card_types';

SELECT COUNT(*) FROM bl_3nf.ce_card_types;
SELECT * FROM bl_3nf.ce_card_types;

-- =========================================================
--  CE_DEVICE_TYPES (ONLINE)
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_device_types(
    p_last_dts_online timestamptz
)
RETURNS TABLE (
    device_type_name varchar(40),
    source_system    varchar(30),
    source_entity    varchar(60),
    source_id        varchar(100),
    max_load_dts     timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(son.device_type), ''), 'n. a.') AS device_type_name,
        'sa_sales_online'::text                               AS source_system,
        'src_sales_online'::text                              AS source_entity,
        COALESCE(NULLIF(trim(son.device_type), ''), 'n. a.') AS source_id,
        son.load_dts                                          AS load_dts
    FROM sa_sales_online.src_sales_online son
    WHERE son.device_type IS NOT NULL
      AND son.load_dts > p_last_dts_online
)
SELECT
    device_type_name::varchar(40),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_device_types(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_device_types';
    v_run_id uuid := gen_random_uuid();

    v_last_online timestamptz;
    v_new_online  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_online', 'src_sales_online', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_online
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    IF p_full_reload THEN
        v_last_online := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_DEVICE_TYPES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_device_types (
        device_type_id, device_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_device_types WHERE device_type_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_device_types (
        device_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.device_type_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_device_types(v_last_online) p
    ON CONFLICT (device_type_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_device_types.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_online
    FROM bl_cl.fn_prepare_ce_device_types(v_last_online);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_online, v_last_online),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_online' AND source_entity='src_sales_online';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_DEVICE_TYPES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_device_types();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_device_types'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_device_types';

SELECT COUNT(*) FROM bl_3nf.ce_device_types;
SELECT * FROM bl_3nf.ce_device_types;


-- =========================================================
--  CE_TERMINAL_TYPES (POS)
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_terminal_types(
    p_last_dts_pos timestamptz
)
RETURNS TABLE (
    terminal_type_name varchar(40),
    source_system      varchar(30),
    source_entity      varchar(60),
    source_id          varchar(100),
    max_load_dts       timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(spo.terminal_type), ''), 'n. a.') AS terminal_type_name,
        'sa_sales_pos'::text                                    AS source_system,
        'src_sales_pos'::text                                   AS source_entity,
        COALESCE(NULLIF(trim(spo.terminal_type), ''), 'n. a.') AS source_id,
        spo.load_dts                                             AS load_dts
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.terminal_type IS NOT NULL
      AND spo.load_dts > p_last_dts_pos
)
SELECT
    terminal_type_name::varchar(40),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_terminal_types(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_terminal_types';
    v_run_id uuid := gen_random_uuid();

    v_last_pos timestamptz;
    v_new_pos  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_pos', 'src_sales_pos', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_pos := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_TERMINAL_TYPES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_terminal_types (
        terminal_type_id, terminal_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_terminal_types WHERE terminal_type_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_terminal_types (
        terminal_type_name, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.terminal_type_name, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_terminal_types(v_last_pos) p
    ON CONFLICT (terminal_type_name, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_terminal_types.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_terminal_types(v_last_pos);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_TERMINAL_TYPES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_terminal_types();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_terminal_types'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_terminal_types';

SELECT COUNT(*) FROM bl_3nf.ce_terminal_types;
SELECT * FROM bl_3nf.ce_terminal_types;

-- =========================================================
--  CE_TERMINALS (POS)  
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_terminals(
    p_last_dts_pos timestamptz
)
RETURNS TABLE (
    terminal_src_id  varchar(100),
    terminal_type_id bigint,
    store_id         bigint,
    source_system    varchar(30),
    source_entity    varchar(60),
    source_id        varchar(100),
    max_load_dts     timestamptz
)
LANGUAGE sql
AS $$
WITH src_raw AS (
    SELECT
        COALESCE(NULLIF(trim(spo.terminal_id), ''), 'n. a.')      AS terminal_src_id,
        COALESCE(NULLIF(trim(spo.terminal_type), ''), 'n. a.')    AS terminal_type_name,
        COALESCE(NULLIF(trim(spo.store_id), ''), 'n. a.')         AS store_src_id,
        COALESCE(spo.txn_ts, TIMESTAMP '1900-01-01')              AS txn_ts,
        'sa_sales_pos'::text                                      AS source_system,
        'src_sales_pos'::text                                     AS source_entity,
        COALESCE(NULLIF(trim(spo.terminal_id), ''), 'n. a.')      AS source_id,
        spo.load_dts                                              AS load_dts
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.terminal_id IS NOT NULL
      AND spo.load_dts > p_last_dts_pos
),
src AS (
    /* Guarantee uniqueness per conflict key to avoid "affect row twice" */
    SELECT DISTINCT ON (srr.terminal_src_id, srr.source_system, srr.source_entity)
        srr.terminal_src_id,
        srr.terminal_type_name,
        srr.store_src_id,
        srr.source_system,
        srr.source_entity,
        srr.source_id,
        srr.load_dts
    FROM src_raw srr
    ORDER BY
        srr.terminal_src_id,
        srr.source_system,
        srr.source_entity,
        srr.txn_ts DESC,
        srr.load_dts DESC
),
map AS (
    SELECT
        src.terminal_src_id,
        COALESCE(tty.terminal_type_id, -1) AS terminal_type_id,
        COALESCE(str.store_id, -1)         AS store_id,
        src.source_system,
        src.source_entity,
        src.source_id,
        src.load_dts
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
SELECT
    map.terminal_src_id::varchar(100),
    map.terminal_type_id,
    map.store_id,
    map.source_system::varchar(30),
    map.source_entity::varchar(60),
    map.source_id::varchar(100),
    max(map.load_dts)::timestamptz AS max_load_dts
FROM map
GROUP BY 1,2,3,4,5,6;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_terminals(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_terminals';
    v_run_id uuid := gen_random_uuid();

    v_last_pos timestamptz;
    v_new_pos  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_pos', 'src_sales_pos', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_pos := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_TERMINALS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_terminals (
        terminal_id, terminal_src_id, terminal_type_id, store_id,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', -1, -1,
        'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_terminals WHERE terminal_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

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
        p.terminal_src_id,
        p.terminal_type_id,
        p.store_id,
        p.source_system,
        p.source_entity,
        p.source_id,
        now(),
        now()
    FROM bl_cl.fn_prepare_ce_terminals(v_last_pos) p
    ON CONFLICT (terminal_src_id, source_system, source_entity)
    DO UPDATE
    SET
        terminal_type_id = EXCLUDED.terminal_type_id,
        store_id         = EXCLUDED.store_id,
        source_id        = EXCLUDED.source_id,
        ta_update_dt     = now()
    WHERE (bl_3nf.ce_terminals.terminal_type_id,
           bl_3nf.ce_terminals.store_id,
           bl_3nf.ce_terminals.source_id)
          IS DISTINCT FROM
          (EXCLUDED.terminal_type_id,
           EXCLUDED.store_id,
           EXCLUDED.source_id);

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_terminals(v_last_pos);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_TERMINALS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_terminals();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_terminals'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_terminals';

SELECT COUNT(*) FROM bl_3nf.ce_terminals;
SELECT * FROM bl_3nf.ce_terminals;

-- =========================================================
--  CE_SHIFTS (POS)
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_shifts(
    p_last_dts_pos timestamptz
)
RETURNS TABLE (
    shift_src_id   varchar(60),
    source_system  varchar(30),
    source_entity  varchar(60),
    source_id      varchar(100),
    max_load_dts   timestamptz
)
LANGUAGE sql
AS $$
WITH src AS (
    SELECT DISTINCT
        COALESCE(NULLIF(trim(spo.shift_id), ''), 'n. a.') AS shift_src_id,
        'sa_sales_pos'::text                               AS source_system,
        'src_sales_pos'::text                              AS source_entity,
        COALESCE(NULLIF(trim(spo.shift_id), ''), 'n. a.') AS source_id,
        spo.load_dts                                       AS load_dts
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.shift_id IS NOT NULL
      AND spo.load_dts > p_last_dts_pos
)
SELECT
    shift_src_id::varchar(60),
    source_system::varchar(30),
    source_entity::varchar(60),
    source_id::varchar(100),
    max(load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_shifts(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_shifts';
    v_run_id uuid := gen_random_uuid();

    v_last_pos timestamptz;
    v_new_pos  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_pos', 'src_sales_pos', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_pos := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_SHIFTS.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_shifts (
        shift_id, shift_src_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT -1, 'n. a.', 'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_shifts WHERE shift_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    INSERT INTO bl_3nf.ce_shifts (
        shift_src_id, source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT p.shift_src_id, p.source_system, p.source_entity, p.source_id, now(), now()
    FROM bl_cl.fn_prepare_ce_shifts(v_last_pos) p
    ON CONFLICT (shift_src_id, source_system, source_entity)
    DO UPDATE
       SET source_id    = EXCLUDED.source_id,
           ta_update_dt = now()
     WHERE bl_3nf.ce_shifts.source_id IS DISTINCT FROM EXCLUDED.source_id;

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_shifts(v_last_pos);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_SHIFTS loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;


-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_shifts();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_shifts'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_shifts';

SELECT COUNT(*) FROM bl_3nf.ce_shifts;
SELECT * FROM bl_3nf.ce_shifts;

-- =========================================================
--  CE_EMPLOYEES (POS) 
-- =========================================================

CREATE OR REPLACE FUNCTION bl_cl.fn_prepare_ce_employees(
    p_last_dts_pos timestamptz
)
RETURNS TABLE (
    employee_src_id varchar(100),
    first_name      varchar(100),
    last_name       varchar(100),
    department      varchar(60),
    "position"        varchar(60),
    hire_dt         date,
    source_system   varchar(30),
    source_entity   varchar(60),
    source_id       varchar(100),
    max_load_dts    timestamptz
)
LANGUAGE sql
AS $$
WITH src_raw AS (
    SELECT
        COALESCE(NULLIF(trim(spo.cashier_id), ''), 'n. a.')            AS employee_src_id,
        COALESCE(NULLIF(trim(spo.cashier_first_name), ''), 'n. a.')    AS first_name,
        COALESCE(NULLIF(trim(spo.cashier_last_name), ''), 'n. a.')     AS last_name,
        COALESCE(NULLIF(trim(spo.cashier_dept), ''), 'n. a.')          AS department,
        COALESCE(NULLIF(trim(spo.cashier_position), ''), 'n. a.')      AS position,
        COALESCE(spo.cashier_hire_dt, DATE '1900-01-01')               AS hire_dt,
        COALESCE(spo.txn_ts, TIMESTAMP '1900-01-01')                   AS txn_ts,
        'sa_sales_pos'::text                                           AS source_system,
        'src_sales_pos'::text                                          AS source_entity,
        COALESCE(NULLIF(trim(spo.cashier_id), ''), 'n. a.')            AS source_id,
        spo.load_dts                                                   AS load_dts
    FROM sa_sales_pos.src_sales_pos spo
    WHERE spo.cashier_id IS NOT NULL
      AND spo.load_dts > p_last_dts_pos
),
src AS (
    /* Guarantee uniqueness per conflict key */
    SELECT DISTINCT ON (srr.employee_src_id, srr.source_system, srr.source_entity)
        srr.employee_src_id,
        srr.first_name,
        srr.last_name,
        srr.department,
        srr.position,
        srr.hire_dt,
        srr.source_system,
        srr.source_entity,
        srr.source_id,
        srr.load_dts
    FROM src_raw srr
    ORDER BY
        srr.employee_src_id,
        srr.source_system,
        srr.source_entity,
        srr.txn_ts DESC,
        srr.load_dts DESC
)
SELECT
    src.employee_src_id::varchar(100),
    src.first_name::varchar(100),
    src.last_name::varchar(100),
    src.department::varchar(60),
    src.position::varchar(60),
    src.hire_dt,
    src.source_system::varchar(30),
    src.source_entity::varchar(60),
    src.source_id::varchar(100),
    max(src.load_dts)::timestamptz AS max_load_dts
FROM src
GROUP BY 1,2,3,4,5,6,7,8,9;
$$;


CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_employees(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_employees';
    v_run_id uuid := gen_random_uuid();

    v_last_pos timestamptz;
    v_new_pos  timestamptz;

    v_rows_default bigint := 0;
    v_rows_main    bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    INSERT INTO bl_cl.mta_load_control (procedure_name, source_system, source_entity, last_success_load_dts)
    VALUES (v_proc, 'sa_sales_pos', 'src_sales_pos', '1900-01-01'::timestamptz)
    ON CONFLICT (procedure_name, source_system, source_entity) DO NOTHING;

    SELECT last_success_load_dts INTO v_last_pos
    FROM bl_cl.mta_load_control
    WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    IF p_full_reload THEN
        v_last_pos := '1900-01-01'::timestamptz;
    END IF;

    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_EMPLOYEES.',NULL,NULL,NULL,v_run_id);

    INSERT INTO bl_3nf.ce_employees (
        employee_id, employee_src_id, first_name, last_name, department, position, hire_dt,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', DATE '1900-01-01',
        'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_employees WHERE employee_id=-1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

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
        p.employee_src_id,
        p.first_name,
        p.last_name,
        p.department,
        p.position,
        p.hire_dt,
        p.source_system,
        p.source_entity,
        p.source_id,
        now(),
        now()
    FROM bl_cl.fn_prepare_ce_employees(v_last_pos) p
    ON CONFLICT (employee_src_id, source_system, source_entity)
    DO UPDATE
    SET
        first_name   = EXCLUDED.first_name,
        last_name    = EXCLUDED.last_name,
        department   = EXCLUDED.department,
        position     = EXCLUDED.position,
        hire_dt      = EXCLUDED.hire_dt,
        source_id    = EXCLUDED.source_id,
        ta_update_dt = now()
    WHERE (bl_3nf.ce_employees.first_name,
           bl_3nf.ce_employees.last_name,
           bl_3nf.ce_employees.department,
           bl_3nf.ce_employees.position,
           bl_3nf.ce_employees.hire_dt,
           bl_3nf.ce_employees.source_id)
          IS DISTINCT FROM
          (EXCLUDED.first_name,
           EXCLUDED.last_name,
           EXCLUDED.department,
           EXCLUDED.position,
           EXCLUDED.hire_dt,
           EXCLUDED.source_id);

    GET DIAGNOSTICS v_rows_main = ROW_COUNT;
    v_rows_total := v_rows_default + v_rows_main;

    SELECT max(max_load_dts) INTO v_new_pos
    FROM bl_cl.fn_prepare_ce_employees(v_last_pos);

    UPDATE bl_cl.mta_load_control
       SET last_success_load_dts = COALESCE(v_new_pos, v_last_pos),
           ta_update_dt = now()
     WHERE procedure_name=v_proc AND source_system='sa_sales_pos' AND source_entity='src_sales_pos';

    CALL bl_cl.pr_log_write(v_proc,'SUCCESS',v_rows_total,'CE_EMPLOYEES loaded successfully.',NULL,NULL,NULL,v_run_id);

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_employees();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_employees'
ORDER BY log_dts DESC;

Select * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_employees';

SELECT COUNT(*) FROM bl_3nf.ce_employees;
SELECT * FROM bl_3nf.ce_employees;



CREATE OR REPLACE PROCEDURE bl_cl.pr_load_ce_customers_scd(p_full_reload boolean DEFAULT false)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc text := 'pr_load_ce_customers_scd';
    v_run_id uuid := gen_random_uuid();

    v_rows_default bigint := 0;
    v_rows_upd     bigint := 0;
    v_rows_ins     bigint := 0;
    v_rows_total   bigint := 0;
BEGIN
    -- 0) start log
    CALL bl_cl.pr_log_write(v_proc,'START',0,'Start loading CE_CUSTOMERS_SCD (latest-only, no function).',NULL,NULL,NULL,v_run_id);

    ----------------------------------------------------------------------
    -- 1) default row
    ----------------------------------------------------------------------
    INSERT INTO bl_3nf.ce_customers_scd (
        customer_id, customer_src_id, first_name, last_name, email, phone, age_grp, customer_segment, gender,
        start_dt, end_dt, is_active,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
    )
    SELECT
        -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.',
        DATE '1900-01-01', DATE '9999-12-31', TRUE,
        'manual', 'manual', 'n. a.', timestamp '1900-01-01', timestamp '1900-01-01'
    WHERE NOT EXISTS (SELECT 1 FROM bl_3nf.ce_customers_scd WHERE customer_id = -1);

    GET DIAGNOSTICS v_rows_default = ROW_COUNT;

    CALL bl_cl.pr_log_write(
        v_proc,'INFO',v_rows_default,
        format('Default row ensured. inserted=%s', v_rows_default),
        NULL,'manual','manual',v_run_id
    );

    ----------------------------------------------------------------------
    -- 2) UPDATE step (close changed) 
    ----------------------------------------------------------------------
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
            'sa_sales_online'                            AS source_system,
            'src_sales_online'                           AS source_entity,
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
            'sa_sales_pos'                               AS source_system,
            'src_sales_pos'                              AS source_entity,
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
            cur.customer_id
        FROM src
        JOIN cur
          ON cur.customer_src_id = src.customer_src_id
         AND cur.source_system   = src.source_system
         AND cur.source_entity   = src.source_entity
        WHERE cur.first_name       IS DISTINCT FROM src.first_name
           OR cur.last_name        IS DISTINCT FROM src.last_name
           OR cur.email            IS DISTINCT FROM src.email
           OR cur.phone            IS DISTINCT FROM src.phone
           OR cur.age_grp          IS DISTINCT FROM src.age_grp
           OR cur.customer_segment IS DISTINCT FROM src.customer_segment
           OR cur.gender           IS DISTINCT FROM src.gender
    )
    UPDATE bl_3nf.ce_customers_scd cus
    SET
        
        end_dt       = GREATEST(cus.start_dt, CURRENT_DATE - 1),
        is_active    = FALSE,
        ta_update_dt = now()
    WHERE cus.is_active = TRUE
      AND cus.end_dt = DATE '9999-12-31'
      AND EXISTS (SELECT 1 FROM chg WHERE chg.customer_id = cus.customer_id);

    GET DIAGNOSTICS v_rows_upd = ROW_COUNT;

    CALL bl_cl.pr_log_write(
        v_proc,'INFO',v_rows_upd,
        format('SCD2 close step done. updated=%s', v_rows_upd),
        NULL,NULL,NULL,v_run_id
    );

    ----------------------------------------------------------------------
    -- 3) INSERT step (insert new_or_changed) 
    ----------------------------------------------------------------------
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
            'sa_sales_online'                            AS source_system,
            'src_sales_online'                           AS source_entity,
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
            'sa_sales_pos'                               AS source_system,
            'src_sales_pos'                              AS source_entity,
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
        WHERE cur.first_name       IS DISTINCT FROM src.first_name
           OR cur.last_name        IS DISTINCT FROM src.last_name
           OR cur.email            IS DISTINCT FROM src.email
           OR cur.phone            IS DISTINCT FROM src.phone
           OR cur.age_grp          IS DISTINCT FROM src.age_grp
           OR cur.customer_segment IS DISTINCT FROM src.customer_segment
           OR cur.gender           IS DISTINCT FROM src.gender
    ),
    new_rows AS (
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
    ),
    new_or_changed AS (
        SELECT * FROM new_rows
        UNION ALL
        SELECT * FROM chg
    )
    INSERT INTO bl_3nf.ce_customers_scd (
        customer_src_id, first_name, last_name, email, phone, age_grp, customer_segment, gender,
        start_dt, end_dt, is_active,
        source_system, source_entity, source_id, ta_insert_dt, ta_update_dt
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
        now(), now()
    FROM new_or_changed nac
    LEFT JOIN bl_3nf.ce_customers_scd cus
      ON cus.customer_src_id = nac.customer_src_id
     AND cus.source_system   = nac.source_system
     AND cus.source_entity   = nac.source_entity
     AND cus.start_dt        = CURRENT_DATE
    WHERE cus.customer_id IS NULL;

    GET DIAGNOSTICS v_rows_ins = ROW_COUNT;

    CALL bl_cl.pr_log_write(
        v_proc,'INFO',v_rows_ins,
        format('SCD2 insert step done. inserted=%s', v_rows_ins),
        NULL,NULL,NULL,v_run_id
    );

    ----------------------------------------------------------------------
    -- 4) final success
    ----------------------------------------------------------------------
    v_rows_total := v_rows_default + v_rows_upd + v_rows_ins;

    CALL bl_cl.pr_log_write(
        v_proc,'SUCCESS',v_rows_total,
        format('CE_CUSTOMERS_SCD loaded successfully. default=%s, updated=%s, inserted=%s',
               v_rows_default, v_rows_upd, v_rows_ins),
        NULL,NULL,NULL,v_run_id
    );

EXCEPTION WHEN OTHERS THEN
    CALL bl_cl.pr_log_write(v_proc,'ERROR',0,SQLERRM,SQLSTATE,NULL,NULL,v_run_id);
    RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_ce_customers_scd();
SELECT log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name = 'pr_load_ce_customers_scd'
ORDER BY log_dts DESC;

SELECT * FROM bl_cl.mta_load_control
WHERE procedure_name = 'pr_load_ce_customers_scd';

SELECT COUNT(*) FROM bl_3nf.ce_customers_scd;
SELECT * FROM bl_3nf.ce_customers_scd;


SELECT  customer_src_id, first_name, last_name, is_active, start_dt, end_dt, source_system, source_entity
FROM bl_3nf.ce_customers_scd
WHERE customer_src_id = 'c0857672';




