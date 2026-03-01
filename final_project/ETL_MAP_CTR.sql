CREATE OR REPLACE PROCEDURE bl_cl.pr_load_map_countries(
    p_run_id uuid DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc   text := 'bl_cl.pr_load_map_countries';
    v_run_id uuid := COALESCE(p_run_id, gen_random_uuid());
    v_rows   bigint := 0;
BEGIN
    CALL bl_cl.pr_log_write(
        p_procedure_name := v_proc,
        p_status         := 'START',
        p_rows_affected  := 0,
        p_message        := 'Loading/refreshing mapping countries from SA',
        p_sqlstate       := NULL,
        p_source_system  := 'bl_cl',
        p_source_entity  := 't_map_countries',
        p_run_id         := v_run_id
    );

    WITH src AS (
        SELECT DISTINCT
            trim(country)        AS country_src_name,
            'src_sales_online'   AS source_entity,
            'sa_sales_online'    AS source_system
        FROM sa_sales_online.src_sales_online
        WHERE country IS NOT NULL AND trim(country) <> ''

        UNION ALL

        SELECT DISTINCT
            trim(country)        AS country_src_name,
            'src_sales_pos'      AS source_entity,
            'sa_sales_pos'       AS source_system
        FROM sa_sales_pos.src_sales_pos
        WHERE country IS NOT NULL AND trim(country) <> ''
    ),
    canon AS (
        SELECT
            s.country_src_name,
            COALESCE(a.country_canonical, initcap(lower(s.country_src_name))) AS country_name,
            lower(COALESCE(a.country_canonical, initcap(lower(s.country_src_name)))) AS country_name_lc,
            s.source_entity,
            s.source_system
        FROM src s
        LEFT JOIN bl_cl.t_country_aliases a
            ON lower(a.country_alias) = lower(s.country_src_name)
    ),
    existing_countries AS (
        SELECT DISTINCT
            lower(country_name) AS country_name_lc,
            country_id
        FROM bl_cl.t_map_countries
    ),
    canonical_ids AS (
        SELECT
            c.country_name_lc,
            MIN(c.country_name) AS country_name,
            COALESCE(ec.country_id, nextval('bl_cl.seq_country_id')) AS country_id
        FROM canon c
        LEFT JOIN existing_countries ec
            ON ec.country_name_lc = c.country_name_lc
        GROUP BY c.country_name_lc, ec.country_id
    ),
    to_insert AS (
        SELECT
            ci.country_id,
            ci.country_name,
            c.country_src_name,
            c.source_entity,
            c.source_system
        FROM canon c
        JOIN canonical_ids ci
            ON ci.country_name_lc = c.country_name_lc
    )
    INSERT INTO bl_cl.t_map_countries (
        country_id, country_name, country_src_name, source_entity, source_system
    )
    SELECT
        country_id, country_name, country_src_name, source_entity, source_system
    FROM to_insert
    ON CONFLICT (country_src_name, source_entity, source_system) DO NOTHING;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_cl.pr_log_write(
        p_procedure_name := v_proc,
        p_status         := 'SUCCESS',
        p_rows_affected  := v_rows,
        p_message        := 'Countries mapping load completed',
        p_sqlstate       := NULL,
        p_source_system  := 'bl_cl',
        p_source_entity  := 't_map_countries',
        p_run_id         := v_run_id
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.pr_log_write(
            p_procedure_name := v_proc,
            p_status         := 'ERROR',
            p_rows_affected  := COALESCE(v_rows,0),
            p_message        := SQLERRM,
            p_sqlstate       := SQLSTATE,
            p_source_system  := 'bl_cl',
            p_source_entity  := 't_map_countries',
            p_run_id         := v_run_id
        );
        RAISE;
END;
$$;

-- =========================================================
--  TEST QUERIES 
-- =========================================================
CALL bl_cl.pr_load_map_countries();


SELECT log_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name='bl_cl.pr_load_map_countries'
ORDER BY log_id DESC;

SELECT* FROM bl_cl.t_map_countries; 


