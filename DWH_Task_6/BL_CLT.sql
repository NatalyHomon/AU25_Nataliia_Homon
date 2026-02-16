BEGIN;
--able with alias for countries
CREATE SCHEMA IF NOT EXISTS BL_CL;

CREATE TABLE IF NOT EXISTS bl_cl.t_country_aliases (
    country_alias      text NOT NULL,   -- UA, Ukr, Ukraine, UKRAINE
    country_canonical  text NOT NULL,   -- Ukraine

    ins_dts            timestamp DEFAULT now(),

    CONSTRAINT pk_country_aliases PRIMARY KEY (country_alias));
    

 
INSERT INTO bl_cl.t_country_aliases (country_alias, country_canonical) VALUES
('UA',       'Ukraine'),
('Ukr',      'Ukraine'),
('UKRAINE',  'Ukraine'),
('Ukraine',  'Ukraine'),
('Україна',  'Ukraine')
ON CONFLICT (country_alias) DO NOTHING
RETURNING *;

SELECT* FROM bl_cl.t_country_aliases;
--map table
COMMIT;

BEGIN;

CREATE SEQUENCE IF NOT EXISTS bl_cl.seq_country_id START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS bl_cl.t_map_countries (
   
    country_id        bigint NOT NULL,   
    country_name      text   NOT NULL,  
    country_src_name  text   NOT NULL,   

    source_entity      text   NOT NULL,   
    source_system     text   NOT NULL,  

   
    insert_dts           timestamp NOT NULL DEFAULT now(),
    update_dts           timestamp NOT NULL DEFAULT now(),

    CONSTRAINT pk_t_map_countries
        PRIMARY KEY (country_src_name, source_entity, source_system)
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
-- 1) we take UNIQUE canonical countries (by country_name_lc) and determine their country_id
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
-- 2) we return to all source values ​​and substitute the ready-made id
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
ON CONFLICT (country_src_name, source_entity, source_system) DO NOTHING
RETURNING *;

SELECT* FROM bl_cl.t_map_countries;




