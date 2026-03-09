DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'bl_dm'
    LOOP
        EXECUTE format(
            'TRUNCATE TABLE bl_dm.%I RESTART IDENTITY CASCADE;',
            r.tablename
        );
    END LOOP;
END $$;

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'bl_3nf'
    LOOP
        EXECUTE format(
            'TRUNCATE TABLE bl_3nf.%I RESTART IDENTITY CASCADE;',
            r.tablename
        );
    END LOOP;
END $$;

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'bl_cl'
    LOOP
        EXECUTE format(
            'TRUNCATE TABLE bl_cl.%I RESTART IDENTITY CASCADE;',
            r.tablename
        );
    END LOOP;
END $$;

TRUNCATE sa_sales_online.src_sales_online RESTART IDENTITY CASCADE;
TRUNCATE sa_sales_pos.src_sales_pos RESTART IDENTITY CASCADE;
TRUNCATE bl_cl.mta_load_control;