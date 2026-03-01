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