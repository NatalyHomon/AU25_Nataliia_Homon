-- =========================================================
--  ETL SECURITY SETUP (roles + grants + default privileges)
--  Owner of objects: postgres
--  DB: sales_project
--  Schemas: sa_sales_pos, sa_sales_online, bl_cl, bl_3nf, bl_dm
-- =========================================================

-- -------------------------
-- 0) Safety: create roles if not exists
-- -------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bl_etl') THEN
        CREATE ROLE bl_etl NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_user_template') THEN
        -- Reusable LOGIN role (template) for running ETL
        -- You can grant this role to multiple real users, or use it directly for a service account.
        CREATE ROLE etl_user_template LOGIN;
        
    END IF;
END$$;


-- -------------------------
-- 1) Database connect
-- -------------------------
GRANT CONNECT ON DATABASE test TO bl_etl;
GRANT CONNECT ON DATABASE test TO etl_user_template;

--  prevent PUBLIC from connecting 
REVOKE CONNECT ON DATABASE test FROM PUBLIC;


-- -------------------------
-- 2) Membership: login role inherits ETL permissions
-- -------------------------
GRANT bl_etl TO etl_user_template;


-- -------------------------
-- 3) Schema usage
-- -------------------------
GRANT USAGE ON SCHEMA sa_sales_pos, sa_sales_online, bl_cl, bl_3nf, bl_dm TO bl_etl;


-- -------------------------
-- 4) Existing objects permissions
-- -------------------------

-- 4.1 SA (read only)
GRANT SELECT ON ALL TABLES IN SCHEMA sa_sales_pos, sa_sales_online TO bl_etl;

-- 4.2 BL_CL (control/log/mapping + execute)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bl_cl TO bl_etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bl_cl TO bl_etl;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bl_cl TO bl_etl;

-- 4.3 BL_3NF (write + read for lookups)
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bl_3nf TO bl_etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bl_3nf TO bl_etl;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bl_3nf TO bl_etl;

-- 4.4 BL_DM (usually write for marts)
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bl_dm TO bl_etl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bl_dm TO bl_etl;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA bl_dm TO bl_etl;


-- -------------------------
-- 5) Make execution stable: search_path
-- -------------------------
ALTER ROLE bl_etl SET search_path = bl_cl, bl_3nf, bl_dm, sa_sales_pos, sa_sales_online, public;
ALTER ROLE etl_user_template SET search_path = bl_cl, bl_3nf, bl_dm, sa_sales_pos, sa_sales_online, public;


-- -------------------------
-- 6) DEFAULT PRIVILEGES (future objects) -- IMPORTANT
--     These apply ONLY to objects created by postgres in those schemas.
-- -------------------------

-- SA: if new tables appear, keep them readable
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA sa_sales_pos
GRANT SELECT ON TABLES TO bl_etl;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA sa_sales_online
GRANT SELECT ON TABLES TO bl_etl;

-- BL_CL: tables + sequences + functions
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bl_cl
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO bl_etl;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bl_cl
GRANT USAGE, SELECT ON SEQUENCES TO bl_etl;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bl_cl
GRANT EXECUTE ON FUNCTIONS TO bl_etl;

-- BL_3NF: tables + sequences + functions
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bl_3nf
GRANT SELECT, INSERT, UPDATE ON TABLES TO bl_etl;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bl_3nf
GRANT USAGE, SELECT ON SEQUENCES TO bl_etl;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bl_3nf
GRANT EXECUTE ON FUNCTIONS TO bl_etl;

-- BL_DM: tables + sequences + functions
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bl_dm
GRANT SELECT, INSERT, UPDATE ON TABLES TO bl_etl;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bl_dm
GRANT USAGE, SELECT ON SEQUENCES TO bl_etl;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bl_dm
GRANT EXECUTE ON FUNCTIONS TO bl_etl;


ALTER ROLE etl_user_template PASSWORD 'strong_password';

-- -------------------------
-- 7)  verification queries
-- -------------------------
-- Show default ACL rules set for postgres
SELECT defaclrole::regrole, defaclnamespace::regnamespace, defaclobjtype, defaclacl
FROM pg_default_acl
WHERE defaclrole = 'postgres'::regrole;

-- Show membership
SELECT r.rolname AS role, m.rolname AS member
FROM pg_auth_members am
JOIN pg_roles r ON r.oid = am.roleid
JOIN pg_roles m ON m.oid = am.member
WHERE r.rolname = 'bl_etl';

