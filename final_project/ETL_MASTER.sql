CREATE OR REPLACE PROCEDURE bl_cl.prc_run_etl_master(
     p_full_reload boolean DEFAULT false,
     p_months_back int     DEFAULT 3,
     p_run_id      uuid    DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc   text := 'bl_cl.prc_run_etl_master';
    v_run_id uuid := COALESCE(p_run_id, gen_random_uuid());
BEGIN
    -- MASTER START
    CALL bl_cl.pr_log_write(
        p_procedure_name := v_proc,
        p_status         := 'START',
        p_rows_affected  := 0,
        p_message        := 'ETL master started (SA -> MAP -> 3NF -> DM)',
        p_sqlstate       := NULL,
        p_source_system  := 'bl_cl',
        p_source_entity  := 'ETL_MASTER',
        p_run_id         := v_run_id
    );

    -- =======================
    -- SA (Source Area)
    -- =======================
    CALL bl_cl.prc_load_sa_sales_online_src('src_sales_online.csv', v_run_id);
    CALL bl_cl.prc_load_sa_sales_pos_src('src_sales_pos.csv', v_run_id);

    -- =======================
    -- Mapping (only countries)
    -- =======================
    -- Якщо ти лишаєш назву саме так, як ми робили: bl_cl.prc_load_map_countries(p_run_id)
    CALL bl_cl.pr_load_map_countries(v_run_id);

    -- =======================
    -- 3NF / Core Entities (CE)
    -- порядок: довідники -> залежні сутності -> SCD customer
    -- =======================
    CALL bl_cl.pr_load_ce_brands(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_unit_of_measures(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_suppliers(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_product_departments(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_product_subcategories(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_products(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_promotions(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_countries_from_map(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_regions_via_map(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_cities_via_map(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_store_formats(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_stores(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_delivery_addresses(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_fulfillment_centers(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_delivery_types(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_delivery_providers(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_sales_channels(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_payment_methods(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_payment_gateways(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_order_statuses(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_receipt_types(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_card_types(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_device_types(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_terminal_types(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_terminals(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_shifts(p_full_reload, v_run_id);
    CALL bl_cl.pr_load_ce_employees(p_full_reload, v_run_id);

    CALL bl_cl.pr_load_ce_customers_scd(p_full_reload, v_run_id);

    -- =======================
    -- 3NF Facts (transactions)
    -- =======================
    CALL bl_cl.pr_load_ce_transactions(p_full_reload, v_run_id);

    -- =======================
    -- DM (Dimensions)
    -- =======================
   


    CALL bl_cl.pr_load_dim_products_dm_simple(v_run_id);
    CALL bl_cl.pr_load_dim_stores_dm_simple(v_run_id);
    CALL bl_cl.pr_load_dim_terminals_dm_simple(v_run_id);
    CALL bl_cl.pr_load_dim_employees_dm_simple(v_run_id);
    CALL bl_cl.pr_load_dim_promotions_dm_simple(v_run_id);
    CALL bl_cl.pr_load_dim_delivery_providers_dm_simple(v_run_id);
    CALL bl_cl.pr_load_dim_junk_context_dm_simple(v_run_id);
    CALL bl_cl.pr_load_dim_customers_scd_dm_simple(v_run_id);

    -- =======================
    -- DM Fact (partition mgmt + load)
    -- =======================
    
    CALL bl_cl.pr_load_fct_sales_daily_dm(p_months_back, v_run_id);

    -- MASTER SUCCESS
    CALL bl_cl.pr_log_write(
        p_procedure_name := v_proc,
        p_status         := 'SUCCESS',
        p_rows_affected  := 0,
        p_message        := 'ETL master finished successfully',
        p_sqlstate       := NULL,
        p_source_system  := 'bl_cl',
        p_source_entity  := 'ETL_MASTER',
        p_run_id         := v_run_id
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.pr_log_write(
            p_procedure_name := v_proc,
            p_status         := 'ERROR',
            p_rows_affected  := 0,
            p_message        := SQLERRM,
            p_sqlstate       := SQLSTATE,
            p_source_system  := 'bl_cl',
            p_source_entity  := 'ETL_MASTER',
            p_run_id         := v_run_id
        );
        RAISE;
END;
$$;

-- incremental default
CALL bl_cl.prc_run_etl_master();

-- full reload:
CALL bl_cl.prc_run_etl_master(true);

SELECT count(*) FROM bl_3nf.ce_transactions;


SELECT* FROM bl_3nf.ce_transactions
WHERE txn_src_id = 'WO20260216000001';

-- more/less months
CALL bl_cl.prc_run_etl_master(false, 6);
SELECT* FROM bl_cl.mta_etl_log;

SELECT log_id, log_dts, procedure_name, status, rows_affected, message
FROM bl_cl.mta_etl_log
WHERE procedure_name='bl_cl.prc_run_etl_master'
ORDER BY log_id DESC;

