--SA
CALL bl_cl.prc_load_sa_sales_online_src('src_sales_online.csv');
CALL bl_cl.prc_load_sa_sales_pos_src('src_sales_pos.csv');

--3nf
CALL bl_cl.prc_run_etl_master_3nf();

--CALL bl_cl.pr_load_ce_transactions();

--dm
CALL bl_cl.prc_run_etl_master_dm();  

CALL bl_cl.pr_load_fct_sales_daily_dm();