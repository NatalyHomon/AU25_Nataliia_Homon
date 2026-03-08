-- =========================================================
-- 1) ONLINE -> sa_sales_online.src_sales_online (UPSERT + LOG)
-- =========================================================
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_sa_sales_online_src(
    IN p_source_file text DEFAULT 'src_sales_online.csv',
    IN p_run_id      uuid DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows bigint := 0;
    v_run  uuid   := COALESCE(p_run_id, gen_random_uuid());
BEGIN
    CALL bl_cl.pr_log_write(
        p_procedure_name := 'bl_cl.prc_load_sa_sales_online_src',
        p_status         := 'START',
        p_rows_affected  := 0,
        p_message        := 'Loading ONLINE source to sa_sales_online.src_sales_online',
        p_sqlstate       := NULL,
        p_source_system  := 'sales_online',
        p_source_entity  := 'sa_sales_online.src_sales_online',
        p_run_id         := v_run
    );

    INSERT INTO sa_sales_online.src_sales_online (
        web_order_id,
        txn_ts,
        order_status,
        customer_src_id,
        customer_first_name,
        customer_last_name,
        customer_email,
        customer_phone,
        gender,
        customer_age,
        customer_age_group,
        customer_segment,
        country,
        region,
        city,
        delivery_postal_code,
        delivery_address_line1,
        fulfillment_center_id,
        fulfillment_city,
        delivery_type,
        carrier_name,
        tracking_id,
        promised_delivery_dt,
        device_type,
        payment_gateway,
        promo_code,
        discount_pct,
        discount_amt,
        shipping_fee_amt,
        product_dept,
        product_subcategory,
        product_sku,
        product_name,
        brand,
        unit_of_measure,
        supplier_id,
        unit_price_amt,
        qty,
        tax_amt,
        sales_amt,
        cost_amt,
        gross_profit_amt,
        customer_rating,
        source_file
    )
    SELECT
        web_order_id,
        txn_ts,
        order_status,
        customer_src_id,
        customer_first_name,
        customer_last_name,
        customer_email,
        customer_phone,
        gender,
        customer_age,
        customer_age_group,
        customer_segment,
        country,
        region,
        city,
        delivery_postal_code,
        delivery_address_line1,
        fulfillment_center_id,
        fulfillment_city,
        delivery_type,
        carrier_name,
        tracking_id,
        promised_delivery_dt,
        device_type,
        payment_gateway,
        promo_code,
        discount_pct,
        discount_amt,
        shipping_fee_amt,
        product_dept,
        product_subcategory,
        product_sku,
        product_name,
        brand,
        unit_of_measure,
        supplier_id,
        unit_price_amt,
        qty,
        tax_amt,
        sales_amt,
        cost_amt,
        gross_profit_amt,
        customer_rating,
        p_source_file::text
    FROM sa_sales_online.ext_sales_online
    ON CONFLICT (web_order_id, product_sku)
    DO UPDATE
    SET
        txn_ts               = EXCLUDED.txn_ts,
        order_status         = EXCLUDED.order_status,
        customer_src_id      = EXCLUDED.customer_src_id,
        customer_first_name  = EXCLUDED.customer_first_name,
        customer_last_name   = EXCLUDED.customer_last_name,
        customer_email       = EXCLUDED.customer_email,
        customer_phone       = EXCLUDED.customer_phone,
        gender               = EXCLUDED.gender,
        customer_age         = EXCLUDED.customer_age,
        customer_age_group   = EXCLUDED.customer_age_group,
        customer_segment     = EXCLUDED.customer_segment,
        country              = EXCLUDED.country,
        region               = EXCLUDED.region,
        city                 = EXCLUDED.city,
        delivery_postal_code = EXCLUDED.delivery_postal_code,
        delivery_address_line1  = EXCLUDED.delivery_address_line1,
        fulfillment_center_id   = EXCLUDED.fulfillment_center_id,
        fulfillment_city        = EXCLUDED.fulfillment_city,
        delivery_type        = EXCLUDED.delivery_type,
        carrier_name         = EXCLUDED.carrier_name,
        tracking_id          = EXCLUDED.tracking_id,
        promised_delivery_dt = EXCLUDED.promised_delivery_dt,
        device_type          = EXCLUDED.device_type,
        payment_gateway      = EXCLUDED.payment_gateway,
        promo_code           = EXCLUDED.promo_code,
        discount_pct         = EXCLUDED.discount_pct,
        discount_amt         = EXCLUDED.discount_amt,
        shipping_fee_amt     = EXCLUDED.shipping_fee_amt,
        product_dept         = EXCLUDED.product_dept,
        product_subcategory  = EXCLUDED.product_subcategory,
        product_name         = EXCLUDED.product_name,
        brand                = EXCLUDED.brand,
        unit_of_measure      = EXCLUDED.unit_of_measure,
        supplier_id          = EXCLUDED.supplier_id,
        unit_price_amt       = EXCLUDED.unit_price_amt,
        qty                  = EXCLUDED.qty,
        tax_amt              = EXCLUDED.tax_amt,
        sales_amt            = EXCLUDED.sales_amt,
        cost_amt             = EXCLUDED.cost_amt,
        gross_profit_amt     = EXCLUDED.gross_profit_amt,
        customer_rating      = EXCLUDED.customer_rating,
        load_dts             = now(),
        source_file          = EXCLUDED.source_file;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_cl.pr_log_write(
        p_procedure_name := 'bl_cl.prc_load_sa_sales_online_src',
        p_status         := 'SUCCESS',
        p_rows_affected  := v_rows,
        p_message        := 'ONLINE load finished successfully',
        p_sqlstate       := NULL,
        p_source_system  := 'sales_online',
        p_source_entity  := 'sa_sales_online.src_sales_online',
        p_run_id         := v_run
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.pr_log_write(
            p_procedure_name := 'bl_cl.prc_load_sa_sales_online_src',
            p_status         := 'ERROR',
            p_rows_affected  := COALESCE(v_rows,0),
            p_message        := SQLERRM,
            p_sqlstate       := SQLSTATE,
            p_source_system  := 'sales_online',
            p_source_entity  := 'sa_sales_online.src_sales_online',
            p_run_id         := v_run
        );
        RAISE;
END;
$$;


-- =====================================================
-- 2) POS -> sa_sales_pos.src_sales_pos (UPSERT + LOG)
-- =====================================================
CREATE OR REPLACE PROCEDURE bl_cl.prc_load_sa_sales_pos_src(
    IN p_source_file text DEFAULT 'src_sales_pos.csv',
    IN p_run_id      uuid DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows bigint := 0;
    v_run  uuid   := COALESCE(p_run_id, gen_random_uuid());
BEGIN
    CALL bl_cl.pr_log_write(
        p_procedure_name := 'bl_cl.prc_load_sa_sales_pos_src',
        p_status         := 'START',
        p_rows_affected  := 0,
        p_message        := 'Loading POS source to sa_sales_pos.src_sales_pos',
        p_sqlstate       := NULL,
        p_source_system  := 'sales_pos',
        p_source_entity  := 'sa_sales_pos.src_sales_pos',
        p_run_id         := v_run
    );

    INSERT INTO sa_sales_pos.src_sales_pos (
        ckout,
        txn_ts,
        customer_src_id,
        customer_phone,
        customer_age_group,
        customer_segment,
        product_dept,
        product_subcategory,
        product_sku,
        product_name,
        brand,
        unit_of_measure,
        supplier_id,
        store_id,
        store_format,
        store_open_dt,
        store_open_time,
        store_close_time,
        country,
        region,
        city,
        terminal_id,
        terminal_type,
        cashier_id,
        cashier_first_name,
        cashier_last_name,
        cashier_dept,
        cashier_position,
        cashier_hire_dt,
        shift_id,
        payment_method,
        card_type,
        receipt_type,
        promo_code,
        promo_type,
        discount_pct,
        discount_amt,
        loyalty_points_earned,
        unit_price_amt,
        qty,
        tax_amt,
        sales_amt,
        cost_amt,
        gross_profit_amt,
        customer_rating,
        source_file
    )
    SELECT
        ckout,
        txn_ts,
        customer_src_id,
        customer_phone,
        customer_age_group,
        customer_segment,
        product_dept,
        product_subcategory,
        product_sku,
        product_name,
        brand,
        unit_of_measure,
        supplier_id,
        store_id,
        store_format,
        store_open_dt,
        store_open_time,
        store_close_time,
        country,
        region,
        city,
        terminal_id,
        terminal_type,
        cashier_id,
        cashier_first_name,
        cashier_last_name,
        cashier_dept,
        cashier_position,
        cashier_hire_dt,
        shift_id,
        payment_method,
        card_type,
        receipt_type,
        promo_code,
        promo_type,
        discount_pct,
        discount_amt,
        loyalty_points_earned,
        unit_price_amt,
        qty,
        tax_amt,
        sales_amt,
        cost_amt,
        gross_profit_amt,
        customer_rating,
        p_source_file::text
    FROM sa_sales_pos.ext_sales_pos
    ON CONFLICT (ckout, product_sku)
    DO UPDATE
    SET
        txn_ts                = EXCLUDED.txn_ts,
        customer_src_id       = EXCLUDED.customer_src_id,
        customer_phone        = EXCLUDED.customer_phone,
        customer_age_group    = EXCLUDED.customer_age_group,
        customer_segment      = EXCLUDED.customer_segment,
        product_dept          = EXCLUDED.product_dept,
        product_subcategory   = EXCLUDED.product_subcategory,
        product_name          = EXCLUDED.product_name,
        brand                 = EXCLUDED.brand,
        unit_of_measure       = EXCLUDED.unit_of_measure,
        supplier_id           = EXCLUDED.supplier_id,
        store_id              = EXCLUDED.store_id,
        store_format          = EXCLUDED.store_format,
        store_open_dt         = EXCLUDED.store_open_dt,
        store_open_time       = EXCLUDED.store_open_time,
        store_close_time      = EXCLUDED.store_close_time,
        country               = EXCLUDED.country,
        region                = EXCLUDED.region,
        city                  = EXCLUDED.city,
        terminal_id           = EXCLUDED.terminal_id,
        terminal_type         = EXCLUDED.terminal_type,
        cashier_id            = EXCLUDED.cashier_id,
        cashier_first_name    = EXCLUDED.cashier_first_name,
        cashier_last_name     = EXCLUDED.cashier_last_name,
        cashier_dept          = EXCLUDED.cashier_dept,
        cashier_position      = EXCLUDED.cashier_position,
        cashier_hire_dt       = EXCLUDED.cashier_hire_dt,
        shift_id              = EXCLUDED.shift_id,
        payment_method        = EXCLUDED.payment_method,
        card_type             = EXCLUDED.card_type,
        receipt_type          = EXCLUDED.receipt_type,
        promo_code            = EXCLUDED.promo_code,
        promo_type            = EXCLUDED.promo_type,
        discount_pct          = EXCLUDED.discount_pct,
        discount_amt          = EXCLUDED.discount_amt,
        loyalty_points_earned = EXCLUDED.loyalty_points_earned,
        unit_price_amt        = EXCLUDED.unit_price_amt,
        qty                   = EXCLUDED.qty,
        tax_amt               = EXCLUDED.tax_amt,
        sales_amt             = EXCLUDED.sales_amt,
        cost_amt              = EXCLUDED.cost_amt,
        gross_profit_amt      = EXCLUDED.gross_profit_amt,
        customer_rating       = EXCLUDED.customer_rating,
        load_dts              = now(),
        source_file           = EXCLUDED.source_file;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    CALL bl_cl.pr_log_write(
        p_procedure_name := 'bl_cl.prc_load_sa_sales_pos_src',
        p_status         := 'SUCCESS',
        p_rows_affected  := v_rows,
        p_message        := 'POS load finished successfully',
        p_sqlstate       := NULL,
        p_source_system  := 'sales_pos',
        p_source_entity  := 'sa_sales_pos.src_sales_pos',
        p_run_id         := v_run
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL bl_cl.pr_log_write(
            p_procedure_name := 'bl_cl.prc_load_sa_sales_pos_src',
            p_status         := 'ERROR',
            p_rows_affected  := COALESCE(v_rows,0),
            p_message        := SQLERRM,
            p_sqlstate       := SQLSTATE,
            p_source_system  := 'sales_pos',
            p_source_entity  := 'sa_sales_pos.src_sales_pos',
            p_run_id         := v_run
        );
        RAISE;
END;
$$;

CALL bl_cl.prc_load_sa_sales_online_src('src_sales_online.csv');
CALL bl_cl.prc_load_sa_sales_pos_src('src_sales_pos.csv');


SELECT * FROM sa_sales_online.src_sales_online;

SELECT * FROM sa_sales_online.src_sales_online cus
WHERE cus.customer_src_id IN(
SELECT cus.customer_src_id
FROM sa_sales_online.src_sales_online cus
GROUP BY cus.customer_src_id
HAVING count(cus.customer_src_id) >=3)
ORDER BY cus.customer_src_id;


FROM sa_sales_online.src_sales_online
SELECT ckout, product_sku, COUNT(*)
FROM sa_sales_pos.src_sales_pos
GROUP BY ckout, product_sku
HAVING COUNT(*) > 1;

SELECT web_order_id, product_sku, COUNT(*)
FROM sa_sales_online.src_sales_online
GROUP BY web_order_id, product_sku
HAVING COUNT(*) > 1;


