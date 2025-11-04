CALL silver.load_silver();


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_batch_start_time TIMESTAMP;
    v_batch_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_error_msg TEXT;
BEGIN
    v_batch_start_time := clock_timestamp();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    BEGIN
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '------------------------------------------------';

        -- Load crm_cust_info
        v_start_time := clock_timestamp();
    	RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    	TRUNCATE TABLE silver.crm_cust_info;
    	RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
    	INSERT INTO silver.crm_cust_info(
        	cst_id,
        	cst_key,
        	cst_firstname,
        	cst_lastname,
        	cst_marital_status,
        	cst_gndr,
        	cst_create_date
    	)
    	SELECT
        	COALESCE(cst_id, 0),
        	cst_key,
        	COALESCE(TRIM(cst_firstname), 'n/a') AS cst_firstname,
        	COALESCE(TRIM(cst_lastname), 'n/a') AS cst_lastname,
        	CASE
            	WHEN cst_marital_status IS NULL THEN 'n/a'
            	WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
            	WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
            	ELSE 'n/a'
        	END AS cst_marital_status,
        	CASE
            	WHEN cst_gndr IS NULL THEN 'n/a'
            	WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
           		WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
            	ELSE 'n/a'
        	END AS cst_gndr,
        	COALESCE(cst_create_date, DATE '1900-01-01') AS cst_create_date
    	FROM (
        	SELECT *,
               	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        	FROM bronze.crm_cust_info
    	) AS t
    	WHERE flag_last = 1;

    	v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> -------------';
------------------------------------------------------------------------------------------------------------------------------
--Loading silver.crm_prd_info
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTR(prd_key, 7) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        DATE(prd_start_dt) AS prd_start_dt,
        (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)::date - INTERVAL '1 day')::date
            AS prd_end_dt
    FROM bronze.crm_prd_info;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
    RAISE NOTICE '>> -------------';

-----------------------------------------------------------------------------------------------------------------------------
-- Loading silver.crm_sales_details
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt IS NULL THEN NULL
            WHEN sls_order_dt::numeric <= 0 OR char_length(sls_order_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt IS NULL THEN NULL
            WHEN sls_ship_dt::numeric <= 0 OR char_length(sls_ship_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt IS NULL THEN NULL
            WHEN sls_due_dt::numeric <= 0 OR char_length(sls_due_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
        END AS sls_due_dt,
        -- sls_sales
        CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * sls_price
            ELSE sls_sales
        END AS sls_sales,
        -- sls_quantity
        sls_quantity,
        -- sls_price
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN (sls_sales / NULLIF(sls_quantity, 0))
            ELSE ABS(sls_price)
        END AS sls_price
    FROM bronze.crm_sales_details;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
    RAISE NOTICE '>> -------------';
-----------------------------------------------------------------------------------------------------------------------------
    RAISE NOTICE '---------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '---------------------------------------------';

    -- Loading silver.erp_cust_az12
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12(
        cid,
        bdate,
        gen
    )
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4) ELSE cid END AS cid,
        CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
    RAISE NOTICE '>> -------------';
------------------------------------------------------------------------------------------------------------------------------
-- Loading silver.erp_loc_a101
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101(cid, cntry)
    SELECT
        REPLACE(TRIM(cid), '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
    RAISE NOTICE '>> -------------';
    ----------------------------------------------------------------
    -- Loading silver.erp_px_cat_g1v2
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
    RAISE NOTICE '>> -------------';

    v_batch_end_time := clock_timestamp();
    v_duration := v_batch_end_time - v_batch_start_time;
        
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver  Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
    RAISE NOTICE '==========================================';

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
            RAISE NOTICE 'Error Message: %', v_error_msg;
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE '==========================================';
            RAISE EXCEPTION 'Silver layer load failed: %', v_error_msg;
END;
END;
$$;