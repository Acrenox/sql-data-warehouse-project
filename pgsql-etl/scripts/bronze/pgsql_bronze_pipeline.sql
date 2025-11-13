/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    BEGIN
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '------------------------------------------------';

        -- Load crm_cust_info
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
        COPY bronze.crm_cust_info
        FROM '/var/lib/postgresql/data/source_crm/cust_info.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );
        
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> -------------';

        -- Load crm_prd_info
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
        COPY bronze.crm_prd_info
        FROM '/var/lib/postgresql/data/source_crm/prd_info.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );
        
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> -------------';

        -- Load crm_sales_details
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
        COPY bronze.crm_sales_details
        FROM '/var/lib/postgresql/data/source_crm/sales_details.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );
        
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> -------------';

        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '------------------------------------------------';

        -- Load erp_loc_a101
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101
        FROM '/var/lib/postgresql/data/source_erp/loc_a101.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );
        
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> -------------';

        -- Load erp_cust_az12
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12
        FROM '/var/lib/postgresql/data/source_erp/cust_az12.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );
        
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> -------------';

        -- Load erp_px_cat_g1v2
        v_start_time := clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        
        RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2
        FROM '/var/lib/postgresql/data/source_erp/px_cat_g1v2.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );
        
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '>> -------------';

        v_batch_end_time := clock_timestamp();
        v_duration := v_batch_end_time - v_batch_start_time;
        
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'Loading Bronze Layer is Completed';
        RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM v_duration)::INTEGER;
        RAISE NOTICE '==========================================';

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT;
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error Message: %', v_error_msg;
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE '==========================================';
            RAISE EXCEPTION 'Bronze layer load failed: %', v_error_msg;
    END;
END;
$$;