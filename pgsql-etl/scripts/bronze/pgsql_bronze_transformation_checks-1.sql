-- Check record count in all bronze tables
SELECT 
    'bronze.crm_cust_info' AS table_name, COUNT(*) AS record_count FROM bronze.crm_cust_info
UNION ALL
SELECT 'bronze.crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
UNION ALL
SELECT 'bronze.crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
UNION ALL
SELECT 'bronze.erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
UNION ALL
SELECT 'bronze.erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12
UNION ALL
SELECT 'bronze.erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;
--✅ Purpose: Ensures data successfully loaded and not truncated due to path or format errors.


-- crm_cust_info
SELECT cst_id, COUNT(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1;

-- crm_prd_info
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING prd_id IS NULL OR COUNT(*) > 1;

-- crm_sales_details
SELECT sls_ord_num, COUNT(*)
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING sls_ord_num IS NULL OR COUNT(*) > 1;

-- erp_cust_az12
SELECT cid, COUNT(*)
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) > 1;
--✅ Purpose: Detects missing or duplicate identifiers.

-- Check across major text columns
SELECT *
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key)
   OR cst_firstname != TRIM(cst_firstname)
   OR cst_lastname != TRIM(cst_lastname);

SELECT *
FROM bronze.erp_loc_a101
WHERE cid != TRIM(cid)
   OR cntry != TRIM(cntry);
--✅ Purpose: Prevents string mismatches in joins and transformations.

-- Check for unrealistic dates
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt < '1900-01-01'
   OR sls_order_dt > CURRENT_DATE
   OR sls_order_dt IS NULL;

SELECT *
FROM bronze.erp_cust_az12
WHERE bdate < '1900-01-01'
   OR bdate > CURRENT_DATE
   OR bdate IS NULL;
--✅ Purpose: Detects corrupted or future date values.

-- CRM product line validation
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info
WHERE UPPER(TRIM(prd_line)) NOT IN ('M', 'R', 'S', 'T', 'N/A', '');

-- CRM gender validation
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
WHERE UPPER(TRIM(cst_gndr)) NOT IN ('M', 'F', '', 'N/A');
--✅ Purpose: Identifies inconsistent text or coding standards.

-- Compare expected vs loaded counts
-- Replace <expected_count> with known row numbers
SELECT 'bronze.crm_cust_info' AS table_name, COUNT(*) AS loaded_count, <expected_count> AS expected_count
FROM bronze.crm_cust_info
HAVING COUNT(*) != <expected_count>;
--✅ Purpose: Ensures no partial file load or missing data.

-- Product keys in sales must exist in product info
SELECT DISTINCT s.sls_prd_key
FROM bronze.crm_sales_details s
LEFT JOIN bronze.crm_prd_info p ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL;
--✅ Purpose: Detects broken references due to missing master data.

-- Detect exact duplicate rows
SELECT *, COUNT(*) 
FROM bronze.crm_sales_details
GROUP BY sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
HAVING COUNT(*) > 1;
--✅ Purpose: Identifies duplicate data ingestion.

-- Check existence of all source files (run in shell or metadata table)
SELECT '/var/lib/postgresql/data/source_crm/cust_info.csv' AS path, 
       pg_stat_file('/var/lib/postgresql/data/source_crm/cust_info.csv', true) IS NOT NULL AS exists;
--✅ Purpose: Confirms that expected CSV files exist before next batch run.

INSERT INTO audit.bronze_load_audit(batch_start_time, batch_end_time, total_duration_sec, status)
VALUES (v_batch_start_time, v_batch_end_time, EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time))::INT, 'SUCCESS');
--✅ Purpose: Creates audit trail for each ingestion cycle.
