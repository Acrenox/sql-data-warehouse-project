
------------------------------------------------------------------------------------------------
--UPDATED silver.crm_sales_details WITH THE TRANSFORMATIONS (Not to be run without the Pipeline):---
------------------------------------------------------------------------------------------------
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

------------------------------------------------------------------------------------------------
--CHECKS STARTS HERE--
------------------------------------------------------------------------------------------------
-- NULL or duplicate primary key check
SELECT sls_ord_num, COUNT(*) AS duplicate_count
FROM silver.crm_sales_details
GROUP BY sls_ord_num
HAVING sls_ord_num IS NULL OR COUNT(*) > 1;
--🟢 Purpose: Detects missing or duplicate order numbers.

-- Unwanted spaces in order and product keys
SELECT *
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)
   OR sls_prd_key != TRIM(sls_prd_key)
   OR sls_cust_id::text != TRIM(sls_cust_id::text);
--🟢 Purpose: Ensures there are no leading or trailing spaces that can cause join mismatches (e.g., with product or customer tables).


-- Check for invalid order or product key formats
SELECT *
FROM silver.crm_sales_details
WHERE sls_ord_num ~ '[^A-Za-z0-9-]'
   OR sls_prd_key ~ '[^A-Za-z0-9-]';

-- Check for negative or zero sales/quantity/price
SELECT *
FROM silver.crm_sales_details
WHERE COALESCE(sls_sales, 0) <= 0
   OR COALESCE(sls_quantity, 0) <= 0
   OR COALESCE(sls_price, 0) <= 0;
--🟢 Purpose: Ensures standard formats and consistent numerical data.


-- Check for invalid or inconsistent date ranges
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_order_dt > CURRENT_DATE
   OR (sls_ship_dt IS NOT NULL AND sls_ship_dt < sls_order_dt)
   OR (sls_due_dt IS NOT NULL AND sls_due_dt < sls_order_dt)
   OR (sls_ship_dt IS NOT NULL AND sls_due_dt IS NOT NULL AND sls_ship_dt > sls_due_dt);
--🟢 Purpose: Ensures order, ship, and due dates are valid and in correct sequence.

-- Check if sales mismatch with quantity * price
SELECT *
FROM silver.crm_sales_details
WHERE ROUND(sls_sales, 2) <> ROUND(sls_quantity * sls_price, 2);

-- Check for missing product references
SELECT sls_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT DISTINCT prd_key FROM silver.crm_prd_info
);

-- Check for missing customer references
SELECT sls_cust_id
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT DISTINCT cst_id FROM silver.crm_cust_info
);
--🟢 Purpose: Ensures referential and logical consistency across the data model.

