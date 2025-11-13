----------------------------------------------------------------------------------------------------
--UPDATED silver.crm_prd_info WITH THE TRANSFORMATIONS (Not to be run without the Pipeline):---
----------------------------------------------------------------------------------------------------
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
------------------------------------------------------------------------------------------------
--CHECKS STARTS HERE--
------------------------------------------------------------------------------------------------
-- Check for NULL or duplicate product IDs
SELECT prd_id, COUNT(*) 
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING prd_id IS NULL OR COUNT(*) > 1;


-- Check for leading/trailing spaces
SELECT prd_id, prd_key, prd_nm, prd_line
FROM bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key)
   OR prd_nm != TRIM(prd_nm)
   OR prd_line != TRIM(prd_line);


-- Check for invalid or future product start dates
SELECT prd_id, prd_start_dt, prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_start_dt IS NULL
   OR prd_start_dt > CURRENT_DATE
   OR (prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt);


-- Check for prd_key pattern mismatch
SELECT prd_id, prd_key
FROM bronze.crm_prd_info
WHERE prd_key NOT LIKE 'AC-%';


-- Verify cat_id extraction (should match first 5 chars of prd_key, '-' replaced with '_')
SELECT prd_id, prd_key, REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS derived_cat_id, cat_id
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') != cat_id;


-- Check for negative or NULL product cost
SELECT prd_id, prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;


-- Detect overlapping effective date ranges for the same product
SELECT prd_key, prd_start_dt, prd_end_dt
FROM silver.crm_prd_info s1
WHERE EXISTS (
    SELECT 1
    FROM silver.crm_prd_info s2
    WHERE s1.prd_key = s2.prd_key
      AND s1.prd_start_dt < s2.prd_end_dt
      AND s2.prd_start_dt < s1.prd_end_dt
      AND s1.ctid <> s2.ctid
);

