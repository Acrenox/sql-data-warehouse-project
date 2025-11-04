select * from bronze.erp_cust_az12;

------------------------------------------------------------------------------------------------
--Updated silver.erp_cust_az12 with the transformations (Not to be run without the Pipeline):---
------------------------------------------------------------------------------------------------
INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END cid,
CASE WHEN bdate > CURRENT_DATE THEN NULL
	ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	ELSE 'n/a'
END gen
from bronze.erp_cust_az12;

------------------------------------------------------------------------------------------------
--CHECKS STARTS HERE--
------------------------------------------------------------------------------------------------

-- Check for NULL or duplicate primary keys (cid)
SELECT cid, COUNT(*) AS duplicate_count
FROM silver.erp_cust_az12
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) > 1;

-- Check for leading/trailing spaces in string fields
SELECT *
FROM silver.erp_cust_az12
WHERE cid LIKE ' %' OR cid LIKE '% '
   OR gen LIKE ' %' OR gen LIKE '% ';

-- Check for inconsistent gender values
SELECT DISTINCT gen
FROM silver.erp_cust_az12
WHERE UPPER(TRIM(gen)) NOT IN ('MALE', 'FEMALE', 'N/A');

-- Check for invalid or unrealistic birth dates
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE;

-- Check for cid values in customer table not present in location table
SELECT a.cid
FROM silver.erp_cust_az12 a
LEFT JOIN silver.erp_loc_a101 b ON a.cid = b.cid
WHERE b.cid IS NULL;

