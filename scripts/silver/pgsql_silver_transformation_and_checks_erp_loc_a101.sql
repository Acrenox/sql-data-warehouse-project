------------------------------------------------------------------------------------------------
--Updated silver.erp_cust_az12 with the transformations (Not to be run without the Pipeline):---
------------------------------------------------------------------------------------------------
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
REPLACE(TRIM(cid),'-','') AS cid,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
from bronze.erp_loc_a101;

------------------------------------------------------------------------------------------------
--CHECKS STARTS HERE--
------------------------------------------------------------------------------------------------

-- Bronze: Check for NULL or duplicate CIDs
SELECT cid, COUNT(*) AS duplicate_count
FROM bronze.erp_loc_a101
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) > 1;

-- Silver: Check for NULL or duplicate CIDs
SELECT cid, COUNT(*) AS duplicate_count
FROM silver.erp_loc_a101
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) > 1;


-- Bronze: Detect unwanted spaces in text fields
SELECT *
FROM bronze.erp_loc_a101
WHERE cid LIKE ' %' OR cid LIKE '% '
   OR cntry LIKE ' %' OR cntry LIKE '% ';

-- Silver: Detect unwanted spaces (should ideally be clean after transformation)
SELECT *
FROM silver.erp_loc_a101
WHERE cid LIKE ' %' OR cid LIKE '% '
   OR cntry LIKE ' %' OR cntry LIKE '% ';


-- Bronze: Find unstandardized or unexpected country codes
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
WHERE UPPER(TRIM(cntry)) NOT IN ('DE', 'US', 'USA', 'GERMANY', 'UNITED STATES', '', 'N/A');

-- Silver: Find inconsistent country names (should only be clean standardized values)
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
WHERE cntry NOT IN ('Germany', 'United States', 'n/a')
  AND cntry IS NOT NULL;


-- Bronze: Find null or empty country entries
SELECT *
FROM bronze.erp_loc_a101
WHERE cntry IS NULL OR TRIM(cntry) = '';

-- Silver: Ensure no missing or placeholder country info
SELECT *
FROM silver.erp_loc_a101
WHERE cntry IS NULL OR cntry = 'n/a';

-- Silver: Check if CID in erp_loc_a101 exists in erp_cust_az12
SELECT a.cid
FROM silver.erp_loc_a101 a
LEFT JOIN silver.erp_cust_az12 b ON a.cid = b.cid
WHERE b.cid IS NULL;



