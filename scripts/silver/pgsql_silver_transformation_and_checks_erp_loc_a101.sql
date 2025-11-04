select * from bronze.erp_loc_a101;
select * from silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
REPLACE(TRIM(cid),'-','') AS cid,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
from bronze.erp_loc_a101;

--Data standardization & consistency
select distinct CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry from bronze.erp_loc_a101 order by cntry;