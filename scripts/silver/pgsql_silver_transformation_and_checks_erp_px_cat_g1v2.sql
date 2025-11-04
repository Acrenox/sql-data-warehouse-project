select * from bronze.erp_px_cat_g1v2;
select * from silver.erp_px_cat_g1v2;
select * from bronze.erp_cust_az12;
select * from bronze.erp_loc_a101;
select * from silver.crm_prd_info;

INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT
id,
cat, 
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;


select id
from silver.erp_px_cat_g1v2
where id NOT IN (select cat_id from silver.crm_prd_info)

--Check for unwanted spaces
select cat
from silver.erp_px_cat_g1v2
where cat!=TRIM(cat)
or subcat!=TRIM(subcat)
or maintenance!=TRIM(maintenance);

--Data Standardization & Consistency
select distinct cat from silver.erp_px_cat_g1v2;
select distinct subcat from silver.erp_px_cat_g1v2;
select distinct maintenance from silver.erp_px_cat_g1v2;