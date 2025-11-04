select * from bronze.erp_cust_az12;
select * from silver.erp_cust_az12;
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


--Checks
select distinct * 
from silver.erp_cust_az12 
where cid is NULL
or bdate IS NULL
or gen is NULL;

--passed
SELECT cid
FROM silver.erp_cust_az12 
WHERE cid!=TRIM(cid);

--not passed
SELECT gen
FROM silver.erp_cust_az12 
WHERE gen!=TRIM(gen);

select distinct gen
from silver.erp_cust_az12;

select distinct bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' OR bdate>CURRENT_DATE;
