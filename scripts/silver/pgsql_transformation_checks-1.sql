select * from bronze.crm_prd_info
--Check for nulls or duplicates in primary key
--Expectation: No Result
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd IS NULL;

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost<0 OR prd_cost IS NULL;


--Check for unwanted spaces
--Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm!=TRIM(prd_nm)


--Data standardization & consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info


--Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)::date - INTERVAL '1 day')::date AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');