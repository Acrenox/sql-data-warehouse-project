select * from bronze.crm_prd_info;
select * from silver.crm_prd_info;

--Check for nulls or duplicates in primary key
--Expectation: No Result
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

SELECT *
FROM silver.crm_cust_info
WHERE cst_id IS NULL


--Check for unwanted spaces
--Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info 
WHERE prd_nm!=TRIM(prd_nm)

--Check for NULLS or Negative Numbers
--Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost<0 OR prd_cost IS NULL;

--Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

--Check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt<prd_start_dt;



