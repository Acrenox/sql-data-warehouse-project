------------------------------------------------------------------------------------------------
--UPDATED silver.crm_cust_info WITH THE TRANSFORMATIONS (Not to be run without the Pipeline):---
------------------------------------------------------------------------------------------------
INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)

SELECT 
COALESCE(cst_id, 0), 
cst_key,
COALESCE(TRIM(cst_firstname), 'n/a') AS cst_firstname, --Transformation: By using TRIM() we are removing the unwanted spaces 
COALESCE(TRIM(cst_lastname), 'n/a') AS cst_lastname,
CASE WHEN cst_marital_status IS NULL THEN 'n/a' 
	WHEN TRIM(UPPER(cst_marital_status))='M' THEN 'Married'
	WHEN TRIM(UPPER(cst_marital_status))='S' THEN 'Single'
	ELSE 'n/a'
END cst_marital_status, --Transformation: By using CASE statement, we are normalizing/standardizing the data 
CASE WHEN cst_gndr IS NULL THEN 'n/a' -- Transformation: By using WHEN statement we are handling missing/NULL data
	WHEN TRIM(UPPER(cst_gndr))='M' THEN 'Male'
	WHEN TRIM(UPPER(cst_gndr))='F' THEN 'Female'
	ELSE 'n/a' 
END cst_gndr,
COALESCE(cst_create_date, '1900-01-01'::date) AS cst_create_date
FROM(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info;
------------------------------------------------------------------------------------------------
--CHECKS STARTS HERE--
------------------------------------------------------------------------------------------------
-- Check for NULL or duplicate customer IDs
SELECT cst_id, COUNT(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1;


-- Check for leading/trailing spaces in key text columns
SELECT cst_id, cst_key, cst_firstname, cst_lastname
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key)
   OR cst_firstname != TRIM(cst_firstname)
   OR cst_lastname != TRIM(cst_lastname);


-- Check for unexpected marital status values (before standardization)
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info
WHERE UPPER(TRIM(cst_marital_status)) NOT IN ('M', 'S', 'N/A', '', NULL);

-- Check for unexpected gender values (before standardization)
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
WHERE UPPER(TRIM(cst_gndr)) NOT IN ('M', 'F', 'N/A', '', NULL);


-- Check for invalid or future customer creation dates
SELECT cst_id, cst_create_date
FROM bronze.crm_cust_info
WHERE cst_create_date IS NULL
   OR cst_create_date > CURRENT_DATE
   OR cst_create_date < '1900-01-01';


-- Check for mismatched first and last names (e.g., blank or identical)
SELECT cst_id, cst_firstname, cst_lastname
FROM bronze.crm_cust_info
WHERE TRIM(cst_firstname) = '' 
   OR TRIM(cst_lastname) = ''
   OR TRIM(LOWER(cst_firstname)) = TRIM(LOWER(cst_lastname));


-- Ensure duplicates are correctly identified for removal
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;


--POST-TRANSFORMATION VALIDATIONS--
-- Confirm marital status standardized
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Married', 'Single', 'n/a');

-- Confirm gender standardized
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr NOT IN ('Male', 'Female', 'n/a');

-- Confirm placeholder default date applied correctly
SELECT COUNT(*) AS invalid_dates
FROM silver.crm_cust_info
WHERE cst_create_date < '1900-01-01' OR cst_create_date > CURRENT_DATE;


-- Check for records missing both name and gender (potential bad data)
SELECT cst_id, cst_firstname, cst_lastname, cst_gndr
FROM silver.crm_cust_info
WHERE (cst_firstname = 'n/a' AND cst_lastname = 'n/a')
  AND cst_gndr = 'n/a';	
)AS T WHERE flag_last=1; --Transformation: We are reming data duplicates using this SELECT state
