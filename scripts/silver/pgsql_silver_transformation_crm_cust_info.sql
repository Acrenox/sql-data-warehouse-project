TRUNCATE TABLE silver.crm_cust_info;
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
FROM bronze.crm_cust_info
)AS T WHERE flag_last=1; --Transformation: We are reming data duplicates using this SELECT statement