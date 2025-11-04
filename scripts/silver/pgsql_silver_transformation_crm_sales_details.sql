select * from bronze.crm_sales_details;
select * from silver.crm_prd_info;

Truncate table silver.crm_sales_details;
INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt<=0 OR LENGTH(sls_order_dt::text)!=8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END sls_order_dt,
CASE WHEN sls_ship_dt<=0 OR LENGTH(sls_ship_dt::text)!=8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END sls_ship_dt,
CASE WHEN sls_due_dt<=0 OR LENGTH(sls_due_dt::text)!=8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END sls_due_dt,
--Rules
/* 	1. If sls_sales is negative, 0, Null, then derive it using Quantity and Price.
	2. If sls_price is zero or null, calculate it using sls_sales and sls_quantity.
	3. If price is negative, convert it to a positive value. */
--sls_sales
CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price) 
		THEN sls_quantity*sls_price
	ELSE sls_sales
END sls_sales,
--sls_quantity
sls_quantity,
---sls_price
CASE WHEN sls_price IS NULL OR sls_price<=0
		THEN sls_sales/NULLIF(sls_quantity, 0)
	ELSE ABS(sls_price)
END sls_price
FROM bronze.crm_sales_details;