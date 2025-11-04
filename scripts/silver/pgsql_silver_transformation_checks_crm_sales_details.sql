select * from bronze.crm_sales_details;
select sls_prd_key, sls_cust_id from bronze.crm_sales_details;
select prd_key from silver.crm_prd_info;
select cst_id from silver.crm_cust_info; 
select * from silver.crm_sales_details;

--Check for unwanted spaces
--Expectation: No result
SELECT sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_ord_num!=TRIM(sls_ord_num)
OR sls_prd_key !=TRIM(sls_prd_key);

--Check if the columns of crm_sales_details are matching with the other tables so that we can join the tables
--Expectations : No results (That means we can join the two tables without any issues)
SELECT sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key from silver.crm_prd_info)
OR sls_cust_id NOT IN (SELECT cst_id from silver.crm_cust_info);


--Check for 0 or NULL values
SELECT sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt::text <= '0'
OR LENGTH(sls_order_dt::text) != 8
OR sls_order_dt::text > '20500101'
OR sls_order_dt::text < '19000101';


--Error data (like order date should be less than shipping date and due date)
select sls_order_dt, sls_due_dt, sls_ship_dt 
from silver.crm_sales_details
WHERE sls_order_dt>sls_ship_dt
OR sls_order_dt>sls_due_dt;

--Business rules like 'sales=quantity*price' and 'negative, null, zeros' are not allowed
select DISTINCT sls_sales, sls_quantity, sls_price
from silver.crm_sales_details
WHERE sls_sales != (sls_quantity*sls_price);


select DISTINCT sls_sales, sls_quantity, sls_price
from bronze.crm_sales_details
WHERE sls_sales IS NULL
OR sls_sales<=0;

select DISTINCT sls_sales, sls_quantity, sls_price
from silver.crm_sales_details
WHERE sls_quantity IS NULL
OR sls_quantity<=0;

select DISTINCT sls_sales, sls_quantity, sls_price
from silver.crm_sales_details
WHERE sls_price IS NULL
OR sls_price<=0;
