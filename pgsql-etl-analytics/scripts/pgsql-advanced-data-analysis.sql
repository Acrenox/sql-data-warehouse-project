--Advance Data Analytics
/* 
--Complex Queries
--Window Functions
--CTE
--Subqueries
--Generate Reports
*/

--Change over time
select * from gold.fact_sales;

select 
	extract(year from order_date) as order_year,
	sum(sales_amount) as total_sales,
	count(distinct customer_key) as total_customers,
	sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by extract(year from order_date)
order by order_year;

select
	extract(year from order_date) as order_year,
	extract(month from order_date) as order_month,
	sum(sales_amount) as total_sales,
	count(distinct customer_key) as total_customers,
	sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by extract(month from order_date), extract(year from order_date)
order by order_month, order_year;

--CUMULATIVE ANALYSIS
--Calculate the total sales per month
--Calculate the running total of sales over time
select * from gold.fact_sales;
select * from gold.dim_products;
select * from gold.dim_customers;

select 
	order_date,
	total_sales,
	sum(total_sales) over (partition by order_date order by order_date) as running_total_sales
from
(
select 
	DATE_TRUNC('month', order_date) as order_date,
	sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by DATE_TRUNC('month', order_date)
order by order_date
)t;

select 
	order_date,
	total_sales,
	--window function
	sum(total_sales) over (partition by order_date order by order_date) as running_total_sales
from(
select 
	DATE_TRUNC('month', order_date) as order_date,
	sum(sales_amount) as total_sales 
from gold.fact_sales
where order_date is not null
group by DATE_TRUNC('month', order_date)
order by order_date
)t;

--Cumulative sales per year
select 
	order_date,
	total_sales,
	--window function
	sum(total_sales) over (order by order_date) as running_total_sales
from(
select 
	DATE_TRUNC('year', order_date) as order_date,
	sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by DATE_TRUNC('year', order_date)
order by order_date
)t;

--Adding average sales
select 
	order_date,
	total_sales,
	--window function
	sum(total_sales) over (order by order_date) as running_total_sales,
	round(avg(avg_price) over (order by order_date), 2) as running_avg_price
from(
select 
	DATE_TRUNC('year', order_date) as order_date,
	sum(sales_amount) as total_sales,
	avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by DATE_TRUNC('year', order_date)
order by order_date
)t;

--PERFORMANCE ANALYSIS: Comparing the current value to a target value
/* 
--current_sales-average_sales
--current_year_sales-previous_year_sales
--current_sales-lowest_sales
*/

--Analyze the yearly performance of products by comparing each product's sales to both its
--average sales performance and the previous year's sales
with yearly_product_sales as(
select 
	extract(year from f.order_date) as order_year,
	p.product_name,
	sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
where f.order_date is not null
group by extract(year from f.order_date), p.product_name
)
select
	order_year,
	product_name,
	current_sales,
	round(avg(current_sales) over (partition by product_name),2) avg_sales,
	current_sales-round(avg(current_sales) over (partition by product_name),2) as diff_avg,
	case when current_sales-round(avg(current_sales) over (partition by product_name),2)>0 THEN 'Above Average'
		when current_sales-round(avg(current_sales) over (partition by product_name),2)<0 THEN 'Below Average'
		else 'Average'
	end avg_change,
	--year-over-year analysis
	lag(current_sales) over (partition by product_name order by order_year) py_sales,
	current_sales-lag(current_sales) over (partition by product_name order by order_year) as diff_py,
	case when current_sales-lag(current_sales) over (partition by product_name order by order_year)>0 THEN 'Increase'
		when current_sales-lag(current_sales) over (partition by product_name order by order_year)<0 THEN 'Decrease'
		else 'No change'
	end py_change
from yearly_product_sales
order by product_name, order_year;

--PART-TO-WHOLE ANALYSIS/PROPORTIONAL ANALYSIS
/* Analyze how an individual part is performing comapred to the overall,
allowing us to understand which category has the greatest impact on the business. 
--(sales/total_sales)*100 by category
--(quantity/total_quantity)*100 by country
*/

--Which categories contribute the most to overall sales?
with category_sales as 
(
select 
	p.category,
	sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
group by p.category
)
select
	category,
	total_sales,
	sum(total_sales) over () as overall_sales,
	round((total_sales/sum(total_sales) over ())*100, 2)||'%' as percenatge_of_total 
from category_sales
order by total_sales desc;

--DATA SEGMENTATION
/* Group the data based on a specific range.
Helps understand the correlation between two measures. 
--Total products by sales range 
--Total customers by age
*/

--Segment products into cost ranges and
--count how many products fall into each segment */
with product_segments as (
select	
	product_key,
	product_name,
	cost,
	case when cost<100 then 'Below 100'
		when cost between 100 and 500 then '100-500'
		when cost between 500 and 1000 then '500-1000'
		else 'Above 1000'
	end cost_range
from gold.dim_products)
select 
	cost_range,
	count(product_key) as total_products
from product_segments
group by cost_range
order by total_products desc;

/* Group customers into three segments based on their spending behaviour:
	-VIP: Customer with atleast 12 months of history and spending more than 5000.
	-Regular: Customers with atleast 12 months of history but spending 5000 or less.
	-New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group */


select
c.customer_key,
f.sales_amount,
f.order_date
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key=c.customer_key




