create view gold.report_customers_for_analysis as
with base_query as
--1) Base Query: Retrives core columns from tables
(
select 
	f.order_number,
	f.product_key,
	f.order_date,
	f.sales_amount,
	f.quantity,
	c.customer_key,
	c.customer_number,
	c.first_name||' '||c.last_name as customer_name,
	c.birthdate,
	extract('year' from age(c.birthdate::date)) as age
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key=f.customer_key
where order_date is not null
)
, customer_aggregation as
(
-- Customer Aggregations: Summarizes key metrics at the customer level
select 
	customer_key,
	customer_number,
	customer_name,
	age,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	count(distinct product_key) as total_product,
	max(order_date) as last_order_date,
	(
  		extract(year from age(max(order_date), min(order_date))) * 12
  		+ extract(month from age(max(order_date), min(order_date)))
	) AS lifespan
from base_query
group by
	customer_key,
	customer_number,
	customer_name,
	age
)
select 
	customer_key,
	customer_number,
	customer_name,
	age,
	case 
		when age<20 then 'Under 20'
		when age between 20 and 29 then '20-29'
		when age between 30 and 39 then '30-39'
		when age between 40 and 49 then '40-49'
		else '50 and above'
	end as age_group,
	case
		when lifespan>=12 and total_sales>5000 then 'VIP'
		when lifespan>=12 and total_sales<5000 then 'Regular'
		else 'New'
	end as customer_segment,
	last_order_date,
	(
		extract(year from age(current_date, last_order_date))*12
		+ extract(month from age(current_date, last_order_date))
	) as recency,
	total_orders,
	total_sales,
	total_quantity,
	total_product,
	lifespan,
	-- Compute average order value (AVO)
	case
		when total_orders=0 then 0
		else round(total_sales/total_orders, 2)
	end as avg_order_value,
	--Compute average monthly spend
	case 
		when lifespan=0 then total_sales
		else round(total_sales/lifespan, 2)
	end as avg_monthly_spend
from customer_aggregation;