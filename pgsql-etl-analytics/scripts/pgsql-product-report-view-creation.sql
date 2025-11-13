/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/
-- =============================================================================
-- Create Report: gold.report_products
-- =============================================================================
create view gold.report_product_for_analysis as
with base_query as (
--Base Query: Retrieves core columns from fact_sales and dim_products
	select
		f.order_number,
		f.order_date,
		f.customer_key,
		f.sales_amount,
		f.quantity,
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost
	from gold.fact_sales f
	left join gold.dim_products p
		on f.product_key=p.product_key
	where order_date is not null --only consider valid sales dates
),
product_aggregations as (
--Product Aggregations: Summarizes key metrics at the product level
select 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	(
  		extract(year from age(max(order_date), min(order_date))) * 12
  		+ extract(month from age(max(order_date), min(order_date)))
	) AS lifespan,
	max(order_date) as last_sale_date,
	count(distinct order_number) as total_orders,
	count(distinct customer_key) as total_customers,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	round(sum(sales_amount)::numeric / NULLIF(sum(quantity), 0), 1) AS avg_selling_price
from base_query
group by 
	product_key,
	product_name,
	category,
	subcategory,
	cost
)
--Final query: Combines all product results into one output
select 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	(
		extract(year from age(current_date, last_sale_date))*12
		+ extract(month from age(current_date, last_sale_date))
	) as recency_in_months,
	case
		when total_sales>50000 then 'High Performer'
		when total_sales>=10000 then 'Mid-Range'
		else 'Low-Performer'
	end as product_segment,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	--Average Order Revenue (AOR)
	case
		when total_orders=0 then 0
		else round(total_sales/total_orders, 2)
	end as avg_order_revenue
from product_aggregations;
	
