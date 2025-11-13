CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_key      INT PRIMARY KEY,
    customer_id       INT,
    customer_number   VARCHAR(20),
    first_name        VARCHAR(50),
    last_name         VARCHAR(50),
    country           VARCHAR(50),
    marital_status    VARCHAR(20),
    gender            VARCHAR(10),
    birthdate         DATE,
    create_date       DATE
);

select * from gold.dim_customers;

------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_products CASCADE;

CREATE TABLE gold.dim_products (
    product_key INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_number VARCHAR(50),
    product_name VARCHAR(255),
    category_id VARCHAR(50),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    maintenance VARCHAR(10),
    cost INTEGER,
    product_line VARCHAR(50),
    start_date DATE
);

select * from gold.dim_products;
-----------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_sales CASCADE;

CREATE TABLE gold.fact_sales (
    order_number VARCHAR(50),
    product_key INTEGER,
    customer_key INTEGER,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount DECIMAL(10,2),
    quantity INTEGER,
    price DECIMAL(10,2),
    CONSTRAINT fk_product FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key),
    CONSTRAINT fk_customer FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key)
);

select * from gold.fact_sales;
---------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.report_customers CASCADE;

CREATE TABLE gold.report_customers (
    customer_key INTEGER PRIMARY KEY,
    customer_number VARCHAR(50),
    customer_name VARCHAR(255),
    age INTEGER,
    age_group VARCHAR(50),
    customer_segment VARCHAR(50),
    last_order_date DATE,
    recency INTEGER,
    total_orders INTEGER,
    total_sales DECIMAL(12,2),
    total_quantity INTEGER,
    lifespan INTEGER,
    avg_order_value DECIMAL(10,2),
    avg_monthly_spend DECIMAL(10,2)
);

select * from gold.report_customers;
------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.report_products CASCADE;

CREATE TABLE gold.report_products (
    product_key INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    cost INTEGER,
    last_sale_date DATE,
    recency_in_months INTEGER,
    product_segment VARCHAR(50),
    lifespan INTEGER,
    total_orders INTEGER,
    total_sales DECIMAL(12,2),
    total_quantity INTEGER,
    total_customers INTEGER,
    avg_selling_price DECIMAL(10,2),
    avg_order_revenue DECIMAL(10,2),
    avg_monthly_revenue DECIMAL(10,2)
);

select * from gold.report_products;