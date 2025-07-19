
/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================


if object_id('gold.dim_customers','V') is not null
    drop view gold.dim_customers
go

create view gold.dim_customers as 
select
	row_number() over(order by cst_key) as customer_key,
	cc.cst_id             as customer_id,
	cc.cst_key            as customer_number,
	cc.cst_firstname      as first_name,
	cc.cst_lastname       as last_name,
	el.cntry              as country,
	cc.cst_marital_status as marital_status,
	case when cc.cst_gndr != 'n/a' or cc.cst_gndr is not null then cc.cst_gndr
			else coalesce(ec.gen,'n/a')
	end as gender,
	ec.bdate              as birth_date,
	cc.cst_create_date    as create_date
from silver.crm_cust_info cc
left join silver.erp_cust_az12 ec
	on cc.cst_key = ec.cid
left join silver.erp_loc_a101 el
	on cc.cst_key = el.cid
go

if object_id('gold.dim_products','V') is not null
    drop view gold.dim_products
go

create view gold.dim_products as 
select 
	row_number() over(order by cp.prd_key) as product_key,
    cp.prd_id       as product_id,
    cp.prd_key      as product_number,
    cp.prd_nm       as product_name,
    cp.cat_id       as category_id,
    ep.cat          as category,
    ep.subcat       as subcategory,
    ep.maintenance  as maintenance,
    cp.prd_cost     as cost,
    cp.prd_line     as product_line,
    cp.prd_start_dt as start_date
from silver.crm_prd_info cp
left join silver.erp_px_cat_g1v2 ep
on cp.cat_id = ep.id
where cp.prd_end_dt is null
go

if object_id('gold.fact_sales','V') is not null
    drop view gold.fact_sales
go

create view gold.fact_sales as 
select 
    cs.sls_ord_num  AS order_number,
    dp.product_key  AS product_key,
    dc.customer_key AS customer_key,
    cs.sls_order_dt AS order_date,
    cs.sls_ship_dt  AS shipping_date,
    cs.sls_due_dt   AS due_date,
    cs.sls_sales    AS sales_amount,
    cs.sls_quantity AS quantity,
    cs.sls_price    AS price from silver.crm_sales_details cs
left join gold.dim_customers dc
on cs.sls_cust_id = dc.customer_id
left join gold.dim_products dp
on cs.sls_prd_key = dp.product_number
go