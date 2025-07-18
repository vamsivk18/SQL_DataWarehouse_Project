 --Check for Nulls or Duplicates in primary key
 --Expectation: No Result

select 
cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;


 --Check for unwanted spaces
 --Expected: No Result 


select * from silver.crm_cust_info
where cst_firstname!=trim(cst_firstname)
or cst_lastname!=trim(cst_lastname)
;


select distinct cst_gndr from silver.crm_cust_info

select distinct cst_marital_status from silver.crm_cust_info;

------------------------------------------------------------------------

--Check for Nulls or Duplicate Primary key
select 
count(prd_id) 
from 
silver.crm_prd_info
group by prd_id
having count(prd_id)>1 or prd_id is null;

-- Invalid product costs
select * from silver.crm_prd_info
where prd_cost<0 or prd_cost is null

-- Data Standardization & Consistency
select distinct prd_line
from silver.crm_prd_info

-- Invalid Order dates
select * from silver.crm_prd_info
where prd_start_dt > prd_end_dt


---------------------------------------------------------------------

-- Checking for Nulls and Duplicates
select count(sls_ord_num)
from 
bronze.crm_sales_details
group by sls_ord_num,sls_prd_key
having count(sls_ord_num)!=1

-- Check for Invalid product key
select * from bronze.crm_sales_details
where sls_prd_key not in (
select prd_key from silver.crm_prd_info
)

-- Check for Invalid Customer Id
select * from bronze.crm_sales_details
where sls_cust_id not in (
select cst_id from silver.crm_cust_info)

select * from bronze.crm_sales_details where sls_price<0

-- Other checks, should update later
select distinct cntry from bronze.erp_loc_a101
select cid from bronze.erp_loc_a101 where replace(cid,'-','') not in (select cst_key from silver.crm_cust_info)


select * from bronze.erp_px_cat_g1v2 where id not in (

select cat_id from silver.crm_prd_info)