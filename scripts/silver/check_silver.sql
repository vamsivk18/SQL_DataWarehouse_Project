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



