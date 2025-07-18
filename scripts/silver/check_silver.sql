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
