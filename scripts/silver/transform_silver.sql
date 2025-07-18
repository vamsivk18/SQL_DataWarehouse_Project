insert into silver.crm_cust_info
(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname, -- Data Cleansing, Removing Unwanted Spaces
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_marital_status))='S' then 'Single'
	 when upper(trim(cst_marital_status))='M' then 'Married'
	 when upper(trim(cst_marital_status)) is null then 'n/a' 
	 else upper(trim(cst_marital_status))
	 end as cst_marital_status, -- Data Normalization/Standardization, Handling Missing Data
case when upper(trim(cst_gndr))='F' then 'Female'
	 when upper(trim(cst_gndr))='M' then 'Male'
	 when upper(trim(cst_gndr)) is null then 'n/a' 
	 else upper(trim(cst_gndr))
	 end as cst_gndr,
cst_create_date
from (
	select 
	*,row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null
)t where flag_last = 1 -- Removing Duplicates, Data Filtering

select trim(null)