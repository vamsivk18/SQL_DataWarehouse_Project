/*
===============================================================================
Bronze Layer Load Script: Truncate and Load CRM and ERP csv files into respective tables 
===============================================================================
Script Purpose:
    This script truncates and bulk loads all the CRM and ERP csv files into their respective tables in the 'bronze' schema
	  Run this script to re-load the databases into  the 'bronze' Tables
===============================================================================
*/

create or alter procedure bronze.load_bronze as
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = getdate();
		print '=================================================';
		print 'Loading Bronze Layer';
		print '=================================================';
		print '-------------------------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------------------------';

		set @start_time = getdate();

		print '>> Truncating Table: bronze.crm_cust_info';
	
		truncate table bronze.crm_cust_info;

		print '>> Inserting Table: bronze.crm_cust_info';

		bulk insert bronze.crm_cust_info
		from 'D:\A_Projects\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select count(*) from bronze.crm_cust_info;
		set @end_time = getdate();
		print 'Load Duration: '+cast(round(convert(float,datediff(millisecond,@start_time,@end_time))/1000,3) as nvarchar)+' seconds';

		---------------------------------------------------------------
		set @start_time = getdate();
		print '>> Truncating Table: bronze.crm_prd_info';
	
		truncate table bronze.crm_prd_info;

		print '>> Inserting Table: bronze.crm_prd_info';

		bulk insert bronze.crm_prd_info
		from 'D:\A_Projects\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select count(*) from bronze.crm_prd_info;
		set @end_time = getdate();
		print 'Load Duration: '+cast(round(convert(float,datediff(millisecond,@start_time,@end_time))/1000,3) as nvarchar)+' seconds';

		---------------------------------------------------------------
		print '>> Truncating Table: bronze.crm_sales_details';
		set @start_time = getdate();

		truncate table bronze.crm_sales_details;

		print '>> Inserting Table: bronze.crm_sales_details';

		bulk insert bronze.crm_sales_details
		from 'D:\A_Projects\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select count(*) from bronze.crm_sales_details;
		set @end_time = getdate();
		print 'Load Duration: '+cast(round(convert(float,datediff(millisecond,@start_time,@end_time))/1000,3) as nvarchar)+' seconds';

		---------------------------------------------------------------

		print '-------------------------------------------------';
		print 'Loading ERP Tables';
		print '-------------------------------------------------';

		print '>> Truncating Table: bronze.erp_cust_az12';
		set @start_time = getdate();

		truncate table bronze.erp_cust_az12;

		print '>> Inserting Table: bronze.erp_cust_az12';

		bulk insert bronze.erp_cust_az12
		from 'D:\A_Projects\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select count(*) from bronze.erp_cust_az12;
		set @end_time = getdate();
		print 'Load Duration: '+cast(round(convert(float,datediff(millisecond,@start_time,@end_time))/1000,3) as nvarchar)+' seconds';

		---------------------------------------------------------------
		print '>> Truncating Table: bronze.erp_loc_a101';
		
		set @start_time = getdate();

		truncate table bronze.erp_loc_a101;

		print '>> Inserting Table: bronze.erp_loc_a101';

		bulk insert bronze.erp_loc_a101
		from 'D:\A_Projects\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select count(*) from bronze.erp_loc_a101;
		set @end_time = getdate();
		print 'Load Duration: '+cast(round(convert(float,datediff(millisecond,@start_time,@end_time))/1000,3) as nvarchar)+' seconds';

		---------------------------------------------------------------
		
		print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		
		set @start_time = getdate();

		truncate table bronze.erp_px_cat_g1v2;

		print '>> Inserting Table: bronze.erp_px_cat_g1v2';

		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\A_Projects\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		--select count(*) from bronze.erp_px_cat_g1v2;
		set @end_time = getdate();
		print 'Load Duration: '+cast(round(convert(float,datediff(millisecond,@start_time,@end_time))/1000,3) as nvarchar)+' seconds';

		--------------------------------------------------------------
		set @batch_end_time = getdate();
		print '=================================================';
		print 'Total Bronze Layer Load Duration: '+cast(round(convert(float,datediff(millisecond,@batch_start_time,@batch_end_time))/1000,3) as nvarchar)+' seconds';
		print '=================================================';
	end try
	begin catch

		print '=================================================';
		print 'Error Occured During Loading Bronze Layer';
		print 'Error Message'+error_message();
		print 'Error Message'+cast(error_number() as nvarchar);
		print 'Error Message'+cast(error_state() as nvarchar);
		print '=================================================';
		set @batch_end_time = getdate();
		print '=================================================';
		print 'Total Bronze Layer Load Duration: '+cast(round(convert(float,datediff(millisecond,@batch_start_time,@batch_end_time))/1000,3) as nvarchar)+' seconds';
		print '=================================================';
	end catch
end