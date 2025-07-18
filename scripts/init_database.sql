/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


use master;
go

-- Drop and recreate the 'DataWarehouse' database
if exists (select 1 from sys.databases where name='DataWarehouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
go

create database DataWarehouse;
go

-- Wait until database is ready
DECLARE @db_ready BIT = 0;
WHILE @db_ready = 0
BEGIN
    IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
        SET @db_ready = 1;
    ELSE
        WAITFOR DELAY '00:00:01'; -- wait 1 second before checking again
END
GO

use DataWarehouse;
go

create schema bronze;
go

create schema silver;
go

create schema gold;
go
