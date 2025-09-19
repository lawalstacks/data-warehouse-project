/*
+++++++++++++++++++++++++++++++++++++++++++++++++
Create Database and Schemas
+++++++++++++++++++++++++++++++++++++++++++++++++
Script Purpose:
        This script creates a new database with the name 'DataWarehouse'. First it check if it exists, if it exist it drop and recreate. The script also setup 3 schema within the database: 'bronze', 'silver' and 'gold'
CAUTION:
    The entire data in  'DataWarehouse' will be dropped if it already exists. ensure you have proper backups before running the script
*/


USE master;
GO

--drop and recreate the 'Datawherehouse' database

IF EXISTS( SELECT 1 FROM sys.databases WHERE name='DataWharehouse')
BEGIN
    ALTER DATABASE DataWharehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWharehouse;
END;
GO

--Create the database
CREATE DATABASE DataWharehouse
GO

USE DataWharehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO