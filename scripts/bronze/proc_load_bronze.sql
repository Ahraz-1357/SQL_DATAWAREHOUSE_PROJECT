/*
============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:

    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
DECLARE @start_time_bronze DATETIME,@end_time_bronze DATETIME;
SET @start_time_bronze=GETDATE();
BEGIN

	DECLARE @start_time DATETIME,@end_time DATETIME;
	
	BEGIN TRY
		PRINT '==========================================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '==========================================================================';

		PRINT '----------------------------------------------------------------------------';
		PRINT 'LOADING CRM TABLES'
		PRINT '----------------------------------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING AND INSERTING TABLE : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\mohda\OneDrive\Desktop\SQL_DATA\data_warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			 FIRSTROW=2,
			 FIELDTERMINATOR = ',',
			 TABLOCK

		);
	
		SET @end_time=GETDATE();
		PRINT '>> Load Duration' +CAST(DATEDIFF(SECOND, @start_time,@end_time)AS NVARCHAR) + 'seconds';
		PRINT '----------------'

	
		-- File 2
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING AND INSERTING TABLE : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\mohda\OneDrive\Desktop\SQL_DATA\data_warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			 FIRSTROW=2,
			 FIELDTERMINATOR = ',',
			 TABLOCK

		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration' +CAST(DATEDIFF(SECOND, @start_time,@end_time)AS NVARCHAR) + 'seconds';
		PRINT '----------------'


		-- File 3
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING AND INSERTING TABLE : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\mohda\OneDrive\Desktop\SQL_DATA\data_warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			 FIRSTROW=2,
			 FIELDTERMINATOR = ',',
			 TABLOCK

		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration' +CAST(DATEDIFF(SECOND, @start_time,@end_time)AS NVARCHAR) + 'seconds';
		PRINT '-------------------------'
	
		PRINT '----------------------------------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '----------------------------------------------------------------------------';

		--File 1 erp
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING AND INSERTING TABLE : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\mohda\OneDrive\Desktop\SQL_DATA\data_warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			 FIRSTROW=2,
			 FIELDTERMINATOR = ',',
			 TABLOCK

		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration' +CAST(DATEDIFF(SECOND, @start_time,@end_time)AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		--File 2 erp
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING AND INSERTING TABLE : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\mohda\OneDrive\Desktop\SQL_DATA\data_warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			 FIRSTROW=2,
			 FIELDTERMINATOR = ',',
			 TABLOCK

		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration' +CAST(DATEDIFF(SECOND, @start_time,@end_time)AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		--File 3 erp
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING AND INSERTING TABLE : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\mohda\OneDrive\Desktop\SQL_DATA\data_warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			 FIRSTROW=2,
			 FIELDTERMINATOR = ',',
			 TABLOCK

		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration' +CAST(DATEDIFF(SECOND, @start_time,@end_time)AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

	END TRY
	BEGIN CATCH
		PRINT '============================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'TRY AGAIN AFTER SOME TIME';
		PRINT '============================================================';
	END CATCH

SET @end_time_bronze=GETDATE();
PRINT '>> Load Duration Bronze Layer'  + CAST(DATEDIFF(SECOND, @start_time,@end_time)AS NVARCHAR)  + 'seconds';
PRINT '----------------'

END


DROP PROCEDURE bronze.load_bronze;
EXEC bronze.load_bronze;
