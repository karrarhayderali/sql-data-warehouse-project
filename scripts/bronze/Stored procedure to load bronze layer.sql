CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===================================';
		PRINT 'Loading Bronze Layers';
		PRINT '===================================';

		PRINT '-----------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE:bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;
		-- delete all rows then insert the data
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\datasets\source_crm\cust_info.csv' -- replcae with file path

		WITH(

			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds'
		PRINT'--------------------'

		--************************************
		SET @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE:bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;
		-- delete all rows then insert the data
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\datasets\source_crm\crm_prd_info' -- replcae with file path

		WITH(

			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds'
		PRINT'--------------------'




		--********************************************************
		SET @start_time = GETDATE();

		PRINT('>> TRUNCATING TABLE:crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\datasets\source_crm\crm_sales_details' -- replcae with file path

		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds'
		PRINT'--------------------'




		--*******************************************************


		PRINT '-----------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE:bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;

		BULK INSERT bronze.erp_cust_az12

		FROM 'C:\Users\datasets\source_crm\erp_cust_az12' -- replcae with file path

		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK

		)
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds'
		PRINT'--------------------'





		--*****************************************************
		SET @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE:bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\datasets\source_crm\erp_loc_a101' -- replcae with file path
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds'
		PRINT'--------------------'





		--***********************************************
		SET @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE:erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\datasets\source_crm\erp_px_cat_g1v2' -- replcae with file path
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds'
		PRINT'--------------------'

		SET @batch_end_time = GETDATE();
		PRINT'======================================';
		PRINT'LOADING BRONZE LAYER COMPLETED'
		PRINT'======================================';
		PRINT'>> Total Load Duration:' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' Seconds'
		PRINT'--------------------'


	END TRY
	BEGIN CATCH
	PRINT'========================================'
	PRINT'ERRORS OCUURED DURING LOADING BRONZE LAYER'
	PRINT'ERROR MESSGAE:'+ ERROR_MESSAGE();
	PRINT'ERROR NUMBER:' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT'ERROR STATE:'  + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT('ERROR LINE:'+ CAST(ERROR_line() AS NVARCHAR));
	PRINT'========================================'
	END CATCH
END


