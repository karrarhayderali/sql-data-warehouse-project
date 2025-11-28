CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===================================';
		PRINT 'Loading Silver Layers';
		PRINT '===================================';

		PRINT '-----------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------';
		SET @start_time = GETDATE();
		PRINT('>> Truncating Table: silver.crm_cust_info');
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT('>> Inerting Data Into: silver.crm_cust_info');
		INSERT INTO silver.crm_cust_info(
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
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS  cst_lastname,
			Case 
				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Maried'
				 ELSE 'n/a'
			END cst_marital_status,-- Normalize gender values to readable format

			CASE 
				 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'   
				 Else 'n/a'
			END cst_gndr, --Normalize gender values to readable format
			cst_create_date



		From (
			select 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id order by cst_create_date DESC) as flag_last
			from bronze.crm_cust_info
			where cst_id IS NOT NULL

		) t 
		where flag_last = 1 -- select the most recent record per customer
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT'--------------------------------------------'

		PRINT('>> Truncating Table: silver.crm_prd_info');
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT('>> Inerting Data Into: silver.crm_prd_info');

		SET @start_time = GETDATE();
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt

		)


		select

			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost, -- NULLIF return null if value is zero

			CASE UPPER(TRIM(prd_line))
				 WHEN  'M' THEN 'Mountain'
				 WHEN  'T' THEN 'Touring'
				 WHEN  'S' THEN 'Other Sales'
				 WHEN  'R' THEN 'Road'
				 ELSE 'n/a'
			END AS prd_line,

			CAST(prd_start_dt AS DATE) AS prd_start_dt,

			DATEADD(
					DAY,-1,LEAD(
								prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)  ) AS prd_end_dt

		from bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT'--------------------------------------------'

		SET @start_time = GETDATE();
		PRINT('>> Truncating Table: silver.crm_sales_details');
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT('>> Inerting Data Into: silver.crm_sales_details');

		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)



		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				 WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- Convrt int type to VARCHAR then to Date
			END sls_order_dt,

			CASE 
				 WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- Convrt int type to VARCHAR then to Date
			END sls_ship_dt,


			CASE 
				 WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- Convrt int type to VARCHAR then to Date
			END sls_due_dt,

			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price 
				THEN ABS(sls_quantity) * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,

			sls_quantity,

			CASE 
				WHEN sls_price IS NULL or sls_price <=0 THEN ABS(sls_sales) / Nullif(ABS(sls_quantity),0)
				ELSE sls_price
			END sls_price

		from bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT'--------------------------------------------'


		PRINT '-----------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------';

		SET @start_time = GETDATE();
		PRINT('>> Truncating Table: silver.erp_cust_az12');
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT('>> Inerting Data Into: silver.erp_cust_az12');

		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		select

			CASE
				 WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
				 ELSE cid
			END cid,

			CASE 
				 WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END bdate,


			CASE 
				 WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M','MALE')   THEN 'Male'
				 ELSE 'n/a'
			END AS gen
		from bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT'--------------------------------------------'


		SET @start_time = GETDATE();
		PRINT('>> Truncating Table: silver.erp_loc_a101');
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT('>> Inerting Data Into: silver.erp_loc_a101');

		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		select
			REPLACE(cid,'-','') AS cid,

			CASE 
				 WHEN TRIM(cntry)  = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry)  IN ('US','USA') THEN 'United States'
				 WHEN TRIM(cntry)  = '' OR TRIM(cntry) IS NULL THEN 'n/a'
				 ELSE cntry
			END cntry
		from bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT'--------------------------------------------'

		SET @start_time = GETDATE();
		PRINT('>> Truncating Table: silver.erp_px_cat_g1v2');
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT('>> Inerting Data Into: silver.erp_px_cat_g1v2');

		INSERT INTO silver.erp_px_cat_g1v2 (
			id, 
			cat, 
			subcat, 
			maintenance
		)

		select 
			id, 
			cat, 
			subcat, 
			maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS'
		PRINT'--------------------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT'======================================';
		PRINT'LOADING SILVER LAYER COMPLETED'
		PRINT'======================================';
		PRINT'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' SECONDS'
		PRINT('--------------------------------------')
	END TRY
	BEGIN CATCH
	PRINT'=========================================='
	PRINT'ERRORS OCCURED DURING LOADING SILVER LAYER'
	PRINT'=========================================='
	PRINT('ERROR MESSAGE:'+ ERROR_MESSAGE())
	PRINT('ERROR NUMBER:'+ CAST(ERROR_NUMBER() AS NVARCHAR))
	PRINT('ERROR STATE:'+ CAST(ERROR_STATE() AS NVARCHAR))
	PRINT('ERROR LINE:'+ CAST(ERROR_line() AS NVARCHAR))
	PRINT'=========================================='
	END CATCH
END



