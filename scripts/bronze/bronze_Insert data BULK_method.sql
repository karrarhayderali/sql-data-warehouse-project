TRUNCATE TABLE bronze.crm_cust_info;
-- delete all rows then insert the data
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\datasets\source_crm\cust_info.csv' -- replcae with file path

WITH(

FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
go


--************************************

TRUNCATE TABLE bronze.crm_prd_info;
-- delete all rows then insert the data
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\datasets\source_crm\crm_prd_info' -- replcae with file path

WITH(

FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
go



--********************************************************

TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\datasets\source_crm\crm_sales_details' -- replcae with file path

WITH(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
GO



--*******************************************************

TRUNCATE TABLE bronze.erp_cust_az12;

BULK INSERT bronze.erp_cust_az12

FROM 'C:\Users\datasets\source_crm\erp_cust_az12' -- replcae with file path

WITH (
 FIRSTROW = 2,
 FIELDTERMINATOR = ',',
 TABLOCK

)
GO



--*****************************************************

TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\datasets\source_crm\erp_loc_a101' -- replcae with file path

WITH(
FIRSTROW=2,
FIELDTERMINATOR = ',',
TABLOCK
);
go



--***********************************************

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\datasets\source_crm\erp_px_cat_g1v2' -- replcae with file path


WITH(
FIRSTROW=2,
FIELDTERMINATOR = ',',
TABLOCK
)
GO





