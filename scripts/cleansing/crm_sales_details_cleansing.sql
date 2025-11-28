-- check the sls_prd_key uniqueness from sales_details  in  crm_prd_info
select *
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info )

go
-- check the sls_cust_id uniqueness from sales_details  in  crm_cust_info
select *
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info )


-- the sls_order_dt is int type so we check if it has negative or zeros
-- it has zeros and not 8 digits (the date should be 8)
select 
nullif(sls_order_dt,0)
from bronze.crm_sales_details
where sls_order_dt <= 0 
OR len(sls_order_dt) != 8
OR sls_order_dt > 20300101
OR sls_order_dt < 19000101

-- FIRST we sls_order_dt to nvarchar or varchar then to date since can't convert int to date directly
--********
select 
nullif(sls_ship_dt,0) AS sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 
OR len(sls_ship_dt) != 8
OR sls_ship_dt > 20300101
OR sls_ship_dt < 19000101

--
select 
nullif(sls_due_dt,0) AS sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 
OR len(sls_due_dt) != 8
OR sls_due_dt > 20300101
OR sls_due_dt < 19000101

-- check the order date > ship and due date
select * from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- check for sales and quantity

select DISTINCT
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price
/*
#1 Solution
Data Issues will be fixed direct in source system
#2 Solution
Data Issues has to be fixed in data warehouse 
*/
/*

Rules
If Sales is negative, zero, or null, derive it using Quantity and Price.
If Price is zero or null, calculate it using Sales and Quantity.
If Price is negative, convert it to a positive value
*/

select
sls_sales    AS old_sls_sales,
sls_quantity AS old_sls_quantity,
sls_price    AS old_sls_price,


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

Where sls_sales IS NULL OR sls_sales <= 0 
OR sls_price IS NULL OR sls_price <= 0 
OR sls_quantity IS NULL OR sls_quantity <= 0 