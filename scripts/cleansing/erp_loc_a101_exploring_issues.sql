-- check cid column after cleaning and checking for not existing IDs
select
REPLACE(cid,'-','') AS cid,
cntry
from bronze.erp_loc_a101
where REPLACE(cid,'-','')
NOT IN (select cst_key from silver.crm_cust_info)

-- DATA Standardization and Consistency (in low cardinality column)
select distinct cntry AS old_cntry,
CASE WHEN TRIM(cntry)  = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry)  IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry)  = '' OR TRIM(cntry) IS NULL THEN 'n/a'
	 ELSE cntry
END cntry
from bronze.erp_loc_a101
