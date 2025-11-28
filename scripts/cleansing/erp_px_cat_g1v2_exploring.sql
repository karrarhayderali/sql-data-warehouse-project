-- check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- DATA Standardization & Consistency
select distinct 
maintenance
from bronze.erp_px_cat_g1v2
--
select distinct 
cat
from bronze.erp_px_cat_g1v2
--
select distinct 
subcat
from bronze.erp_px_cat_g1v2