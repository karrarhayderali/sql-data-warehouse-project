/*
check the cid from erp_cust_az12 is same as 
cst_key from crm_cust_info 
after removing NAS (first 3 chars)
*/
select 
cid,
bdate,
gen
from bronze.erp_cust_az12
where cid like '%AW00011000%'

-- remving first 3 chars then check if all cid exists as cst_key in crm_cust_info
select

CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	 ELSE cid
END cid,

bdate,
gen
from bronze.erp_cust_az12
where CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	 ELSE cid
END NOT IN (select distinct cst_key from bronze.crm_cust_info )


-- check for invalid dates
select bdate
from bronze.erp_cust_az12
order by bdate asc
--
select bdate
from bronze.erp_cust_az12
where bdate > getdate()

--
select bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' OR bdate > GETDATE()
--

select count(bdate)
from bronze.erp_cust_az12
where bdate < '1924-01-01'
--
select count(bdate)
from bronze.erp_cust_az12
where bdate > getdate()
-- check for data consistency in gen == DATA standardazation and Consistency
select distinct gen
from bronze.erp_cust_az12

