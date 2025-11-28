
-- check for primary key duplicates
select prd_id, count(*) c
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null


-- unwanted spaces
select prd_nm
from bronze.crm_prd_info
where prd_nm != Trim(prd_nm)

-- check for negative numbers or nulls
-- nulls exists
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost IS NULL

-- check for data consistency
select distinct prd_line
from bronze.crm_prd_info

-- check for ivalid date orders
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt
/*
there are end dates earlier than start date to fix this issue,
we may copy some cells to excel and brain storm for solution
sol_1: replace end with start, but we we get date overlapping
like from 2007 to 2011 the cost is 12 and at the same time
from 2008 to 2012 the cost is 14, which means in 2010 the cost
is 12 and 14 at the same time,

sol_2: we take the end date from the next start date and minus one day
2011-07-01	2007-12-28
2012-07-01	
will be
2011-07-01	2012-06-30
2012-07-01	

also in this case the end date it's ok to 
contain null but not the start
*/

select * ,
CAST(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) AS DATE) as test_dt
from bronze.crm_prd_info;

-- above function will not allow end_dt be -1 day, int and date clash so will use dateadd function
select * ,
DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)  ) AS prd_end_dt
from bronze.crm_prd_info;


-- replace bronze with silver and run same above code to see if issues solved