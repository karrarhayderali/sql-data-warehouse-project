-- Check for Duplicates in primary key

SELECT cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null


-- Check unwanted spaces in cst_firstname
select cst_firstname 
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

-- Check unwanted spaces in cst_lastname
select cst_lastname 
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

-- Data Standardization and consistency
select distinct(cst_marital_status)
from bronze.crm_cust_info


-- Data Standardization and consistency
select distinct(cst_gndr)
from bronze.crm_cust_info


--check for each category values
select cst_marital_status, count(*) as c
from bronze.crm_cust_info
group by cst_marital_status
order by c