CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number, -- put keys group first
	cu.customer_key,
	pr.product_key,
	sd.sls_order_dt AS order_date, -- then dates
	sd.sls_ship_dt	AS shipping_date,
	sd.sls_due_dt	AS due_date,
	sd.sls_sales	AS sales_amount, -- then measures
	sd.sls_quantity AS sales_quantity,
	sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON pr.product_number = sd.sls_prd_key
LEFT JOIN gold.dim_customers cu
ON cu.customer_id = sd.sls_cust_id




