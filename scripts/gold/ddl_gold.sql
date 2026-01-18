/*
===============================================================
DDL Script: Create Gold Views
===============================================================

Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================
*/

CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_material_status as marital_status,
CASE WHEN ci.cst_gndr != 'n/a' or ci.cst_gndr != 'n/a' THEN ci.cst_gndr /*with the help of bith the tanles gender column we decide the correct gender*/
	 ELSE COALESCE(ca.gen,'n/a')
END gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid

LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid

/*----------------------------------------------------------------------*/
CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER(order by pn.prd_start_dt,pn.prd_key)as product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS Category_id,

pn.prd_cost as cost,
pn.prd_line,
pn.prd_start_dt as start_date,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance
--pn.prd_end_dt removing end date bcz its always null
from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE pn.prd_end_dt IS NULL-- Filter all historical data
/*----------------------------------crm_sales_details-------------------------------------*/
CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num as order_number,
pr.product_key,--dimension
cu.customer_key,--keys
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver .crm_sales_details sd
lEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
lEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
