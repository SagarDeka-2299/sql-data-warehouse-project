/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

--Dropping the fact table first because it depends on the dimension tables, 
--postgres won't allow to drop dimension tables because of the dependency
RAISE INFO 'Dropping gold.fact_sales view';
DROP VIEW IF EXISTS gold.fact_sales;

RAISE INFO 'Dropping gold.dim_products view';
DROP VIEW IF EXISTS gold.dim_products;

RAISE INFO 'Dropping gold.dim_customers view';
DROP VIEW IF EXISTS gold.dim_customers;

--Creating dimension views
--products
RAISE INFO 'Creating and loading data to view: gold.dim_products';
CREATE VIEW gold.dim_products AS(
	WITH joinedtable AS(
		SELECT *
		FROM 
		silver.crm_prd_info pi
		LEFT JOIN silver.erp_px_cat_g1v2 pc
		on pi.cat_id=pc.id
	)
	SELECT
	ROW_NUMBER() OVER(ORDER BY prd_start_dt) AS product_key,
	prd_id AS product_id,
	prd_key AS product_number,
	prd_nm AS product_name,
	cat_id AS category_id,
	cat AS category,
	subcat AS subcategory,
	maintenance,
	prd_cost AS cost,
	prd_line AS production_line,
	prd_start_dt AS start_date
	FROM joinedtable
	WHERE prd_end_dt IS NULL
);

--customers
RAISE INFO 'Creating and loading data to view: gold.dim_customers';
CREATE VIEW gold.dim_customers AS(
	WITH joinedtable AS(
		SELECT *
		FROM 
		silver.crm_cust_info ci
		LEFT JOIN silver.erp_cust_az12 ca
		on ci.cst_key=ca.cid
		LEFT JOIN silver.erp_loc_a101 la
		on ci.cst_key=la.cid
	)
	SELECT
		ROW_NUMBER() OVER(ORDER BY cst_create_date) customer_key,
		cst_id AS customer_id,
		cst_key AS customer_number,
		cst_firstname AS first_name,
		cst_lastname AS last_name,
		CASE
		WHEN cst_gndr<>'n/a' THEN cst_gndr
		ELSE COALESCE(gen, 'n/a')
		END AS gender,
		cntry AS country,
		bdate AS birth_date,
		cst_marital_status AS marital_status,
		cst_create_date AS create_date
	FROM joinedtable
);

--Creating fact view
--sales
RAISE INFO 'Creating and loading data to view: gold.fact_sales';
CREATE VIEW gold.fact_sales AS(
	WITH jointable AS (
		SELECT * FROM
		silver.crm_sales_details s
		LEFT JOIN gold.dim_products p
		ON p.product_number=s.sls_prd_key
		LEFT JOIN gold.dim_customers c
		ON c.customer_id=s.sls_cust_id
	)
	SELECT
		sls_ord_num AS order_num,
		product_key,
		customer_key,
		sls_order_dt AS order_date,
		sls_ship_dt AS shipping_date,
		sls_due_dt AS due_date,
		sls_sales AS sales_amount,
		sls_quantity AS qunatity,
		sls_price AS price
	FROM jointable
);
