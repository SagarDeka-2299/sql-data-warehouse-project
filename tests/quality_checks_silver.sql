/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================


-- Check duplicate records in cst_id
-- Expect: No result (As we are keeping only latest created record only, for each cst_id)
SELECT 
cst_key, count(*) 
FROM silver.crm_cust_info
GROUP BY cst_key
HAVING count(*)>1;

-- Check duplicate records in cst_id
-- Expect: No result
SELECT 
cst_id, count(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*)>1;

-- Check unwanted spaces in firstname
-- Expect: No result
SELECT *
from silver.crm_cust_info
where cst_firstname <> trim(cst_firstname);


-- Check unwanted spaces in lastname
-- Expect: No result
SELECT cst_lastname
from silver.crm_cust_info
where cst_lastname <> trim(cst_lastname);


-- Check data consistency in gender (Low cardinality)
-- Expect: 'male', 'female', 'n/a'(optional)
select distinct cst_gndr
from silver.crm_cust_info;

-- Check data consistency in marital status (Low cardinality)
-- Expect: 'married', 'single', 'n/a'(optional)
select distinct cst_marital_status
from silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

-- Check uplicate id
-- Expect: No record
SELECT 
prd_id, count(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(*)>1 OR prd_id IS NULL;


-- Check negative or null value in cost
-- Expect: No record
SELECT * FROM silver.crm_prd_info
WHERE prd_cost<0 or prd_cost IS NULL;

-- Check unneeded spaces in names
-- Expect: No record
SELECT * FROM silver.crm_prd_info
WHERE prd_nm<>TRIM(prd_nm);

-- Check distinct values in prd_line (Low cardinality)
--Expect: 'touring','other sales','mountain','road','n/a'
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check date order, check if start date is no latter than end date
-- Expect: No record
SELECT * FROM silver.crm_prd_info
WHERE prd_start_dt>prd_end_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

-- Check if product key foreign key is valid
-- Expect: No record
select *
from silver.crm_sales_details 
where sls_prd_key not in (
	select prd_key from silver.crm_prd_info
);

-- Check if customer id foreign key is valid
-- Expect: No record
select *
from silver.crm_sales_details 
where sls_cust_id not in (
	select cst_id from silver.crm_cust_info
);

-- Check if the formula sales=quantity*price holds true and price, quantity, sales etc are valid quantity (not null, 0 or less than zero)
-- Expect: No record
select * 
from silver.crm_sales_details
where 
sls_sales<>sls_quantity*sls_price or
sls_sales is null or 
sls_quantity is null or 
sls_price is null or
sls_sales <=0 or 
sls_quantity <=0 or 
sls_price <=0;

-- Check if date order is correct (order date should be before shipping, due date)
-- Expect: No record
select * 
from silver.crm_sales_details
where sls_order_dt> sls_due_dt or sls_order_dt>sls_ship_dt;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================

-- Check for invalid cid, all cid must be present in crm_cust_info
-- Expect: No record
select * 
from silver.erp_cust_az12 
where cid not in (
	select cst_key from silver.crm_cust_info
);

-- Check if bdate in valid range (Not in a future date)
-- Expect: No record
select * 
from silver.erp_cust_az12
where bdate> CURRENT_DATE;

-- Check unique values in gen
-- Expect 'male', 'female', 'n/a' (optional)
select distinct gen 
from silver.erp_cust_az12

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- Check if cid is valid, all values must be present in crm_cust_info
-- Expect: No record
select * from silver.erp_loc_a101 where cid not in (
	select cst_key from silver.crm_cust_info
);

-- Check unique values in cntry
-- Expect standardized values
select distinct cntry from silver.erp_loc_a101;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check if cat value are consistent with no duplicate, it is low cardinality.
-- Expect: Consistent string values with no duplicate
select distinct cat from bronze.erp_px_cat_g1v2;


-- Check if subcat value are consistent with no duplicate, it is low cardinality.
-- Expect: Consistent string values with no duplicate
select distinct subcat from bronze.erp_px_cat_g1v2;


-- Check if maintenance value are consistent with no duplicate, it is low cardinality.
-- Expect: 'Yes', 'No'
select distinct maintenance from bronze.erp_px_cat_g1v2;


