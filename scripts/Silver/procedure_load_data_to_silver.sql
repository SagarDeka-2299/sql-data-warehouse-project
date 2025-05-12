/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script creates a stored procedure 'load_data()' inside silver schema. This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed by the procedure:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL Silver.load_silver;
===============================================================================
*/
DROP PROCEDURE IF EXISTS silver.load_silver;
CREATE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_seconds FLOAT;
	overall_start_time TIMESTAMP;
    overall_end_time TIMESTAMP;
    overall_duration_seconds FLOAT;
	num_rows INT;
BEGIN
	RAISE INFO '####Loading silver layer data####';
	overall_start_time:=clock_timestamp();
	BEGIN
		RAISE INFO '###Loading CRM tables###';
		start_time:=clock_timestamp();
		RAISE INFO '##Truncating silver.crm_cust_info table ##';
		TRUNCATE TABLE silver.crm_cust_info;
		RAISE INFO '##Loading data into silver.crm_cust_info table from bronze.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		WITH ranked_by_creation_latest_to_oldest AS(
			SELECT 
			*, 
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as creation_order
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
				CASE LOWER(TRIM(cst_marital_status))
				WHEN 'm' THEN 'married'
				WHEN 's' THEN 'single'
				ELSE 'n/a'
			END as cst_marital_status,
			CASE LOWER(TRIM(cst_gndr))
				WHEN 'm' THEN 'male'
				WHEN 'f' THEN 'female'
				ELSE 'n/a'
			END as cst_gndr,
			cst_create_date
		FROM ranked_by_creation_latest_to_oldest
		WHERE creation_order=1;
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM silver.crm_cust_info;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';

		start_time:=clock_timestamp();
		RAISE INFO '##Truncating silver.crm_prd_info table ##';
		TRUNCATE TABLE silver.crm_prd_info;
		RAISE INFO '##Loading data into silver.crm_prd_info table from bronze.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
			prd_nm AS prd_nm,
			CASE
				WHEN prd_cost IS NULL THEN 0
				ELSE prd_cost
			END AS prd_cost,
		CASE LOWER(TRIM(prd_line))
			WHEN 'm' THEN 'mountain'
			WHEN 'r' THEN 'road'
			WHEN 's' THEN 'other sales'
			WHEN 't' THEN 'touring'
			ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt))-1 prd_end_dt
		FROM bronze.crm_prd_info;
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM silver.crm_prd_info;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';

		start_time:=clock_timestamp();
		RAISE INFO '##Truncating silver.crm_sales_details table ##';
		TRUNCATE TABLE silver.crm_sales_details;
		RAISE INFO '##Loading data into silver.crm_sales_details table from bronze.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN LENGTH(CAST(sls_order_dt AS TEXT))<>8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS TEXT)AS DATE)
			END AS sls_order_dt,
			CASE
				WHEN LENGTH(CAST(sls_ship_dt AS TEXT))<>8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS TEXT)AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN LENGTH(CAST(sls_due_dt AS TEXT))<>8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS TEXT)AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales <> sls_quantity* ABS(sls_price) THEN sls_quantity* ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price<=0 THEN sls_sales/ NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM silver.crm_sales_details;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';
		
		RAISE INFO '###Loading ERP tables###';
		start_time:=clock_timestamp();
		RAISE INFO '##Truncating silver.erp_cust_az12 table ##';
		TRUNCATE TABLE silver.erp_cust_az12;
		RAISE INFO '##Loading data into silver.erp_cust_az12 table from bronze.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))
				ELSE cid
			END AS cid,
			CASE
				WHEN bdate>=CURRENT_DATE THEN NULL
				ELSE bdate
			END AS bdate,
			CASE
				WHEN LOWER(TRIM(gen)) in ('male', 'm') THEN 'male'
				WHEN LOWER(TRIM(gen)) in ('female', 'f') THEN 'female'
				ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12;
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM silver.erp_cust_az12;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';

		start_time:=clock_timestamp();
		RAISE INFO '##Truncating silver.erp_loc_a101 table ##';
		TRUNCATE TABLE silver.erp_loc_a101;
		RAISE INFO '##Loading data into silver.erp_loc_a101 table from bronze.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
			cid,cntry 
		)
		select
			REPLACE(cid,'-','') AS cid,
			CASE
				WHEN TRIM(cntry)='DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN LENGTH(TRIM(cntry))=0 or cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry 
		from bronze.erp_loc_a101;
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM silver.erp_loc_a101;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';

		start_time:=clock_timestamp();
		RAISE INFO '##Truncating silver.erp_px_cat_g1v2 table ##';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		RAISE INFO '##Loading data into silver.erp_px_cat_g1v2 table from bronze.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM silver.erp_px_cat_g1v2;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Error occured:\n %',SQLERRM;
	END;
	overall_end_time:=clock_timestamp();
	overall_duration_seconds := EXTRACT(EPOCH FROM (overall_end_time - overall_start_time));
	RAISE INFO 'Total execution time: % seconds', overall_duration_seconds;
END;
$$;
