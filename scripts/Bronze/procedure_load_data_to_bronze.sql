/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source (.csv files) -> Bronze)
===============================================================================
Script Purpose:
    Running this script will create the stored procedure load_data() in 'bronze' schema.
    This stored procedure loads data into the tables of 'bronze' schema from external CSV files. 
    The procedure performs the following actions:
    - Truncates the bronze tables before loading data.
    - Load data from csv Files to bronze tables.

Parameters:
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
===============================================================================
*/

DROP PROCEDURE IF EXISTS bronze.load_data;
CREATE PROCEDURE bronze.load_data()
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
	RAISE INFO '####Loading bronze layer data####';
	overall_start_time:=clock_timestamp();
	BEGIN
		RAISE INFO '###Loading CRM tables###';
		start_time:=clock_timestamp();
		RAISE INFO '##Truncating bronze.crm_cust_info table ##';
		TRUNCATE TABLE bronze.crm_cust_info;
		RAISE INFO '##Loading data into bronze.crm_cust_info table from cust_info.csv##';
		COPY bronze.crm_cust_info FROM
		'/private/tmp/datasets/source_crm/cust_info.csv'
		WITH (FORMAT csv, HEADER true);
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM bronze.crm_cust_info;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';

		start_time:=clock_timestamp();
		RAISE INFO '##Truncating bronze.crm_prd_info table ##';
		TRUNCATE TABLE bronze.crm_prd_info;
		RAISE INFO '##Loading data into bronze.crm_prd_info table from prd_info.csv##';
		COPY bronze.crm_prd_info FROM
		'/private/tmp/datasets/source_crm/prd_info.csv'
		WITH (FORMAT csv, HEADER true);
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM bronze.crm_prd_info;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';

		start_time:=clock_timestamp();
		RAISE INFO '##Truncating bronze.crm_sales_details table ##';	
		TRUNCATE TABLE bronze.crm_sales_details;
		RAISE INFO '##Loading data into bronze.crm_sales_details table from sales_details.csv##';
		COPY bronze.crm_sales_details FROM
		'/private/tmp/datasets/source_crm/sales_details.csv'
		WITH (FORMAT csv, HEADER true);
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM bronze.crm_sales_details;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';
	
		RAISE INFO '###Loading ERP tables###';

		start_time:=clock_timestamp();
		RAISE INFO '##Truncating bronze.erp_cust_az12 table ##';
		TRUNCATE TABLE bronze.erp_cust_az12;
		RAISE INFO '##Loading data into bronze.erp_cust_az12 table from CUST_AZ12.csv##';
		COPY bronze.erp_cust_az12 FROM
		'/private/tmp/datasets/source_erp/CUST_AZ12.csv'
		WITH (FORMAT csv, HEADER true);
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM bronze.erp_cust_az12;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';

		start_time:=clock_timestamp();
		RAISE INFO '##Truncating bronze.erp_loc_a101 table ##';
		TRUNCATE TABLE bronze.erp_loc_a101;
		RAISE INFO '##Loading data into bronze.erp_loc_a101 table from LOC_A101.csv##';
		COPY bronze.erp_loc_a101 FROM
		'/private/tmp/datasets/source_erp/LOC_A101.csv'
		WITH (FORMAT csv, HEADER true);
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM bronze.erp_loc_a101;
		RAISE INFO 'Loaded % rows in time: % seconds', num_rows,duration_seconds;
		RAISE INFO '-------------------------';

		start_time:=clock_timestamp();
		RAISE INFO '##Truncating bronze.erp_px_cat_g1v2 table ##';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		RAISE INFO '##Loading data into bronze.erp_px_cat_g1v2 table from PX_CAT_G1V2.csv##';
		COPY bronze.erp_px_cat_g1v2 FROM
		'/private/tmp/datasets/source_erp/PX_CAT_G1V2.csv'
		WITH (FORMAT csv, HEADER true);
		--Time calculation
		end_time:=clock_timestamp();
		duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
		--Row count calculation
		SELECT COUNT(*) INTO num_rows FROM bronze.erp_px_cat_g1v2;
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
