/*
===============================================================================
Stored Procedure: Load Silver Layer (From Bronze to Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Trasnform, Load) process to populate the 'silver' schema from bronze schema. 
    Actions performed:
    - Truncates the silver tables before loading data.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=========================================================';

		PRINT '---------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------------';
	
		--Truncating and inserting data into silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info
		(
		cst_id
		,cst_key
		,cst_firstname
		,cst_lastname
		,cst_marital_status
		,cst_gndr
		,cst_create_date
		)
		SELECT
			cst_id
			,cst_key
			,TRIM(cst_firstname) AS cst_firstname
			,TRIM(cst_lastname) AS cst_lastname
			,CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				  WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				  ELSE 'NA' 
			END cst_marital_status -- Normalizing marital status values to readable format
			,CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				  WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				  ELSE 'NA' -- replacing NULL with NA
			END cst_gndr -- Normalizing gender values to readable format
			,cst_create_date -- the data type of this column is alrady defined as DATE
		FROM(
			SELECT
			*
			,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE flag_last = 1; -- Retaining the most relevant information
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------';

		--Truncating and inserting data into silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info
		(
		prd_id
		,cat_id
		,prd_key
		,prd_nm
		,prd_cost
		,prd_line
		,prd_start_dt
		,prd_end_dt
		)
		SELECT 
		prd_id
		,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id  --Extract Category ID & Replacing '-' with '_' since ID from erp_px_cat_g1v2 has '_'
		,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key			--Extract product key
		,prd_nm
		,ISNULL(prd_cost, 0) AS prd_cost
		,CASE UPPER(TRIM(prd_line))
			  WHEN 'M' THEN 'Mountain'
			  WHEN 'R' THEN 'Road'
			  WHEN 'S' THEN 'Other Sales'
			  WHEN 'T' THEN 'Touring'
			  ELSE 'NA'
		END AS prd_line			--Map product line codes to descriptive values
		,prd_start_dt
		,DATEADD(
				DAY, -1
				,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)
				) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------';

		--Truncating and inserting data into silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details
		(
		sls_ord_num
		,sls_prd_key
		,sls_cust_id
		,sls_order_dt
		,sls_ship_dt
		,sls_due_dt
		,sls_sales
		,sls_quantity
		,sls_price
		)
		SELECT
			sls_ord_num
			,sls_prd_key
			,sls_cust_id
			,CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
				  ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- We can't convert directly from Integer to Date. Hence converting to VARCHAR first
			END sls_order_dt
			,CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
				  ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- We can't convert directly from Integer to Date. Hence converting to VARCHAR first
			END sls_ship_dt
			,CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
				  ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- We can't convert directly from Integer to Date. Hence converting to VARCHAR first
			END sls_due_dt
			,CASE WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales <=0 OR sls_sales IS NULL 
					THEN sls_quantity * ABS(sls_price)
				  ELSE sls_sales	--Recalculate sales if original value is missing or incorrect
			END AS sls_sales
			,sls_quantity
			,CASE WHEN sls_price <=0 OR sls_price IS NULL 
					THEN sls_sales/NULLIF(sls_quantity,0)
				  ELSE sls_price	--Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------';

		PRINT '---------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------------------';

		--Truncating and inserting data into silver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12
		(
		CID
		,BDATE
		,GEN
		)
		SELECT
		CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING( CID, 4, LEN(CID) ) --Removing 'NAS' prefix if present
			  ELSE CID
		END AS CID
		,CASE WHEN BDATE > GETDATE() THEN NULL
			   ELSE BDATE
		END AS BDATE --Set Future Birthdayes to NULL
		,CASE WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
			  WHEN UPPER(TRIM(GEN))  IN ('F', 'FEMALE') THEN 'Female'
			  ELSE 'NA'
		END AS GEN --Normalize the gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------';

		--Truncating and inserting data into silver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101
		(
		CID
		,CNTRY
		)
		SELECT
		REPLACE(CID, '-', '') AS CID
		,CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			  WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
			  WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'NA'
			  ELSE TRIM(CNTRY)
		END AS CNTRY --Normalize and handle missing or blank country codes
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------';

		--Truncating and inserting data into silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2
		(
		ID
		,CAT
		,SUBCAT
		,MAINTENANCE
		)
		SELECT
		ID
		,CAT
		,SUBCAT
		,MAINTENANCE
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------';
	SET @batch_end_time = GETDATE();
	PRINT '===============================================================';
	PRINT 'Loading Silver Layer is Completed'
	PRINT '>> Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '===============================================================';
	END TRY
	BEGIN CATCH
	PRINT '===============================================================';
	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '===============================================================';
	END CATCH
END