--Removing duplicates from primary key cst_id from bronze.crm_cust_info
--Ranking the cst_id based on the cst_create_date to get only the latest one
USE DataWarehouse;
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