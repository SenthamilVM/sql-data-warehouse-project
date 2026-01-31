--================================================================
--Quality Checks for bronze table
--================================================================

--1.Check for NULLS or Duplicates in Primary Key
--Expectation: No Result

SELECT
cst_id
,COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
--================================================================
	--2.Check for unwanted spaces
	--Expection: No Results
SELECT 
cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
--Result: 15 rows
------------------------
SELECT 
cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname); --checking if the original value is equal after trimming the spaces
--Result: 17 rows
------------------------
SELECT 
cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);
--Result: 0 rows
--Likewise, for other string columns
--==================================================================

--3.Check for consistency of values in low cardinality columns (limited number of possible values)
--Low cardinality columns - cst_gndr and cst_marital_status

--Data Standardization & Consistency
SELECT 
DISTINCT(cst_gndr)
FROM bronze.crm_cust_info;

SELECT 
DISTINCT(cst_marital_status)
FROM bronze.crm_cust_info;

/*================================================================
--Quality Checks for silver table 
--After ingestion, Rerunning the same queries above and check if the data inserted into the 
--silver table doesn't have the issue which were there on the Bronze table
--================================================================*/
--1.Check for NULLS or Duplicates in Primary Key
--Expectation: No Result
SELECT
cst_id
,COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
--Result: No rows
--================================================================
--2.Check for unwanted spaces
--Expection: No Results
SELECT 
cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
--Result: No rows
------------------------
SELECT 
cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname); --checking if the original value is equal after trimming the spaces
--Result: No rows
------------------------
SELECT 
cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);
--Result: 0 rows
--Likewise, for other string columns
--==================================================================

--3.Check for consistency of values in low cardinality columns (limited number of possible values)
--Low cardinality columns - cst_gndr and cst_marital_status

--Data Standardization & Consistency
SELECT 
DISTINCT(cst_gndr)
FROM silver.crm_cust_info;

SELECT 
DISTINCT(cst_marital_status)
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;