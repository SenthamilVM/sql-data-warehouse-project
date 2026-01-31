--================================================================
--Quality Checks for bronze table
--================================================================

SELECT
*
FROM bronze.erp_cust_az12;

--1.Check for NULLS or Duplicates in Primary Key
--Expectation: No Result
SELECT
CID
,COUNT(*)
FROM bronze.erp_cust_az12
GROUP BY CID
HAVING COUNT(*) > 1 OR CID IS NULL;
--Result: No rows

--2.Check if all the related keys are available between related tables (crm_cust_info  and erp_cust_az12) - check the Integration Model for relationships
SELECT cst_key FROM silver.crm_cust_info;

SELECT CID FROM bronze.erp_cust_az12;


SELECT
CID
FROM bronze.erp_cust_az12
WHERE CID LIKE '%AW00011000%';
--The keys are not same. There are extra letters 'NAS'. So, extract the keys alone

--Check if there are not matching values in bronze.erp_cust_az12
--Expectation: No results
SELECT
CID
,CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING( CID, 4, LEN(CID) )
	  ELSE CID
END AS CID
FROM bronze.erp_cust_az12
WHERE 
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING( CID, 4, LEN(CID) )
	  ELSE CID
END 
NOT IN
(SELECT cst_key FROM silver.crm_cust_info);
--Result: No rows -- which meams the transformation is working fine. We can apply this in the table

--3.Check for unwanted spaces
--Expection: No Results
SELECT
CID
FROM bronze.erp_cust_az12
WHERE CID != TRIM(CID)
--Result: No rows

--4.Date column -- Checking for invalid dates (outliers)
SELECT DISTINCT
BDATE
FROM bronze.erp_cust_az12
WHERE BDATE < '1924-01-01' --Checking for very old customers (older than 100 customers)
	  OR BDATE > GETDATE() -- checking if the BDATE is in future which is not possible

--5.Low cardinality column (Gen)
SELECT DISTINCT
GEN
FROM bronze.erp_cust_az12;

--Check if the transformation is actually working
SELECT DISTINCT
CASE WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	  WHEN UPPER(TRIM(GEN))  IN ('F', 'FEMALE') THEN 'Female'
	  ELSE 'NA'
END AS GEN
FROM bronze.erp_cust_az12;

/*================================================================
--Quality Checks for silver table 
--After ingestion, Rerunning the same queries above and check if the data inserted into the 
--silver table doesn't have the issue which were there on the Bronze table
--================================================================*/

--4.Date column -- Checking for invalid dates (outliers)
SELECT DISTINCT
BDATE
FROM silver.erp_cust_az12
WHERE BDATE > GETDATE() -- checking if the BDATE is in future which is not possible


--5.Low cardinality column (Gen)
SELECT DISTINCT
GEN
FROM silver.erp_cust_az12;

--Checking the silver table
SELECT * FROM [silver].[erp_cust_az12]



