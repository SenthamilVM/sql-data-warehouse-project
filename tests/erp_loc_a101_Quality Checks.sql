--================================================================
--Quality Checks for bronze table
--================================================================

SELECT
*
FROM bronze.erp_loc_a101;


SELECT CID FROM bronze.erp_loc_a101;

--1.Check for NULLS or Duplicates in Primary Key
--Expectation: No Result
SELECT
CID
,COUNT(*) 
FROM bronze.erp_loc_a101
GROUP BY CID
HAVING COUNT(*) > 1 OR CID IS NULL;
--Result: No rows

--Checking if the keys are matching between the related tables
SELECT cst_key FROM silver.crm_cust_info
--Result: We have '-' in Bronze table which is NOT there in silver table. So, we need to remove it

--Checking the if the transformation is working fine and all the key values are there
SELECT
REPLACE(CID, '-', '') AS CID
FROM bronze.erp_loc_a101
WHERE REPLACE(CID, '-', '')
NOT IN
(SELECT cst_key FROM silver.crm_cust_info);
--Result: No rows

--Low cardinality -- Data Standardization & Consistency
SELECT DISTINCT
CNTRY 
FROM bronze.erp_loc_a101
ORDER BY CNTRY;
--Result: There are NULLS, BLANKS, Abbreviation etc. These are not consistent

--Checking if the transformation is working
SELECT DISTINCT
CNTRY AS old_cntry
,CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
	  WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
	  WHEN CNTRY = '' OR CNTRY IS NULL THEN 'NA'
	  ELSE CNTRY
END AS CNTRY
FROM bronze.erp_loc_a101

--================================================================
--Quality Checks for silver table after ingestion
--================================================================

--Data standardization & Consistency
SELECT DISTINCT
CNTRY
FROM silver.erp_loc_a101


--Check the silver table
SELECT
*
FROM silver.erp_loc_a101;


