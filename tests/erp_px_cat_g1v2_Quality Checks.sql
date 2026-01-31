--================================================================
--Quality Checks for bronze table
--================================================================

SELECT
*
FROM bronze.erp_px_cat_g1v2;

--1.Check for NULLS or Duplicates in Primary Key
--Expectation: No Result
SELECT
ID
,COUNT(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY ID
HAVING COUNT(*) > 1 OR ID IS NULL;
--Result: No rows

--Checking the key matches
--cat_id is already created in the transformation of silver.crm_prd_info. Hence the key is there

--Check for unwated spaces:
SELECT
*
FROM bronze.erp_px_cat_g1v2
WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE)
--Result: No rows -- There are NO spaces

--Data Standardization & Consistency 
SELECT DISTINCT
CAT
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
SUBCAT
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
MAINTENANCE
FROM bronze.erp_px_cat_g1v2;
--Result: No issues found


SELECT * FROM silver.erp_px_cat_g1v2;



