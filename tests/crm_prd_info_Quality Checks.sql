--Quality Checks for bronze.crm_prd_info

--1.Check for NULLS or Duplicates in Primary Key
--Expectation: No Result

SELECT
prd_id
,COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
--Result: No rows -- no duplicates

--2.Check for unwanted spaces
--Expection: No Results
SELECT
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
--Result: No rows

--Check for NULLS or Negative Numbers
---Expection: No Results
SELECT
prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
--Result: 2 NULLS

--Data Standardization & Consistency
SELECT
DISTINCT(prd_line)
FROM bronze.crm_prd_info;

--Check for invalid Date orders
---Expection: No Results
SELECT
*
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt; -- start date should not be greater than the end date. IT CAN'T BE

--=======================================================================================
--Check the quality of the silver after ingesting the cleaned data from bronze.crm_prd_info
--=======================================================================================

--1.Check for NULLS or Duplicates in Primary Key
--Expectation: No Result

SELECT
prd_id
,COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
--Result: No rows -- no duplicates

--2.Check for unwanted spaces
--Expection: No Results
SELECT
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
--Result: No rows

--Check for NULLS or Negative Numbers
---Expection: No Results
SELECT
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
--Result: No rows

--Data Standardization & Consistency
SELECT
DISTINCT(prd_line)
FROM silver.crm_prd_info;

--Check for invalid Date orders
---Expection: No Results
SELECT
*
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;
--Result: No rows

SELECT * FROM silver.crm_prd_info;