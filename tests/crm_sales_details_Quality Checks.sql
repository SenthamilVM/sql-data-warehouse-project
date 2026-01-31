--=======================================================================================
	-- Quality check for bronze layer: bronze.crm_sales_details
--=======================================================================================

SELECT
*
FROM bronze.crm_sales_details;

--1.Check for NULLS or Duplicates in Primary Key
--Expectation: No Result
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);
--Result: No rows -- no duplicates

--2.Check if the keys are there and correct to relate the tables.
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN
(
SELECT prd_key FROM silver.crm_prd_info
)
--Result: No rows --All the key from Sales Details table are available in prd_info in silver

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN
(
SELECT cst_id FROM silver.crm_cust_info
) --Result: No rows --All the keys are available

--Check for invalid dates: sls_order_dt
SELECT 
NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 --Checking for negative and zero values in sls_order_dt
OR LEN(sls_order_dt) !=8
OR sls_order_dt > 20500101 -- checking for the outliers
OR sls_order_dt < 19000101;
--Result: There are some errors

--Check for invalid dates for: sls_ship_dt
SELECT 
NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <=0 --Checking for negative and zero values in sls_order_dt
OR LEN(sls_ship_dt) !=8
OR sls_ship_dt > 20500101 -- checking for the outliers
OR sls_ship_dt < 19000101;
--Result: No Errors

--Check for invalid dates for: sls_due_dt
SELECT 
NULLIF(sls_due_dt,0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <=0 --Checking for negative and zero values in sls_order_dt
OR LEN(sls_due_dt) !=8
OR sls_due_dt > 20500101 -- checking for the outliers
OR sls_due_dt < 19000101;
--Result: No Errors

--Order date must always be earlier/smaller than the shipping date or due date
--Expection: No Results
SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;
--Result: no rows -- which means no order date is greater than ship date or due date

--Business Rule:
--Sales = Quantity * Price
--Negative, zeros and NULLs are NOT Allowed in neither in Sales, Quantity and Price
SELECT
sls_sales
,sls_quantity
,sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

--=======================================================================================
	-- Quality check for silver layer: silver.crm_sales_details after ingestion
--=======================================================================================

SELECT
*
FROM silver.crm_sales_details;

--1.Check for NULLS or Duplicates in Primary Key
--Expectation: No Result
SELECT
*
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);
--Result: No rows -- no duplicates

--2.Check if the keys are there and correct to relate the tables.
SELECT
*
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN
(
SELECT prd_key FROM silver.crm_prd_info
)
--Result: No rows --All the key from Sales Details table are available in prd_info in silver

SELECT
*
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN
(
SELECT cst_id FROM silver.crm_cust_info
) --Result: No rows --All the keys are available

--Order date must always be earlier/smaller than the shipping date or due date
--Expection: No Results
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;
--Result: no rows -- which means no order date is greater than ship date or due date

--Business Rule:
--Sales = Quantity * Price
--Negative, zeros and NULLs are NOT Allowed in neither in Sales, Quantity and Price
SELECT
sls_sales
,sls_quantity
,sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details;





