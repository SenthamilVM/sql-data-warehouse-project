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

/*RULES:
--Business Rule:
--Sales = Quantity * Price
--Negative, zeros and NULLs are NOT Allowed in neither in Sales, Quantity and Price
--For the bad data here and to improve the quality of the data, we can do the following:
	1.If Sales is negative, zero or NULL, derive it using Quantity and Price
	2.If Price is zero or NULL, calculate it using sales and quantity
	3.If price is negative, convert it to positive value
*/


