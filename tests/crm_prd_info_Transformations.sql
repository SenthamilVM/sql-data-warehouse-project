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