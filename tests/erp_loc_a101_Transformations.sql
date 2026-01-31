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

