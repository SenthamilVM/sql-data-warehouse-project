INSERT INTO silver.erp_cust_az12
(
CID
,BDATE
,GEN
)
SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING( CID, 4, LEN(CID) ) --Removing 'NAS' prefix if present
	  ELSE CID
END AS CID
,CASE WHEN BDATE > GETDATE() THEN NULL
	   ELSE BDATE
END AS BDATE --Set Future Birthdayes to NULL
,CASE WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
	  WHEN UPPER(TRIM(GEN))  IN ('F', 'FEMALE') THEN 'Female'
	  ELSE 'NA'
END AS GEN --Normalize the gender values and handle unknown cases
FROM bronze.erp_cust_az12;

