TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2
(
ID
,CAT
,SUBCAT
,MAINTENANCE
)
SELECT
ID
,CAT
,SUBCAT
,MAINTENANCE
FROM bronze.erp_px_cat_g1v2;
