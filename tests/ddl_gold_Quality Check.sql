/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--Product Dimension
--Quality check for duplicates
SELECT 
prd_key
,COUNT(*)
FROM
(
	SELECT
		pn.prd_id
		,pn.cat_id
		,pn.prd_key
		,pn.prd_nm
		,prd_cost
		,pn.prd_line
		,pn.prd_start_dt
		,pc.CAT
		,pc.SUBCAT
		,pc.MAINTENANCE
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.ID
	WHERE pn.prd_end_dt IS NULL
) t
GROUP BY prd_key
HAVING COUNT(*) > 1;

--gold.fact_sales view --Quality check
--check if all dimension tables can be successfully joined to the fact table
--Foreign Key Integrity(Dimensions)

--1.Customer dimension
SELECT
*
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
WHERE c.customer_key IS NULL
--Result: no rows -- which means everything is matching and there is NO issue

--2.Product dimension
SELECT
*
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key = p.product_key
WHERE p.product_key IS NULL
--Result: no rows -- which means everything is matching and there is NO issue
