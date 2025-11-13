------------------------------------------------------------------------------------------------
--UPDATED silver.erp_px_cat_g1v2 WITH THE TRANSFORMATIONS (Not to be run without the Pipeline):---
------------------------------------------------------------------------------------------------
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT
id,
cat, 
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

------------------------------------------------------------------------------------------------
--CHECKS STARTS HERE--
------------------------------------------------------------------------------------------------
-- NULL or duplicate primary key check
SELECT id, COUNT(*) AS duplicate_count
FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING id IS NULL OR COUNT(*) > 1;
--🟢 Purpose: Ensures every id is unique and not null.

-- Unwanted spaces in category-related fields
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);
--🟢 Purpose: Detects trailing, leading, or excessive whitespace that may cause mismatches or inconsistent joins.

-- Check for inconsistent category naming conventions
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2
ORDER BY cat;

SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2
ORDER BY subcat;

-- Example- validate only expected category values
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2
WHERE cat NOT IN ('Bikes', 'Accessories', 'Clothing', 'Components', 'n/a');
--🟢 Purpose: Ensures that the standardized category and subcategory naming conventions are enforced.

-- Referential integrity check between erp_px_cat_g1v2 and crm_prd_info
SELECT id
FROM silver.erp_px_cat_g1v2
WHERE id NOT IN (
    SELECT DISTINCT cat_id
    FROM silver.crm_prd_info
);
--🟢 Purpose: Ensures every product category ID exists in product master (crm_prd_info).

-- Consistency: one category should not map to multiple distinct subcategories
SELECT cat, COUNT(DISTINCT subcat) AS subcat_count
FROM silver.erp_px_cat_g1v2
GROUP BY cat
HAVING COUNT(DISTINCT subcat) > 1;
🟢 Purpose: Detects logical inconsistencies between category and subcategory mappings.

