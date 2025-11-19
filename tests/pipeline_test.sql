/*===============================================================================
ETL PIPELINE TEST SUITE  
Covers Bronze → Silver → Gold Layers
Run after loading:
    EXEC bronze.load_bronze
    EXEC silver.load_silver
    SELECT * FROM gold.dim_customers
===============================================================================*/

/*===============================================================================
SECTION 1: BRONZE LAYER TESTS  
Validate bulk load integrity, file ingestion, row counts, data sanity
===============================================================================*/

PRINT '================ BRONZE TESTS ================';

/* 1.1 – Check Bronze tables are NOT empty after load */
SELECT 'bronze.crm_cust_info' AS table_name, COUNT(*) AS row_count 
FROM bronze.crm_cust_info;

SELECT 'bronze.crm_prd_info', COUNT(*) 
FROM bronze.crm_prd_info;

SELECT 'bronze.crm_sales_details', COUNT(*) 
FROM bronze.crm_sales_details;

SELECT 'bronze.erp_loc_a101', COUNT(*) 
FROM bronze.erp_loc_a101;

SELECT 'bronze.erp_cust_az12', COUNT(*) 
FROM bronze.erp_cust_az12;

SELECT 'bronze.erp_px_cat_g1v2', COUNT(*) 
FROM bronze.erp_px_cat_g1v2;

/* 1.2 – Check for NULL primary keys (should not exist in source CSVs) */
SELECT * FROM bronze.crm_cust_info WHERE cst_id IS NULL;
SELECT * FROM bronze.crm_prd_info WHERE prd_id IS NULL;
SELECT * FROM bronze.crm_sales_details WHERE sls_ord_num IS NULL;

/* 1.3 – Check invalid dates (not 8-digit numbers in CRM sales) */
SELECT sls_order_dt FROM bronze.crm_sales_details WHERE LEN(sls_order_dt) != 8;

/*===============================================================================
SECTION 2: SILVER LAYER TESTS  
Validate transformations, cleansing, deduplication, mapping, normalization
===============================================================================*/

PRINT '================ SILVER TESTS ================';

/* 2.1 – Check Silver tables contain rows (ETL ran correctly) */
SELECT 'silver.crm_cust_info' AS table_name, COUNT(*) AS row_count  
FROM silver.crm_cust_info;

SELECT 'silver.crm_prd_info', COUNT(*) 
FROM silver.crm_prd_info;

SELECT 'silver.crm_sales_details', COUNT(*) 
FROM silver.crm_sales_details;

SELECT 'silver.erp_cust_az12', COUNT(*) 
FROM silver.erp_cust_az12;

SELECT 'silver.erp_loc_a101', COUNT(*) 
FROM silver.erp_loc_a101;

SELECT 'silver.erp_px_cat_g1v2', COUNT(*) 
FROM silver.erp_px_cat_g1v2;

/* 2.2 – CRM Customer: Check only latest record per cst_id */
SELECT cst_id, COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

/* 2.3 – CRM Customer: Check marital status mapping */
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;

/* 2.4 – CRM Product: Check category_id was extracted correctly */
SELECT TOP 20 prd_key, cat_id 
FROM silver.crm_prd_info;

/* 2.5 – CRM Product: Check prd_line normalization */
SELECT DISTINCT prd_line 
FROM silver.crm_prd_info;

/* 2.6 – Sales: Check sales_amount = quantity * price */
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * ABS(sls_price);

/* 2.7 – ERP Customer: Check gender normalization */
SELECT DISTINCT gen 
FROM silver.erp_cust_az12;

/* 2.8 – ERP Location: Check country name mapping */
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101;

/*===============================================================================
SECTION 3: GOLD LAYER TESTS  
Validate star-schema integrity, joins, surrogate keys, referential continuity
===============================================================================*/

PRINT '================ GOLD TESTS ================';

/* 3.1 – Check Gold views return rows */
SELECT COUNT(*) AS dim_customers_count FROM gold.dim_customers;
SELECT COUNT(*) AS dim_products_count  FROM gold.dim_products;
SELECT COUNT(*) AS fact_sales_count     FROM gold.fact_sales;

/* 3.2 – Check surrogate keys exist */
SELECT TOP 20 customer_key, customer_id FROM gold.dim_customers;
SELECT TOP 20 product_key, product_id   FROM gold.dim_products;

/* 3.3 – Fact foreign keys must match dimensions */
SELECT fs.*
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;

/* Missing product_key */
SELECT fs.*
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL;

/* 3.4 – Check no NULL dates where not expected */
SELECT * FROM gold.fact_sales WHERE order_date IS NULL;

/* 3.5 – Product dimension: Ensure only current products (prd_end_dt IS NULL) */
SELECT *
FROM gold.dim_products
WHERE product_id IN (
    SELECT prd_id FROM silver.crm_prd_info WHERE prd_end_dt IS NOT NULL
);

/* 3.6 – Customer dimension: Gender source hierarchy logic validation */
SELECT customer_id, gender
FROM gold.dim_customers
WHERE gender = 'n/a';

/* 3.7 – Check country exists for each customer (nullable allowed but track missing) */
SELECT *
FROM gold.dim_customers
WHERE country = 'n/a' OR country IS NULL;

/* 3.8 – Sales fact: Ensure measures are positive */
SELECT *
FROM gold.fact_sales
WHERE quantity <= 0 OR price <= 0 OR sales_amount <= 0;

/*===============================================================================
END OF TEST SUITE
===============================================================================*/
