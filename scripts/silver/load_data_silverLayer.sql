-- 1) Table CRM cust_info

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
WITH cte_crm_cust_info AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_date
	FROM bronze.crm_cust_info
)
SELECT 
	cst_id, 
	cst_key, 
	TRIM(cst_firstname) AS cst_firstname, 
	TRIM(cst_lastname) AS cst_lastname, 
	CASE
		WHEN UPPER(TRIM(cst_material_status))= 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_material_status))= 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_material_status,
	CASE 
		WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM cte_crm_cust_info
WHERE flag_date = 1 
	AND cst_id IS NOT NULL 
	AND cst_id !=0;

SELECT *
FROM silver.crm_cust_info;


-- 2) Table CRM prd_info

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
	prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
) 
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'R' THEN 'Road'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/A'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) prd_start_dt,
	CAST(
		DATE_SUB(
	        LEAD(prd_start_dt) OVER (
	            PARTITION BY prd_key 
	            ORDER BY prd_start_dt
	        ),
	        INTERVAL 1 DAY
    ) AS  DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

SELECT *
FROM silver.crm_prd_info;





-- 3) Table CRM sales_details

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE
		WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
		ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d') -- In MySQL does not Allowed Nested CAST, and CASTING TO VARCHER
	END AS sls_order_dt,
	CASE
		WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
		ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d') -- In MySQL does not Allowed Nested CAST, and CASTING TO VARCHER
	END AS sls_ship_dt,
	CASE
		WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
		ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d') -- In MySQL does not Allowed Nested CAST, and CASTING TO VARCHER
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / IF(sls_quantity = 0 , NULL, sls_quantity)
		ELSE sls_price 
	END sls_price
FROM bronze.crm_sales_details;


/*
TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Dates
    CASE
        WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
    END AS sls_order_dt,

    CASE
        WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
    END AS sls_ship_dt,

    CASE
        WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
    END AS sls_due_dt,

    -- SALES (forcer DECIMAL)
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != sls_quantity * ABS(sls_price)
        THEN CAST(sls_quantity * ABS(sls_price) AS DECIMAL(15,2))
        ELSE CAST(sls_sales AS DECIMAL(15,2))
    END AS sls_sales,

    sls_quantity,

    -- PRICE (éviter INT / INT)
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 THEN
            CAST(
                sls_sales / NULLIF(sls_quantity, 0)
                AS DECIMAL(15,2)
            )
        ELSE CAST(sls_price AS DECIMAL(15,2))
    END AS sls_price

FROM bronze.crm_sales_details;
*/






-- 1) Table ERP cust_az12

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
SELECT 
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > CURRENT_TIMESTAMP() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M', 'Male') THEN 'Male'
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F', 'Female') THEN 'Female'
		ELSE 'n/A'
	END gen
FROM bronze.erp_cust_az12;





















-- 2) Table ERP loc_a101

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
)
SELECT
	REPLACE(cid, '-', '') AS cid,
	CASE
    	WHEN cntry IS NULL OR TRIM(cntry) = '\r' THEN 'n/A'
    	WHEN UPPER(TRIM(cntry)) = 'DE\r' THEN 'Germany\r'
    	WHEN UPPER(TRIM(cntry)) IN ('US\r','USA\r') THEN 'United States\r'
    	ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;







-- 3) Table ERP px_cat_g1v2

TRUNCATE TABLE silver.erp_px_cat_g1v2; 

INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
)
SELECT
	IF(id='CO_PD', NULL, id) id,
	cat,
	subcat,
	CASE TRIM(maintenance)
		WHEN 'Yes\r' THEN 'Yes'
		ELSE maintenance
	END maintenance
FROM bronze.erp_px_cat_g1v2;
















