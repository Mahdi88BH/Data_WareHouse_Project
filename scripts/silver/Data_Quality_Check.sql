-- 1) Table CRM cust_info

-- Check for null or Duplicates in PRIMARY KEY

USE bronze ;

SELECT cst_id , COUNT(*) AS "Duplicates Customer_id"
FROM crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

SELECT cst_id , COUNT(*) AS "Duplicates Customer_id"
FROM crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


SELECT *
FROM crm_cust_info
WHERE cst_id = 29466;


SELECT *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_date
FROM crm_cust_info;



WITH cte_crm_cust_info AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_date
	FROM crm_cust_info
)
SELECT * 
FROM cte_crm_cust_info
WHERE flag_date != 1;


WITH cte AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_data DESC) AS flag_date
	FROM crm_cust_info
)
SELECT COUNT(*) AS Total_Number_Duplicates
from cte
WHERE flag_date != 1;


-- Check fro unwanted spaces in string values
-- Trim leading and trailing space from string 

-- cst_id, cst_key, TRIM(cst_firstname), TRIM(cst_lastname), cst_material_status, cst_gndr, cst_create_data 

SELECT *
FROM crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT *
FROM crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- Check the consistency of Value in low cardinality

SELECT DISTINCT cst_gndr
FROM crm_cust_info;


SELECT COUNT(*) AS Available_Gender_value
FROM crm_cust_info
WHERE cst_gndr IS NULL;


SELECT COUNT(*) AS Available_Gender_value
FROM crm_cust_info
WHERE cst_gndr = '';





SELECT DISTINCT cst_material_status
FROM crm_cust_info;

SELECT COUNT(*) AS Available_Gender_value
FROM crm_cust_info
WHERE cst_material_status IS NULL;

SELECT COUNT(*) AS Available_Gender_value
FROM crm_cust_info
WHERE cst_gndr = '';











-- 2) Table CRM cust_info
SELECT *
FROM bronze.crm_prd_info;

-- Check for null or Duplicates in PRIMARY KEY

SELECT
	prd_id,
	COUNT(*) AS Duplicate_id
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;


-- Check Unusfell Value

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
	SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2
);

SELECT * 
FROM bronze.erp_px_cat_g1v2;

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE  SUBSTRING(prd_key, 7, LENGTH(prd_key)) NOT IN(
	SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details
);

SELECT * 
FROM bronze.crm_sales_details;


-- Check for Unwnted spaces

SELECT 
	prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm  != TRIM(prd_nm);


-- Check For Null Or negative number
SELECT
	DISTINCT prd_cost
FROM bronze.crm_prd_info;

SELECT
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL 
	OR prd_cost < 0;

-- Check for Null Values

SELECT 
	DISTINCT prd_line
FROM bronze.crm_prd_info;



-- Check for Invalide data Order
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt IS NULL 
	OR  prd_start_dt IS NULL;



SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt,
	DATE_SUB(
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key 
            ORDER BY prd_start_dt
        ),
        INTERVAL 1 DAY
    ) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');











-- 3) Table CRM sales_details


SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price 
FROM bronze.crm_sales_details;
	

-- Check Unwanted spaces
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num); 


-- Check Unsefel Ids
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
	SELECT prd_key 
	FROM silver.crm_prd_info
);


SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
	SELECT cst_id 
	FROM silver.crm_cust_info
);




-- Check invalid Date
SELECT 
	sls_order_dt
FROM bronze.crm_sales_details;

SELECT 
	sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > 20250101
	OR sls_order_dt < 19000101;


SELECT 
	IF(sls_order_dt <= 0, NULL, sls_order_dt) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
	OR LENGTH(sls_order_dt ) != 8
	OR sls_order_dt > 20250101
	OR sls_order_dt < 19000101;



SELECT 
	IF(sls_ship_dt <= 0, NULL, sls_ship_dt) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
	OR LENGTH(sls_ship_dt ) != 8
	OR sls_ship_dt > 20250101
	OR sls_ship_dt < 19000101;



SELECT 
	IF(sls_due_dt <= 0, NULL, sls_due_dt) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
	OR LENGTH(sls_due_dt ) != 8
	OR sls_due_dt > 20250101
	OR sls_due_dt < 19000101;


-- Check For an Order Date
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check Data Consistency

SELECT 
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;



SELECT 
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT
	IF(sls_sales != sls_price * sls_quantity OR sls_sales <= 0 , sls_price * sls_quantity, sls_sales) AS sls_sales
FROM bronze.crm_sales_details
WHERE IF(sls_sales != sls_price * sls_quantity OR sls_sales <= 0 , sls_price * sls_quantity, sls_sales) != sls_quantity * sls_price;

SELECT
	IF(sls_quantity != sls_sales / sls_price OR sls_quantity <= 0 , sls_sales / sls_price, sls_quantity) AS sls_quantity
FROM bronze.crm_sales_details
WHERE IF(sls_quantity != sls_sales / sls_price OR sls_quantity <= 0 , sls_sales / sls_price, sls_quantity) != sls_sales / sls_price;

SELECT
	IF(sls_price != sls_sales / sls_quantity OR sls_price <= 0 , sls_sales / sls_quantity, sls_price) AS sls_price
FROM bronze.crm_sales_details
WHERE IF(sls_price != sls_sales / sls_quantity OR sls_price <= 0 , sls_sales / sls_quantity, sls_price) != sls_sales / sls_quantity;


SELECT DISTINCT sls_quantity
FROM bronze.crm_sales_details;







-- 1) ERP cust_az12
SELECT
	cid,
	bdate,
	gen
FROM bronze.erp_cust_az12;

-- Check Unseffel Id

WITH CTE AS (
	SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END AS cid
	FROM bronze.erp_cust_az12

)
SELECT * FROM CTE
WHERE cid NOT IN (
	SELECT cst_key 
	FROM silver.crm_cust_info 
);


SELECT * FROM bronze.erp_cust_az12;
SELECT *
FROM silver.crm_cust_info
WHERE cst_key LIKE 'NAS%'; 


-- Check Invalide Date 
SELECT
	bdate
FROM silver.erp_cust_az12
WHERE bdate > CURRENT_TIMESTAMP();

WITH CTE AS (
	SELECT 
		CASE 
			WHEN bdate > CURRENT_TIMESTAMP() THEN NULL
			ELSE bdate
		END AS bdate
	FROM bronze.erp_cust_az12
)
SELECT * 
FROM CTE
WHERE bdate > CURRENT_TIMESTAMP();



-- Check Censensly of cardinal data
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

SELECT DISTINCT gen, HEX(gen)
FROM bronze.erp_cust_az12;

SELECT gen
FROM bronze.erp_cust_az12
WHERE UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) = 'M';

SELECT gen
FROM bronze.erp_cust_az12
WHERE UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) = 'F';


WITH CTE AS(
	SELECT
		CASE
			WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M', 'Male') THEN 'Male'
			WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F', 'Female') THEN 'Female'
			ELSE 'n/A'
		END gen
	FROM bronze.erp_cust_az12
)
SELECT gen 
FROM CTE 
WHERE UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) = 'M'
	OR UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) = 'F';
