-- 1) Create Costumer Deminsion


DROP VIEW IF EXISTS gold.dim_costumer;

-- Counvert to friendly name

CREATE VIEW gold.dim_costumer AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS costumer_key,
	cci.cst_id AS costumer_id,
	cci.cst_key AS costumer_number,
	cci.cst_firstname AS first_name,
	cci.cst_lastname AS last_name,
	ela.cntry AS country,
	cci.cst_material_status AS material_status,
	CASE 
		WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr
		ELSE COALESCE(eca.gen, 'n/a') 
	END AS gender,
	eca.bdate AS birthdate,
	cci.cst_create_date AS create_date
FROM silver.crm_cust_info cci LEFT JOIN silver.erp_cust_az12 eca
ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
ON cci.cst_key = ela.cid;




SELECT *
FROM gold.dim_costumer;




-- Check Duplicate

SELECT 
	cst_id, COUNT(*)
FROM (
	SELECT
		cci.cst_id,
		cci.cst_key,
		cci.cst_firstname,
		cci.cst_lastname,
		cci.cst_material_status,
		cci.cst_gndr,
		cci.cst_create_date,
		eca.bdate,
		eca.gen,
		cntry
	FROM silver.crm_cust_info cci LEFT JOIN silver.erp_cust_az12 eca
	ON cci.cst_key = eca.cid
	LEFT JOIN silver.erp_loc_a101 ela
	ON cci.cst_key = ela.cid
) t
GROUP BY cst_id
HAVING COUNT(*) > 1;


-- Data Integration

SELECT DISTINCT
	cci.cst_gndr,
	eca.gen,
	CASE 
		WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr
		ELSE COALESCE(eca.gen, 'n/a') 
	END AS new_gender
FROM silver.crm_cust_info cci LEFT JOIN silver.erp_cust_az12 eca
ON cci.cst_key = eca.cid
GROUP BY 1, 2;



-- Check Quality of gold layer

SELECT DISTINCT 
	gender
FROM gold.dim_costumer;








-- 2) Create Product Deminsion 

DROP VIEW IF EXISTS gold.dim_product;

CREATE VIEW gold.dim_product AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cpi.prd_start_dt, cpi.prd_key) AS product_key,
	cpi.prd_id AS product_id,
	cpi.prd_key AS product_code,
	cpi.prd_nm AS product_name,
	cpi.cat_id AS category_id,
	epcgv.cat AS category,
	epcgv.subcat As subcategory,
	epcgv.maintenance,
	cpi.prd_cost As cost,
	cpi.prd_line AS product_line,
	cpi.prd_start_dt AS start_date
FROM silver.crm_prd_info cpi LEFT JOIN silver.erp_px_cat_g1v2 epcgv 
ON cpi.cat_id = epcgv.id
WHERE cpi.prd_end_dt IS NULL;
-- Target the current Data

SELECT * FROM gold.dim_product;


SELECT *
FROM silver.erp_px_cat_g1v2 epcgv ;



-- Check Duplicate id 

SELECT 
	prd_key, COUNT(*)
FROM (
	SELECT 
		cpi.prd_id,
		cpi.cat_id,
		cpi.prd_key,
		cpi.prd_nm,
		cpi.prd_cost,
		cpi.prd_line,
		cpi.prd_start_dt,
		epcgv.cat,
		epcgv.subcat,
		epcgv.maintenance 
	FROM silver.crm_prd_info cpi LEFT JOIN silver.erp_px_cat_g1v2 epcgv 
	ON cpi.cat_id = epcgv.id
	WHERE cpi.prd_end_dt IS NULL
) t
GROUP BY prd_key
HAVING COUNT(*) > 1;








-- 3) Create Sales Fact

DROP VIEW IF EXISTS gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT
	csd.sls_ord_num AS order_number,
	dp.product_key,
	dc.costumer_key,
	csd.sls_order_dt AS order_date,
	csd.sls_ship_dt AS shipping_date,
	csd.sls_due_dt AS due_date,
	csd.sls_sales AS sales,
	csd.sls_quantity AS quantity,
	csd.sls_price AS price
FROM silver.crm_sales_details csd LEFT JOIN gold.dim_product dp 
ON csd.sls_prd_key = dp.product_code
LEFT JOIN gold.dim_costumer dc 
ON csd.sls_cust_id = dc.costumer_id;




-- Check FORIEGN KEY Deminsion

SELECT *
FROM gold.fact_sales fs LEFT JOIN gold.dim_costumer dc 
	ON fs.costumer_key = dc.costumer_key 
LEFT JOIN gold.dim_product dp 
	ON fs.product_key = dp.product_key;

SELECT *
FROM gold.fact_sales fs LEFT JOIN gold.dim_costumer dc 
	ON fs.costumer_key = dc.costumer_key 
LEFT JOIN gold.dim_product dp 
	ON fs.product_key = dp.product_key
WHERE dc.costumer_key IS NULL 
	OR dp.product_key IS NULL;

