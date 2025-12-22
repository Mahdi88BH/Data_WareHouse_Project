-- Data Definition Language DDL defines the structure of database tables

-- USE The Bronze Schema
-- USE silver;

/* SQL SERVER Synthaxe
 IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
 	DROP TABLE bronze.crm_cust_info;
 
 CREATE TABLE crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_material_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE,
	
	MetaData Columns
	dwh_create_date DATETIME2 DEFUALT GETDATE()
 );
 
*/

DROP TABLE IF EXISTS silver.crm_cust_info; 

CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_material_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) -- MetaData Column
	
);


DROP TABLE IF EXISTS  silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    cat_id       VARCHAR(50),
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
    dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);


DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    DECIMAL(12,5),
    sls_quantity INT,
    sls_price    DECIMAL(12,5),
    dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);


DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50),
    dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);


DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50),
    dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);


DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
    dwh_create_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
);
