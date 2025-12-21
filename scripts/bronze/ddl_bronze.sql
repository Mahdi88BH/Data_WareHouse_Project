-- Data Definition Language DDL defines the structure of database tables

-- USE The Bronze Schema
USE bronze;

/* SQL SERVER Synthaxe
 IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
 	DROP TABLE bronze.crm_cust_info;
*/

DROP TABLE IF EXISTS crm_cust_info; 

CREATE TABLE crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_material_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_data DATE
);


DROP TABLE IF EXISTS crm_prd_info;

CREATE TABLE crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);


DROP TABLE IF EXISTS crm_sales_details;

CREATE TABLE crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);


DROP TABLE IF EXISTS erp_loc_a101;

CREATE TABLE erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);


DROP TABLE IF EXISTS erp_cust_az12;

CREATE TABLE erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);


DROP TABLE IF EXISTS erp_px_cat_g1v2;

CREATE TABLE erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);
