/*
 CREATE OR ALTER PROCEDURE load_bronze()
 BEGIN
 	
 	PRINT '================================================================'
	PRINT 'Loading Bronze Layers'
	PRINT '================================================================'
 	
 	DECLARE @start_time DATETIME, @end_time DATETIME;
 	
 	
 	BEGIN TRY
 		
 		-- TRACK ETL DURATION
		-- Help to indentify bottlenecks, optimize performance, monitor trends, detects issues
 		SET @start_time = GETDATE();
	 	
	 	TRUNCATE TABLE bronze.crm.cust.info
	 
		 BULK INSERT bronze.crm.cust.info
		 FROM 'Path to CSV'
		 WITH (
		 	FIRSTROW = 2,
		 	FIELDTERMINATOR = ',',
		 	TABLOCK
		 );
		 
		 SET @end_time = GETDATE();
		 PRINT 'Load time duration' + CAST( GETDIFF(second, @start_time, @end_time) AS NVARCHAR()) + ' seconds';
		 
		 -- CHECK that the data has not shifted and is in the correct columns
 		SELECT * FROM crm_cust_info;
		SELECT COUNT(*) AS Number_Of_Rows FROM crm_cust_info; 
		 
	 END TRY
	 BEGIN CATCH
	 PRINT 'Canot LOAD the Data to The bronze Layer';
	 PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	 PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR());
	 PRINT 'ERROR MESSAGE' + CAST(ERROR_SATATE() AS NVARCHAR());
	 END CATCH;
 
 END
 
 EXEC load_bronze;
 */



/*
 * In MySQl LOAD DATA n'est pas autorisé dans une procédure stockée (Erreur 1314). 
 * Ce script doit être exécuté en tant que script SQL direct ou via un outil d'intégration (ETL/Bash).

DELIMITER $$

CREATE PROCEDURE load_data_bronzeLayer()
BEGIN


	--	>	ADD TRY AND CATCH
	--	>	Ensure Error handling, data integrity, and issue logging for easier debugging
	
	🔴 Important limitations :
		❌ Not usable outside stored procedures or triggers
		❌ No line-by-line TRY / CATCH
		❌ Output is always returned as a Result Set (MySQL)
	
	DECLARE  EXIT HANDLER FOR SQLEXEPTION
	BEGIN
		SELECT 'Cannot Load The DATA FROMSource';
	END;
	
	--Try Block
	-- Resetting it to an empty state
	TRUNCATE TABLE crm_cust_info;
	
	-- LOAD DATA With BULK Feature
	LOAD DATA LOCAL INFILE '/home/el7m7c/Desktop/DataEngineer/Data_WareHouse_Project/datasets/source_crm/cust_info.csv'
	INTO TABLE bronze.crm_cust_info
	FIELDS TERMINATED BY ','
	LINES TERMINATED BY '\n'
	IGNORE 1 LINES;
	
	
	-- CHECK that the data has not shifted and is in the correct columns
	
	SELECT * FROM crm_cust_info;
	
	SELECT COUNT(*) AS Number_Of_Rows FROM crm_cust_info; 
	
END $$

DELIMITER ;

-- Execute the procedure
CALL load_data_bronzeLayer();

-- Show the procedure
SHOW PROCEDURE STATUS WHERE Db = 'bronze';
*/









-- Select The DataBase
USE bronze;

-- ADD prints to track execution, debug issues and understand it flow



-- Resetting it to an empty state
TRUNCATE TABLE crm_cust_info;
	
-- LOAD DATA With BULK Feature
	
LOAD DATA LOCAL INFILE '/home/el7m7c/Desktop/DataEngineer/Data_WareHouse_Project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
	

-- CHECK that the data has not shifted and is in the correct columns
	
SELECT * FROM crm_cust_info;
	
SELECT COUNT(*) AS Number_Of_Rows FROM crm_cust_info; 
	
	
-- Resetting it to an empty state
TRUNCATE TABLE crm_prd_info;
	
-- LOAD DATA With BULK Feature
	
LOAD DATA LOCAL INFILE '/home/el7m7c/Desktop/DataEngineer/Data_WareHouse_Project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
	
	
-- CHECK that the data has not shifted and is in the correct columns
	
SELECT * FROM crm_prd_info;
	
SELECT COUNT(*) AS Number_Of_Rows FROM crm_prd_info;
	
	
	
-- Resetting it to an empty state
TRUNCATE TABLE crm_sales_details;
	
-- LOAD DATA With BULK Feature
	
LOAD DATA LOCAL INFILE '/home/el7m7c/Desktop/DataEngineer/Data_WareHouse_Project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
	
	
-- CHECK that the data has not shifted and is in the correct columns
	
SELECT * FROM crm_sales_details;
	
SELECT COUNT(*) AS Number_Of_Rows FROM crm_sales_details; 
	
	





-- Resetting it to an empty state
TRUNCATE TABLE erp_cust_az12;
	
-- LOAD DATA With BULK Feature
	
LOAD DATA LOCAL INFILE '/home/el7m7c/Desktop/DataEngineer/Data_WareHouse_Project/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
	
	
-- CHECK that the data has not shifted and is in the correct columns
	
SELECT * FROM erp_cust_az12;
	
SELECT COUNT(*) AS Number_Of_Rows FROM erp_cust_az12; 
	
	
	
-- Resetting it to an empty state
TRUNCATE TABLE erp_loc_a101;
	
-- LOAD DATA With BULK Feature
	
LOAD DATA LOCAL INFILE '/home/el7m7c/Desktop/DataEngineer/Data_WareHouse_Project/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
	
	
-- CHECK that the data has not shifted and is in the correct columns
	
SELECT * FROM erp_loc_a101;
	
SELECT COUNT(*) AS Number_Of_Rows FROM erp_loc_a101; 
	
	
	
	
-- Resetting it to an empty state
TRUNCATE TABLE erp_px_cat_g1v2;
	
-- LOAD DATA With BULK Feature
	
LOAD DATA LOCAL INFILE '/home/el7m7c/Desktop/DataEngineer/Data_WareHouse_Project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
	
	
-- CHECK that the data has not shifted and is in the correct columns
	
SELECT * FROM erp_px_cat_g1v2;
	
SELECT COUNT(*) AS Number_Of_Rows FROM erp_px_cat_g1v2;



