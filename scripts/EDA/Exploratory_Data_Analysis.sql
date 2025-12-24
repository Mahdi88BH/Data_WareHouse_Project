-- 1) Explore DataBase Structure

-- Explore All Object In The DataBase
SELECT *
FROM INFORMATION_SCHEMA.TABLES;


SELECT *
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold';



-- Explore All Columns In DataBase
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA LIKE 'gold'
	AND TABLE_NAME LIKE 'dim_costumer';



SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME LIKE 'dim_costumer';


-- Explore deeply feature in each of columns
DESCRIBE gold.dim_costumer;
DESCRIBE gold.dim_product;
DESCRIBE gold.fact_sales;




-- 2) Dimensios Exploration 

-- Explore All Contries our costumer come from
SELECT DISTINCT
	country 
FROM gold.dim_costumer;


-- Explore All Category the "Majore devision"
SELECT DISTINCT
	category,
	subcategory,
	product_name 
FROM gold.dim_product
ORDER BY 1, 2, 3;







-- 3) Date Exploration


-- Find The Date Of The First and Last order And range Between

SELECT 
	MIN(order_date) first_date,
	MAX(order_date) last_date,
	DATEDIFF(MAX(order_date), MIN(order_date)) / 365 range_order_by_year
FROM gold.fact_sales;


SELECT 
    MIN(order_date) AS first_date,
    MAX(order_date) AS last_date,
    TIMESTAMPDIFF(YEAR, MIN(order_date), MAX(order_date)) AS range_order_by_year
FROM gold.fact_sales;


-- Rounded The Value of Year

SELECT 
    MIN(order_date) AS first_date,
    MAX(order_date) AS last_date,
    DATEDIFF(MAX(order_date), MIN(order_date)) DIV 365 AS range_order_by_year
FROM gold.fact_sales;


SELECT 
    MIN(order_date) AS first_date,
    MAX(order_date) AS last_date,
    CAST(DATEDIFF(MAX(order_date), MIN(order_date)) / 365 AS UNSIGNED) AS range_order_by_year
FROM gold.fact_sales;



SELECT 
    MIN(order_date) AS first_date,
    MAX(order_date) AS last_date,
    ROUND(DATEDIFF(MAX(order_date), MIN(order_date)) / 365) AS range_order_by_year
FROM gold.fact_sales;


-- Find The Youngest and Older Customer

SELECT
	Max(birthdate),
	TIMESTAMPDIFF(YEAR, MAX(birthdate), CURRENT_DATE()) Age_youngest_costumer,
	Min(birthdate),
	TIMESTAMPDIFF(YEAR, MIN(birthdate), CURRENT_DATE()) Age_Oldest_costumer
FROM gold.dim_costumer;







-- 4) Measures Exploration


-- Find The total Of Sales
SELECT
	SUM(sales) total_sales
FROM gold.fact_sales;



-- Find How many items are sold
SELECT
	SUM(quantity) total_quantity
FROM gold.fact_sales;



-- Find The Average Selling Price
SELECT
	AVG(price) avg_price
FROM gold.fact_sales;


-- Find the total number of order
SELECT
	COUNT(order_number) Total_order
FROM gold.fact_sales;


SELECT
	COUNT(DISTINCT order_number) Total_order
FROM gold.fact_sales;


-- Find The Total Number Of Product
SELECT
	COUNT(product_name) Total_product
FROM gold.dim_product; 

SELECT
	COUNT(DISTINCT product_name) Total_product
FROM gold.dim_product;



-- Find total number of Costumer
SELECT
	COUNT(costumer_id) Total_costumer
FROM gold.dim_costumer;


-- Find The Total of Costumer has placed an order
SELECT
	COUNT(DISTINCT costumer_key)
FROM gold.fact_sales; 


SELECT
	costumer_key, COUNT(order_number) total_order_of_each_costumer 
FROM gold.fact_sales
GROUP BY costumer_key
HAVING COUNT(order_number) = 0
ORDER BY 1;


-- generate a report that show all key metrics of the business

SELECT
	'Total Sales' AS measure_name,
	SUM(sales) As mesuare_value
FROM gold.fact_sales
UNION ALL
SELECT
	'Total Nbr quantity',
	SUM(quantity) 
FROM gold.fact_sales
UNION ALL
SELECT
	'Average price',
	ROUND(AVG(price)) 
FROM gold.fact_sales
UNION ALL
SELECT
	'Total Nbr order',
	COUNT(order_number) 
FROM gold.fact_sales
UNION ALL
SELECT
	'Total Nbr product',
	COUNT(DISTINCT product_name) 
FROM gold.dim_product
UNION ALL
SELECT
	'Total Nbr Costumer',
	COUNT(costumer_id) 
FROM gold.dim_costumer;








-- 5) Analyse magnitude

-- Finde total costumer by country
SELECT
	country,
	COUNT(costumer_key) Total_costumer
FROM gold.dim_costumer
GROUP BY country
ORDER BY 2 DESC;


-- Find Total Costumer by gneder
SELECT
	gender,
	COUNT(costumer_key) Total_costumer
FROM gold.dim_costumer
GROUP BY gender
ORDER BY 2 DESC;


-- Find total product by category
SELECT 
	category,
	COUNT(product_key)
FROM gold.dim_product
GROUP BY category 
ORDER BY 2 DESC;

-- What is Average cost in each category
SELECT
	category,
	Round(AVG(cost))
FROM gold.dim_product
GROUP BY category
ORDER BY 2 DESC;


-- What its total revenu generetad by each category?
SELECT
	category,
	SUM(fs.sales) total_sales
FROM gold.fact_sales fs  LEFT JOIN gold.dim_product dp
ON fs.product_key = dp.product_key 
GROUP BY category
ORDER BY 2 DESC;


-- Find the total revenu genereted by each costumer?

SELECT 
	costumer_key,
	SUM(sales)
FROM gold.fact_sales
GROUP BY costumer_key
ORDER BY 2 DESC; 


SELECT  
	fs.costumer_key,
	SUM(sales)
FROM gold.fact_sales fs LEFT JOIN gold.dim_costumer dc 
ON fs.costumer_key = dc.costumer_key 
GROUP BY fs.costumer_key
ORDER BY 2 DESC; 


-- What is distrubtion of item sold across country?

SELECT
	dc.country,
	dp.subcategory,
	dp.product_name,
	COUNT(dp.product_name)
FROM gold.fact_sales fs  LEFT JOIN gold.dim_costumer dc
ON fs.costumer_key = dc.costumer_key
LEFT JOIN gold.dim_product dp
ON fs.product_key = dp.product_key
GROUP BY dc.country, dp.subcategory, dp.product_name
ORDER BY 1, 3 DESC;







-- 6) Ranking Analysis


-- What is the 5 product generate the highest revenue?

SELECT
	dp.product_name,
	SUM(fs.sales) Total_revenue
FROM gold.fact_sales fs LEFT JOIN gold.dim_product dp 
ON fs.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY 2 DESC
LIMIT 5;



SELECT *
FROM(
	SELECT
		dp.product_name,
		SUM(fs.sales) Total_revenue,
		RANK() OVER(ORDER BY SUM(fs.sales) DESC) top_product
	FROM gold.fact_sales fs LEFT JOIN gold.dim_product dp 
	ON fs.product_key = dp.product_key
	GROUP BY dp.product_name
) t
WHERE top_product <= 5;




-- What is the 5 worst-prformance product in terms of sales?
SELECT
	dp.product_name,
	SUM(fs.sales) Total_revenue
FROM gold.fact_sales fs LEFT JOIN gold.dim_product dp 
ON fs.product_key = dp.product_key
GROUP BY dp.product_name
ORDER BY 2
LIMIT 5;



SELECT *
FROM(
	SELECT
		dp.product_name,
		SUM(fs.sales) Total_revenue,
		RANK() OVER(ORDER BY SUM(fs.sales)) top_product
	FROM gold.fact_sales fs LEFT JOIN gold.dim_product dp 
	ON fs.product_key = dp.product_key
	GROUP BY dp.product_name
) t
WHERE top_product <= 5;













