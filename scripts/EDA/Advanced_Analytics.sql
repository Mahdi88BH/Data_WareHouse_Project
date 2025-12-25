
-- 6) Change Over Times "Trends"


SELECT
	order_date,
	SUM(sales) total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY 1;



SELECT
	YEAR(order_date) AS  order_year,
	SUM(sales) total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY 1;


SELECT
	YEAR(order_date) AS  order_year,
	COUNT(DISTINCT costumer_key) AS number_of_costumer,
	SUM(sales) total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date) 
ORDER BY 1;


SELECT
	MONTH(order_date) AS  order_year,
	SUM(sales) total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY 1;


SELECT
	YEAR(order_date) AS  order_year,
	COUNT(DISTINCT costumer_key) AS number_of_costumer,
	SUM(sales) total_sales,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date) 
ORDER BY 1;


SELECT
    MAKEDATE(YEAR(order_date), 1) AS order_year,
    COUNT(DISTINCT costumer_key ) AS number_of_customer,
    SUM(sales) AS total_sales,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MAKEDATE(YEAR(order_date), 1)
ORDER BY order_year;



SELECT
    MAKEDATE(YEAR(order_date), 1)
      + INTERVAL (MONTH(order_date) - 1) MONTH AS order_month,
    SUM(sales) AS total_sales,
    COUNT(DISTINCT costumer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY
    MAKEDATE(YEAR(order_date), 1)
      + INTERVAL (MONTH(order_date) - 1) MONTH
ORDER BY 1;



SELECT 
    DATE_FORMAT(order_date, '%Y-%M'),
    SUM(sales) AS total_sales,
    COUNT(DISTINCT costumer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_FORMAT(order_date, '%Y-%M')
ORDER BY 1;








-- 7) Cumulative Analysis


SELECT 
	order_date_by_year,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_date_by_year) AS running_total_sales
FROM (
	SELECT
	    YEAR(order_date) order_date_by_year,
	    SUM(sales) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY YEAR(order_date)
	ORDER BY 1
) t;




SELECT 
	order_date_by_year,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_date_by_year) AS running_total_sales,
	avg_price,
	SUM(avg_price) OVER(ORDER BY order_date_by_year) AS running_avg_price
FROM (
	SELECT
	    YEAR(order_date) order_date_by_year,
	    SUM(sales) AS total_sales,
	    AVG(price) AS avg_price
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY YEAR(order_date)
	ORDER BY 1
) t;




SELECT 
	order_date_by_month,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_date_by_month) AS running_total_sales
FROM (
	SELECT
	    MAKEDATE(YEAR(order_date), 1)
	      + INTERVAL (MONTH(order_date) - 1) MONTH AS order_date_by_month,
	    SUM(sales) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY
	    MAKEDATE(YEAR(order_date), 1)
	      + INTERVAL (MONTH(order_date) - 1) MONTH
	ORDER BY 1
) t;







-- 8) performance Analysis

WITH yearly_product_sales AS (
	SELECT
		YEAR(fs.order_date) AS order_year, 
		dp.product_name,
		SUM(fs.sales) AS total_sales 
	FROM gold.fact_sales fs LEFT JOIN gold.dim_product dp
		ON fs.product_key = dp.product_key
	WHERE fs.order_date IS NOT NULL
	GROUP BY YEAR(fs.order_date), dp.product_name
)
SELECT
	order_year,
	product_name,
	total_sales,
	AVG(total_sales) OVER(PARTITION BY product_name) AS avg_sales,
	total_sales - AVG(total_sales) OVER(PARTITION BY product_name) AS diff_avg,
	CASE 
		WHEN total_sales - AVG(total_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Average'
		WHEN total_sales - AVG(total_sales) OVER(PARTITION BY product_name) < 0 THEN 'below Average'
		ELSE 'Average'
	END AS 'Flag Average',
	LEAD(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS previos_sales,
	total_sales - LEAD(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS Flags_changes,
	CASE 
		WHEN total_sales - LEAD(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
		WHEN total_sales - LEAD(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
		ELSE 'No change'
	END AS 'Flag Evolution'
FROM yearly_product_sales;










-- 9) Part to whole Analysis

WITH category_sales AS (
	SELECT
		dp.category,
		SUM(fs.sales) AS total_sales 
	FROM gold.fact_sales fs LEFT JOIN gold.dim_product dp
		ON fs.product_key = dp.product_key
	GROUP BY category
)
SELECT 
	category,
	total_sales,
 	CONCAT(ROUND((total_sales / SUM(total_sales) OVER()) * 100, 2), ' %') AS 'overall_sales'
FROM category_sales
ORDER By total_sales DESC;













-- 10) Data Segmentation



WITH product_range AS(
	SELECT
		product_key,
		product_name,
		cost,
		CASE
			WHEN cost < 100 THEN 'below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			ELSE 'Above 500'
		END AS range_cost
	FROM gold.dim_product
)
SELECT
	range_cost,
	COUNT(product_key)
FROM product_range
GROUP BY range_cost
ORDER BY 1;





WITH order_product AS (
	SELECT 
		dc.costumer_key,
		SUM(fs.sales) AS total_spending,
		MIN(fs.order_date) AS first_order,
		MAX(fs.order_date) AS last_order,
		TIMESTAMPDIFF(MONTH, MIN(fs.order_date), MAX(fs.order_date)) AS life_span
	FROM gold.fact_sales fs
	LEFT JOIN gold.dim_costumer dc
		ON fs.costumer_key = dc.costumer_key
	GROUP BY dc.costumer_key
)
SELECT 
	sc.costumer_segment,
	COUNT(sc.costumer_key) AS total_costumer
FROM (
	SELECT 
		costumer_key,
		CASE
			WHEN life_span <= 12 AND total_spending < 5000 THEN 'Regular'
			WHEN life_span > 12 AND total_spending > 5000 THEN 'VIP'
			ELSE 'New'
		END AS costumer_segment		
	FROM order_product
) AS sc
GROUP BY sc.costumer_segment
ORDER BY 2;

