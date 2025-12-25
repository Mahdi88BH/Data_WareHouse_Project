
DROP VIEW IF EXISTS gold.report_costumers;


CREATE VIEW gold.report_costumers AS
WITH base_query AS(
	SELECT
		fs.order_number,
		fs.product_key,
		fs.order_date,
		fs.sales,
		fs.quantity,
		dc.costumer_key,
		dc.costumer_number,
		CONCAT(dc.first_name, ' ', dc.last_name) AS Full_Name,
		TIMESTAMPDIFF(YEAR, dc.birthdate, CURDATE()) AS age
	FROM gold.fact_sales fs LEFT JOIN gold.dim_costumer dc
		ON fs.costumer_key = dc.costumer_key
	WHERE fs.order_date IS NOT NULL
),
costumer_agregation AS(
	SELECT
		costumer_key,
		costumer_number,
		Full_Name,
		age,
		COUNT(DISTINCT	order_number) total_order,
		SUM(sales) total_spending,
		SUM(quantity) total_quantity,
		COUNT(DISTINCT product_key) total_product,
		MAX(order_date) last_order,
		TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) life_spean
	FROM base_query
	GROUP BY
		costumer_key,
		costumer_number,
		Full_Name,
		age
)
SELECT
		costumer_key,
		costumer_number,
		Full_Name,
		age,
		CASE
			WHEN age < 20 THEN 'Under 20'
			WHEN age BETWEEN 20 AND 30 THEN '20-30'
			WHEN age BETWEEN 31 AND 40 THEN '31-40'
			WHEN age BETWEEN 41 AND 50 THEN '41-50'
			ELSE 'above 50'
		END age_group,
		CASE 
			WHEN life_spean >= 12 AND total_spending >= 5000 THEN 'VIP'
			WHEN life_spean < 12 AND total_spending < 5000 THEN 'Regular'
			ELSE 'New'
		END costumer_sugmentation,
		last_order,
		TIMESTAMPDIFF(MONTH, last_order, CURDATE()) AS recency,
		total_order,
		total_spending,
		total_quantity,
		total_product,
		life_spean,
		CASE
			WHEN total_spending = 0 THEN 0
			ELSE ROUND(total_spending / total_order)
		END AS avg_order_value,
		CASE
			WHEN life_spean = 0 THEN total_spending
			ELSE ROUND(total_spending / life_spean)
		END	avg_mounthly_spean
FROM costumer_agregation;



SELECT *
FROM gold.report_costumers;


SELECT
	age_group,
	SUM(total_spending) AS total_sales
FROM gold.report_costumers
GROUP BY age_group
ORDER BY 2 DESC;

