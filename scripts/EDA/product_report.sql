
DROP VIEW IF EXISTS gold.report_produts;

CREATE VIEW gold.report_produts AS
with base_query AS(
	SELECT 
		fs.order_number,
		fs.order_date,
		fs.costumer_key,
		fs.sales,
		fs.quantity,
		dp.product_key,
		dp.product_name,
		dp.category,
		dp.subcategory,
		dp.cost 
	FROM gold.fact_sales fs LEFT JOIN gold.dim_product dp
		ON fs.product_key = dp.product_key
	WHERE fs.order_date IS NOT NULL
),
product_aggregation AS (
	SELECT 
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		TIMESTAMPDIFF(MONTH, MIN(order_date), MAX(order_date)) life_spane,
		MAX(order_date) last_order,
		COUNT(order_number) total_number,
		COUNT(costumer_key) total_costumer,
		SUM(sales) total_sales,
		SUM(quantity) total_quantity,
		ROUND(AVG(CAST(sales AS FLOAT) / NULLIF(quantity, 0)), 1) avg_selling_price 
	FROM base_query
	GROUP BY 
		product_key,
		product_name,
		category,
		subcategory,
		cost
)
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_order,
	TIMESTAMPDIFF(MONTH, last_order, CURDATE()) AS recency_in_month,
	CASE
		WHEN total_sales > 50000 THEN 'High-Performer'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END AS product_segment,
	CASE
		WHEN life_spane = 0 THEN total_sales
		ELSE ROUND(total_sales / life_spane)
	END AS avg_monthly_revenue
FROM product_aggregation;



SELECT *
FROM gold.report_produts;