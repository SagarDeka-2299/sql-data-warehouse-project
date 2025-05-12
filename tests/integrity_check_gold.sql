-- Foreign key integrity (dimesions)
-- Expect: All records of sales must be returned along with joined table columns
SELECT * FROM
gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON c.customer_key = s.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = s.product_key;
