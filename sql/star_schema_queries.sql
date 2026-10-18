-- ============================================================
-- Star schema queries: fact_sales + dim_customer, dim_date, dim_material
-- fact_sales joins to each dimension through a key:
--   CUSTOMER_KEY -> dim_customer.KUNNR
--   DATE_KEY     -> dim_date.DATE_KEY
--   MATERIAL     -> dim_material.MATERIAL
-- ============================================================


-- Query 1: How much revenue do we make each month?
-- dim_date lets us group by month without calculating it from raw dates.
SELECT d.YEAR, d.MONTH, d.MONTH_NAME,
       ROUND(SUM(f.NET_PRICE), 2) AS REVENUE,
       COUNT(*) AS ORDERS
FROM fact_sales f
JOIN dim_date d ON f.DATE_KEY = d.DATE_KEY
GROUP BY d.YEAR, d.MONTH, d.MONTH_NAME
ORDER BY d.YEAR, d.MONTH;


-- Query 2: Which regions earn the most in each quarter?
-- Joins the fact table to two dimensions at once.
SELECT d.QUARTER, c.REGION,
       ROUND(SUM(f.NET_PRICE), 2) AS REVENUE
FROM fact_sales f
JOIN dim_date d     ON f.DATE_KEY = d.DATE_KEY
JOIN dim_customer c ON f.CUSTOMER_KEY = c.KUNNR
GROUP BY d.QUARTER, c.REGION
ORDER BY d.QUARTER, REVENUE DESC;


-- Query 3: Who are our top 10 customers by revenue?
-- The UNKNOWN row (orders with no matching customer) is excluded.
SELECT c.KUNNR, c.CUSTOMER_NAME, c.REGION,
       ROUND(SUM(f.NET_PRICE), 2) AS REVENUE
FROM fact_sales f
JOIN dim_customer c ON f.CUSTOMER_KEY = c.KUNNR
WHERE c.KUNNR <> 'UNKNOWN'
GROUP BY c.KUNNR, c.CUSTOMER_NAME, c.REGION
ORDER BY REVENUE DESC
LIMIT 10;


-- Query 4: What are the top 3 materials in each region?
-- "Top N per group" pattern: ROW_NUMBER() with PARTITION BY.
WITH region_material AS (
    -- total revenue for every region + material pair
    SELECT c.REGION, f.MATERIAL,
           ROUND(SUM(f.NET_PRICE), 2) AS REVENUE
    FROM fact_sales f
    JOIN dim_customer c ON f.CUSTOMER_KEY = c.KUNNR
    GROUP BY c.REGION, f.MATERIAL
),
ranked AS (
    -- PARTITION BY REGION restarts the numbering for each region;
    -- ORDER BY REVENUE DESC gives rank 1 to the biggest seller
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REVENUE DESC) AS RN
    FROM region_material
)
-- keep only ranks 1 to 3 from each region
SELECT REGION, MATERIAL, REVENUE, RN
FROM ranked
WHERE RN <= 3
ORDER BY REGION, RN;


-- Query 5: What is the cumulative revenue month after month?
-- SUM() OVER (ORDER BY ...) adds each month to all the months before it.
WITH monthly AS (
    SELECT d.YEAR, d.MONTH,
           ROUND(SUM(f.NET_PRICE), 2) AS REVENUE
    FROM fact_sales f
    JOIN dim_date d ON f.DATE_KEY = d.DATE_KEY
    GROUP BY d.YEAR, d.MONTH
)
SELECT YEAR, MONTH, REVENUE,
       ROUND(SUM(REVENUE) OVER (ORDER BY YEAR, MONTH), 2) AS RUNNING_TOTAL
FROM monthly
ORDER BY YEAR, MONTH;


-- Query 6: How much did revenue grow compared with the previous month?
-- LAG() reads the value from the previous row (the previous month).
-- The first month has no previous month, so PREV_MONTH and GROWTH_PCT are NULL.
WITH monthly AS (
    SELECT d.YEAR, d.MONTH,
           ROUND(SUM(f.NET_PRICE), 2) AS REVENUE
    FROM fact_sales f
    JOIN dim_date d ON f.DATE_KEY = d.DATE_KEY
    GROUP BY d.YEAR, d.MONTH
)
SELECT YEAR, MONTH, REVENUE,
       LAG(REVENUE) OVER (ORDER BY YEAR, MONTH) AS PREV_MONTH,
       ROUND((REVENUE - LAG(REVENUE) OVER (ORDER BY YEAR, MONTH))
             / LAG(REVENUE) OVER (ORDER BY YEAR, MONTH) * 100, 2) AS GROWTH_PCT
FROM monthly
ORDER BY YEAR, MONTH;
