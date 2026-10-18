--Query 1: Revenue by month (joins the fact to dim_date)
SELECT d.YEAR, d.MONTH, d.MONTH_NAME,
       ROUND(SUM(f.NET_PRICE), 2) AS REVENUE,
       COUNT(*) AS ORDERS
FROM fact_sales f
JOIN dim_date d ON f.DATE_KEY = d.DATE_KEY
GROUP BY d.YEAR, d.MONTH, d.MONTH_NAME
ORDER BY d.YEAR, d.MONTH

--Query 2: Revenue by quarter and region (joins two dimensions)
SELECT d.QUARTER, c.REGION,
       ROUND(SUM(f.NET_PRICE), 2) AS REVENUE
FROM fact_sales f
JOIN dim_date d     ON f.DATE_KEY = d.DATE_KEY
JOIN dim_customer c ON f.CUSTOMER_KEY = c.KUNNR
GROUP BY d.QUARTER, c.REGION
ORDER BY d.QUARTER, REVENUE DESC

--Query 3: Top 10 customers by revenue (leaves out the UNKNOWN row)
SELECT c.KUNNR, c.CUSTOMER_NAME, c.REGION,
       ROUND(SUM(f.NET_PRICE), 2) AS REVENUE
FROM fact_sales f
JOIN dim_customer c ON f.CUSTOMER_KEY = c.KUNNR
WHERE c.KUNNR <> 'UNKNOWN'
GROUP BY c.KUNNR, c.CUSTOMER_NAME, c.REGION
ORDER BY REVENUE DESC
LIMIT 10

--Query 4: Top 3 materials in each region (window function: ROW_NUMBER)
WITH region_material AS (
    SELECT c.REGION, f.MATERIAL,
           ROUND(SUM(f.NET_PRICE), 2) AS REVENUE
    FROM fact_sales f
    JOIN dim_customer c ON f.CUSTOMER_KEY = c.KUNNR
    GROUP BY c.REGION, f.MATERIAL
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REVENUE DESC) AS RN
    FROM region_material
)
SELECT REGION, MATERIAL, REVENUE, RN
FROM ranked
WHERE RN <= 3
ORDER BY REGION, RN

--Query 5: Running total of revenue by month (window function: SUM() OVER)
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
ORDER BY YEAR, MONTH

--Query 6: Month-over-month growth (window function: LAG)
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
ORDER BY YEAR, MONTH
