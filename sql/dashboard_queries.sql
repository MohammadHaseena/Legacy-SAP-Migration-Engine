-- ============================================================
-- Dashboard datasets: SAP Sales Lakehouse
-- Each query feeds one or more visuals on the dashboard.
-- ============================================================


-- Dataset 1: kpis
-- Feeds three counters: Total Revenue, Total Orders, Quarantine Rate %
SELECT
  (SELECT ROUND(SUM(NET_PRICE), 2) FROM workspace.default.fact_sales) AS total_revenue,
  (SELECT COUNT(*) FROM workspace.default.fact_sales)                 AS total_orders,
  (SELECT COUNT(*) FROM workspace.default.quarantine_error_log)       AS quarantined_rows,
  ROUND(100.0 * (SELECT COUNT(*) FROM workspace.default.quarantine_error_log) /
        ((SELECT COUNT(*) FROM workspace.default.fact_sales) +
         (SELECT COUNT(*) FROM workspace.default.quarantine_error_log)), 2) AS quarantine_rate_pct;


-- Dataset 2: revenue_by_month
-- Feeds the Monthly Revenue chart (MONTH keeps calendar order)
SELECT d.YEAR, d.MONTH, d.MONTH_NAME,
       ROUND(SUM(f.NET_PRICE), 2) AS revenue
FROM workspace.default.fact_sales f
JOIN workspace.default.dim_date d ON f.DATE_KEY = d.DATE_KEY
GROUP BY d.YEAR, d.MONTH, d.MONTH_NAME
ORDER BY d.YEAR, d.MONTH;


-- Dataset 3: revenue_by_region
-- Feeds the Revenue by Region bar chart
SELECT c.REGION,
       ROUND(SUM(f.NET_PRICE), 2) AS revenue,
       COUNT(*) AS orders
FROM workspace.default.fact_sales f
JOIN workspace.default.dim_customer c ON f.CUSTOMER_KEY = c.KUNNR
GROUP BY c.REGION
ORDER BY revenue DESC;


-- Dataset 4: top_customers
-- Feeds the Top 10 Customers table (the UNKNOWN row is excluded)
SELECT c.CUSTOMER_NAME, c.REGION,
       ROUND(SUM(f.NET_PRICE), 2) AS revenue
FROM workspace.default.fact_sales f
JOIN workspace.default.dim_customer c ON f.CUSTOMER_KEY = c.KUNNR
WHERE c.KUNNR <> 'UNKNOWN'
GROUP BY c.CUSTOMER_NAME, c.REGION
ORDER BY revenue DESC
LIMIT 10;


-- Dataset 5: quarantine_by_reason
-- Feeds the Quarantined Rows by Reason bar chart (data quality)
SELECT ERROR_REASON, COUNT(*) AS rows_quarantined
FROM workspace.default.quarantine_error_log
GROUP BY ERROR_REASON
ORDER BY rows_quarantined DESC;
