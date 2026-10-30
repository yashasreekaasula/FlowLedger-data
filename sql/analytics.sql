-- Grain: one row per order. Units: integer paise in the warehouse.
SELECT * FROM order_reconciliation WHERE reconciliation_status <> 'MATCHED'
ORDER BY ABS(difference_cents) DESC;
-- Window function: rank regions by successful revenue within each date.
SELECT *, DENSE_RANK() OVER (PARTITION BY event_date ORDER BY revenue_inr DESC) AS revenue_rank
FROM daily_revenue ORDER BY event_date,revenue_rank;
-- Running revenue across event dates; late events can revise historical results.
WITH daily AS (SELECT event_date,SUM(revenue_inr) AS revenue FROM daily_revenue GROUP BY event_date)
SELECT *,SUM(revenue) OVER (ORDER BY event_date ROWS UNBOUNDED PRECEDING) AS running_revenue
FROM daily;
-- Quality and throughput investigation, NOT a fabricated performance benchmark.
SELECT mode,COUNT(*) AS committed_runs,SUM(input_rows) AS inputs,
SUM(inserted_rows) AS inserts,SUM(rejected_rows) AS rejects,
ROUND(SUM(input_rows)/NULLIF(SUM(duration_seconds),0),2) AS observed_rows_per_second
FROM pipeline_run GROUP BY mode;
