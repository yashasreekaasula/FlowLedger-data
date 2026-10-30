PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS dim_merchant (
 merchant_id TEXT PRIMARY KEY, merchant_name TEXT NOT NULL, region TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fact_order (
 order_id TEXT PRIMARY KEY, merchant_id TEXT NOT NULL REFERENCES dim_merchant,
 order_date TEXT NOT NULL, amount_cents INTEGER NOT NULL CHECK(amount_cents>0),
 currency TEXT NOT NULL CHECK(currency='INR'));
CREATE TABLE IF NOT EXISTS fact_payment (
 event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL REFERENCES fact_order,
 event_time TEXT NOT NULL, amount_cents INTEGER NOT NULL CHECK(amount_cents>0),
 currency TEXT NOT NULL CHECK(currency='INR'), status TEXT NOT NULL,
 fingerprint TEXT NOT NULL, source_file TEXT NOT NULL, ingested_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_payment_order ON fact_payment(order_id);
CREATE INDEX IF NOT EXISTS ix_payment_time ON fact_payment(event_time);
CREATE TABLE IF NOT EXISTS quarantine (
 reject_id TEXT PRIMARY KEY, event_id TEXT, reason TEXT NOT NULL,
 raw_json TEXT NOT NULL, source_file TEXT NOT NULL, first_seen TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pipeline_run (
 run_key TEXT PRIMARY KEY, mode TEXT NOT NULL, completed_at TEXT NOT NULL,
 input_rows INTEGER NOT NULL, inserted_rows INTEGER NOT NULL,
 rejected_rows INTEGER NOT NULL, duplicate_rows INTEGER NOT NULL,
 duration_seconds REAL NOT NULL);
CREATE VIEW IF NOT EXISTS order_reconciliation AS
 SELECT o.order_id,o.order_date,m.merchant_name,m.region,o.amount_cents AS expected_cents,
 COALESCE(SUM(CASE WHEN p.status='SUCCESS' THEN p.amount_cents ELSE 0 END),0) AS paid_cents,
 COALESCE(SUM(CASE WHEN p.status='SUCCESS' THEN p.amount_cents ELSE 0 END),0)-o.amount_cents AS difference_cents,
 CASE WHEN COALESCE(SUM(CASE WHEN p.status='SUCCESS' THEN p.amount_cents ELSE 0 END),0)=0 THEN 'UNPAID'
 WHEN SUM(CASE WHEN p.status='SUCCESS' THEN p.amount_cents ELSE 0 END)=o.amount_cents THEN 'MATCHED'
 ELSE 'MISMATCH' END AS reconciliation_status
 FROM fact_order o JOIN dim_merchant m USING(merchant_id)
 LEFT JOIN fact_payment p USING(order_id) GROUP BY o.order_id;
CREATE VIEW IF NOT EXISTS daily_revenue AS
 SELECT substr(p.event_time,1,10) AS event_date,m.region,
 SUM(CASE WHEN p.status='SUCCESS' THEN p.amount_cents ELSE 0 END)/100.0 AS revenue_inr,
 COUNT(*) AS payment_attempts,
 ROUND(100.0*SUM(CASE WHEN p.status='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS failure_pct
 FROM fact_payment p JOIN fact_order o USING(order_id) JOIN dim_merchant m USING(merchant_id)
 GROUP BY event_date,m.region;
