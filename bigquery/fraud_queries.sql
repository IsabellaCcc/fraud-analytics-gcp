-- ============================================================
-- Fraud Analytics Queries — BigQuery
-- Project:  fraud-analytics-gcp
-- Dataset:  fraud_analytics
-- Source:   migrated from the DuckDB fraud-analytics-agent project
-- ============================================================
--
-- Join keys:
--   transactions.id      = fraud_labels.transaction_id
--   transactions.card_id = cards.id
--   transactions.mcc     = mcc_codes.mcc_code
--   fraud_labels.is_fraud is BOOLEAN
--
-- All queries read from transactions_optimized (partitioned by
-- DATE(date), clustered by mcc, merchant_state) for lower cost.
-- ============================================================


-- ------------------------------------------------------------
-- 1. Overall fraud rate (baseline)
--    Confirms the ~0.15% fraud rate from the original project.
-- ------------------------------------------------------------
SELECT
  COUNT(*)                                            AS total_txns,
  COUNTIF(f.is_fraud)                                 AS fraud_txns,
  ROUND(100 * COUNTIF(f.is_fraud) / COUNT(*), 4)      AS fraud_rate_pct
FROM `fraud-analytics-gcp.fraud_analytics.transactions_optimized` AS t
JOIN `fraud-analytics-gcp.fraud_analytics.fraud_labels`           AS f
  ON t.id = f.transaction_id;


-- ------------------------------------------------------------
-- 2. Fraud rate by merchant category (MCC)
--    Which kinds of merchants see the most fraud?
--    Filtered to categories with enough volume to be meaningful.
-- ------------------------------------------------------------
SELECT
  m.description                                       AS merchant_category,
  COUNT(*)                                            AS total_txns,
  COUNTIF(f.is_fraud)                                 AS fraud_txns,
  ROUND(100 * COUNTIF(f.is_fraud) / COUNT(*), 4)      AS fraud_rate_pct
FROM `fraud-analytics-gcp.fraud_analytics.transactions_optimized` AS t
JOIN `fraud-analytics-gcp.fraud_analytics.fraud_labels`           AS f
  ON t.id = f.transaction_id
LEFT JOIN `fraud-analytics-gcp.fraud_analytics.mcc_codes`         AS m
  ON t.mcc = m.mcc_code
GROUP BY merchant_category
HAVING total_txns >= 1000
ORDER BY fraud_rate_pct DESC
LIMIT 20;


-- ------------------------------------------------------------
-- 3. Fraud rate by card brand and card type
--    Joins through cards on card_id.
-- ------------------------------------------------------------
SELECT
  c.card_brand,
  c.card_type,
  COUNT(*)                                            AS total_txns,
  COUNTIF(f.is_fraud)                                 AS fraud_txns,
  ROUND(100 * COUNTIF(f.is_fraud) / COUNT(*), 4)      AS fraud_rate_pct
FROM `fraud-analytics-gcp.fraud_analytics.transactions_optimized` AS t
JOIN `fraud-analytics-gcp.fraud_analytics.fraud_labels`           AS f
  ON t.id = f.transaction_id
JOIN `fraud-analytics-gcp.fraud_analytics.cards`                  AS c
  ON t.card_id = c.id
GROUP BY c.card_brand, c.card_type
ORDER BY fraud_rate_pct DESC;


-- ------------------------------------------------------------
-- 4. Fraud rate by state
--    Uses the clustering column (merchant_state) for efficiency.
-- ------------------------------------------------------------
SELECT
  t.merchant_state,
  COUNT(*)                                            AS total_txns,
  COUNTIF(f.is_fraud)                                 AS fraud_txns,
  ROUND(100 * COUNTIF(f.is_fraud) / COUNT(*), 4)      AS fraud_rate_pct
FROM `fraud-analytics-gcp.fraud_analytics.transactions_optimized` AS t
JOIN `fraud-analytics-gcp.fraud_analytics.fraud_labels`           AS f
  ON t.id = f.transaction_id
WHERE t.merchant_state IS NOT NULL
GROUP BY t.merchant_state
HAVING total_txns >= 1000
ORDER BY fraud_rate_pct DESC
LIMIT 20;


-- ------------------------------------------------------------
-- 5. Fraud rate by hour of day
--    Tests the "fraud happens at odd hours" hypothesis.
-- ------------------------------------------------------------
SELECT
  EXTRACT(HOUR FROM t.date)                           AS hour_of_day,
  COUNT(*)                                            AS total_txns,
  COUNTIF(f.is_fraud)                                 AS fraud_txns,
  ROUND(100 * COUNTIF(f.is_fraud) / COUNT(*), 4)      AS fraud_rate_pct
FROM `fraud-analytics-gcp.fraud_analytics.transactions_optimized` AS t
JOIN `fraud-analytics-gcp.fraud_analytics.fraud_labels`           AS f
  ON t.id = f.transaction_id
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- ------------------------------------------------------------
-- 6. Average transaction amount: fraud vs legitimate
--    Are fraudulent charges larger on average?
-- ------------------------------------------------------------
SELECT
  f.is_fraud,
  COUNT(*)                                            AS txns,
  ROUND(AVG(t.amount), 2)                             AS avg_amount,
  ROUND(APPROX_QUANTILES(t.amount, 100)[OFFSET(50)], 2) AS median_amount,
  ROUND(MAX(t.amount), 2)                             AS max_amount
FROM `fraud-analytics-gcp.fraud_analytics.transactions_optimized` AS t
JOIN `fraud-analytics-gcp.fraud_analytics.fraud_labels`           AS f
  ON t.id = f.transaction_id
GROUP BY f.is_fraud;


-- ------------------------------------------------------------
-- 7. Monthly fraud trend over time
--    Uses a CTE + window function for a running cumulative count.
--    Demonstrates partition-pruning friendly date filtering.
-- ------------------------------------------------------------
WITH monthly AS (
  SELECT
    DATE_TRUNC(DATE(t.date), MONTH)                   AS month,
    COUNT(*)                                          AS total_txns,
    COUNTIF(f.is_fraud)                               AS fraud_txns
  FROM `fraud-analytics-gcp.fraud_analytics.transactions_optimized` AS t
  JOIN `fraud-analytics-gcp.fraud_analytics.fraud_labels`           AS f
    ON t.id = f.transaction_id
  GROUP BY month
)
SELECT
  month,
  total_txns,
  fraud_txns,
  ROUND(100 * fraud_txns / total_txns, 4)             AS fraud_rate_pct,
  SUM(fraud_txns) OVER (ORDER BY month)               AS cumulative_fraud
FROM monthly
ORDER BY month;
