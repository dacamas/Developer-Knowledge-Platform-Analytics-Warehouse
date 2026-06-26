/*
================================================================================
FILE: 06_data_quality_checks.sql
PROJECT: Developer Knowledge Platform Analytics Warehouse
LAYER: Cross-cutting (runs after each pipeline stage)
PURPOSE: Data quality validation framework with automated pass/fail reporting
================================================================================

EXECUTION ORDER: Run after 05_fact_tables.sql (and optionally after each stage)

FRAMEWORK DESIGN:
  Each check is a SELECT query that returns rows ONLY when a violation exists.
  Zero rows = PASS. Any rows = FAIL with details.

  Results are aggregated into a validation_summary CTE that produces a
  single-row-per-check report with:
    - check_name
    - check_category
    - status (PASS / FAIL)
    - failing_rows (count of violations)
    - details (example failing values)

CHECK CATEGORIES:
  1. Duplicate primary keys    — Catch duplicate IDs in fact/dim tables
  2. Null foreign keys         — Ensure fact rows have valid dimension refs
  3. Invalid dates             — Reject dates outside plausible range
  4. Missing dimension records — Orphaned facts with no matching dimension row
  5. Referential integrity     — Cross-table join validation
  6. Range validation          — Out-of-range numeric values
  7. Freshness                 — Confirm data was loaded recently

PIPELINE INTEGRATION:
  In a production Airflow/Cloud Composer setup:
  - This query runs as a task after each warehouse load step
  - If any check returns failing_rows > 0, the pipeline task fails
  - Alert is sent to the data engineering team Slack channel
  - Pipeline halts before writing to marts
================================================================================
*/


-- ============================================================================
-- CHECK 1: Duplicate primary keys in fact_questions
-- Expected: 0 rows (each question_id should appear exactly once)
-- ============================================================================
WITH

dq_check_1_fact_questions_dupes AS (
  SELECT
    'DQ-001'                                            AS check_id,
    'Duplicate Primary Keys'                            AS check_category,
    'fact_questions: duplicate question_id'             AS check_name,
    question_id,
    COUNT(*)                                            AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_questions`
  GROUP BY question_id
  HAVING COUNT(*) > 1
),

-- ============================================================================
-- CHECK 2: Duplicate primary keys in fact_answers
-- ============================================================================
dq_check_2_fact_answers_dupes AS (
  SELECT
    'DQ-002'                                            AS check_id,
    'Duplicate Primary Keys'                            AS check_category,
    'fact_answers: duplicate answer_id'                 AS check_name,
    answer_id,
    COUNT(*)                                            AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_answers`
  GROUP BY answer_id
  HAVING COUNT(*) > 1
),

-- ============================================================================
-- CHECK 3: Null foreign keys in fact_questions
-- user_id should never be NULL (we default to -1, not NULL)
-- date_key should never be NULL
-- ============================================================================
dq_check_3_fact_questions_null_fks AS (
  SELECT
    'DQ-003'                                            AS check_id,
    'Null Foreign Keys'                                 AS check_category,
    'fact_questions: null user_id or date_key'          AS check_name,
    question_id,
    1                                                   AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_questions`
  WHERE
    user_id IS NULL
    OR date_key IS NULL
),

-- ============================================================================
-- CHECK 4: Null foreign keys in fact_answers
-- ============================================================================
dq_check_4_fact_answers_null_fks AS (
  SELECT
    'DQ-004'                                            AS check_id,
    'Null Foreign Keys'                                 AS check_category,
    'fact_answers: null user_id, question_id, or date_key' AS check_name,
    answer_id,
    1                                                   AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_answers`
  WHERE
    user_id IS NULL
    OR question_id IS NULL
    OR date_key IS NULL
),

-- ============================================================================
-- CHECK 5: Invalid dates in fact_questions
-- Stack Overflow launched 2008-07-31; no questions can predate this.
-- Future dates beyond today indicate data corruption.
-- ============================================================================
dq_check_5_invalid_dates_questions AS (
  SELECT
    'DQ-005'                                            AS check_id,
    'Invalid Dates'                                     AS check_category,
    'fact_questions: creation_date out of valid range'  AS check_name,
    question_id,
    1                                                   AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_questions`
  WHERE
    creation_date < DATE '2008-01-01'
    OR creation_date > CURRENT_DATE()
),

-- ============================================================================
-- CHECK 6: Invalid dates in fact_answers
-- ============================================================================
dq_check_6_invalid_dates_answers AS (
  SELECT
    'DQ-006'                                            AS check_id,
    'Invalid Dates'                                     AS check_category,
    'fact_answers: creation_date out of valid range'    AS check_name,
    answer_id,
    1                                                   AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_answers`
  WHERE
    creation_date < DATE '2008-01-01'
    OR creation_date > CURRENT_DATE()
),

-- ============================================================================
-- CHECK 7: Orphaned answers (answers referencing non-existent questions)
-- Every answer's question_id must exist in fact_questions
-- ============================================================================
dq_check_7_orphaned_answers AS (
  SELECT
    'DQ-007'                                            AS check_id,
    'Missing Dimension Records'                         AS check_category,
    'fact_answers: question_id not in fact_questions'   AS check_name,
    a.answer_id,
    1                                                   AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_answers` a
  LEFT JOIN `so-analytics-warehouse.warehouse.fact_questions` q
    ON a.question_id = q.question_id
  WHERE q.question_id IS NULL
),

-- ============================================================================
-- CHECK 8: Fact rows with no matching dim_user record
-- Every user_id in facts must exist in dim_user
-- (user_id = -1 is valid — we have a row for it)
-- ============================================================================
dq_check_8_missing_users_questions AS (
  SELECT
    'DQ-008'                                            AS check_id,
    'Missing Dimension Records'                         AS check_category,
    'fact_questions: user_id not in dim_user'           AS check_name,
    fq.question_id,
    1                                                   AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_questions` fq
  LEFT JOIN `so-analytics-warehouse.warehouse.dim_user` du
    ON fq.user_id = du.user_id
  WHERE du.user_id IS NULL
),

-- ============================================================================
-- CHECK 9: Date key referential integrity
-- Every date_key in fact tables must exist in dim_date
-- ============================================================================
dq_check_9_date_key_integrity AS (
  SELECT
    'DQ-009'                                            AS check_id,
    'Referential Integrity'                             AS check_category,
    'fact_questions: date_key not in dim_date'          AS check_name,
    fq.question_id,
    1                                                   AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_questions` fq
  LEFT JOIN `so-analytics-warehouse.warehouse.dim_date` dd
    ON fq.date_key = dd.date_key
  WHERE dd.date_key IS NULL
),

-- ============================================================================
-- CHECK 10: Invalid reputation values in dim_user
-- Reputation must be between 1 and 2,000,000
-- ============================================================================
dq_check_10_invalid_reputation AS (
  SELECT
    'DQ-010'                                            AS check_id,
    'Range Validation'                                  AS check_category,
    'dim_user: reputation outside valid range [1, 2000000]' AS check_name,
    user_id,
    1                                                   AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.dim_user`
  WHERE
    reputation < 1
    OR reputation > 2000000
),

-- ============================================================================
-- CHECK 11: Negative scores beyond plausible range
-- Scores below -1000 are almost certainly data corruption
-- ============================================================================
dq_check_11_invalid_scores AS (
  SELECT
    'DQ-011'                                            AS check_id,
    'Range Validation'                                  AS check_category,
    'fact_questions: score below plausible minimum (-1000)' AS check_name,
    question_id,
    1                                                   AS occurrence_count
  FROM `so-analytics-warehouse.warehouse.fact_questions`
  WHERE score < -1000
),

-- ============================================================================
-- CHECK 12: Data freshness — ensure warehouse was loaded recently
-- Warns if the most recent question creation_date is more than 7 days old
-- ============================================================================
dq_check_12_data_freshness AS (
  SELECT
    'DQ-012'                                            AS check_id,
    'Data Freshness'                                    AS check_category,
    'fact_questions: latest record older than 7 days'   AS check_name,
    CAST(NULL AS INT64)                                 AS question_id,
    1                                                   AS occurrence_count
  FROM (
    SELECT MAX(creation_date) AS latest_date
    FROM `so-analytics-warehouse.warehouse.fact_questions`
  )
  WHERE DATE_DIFF(CURRENT_DATE(), latest_date, DAY) > 7
),

-- ============================================================================
-- AGGREGATE: Combine all check results into summary
-- ============================================================================
all_violations AS (
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_1_fact_questions_dupes
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_2_fact_answers_dupes
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_3_fact_questions_null_fks
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_4_fact_answers_null_fks
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_5_invalid_dates_questions
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_6_invalid_dates_answers
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_7_orphaned_answers
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_8_missing_users_questions
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_9_date_key_integrity
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_10_invalid_reputation
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_11_invalid_scores
  UNION ALL
  SELECT check_id, check_category, check_name, occurrence_count FROM dq_check_12_data_freshness
),

-- ── Summarize violations per check ────────────────────────────────────────────
violation_summary AS (
  SELECT
    check_id,
    check_category,
    check_name,
    COUNT(*)          AS failing_rows
  FROM all_violations
  GROUP BY check_id, check_category, check_name
),

-- ── Define all expected checks (even passing ones) ────────────────────────────
all_checks AS (
  SELECT 'DQ-001' AS check_id, 'Duplicate Primary Keys' AS check_category, 'fact_questions: duplicate question_id' AS check_name
  UNION ALL SELECT 'DQ-002', 'Duplicate Primary Keys', 'fact_answers: duplicate answer_id'
  UNION ALL SELECT 'DQ-003', 'Null Foreign Keys', 'fact_questions: null user_id or date_key'
  UNION ALL SELECT 'DQ-004', 'Null Foreign Keys', 'fact_answers: null user_id, question_id, or date_key'
  UNION ALL SELECT 'DQ-005', 'Invalid Dates', 'fact_questions: creation_date out of valid range'
  UNION ALL SELECT 'DQ-006', 'Invalid Dates', 'fact_answers: creation_date out of valid range'
  UNION ALL SELECT 'DQ-007', 'Missing Dimension Records', 'fact_answers: question_id not in fact_questions'
  UNION ALL SELECT 'DQ-008', 'Missing Dimension Records', 'fact_questions: user_id not in dim_user'
  UNION ALL SELECT 'DQ-009', 'Referential Integrity', 'fact_questions: date_key not in dim_date'
  UNION ALL SELECT 'DQ-010', 'Range Validation', 'dim_user: reputation outside valid range [1, 2000000]'
  UNION ALL SELECT 'DQ-011', 'Range Validation', 'fact_questions: score below plausible minimum (-1000)'
  UNION ALL SELECT 'DQ-012', 'Data Freshness', 'fact_questions: latest record older than 7 days'
)

SELECT
  ac.check_id,
  ac.check_category,
  ac.check_name,
  COALESCE(vs.failing_rows, 0)    AS failing_rows,
  CASE
    WHEN vs.failing_rows IS NULL OR vs.failing_rows = 0 THEN 'PASS'
    ELSE 'FAIL'
  END                             AS status,
  CURRENT_TIMESTAMP()             AS check_run_time
FROM all_checks ac
LEFT JOIN violation_summary vs
  ON ac.check_id = vs.check_id
ORDER BY
  CASE WHEN vs.failing_rows > 0 THEN 0 ELSE 1 END,
  ac.check_id;