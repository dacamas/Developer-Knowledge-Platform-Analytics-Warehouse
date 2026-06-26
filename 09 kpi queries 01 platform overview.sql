/*
================================================================================
FILE: 09_kpi_queries.sql
PROJECT: Developer Knowledge Platform Analytics Warehouse
LAYER: Analytics / Reporting
PURPOSE: Executive-level KPI queries for leadership reporting
================================================================================

EXECUTION ORDER: Run ad-hoc or as scheduled queries feeding dashboards

These queries answer the core business questions defined in the project brief.
Each query is designed to be run independently or embedded in Tableau.
They hit the marts layer — not the warehouse — for maximum performance.

QUERIES:
  KPI-01: Platform Overview Metrics
  KPI-02: Fastest Growing Technologies (YoY)
  KPI-03: Declining Technologies (YoY)
  KPI-04: Highest Engagement Technologies
  KPI-05: Tags with Highest Unanswered Rates
  KPI-06: Top Contributors by Contribution Score
  KPI-07: Healthiest Technology Communities
  KPI-08: Contributor Retention Trend
  KPI-09: Emerging Technologies (new entrants in last 3 years)
  KPI-10: Technologies Losing Popularity
================================================================================
*/


-- ============================================================================
-- KPI-01: Platform Overview — Executive Summary Metrics
-- Business Question: What is the overall state of the platform?
-- Powers: Executive Overview dashboard page (KPI tiles)
-- ============================================================================
SELECT
  -- ── Volume metrics ────────────────────────────────────────────────────────
  (SELECT COUNT(DISTINCT question_id) FROM `so-analytics-warehouse.warehouse.fact_questions`)
    AS total_questions,

  (SELECT COUNT(DISTINCT answer_id) FROM `so-analytics-warehouse.warehouse.fact_answers`)
    AS total_answers,

  (SELECT COUNT(DISTINCT user_id)
   FROM `so-analytics-warehouse.warehouse.dim_user`
   WHERE user_id > 0)
    AS total_registered_users,

  (SELECT COUNT(DISTINCT user_id)
   FROM `so-analytics-warehouse.warehouse.dim_user`
   WHERE is_active = TRUE AND user_id > 0)
    AS active_users_last_365_days,

  (SELECT COUNT(DISTINCT tag_name)
   FROM `so-analytics-warehouse.warehouse.dim_tag`)
    AS total_distinct_technologies,

  -- ── Quality metrics ───────────────────────────────────────────────────────
  ROUND(
    (SELECT 100.0 * COUNTIF(answer_count > 0) / NULLIF(COUNT(*), 0)
     FROM `so-analytics-warehouse.warehouse.fact_questions`),
    2
  ) AS platform_answer_rate_pct,

  ROUND(
    (SELECT 100.0 * COUNTIF(has_accepted_answer = TRUE) / NULLIF(COUNT(*), 0)
     FROM `so-analytics-warehouse.warehouse.fact_questions`),
    2
  ) AS platform_acceptance_rate_pct,

  -- ── Engagement metrics ────────────────────────────────────────────────────
  ROUND(
    (SELECT AVG(engagement_score)
     FROM `so-analytics-warehouse.warehouse.fact_questions`),
    2
  ) AS avg_question_engagement_score,

  -- ── Recency metrics ───────────────────────────────────────────────────────
  (SELECT MAX(creation_date) FROM `so-analytics-warehouse.warehouse.fact_questions`)
    AS last_question_date,

  (SELECT MAX(creation_date) FROM `so-analytics-warehouse.warehouse.fact_answers`)
    AS last_answer_date,

  CURRENT_TIMESTAMP() AS report_generated_at;
