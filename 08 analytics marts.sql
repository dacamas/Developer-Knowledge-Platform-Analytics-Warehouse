/*
================================================================================
FILE: 08_analytics_marts.sql
PROJECT: Developer Knowledge Platform Analytics Warehouse
LAYER: Analytics Marts (Platinum)
PURPOSE: Pre-aggregated business intelligence tables for Tableau dashboards
================================================================================

EXECUTION ORDER: Run after 05_fact_tables.sql (and after each incremental ETL)

MARTS CREATED:
  marts.mart_tag_growth              — YoY technology growth rates
  marts.mart_trending_technologies   — Composite popularity score & ranking
  marts.mart_unanswered_questions    — Unanswered rate by technology
  marts.mart_top_contributors        — Top contributor leaderboard
  marts.mart_contributor_retention   — Monthly cohort retention analysis
  marts.mart_answer_latency          — Response time metrics by technology
  marts.mart_community_health        — Composite health score per technology

DESIGN PRINCIPLES:
  1. Marts are fully refreshed daily (not incremental) — they are small
  2. All complex window functions computed here, not in Tableau
  3. Mart columns named for business readability, not technical naming
  4. No PII — user_id is preserved for linking, display_name is used for UI
  5. Marts reference warehouse layer, never raw or staging directly
================================================================================
*/


-- ============================================================================
-- MART 1: marts.mart_tag_growth
-- PURPOSE: Track year-over-year question volume growth per technology
-- KEY METRIC: growth_rate using LAG() to compare YoY
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.marts.mart_tag_growth`
OPTIONS (
  description = 'Annual question volume per technology with YoY growth rate. Uses LAG() window function. Powers Technology Trends dashboard page.',
  labels = [('layer', 'marts'), ('mart', 'tag_growth')]
)
AS
WITH

-- ── Step 1: Explode question-tag relationships ────────────────────────────────
-- Each question may have multiple tags; we create one row per question-tag pair
question_tags AS (
  SELECT
    fq.question_id,
    fq.creation_date,
    EXTRACT(YEAR FROM fq.creation_date)                     AS question_year,
    fq.score,
    fq.view_count,
    fq.answer_count,
    fq.engagement_score,
    fq.user_id,
    tag
  FROM `so-analytics-warehouse.warehouse.fact_questions` fq,
  UNNEST(SPLIT(fq.tags_clean, '|')) AS tag
  WHERE
    fq.tags_clean IS NOT NULL
    AND fq.tags_clean != ''
    AND TRIM(tag) != ''
),

-- ── Step 2: Aggregate by tag and year ────────────────────────────────────────
yearly_tag_volume AS (
  SELECT
    question_year,
    LOWER(TRIM(tag))                                        AS tag_name,
    COUNT(DISTINCT question_id)                             AS question_count,
    COUNT(DISTINCT user_id)                                 AS distinct_contributors,
    ROUND(AVG(score), 2)                                    AS avg_score,
    ROUND(AVG(view_count), 0)                               AS avg_views,
    ROUND(AVG(engagement_score), 4)                         AS avg_engagement,
    SUM(CASE WHEN answer_count > 0 THEN 1 ELSE 0 END)      AS answered_questions
  FROM question_tags
  WHERE question_year >= 2010   -- Pre-2010 has sparse data; exclude for clean trends
  GROUP BY question_year, tag
),

-- ── Step 3: Compute YoY growth rate using LAG() ───────────────────────────────
-- LAG() gets the previous year's value for the same tag
-- growth_rate = (current_year - prior_year) / prior_year * 100
tag_growth_with_lag AS (
  SELECT
    question_year,
    tag_name,
    question_count,
    distinct_contributors,
    avg_score,
    avg_views,
    avg_engagement,
    answered_questions,

    -- ── LAG: prior year's question count ─────────────────────────────────────
    LAG(question_count, 1) OVER (
      PARTITION BY tag_name
      ORDER BY question_year
    )                                                       AS prior_year_question_count,

    -- ── LAG: prior year's contributor count ──────────────────────────────────
    LAG(distinct_contributors, 1) OVER (
      PARTITION BY tag_name
      ORDER BY question_year
    )                                                       AS prior_year_contributors,

    -- ── YoY growth rate (%) ───────────────────────────────────────────────────
    ROUND(
      100.0 * (
        question_count
        - LAG(question_count, 1) OVER (PARTITION BY tag_name ORDER BY question_year)
      ) / NULLIF(
        LAG(question_count, 1) OVER (PARTITION BY tag_name ORDER BY question_year),
        0
      ),
      2
    )                                                       AS yoy_growth_rate_pct,

    -- ── Contributor growth rate ───────────────────────────────────────────────
    ROUND(
      100.0 * (
        distinct_contributors
        - LAG(distinct_contributors, 1) OVER (PARTITION BY tag_name ORDER BY question_year)
      ) / NULLIF(
        LAG(distinct_contributors, 1) OVER (PARTITION BY tag_name ORDER BY question_year),
        0
      ),
      2
    )                                                       AS contributor_growth_rate_pct,

    -- ── Running total of questions over all years ─────────────────────────────
    SUM(question_count) OVER (
      PARTITION BY tag_name
      ORDER BY question_year
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                       AS cumulative_question_count,

    -- ── Rank within year by volume ────────────────────────────────────────────
    RANK() OVER (
      PARTITION BY question_year
      ORDER BY question_count DESC
    )                                                       AS rank_by_volume_in_year

  FROM yearly_tag_volume
)

SELECT
  question_year,
  tag_name,
  question_count,
  prior_year_question_count,
  yoy_growth_rate_pct,
  distinct_contributors,
  prior_year_contributors,
  contributor_growth_rate_pct,
  avg_score,
  avg_views,
  avg_engagement,
  answered_questions,
  ROUND(100.0 * answered_questions / NULLIF(question_count, 0), 2) AS answer_rate_pct,
  cumulative_question_count,
  rank_by_volume_in_year,
  -- ── Growth classification ─────────────────────────────────────────────────
  CASE
    WHEN yoy_growth_rate_pct IS NULL        THEN 'New/Baseline'
    WHEN yoy_growth_rate_pct >= 50          THEN 'Explosive Growth'
    WHEN yoy_growth_rate_pct >= 20          THEN 'Strong Growth'
    WHEN yoy_growth_rate_pct >= 5           THEN 'Moderate Growth'
    WHEN yoy_growth_rate_pct >= -5          THEN 'Stable'
    WHEN yoy_growth_rate_pct >= -20         THEN 'Moderate Decline'
    ELSE 'Strong Decline'
  END                                                       AS growth_classification,
  CURRENT_TIMESTAMP()                                       AS mart_refreshed_at
FROM tag_growth_with_lag
WHERE tag_name IS NOT NULL AND tag_name != ''
ORDER BY question_year DESC, question_count DESC;


-- ============================================================================
-- MART 2: marts.mart_trending_technologies
-- PURPOSE: Composite popularity score for technology ranking
-- FORMULA: 40% question volume + 30% growth rate + 20% engagement + 10% contributor growth
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.marts.mart_trending_technologies`
OPTIONS (
  description = 'Composite technology popularity score and ranking. Weighted formula: 40% question volume, 30% growth rate, 20% engagement, 10% contributor growth. Powers Technology Trends page.',
  labels = [('layer', 'marts'), ('mart', 'trending_technologies')]
)
AS
WITH

-- ── Use the most recent complete year for trending analysis ──────────────────
current_year AS (
  SELECT MAX(question_year) - 1 AS analysis_year  -- Most recent complete year
  FROM `so-analytics-warehouse.marts.mart_tag_growth`
),

-- ── Get this year's and last year's data for trending technologies ────────────
latest_year_data AS (
  SELECT
    tg.tag_name,
    tg.question_year,
    tg.question_count,
    tg.yoy_growth_rate_pct,
    tg.avg_engagement,
    tg.contributor_growth_rate_pct,
    tg.distinct_contributors,
    tg.answer_rate_pct,
    tg.rank_by_volume_in_year,
    dt.tag_category
  FROM `so-analytics-warehouse.marts.mart_tag_growth` tg
  LEFT JOIN `so-analytics-warehouse.warehouse.dim_tag` dt
    ON tg.tag_name = dt.tag_name
  WHERE tg.question_year = (SELECT analysis_year FROM current_year)
  -- Filter to tags with meaningful volume (at least 100 questions this year)
  AND tg.question_count >= 100
),

-- ── Normalize each component to 0-100 scale for scoring ──────────────────────
normalized_metrics AS (
  SELECT
    tag_name,
    tag_category,
    question_count,
    yoy_growth_rate_pct,
    avg_engagement,
    contributor_growth_rate_pct,
    distinct_contributors,
    answer_rate_pct,
    rank_by_volume_in_year,

    -- Normalize question volume: (value - min) / (max - min) * 100
    ROUND(100.0 * (
      question_count - MIN(question_count) OVER ()
    ) / NULLIF(
      MAX(question_count) OVER () - MIN(question_count) OVER (), 0
    ), 2)                                                   AS norm_volume,

    -- Normalize growth rate (cap at -100% to +200% for normalization)
    ROUND(100.0 * (
      LEAST(GREATEST(COALESCE(yoy_growth_rate_pct, 0), -100), 200)
      - (-100)
    ) / 300.0, 2)                                           AS norm_growth,

    -- Normalize engagement score
    ROUND(100.0 * (
      avg_engagement - MIN(avg_engagement) OVER ()
    ) / NULLIF(
      MAX(avg_engagement) OVER () - MIN(avg_engagement) OVER (), 0
    ), 2)                                                   AS norm_engagement,

    -- Normalize contributor growth rate
    ROUND(100.0 * (
      LEAST(GREATEST(COALESCE(contributor_growth_rate_pct, 0), -100), 200)
      - (-100)
    ) / 300.0, 2)                                           AS norm_contributor_growth

  FROM latest_year_data
),

-- ── Apply weighted formula ────────────────────────────────────────────────────
scored AS (
  SELECT
    tag_name,
    tag_category,
    question_count,
    yoy_growth_rate_pct,
    avg_engagement,
    contributor_growth_rate_pct,
    distinct_contributors,
    answer_rate_pct,
    rank_by_volume_in_year,
    norm_volume,
    norm_growth,
    norm_engagement,
    norm_contributor_growth,

    -- ── Composite popularity score (0-100) ───────────────────────────────────
    -- Weights: 40% volume, 30% growth, 20% engagement, 10% contributor growth
    ROUND(
      (0.40 * norm_volume)
      + (0.30 * norm_growth)
      + (0.20 * norm_engagement)
      + (0.10 * norm_contributor_growth),
      2
    )                                                       AS popularity_score

  FROM normalized_metrics
)

SELECT
  tag_name,
  tag_category,
  popularity_score,
  question_count,
  yoy_growth_rate_pct,
  avg_engagement,
  contributor_growth_rate_pct,
  distinct_contributors,
  answer_rate_pct,
  rank_by_volume_in_year,

  -- ── Overall popularity rank ───────────────────────────────────────────────
  RANK() OVER (ORDER BY popularity_score DESC)             AS popularity_rank,

  -- ── Rank within category ─────────────────────────────────────────────────
  RANK() OVER (
    PARTITION BY tag_category
    ORDER BY popularity_score DESC
  )                                                         AS rank_within_category,

  -- ── Trend tier classification ─────────────────────────────────────────────
  CASE
    WHEN RANK() OVER (ORDER BY popularity_score DESC) <= 10  THEN 'Top 10'
    WHEN RANK() OVER (ORDER BY popularity_score DESC) <= 25  THEN 'Top 25'
    WHEN RANK() OVER (ORDER BY popularity_score DESC) <= 50  THEN 'Top 50'
    WHEN RANK() OVER (ORDER BY popularity_score DESC) <= 100 THEN 'Top 100'
    ELSE 'Emerging'
  END                                                       AS trend_tier,

  CURRENT_TIMESTAMP()                                       AS mart_refreshed_at

FROM scored
ORDER BY popularity_rank;


-- ============================================================================
-- MART 3: marts.mart_unanswered_questions
-- PURPOSE: Identify technologies with unmet knowledge demand
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.marts.mart_unanswered_questions`
OPTIONS (
  description = 'Unanswered question rate by technology tag. Identifies knowledge gaps and unmet demand. Powers Community Health dashboard page.',
  labels = [('layer', 'marts'), ('mart', 'unanswered_questions')]
)
AS
WITH

tag_answer_stats AS (
  SELECT
    LOWER(TRIM(tag))                                        AS tag_name,
    COUNT(DISTINCT question_id)                             AS total_questions,
    COUNTIF(answer_count = 0)                               AS unanswered_questions,
    COUNTIF(answer_count > 0)                               AS answered_questions,
    COUNTIF(has_accepted_answer = TRUE)                     AS accepted_answer_questions,
    ROUND(AVG(answer_count), 2)                             AS avg_answers_per_question,
    ROUND(AVG(score), 2)                                    AS avg_question_score,
    ROUND(AVG(view_count), 0)                               AS avg_view_count,
    COUNT(DISTINCT user_id)                                 AS distinct_askers,
    SUM(view_count)                                         AS total_views
  FROM `so-analytics-warehouse.warehouse.fact_questions`,
  UNNEST(SPLIT(tags_clean, '|')) AS tag
  WHERE
    tags_clean IS NOT NULL
    AND tags_clean != ''
    AND TRIM(tag) != ''
  GROUP BY tag_name
  HAVING total_questions >= 50   -- Filter noise from very rare tags
)

SELECT
  tag_name,
  total_questions,
  unanswered_questions,
  answered_questions,
  accepted_answer_questions,
  avg_answers_per_question,
  avg_question_score,
  avg_view_count,
  distinct_askers,
  total_views,

  -- ── Unanswered percentage ─────────────────────────────────────────────────
  ROUND(100.0 * unanswered_questions / NULLIF(total_questions, 0), 2)
                                                            AS unanswered_pct,

  -- ── Acceptance rate ───────────────────────────────────────────────────────
  ROUND(100.0 * accepted_answer_questions / NULLIF(total_questions, 0), 2)
                                                            AS acceptance_rate_pct,

  -- ── Answer rate ──────────────────────────────────────────────────────────
  ROUND(100.0 * answered_questions / NULLIF(total_questions, 0), 2)
                                                            AS answer_rate_pct,

  -- ── Opportunity score: high views + high unanswered = high opportunity ────
  ROUND(
    (CAST(total_views AS FLOAT64) / 1000000.0)
    * (100.0 * unanswered_questions / NULLIF(total_questions, 0)),
    2
  )                                                         AS knowledge_gap_score,

  -- ── Rank by unanswered percentage ────────────────────────────────────────
  RANK() OVER (ORDER BY unanswered_questions DESC)         AS rank_by_unanswered_count,
  RANK() OVER (
    ORDER BY
      100.0 * unanswered_questions / NULLIF(total_questions, 0) DESC
  )                                                         AS rank_by_unanswered_pct,

  CURRENT_TIMESTAMP()                                       AS mart_refreshed_at

FROM tag_answer_stats
ORDER BY unanswered_questions DESC;


-- ============================================================================
-- MART 4: marts.mart_top_contributors
-- PURPOSE: Identify and rank the most valuable community contributors
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.marts.mart_top_contributors`
OPTIONS (
  description = 'Top contributor leaderboard with answer counts, acceptance rates, and reputation metrics. Powers Contributor Analytics dashboard page.',
  labels = [('layer', 'marts'), ('mart', 'top_contributors')]
)
AS
WITH

contributor_answer_stats AS (
  SELECT
    fa.user_id,
    COUNT(*)                                                AS total_answers,
    SUM(fa.accepted_answer_flag)                            AS accepted_answers,
    ROUND(AVG(fa.score), 2)                                 AS avg_answer_score,
    SUM(fa.score)                                           AS total_answer_score,
    MIN(fa.creation_date)                                   AS first_answer_date,
    MAX(fa.creation_date)                                   AS latest_answer_date,
    COUNT(DISTINCT DATE_TRUNC(fa.creation_date, MONTH))     AS active_months
  FROM `so-analytics-warehouse.warehouse.fact_answers` fa
  WHERE fa.user_id > 0
  GROUP BY fa.user_id
),

contributor_question_stats AS (
  SELECT
    fq.user_id,
    COUNT(*)                                                AS total_questions_asked,
    ROUND(AVG(fq.score), 2)                                 AS avg_question_score,
    SUM(fq.view_count)                                      AS total_views_generated
  FROM `so-analytics-warehouse.warehouse.fact_questions` fq
  WHERE fq.user_id > 0
  GROUP BY fq.user_id
),

combined_contributor AS (
  SELECT
    COALESCE(ca.user_id, cq.user_id)                        AS user_id,
    COALESCE(ca.total_answers, 0)                           AS total_answers,
    COALESCE(ca.accepted_answers, 0)                        AS accepted_answers,
    COALESCE(ca.avg_answer_score, 0)                        AS avg_answer_score,
    COALESCE(ca.total_answer_score, 0)                      AS total_answer_score,
    COALESCE(cq.total_questions_asked, 0)                   AS total_questions_asked,
    COALESCE(cq.avg_question_score, 0)                      AS avg_question_score,
    COALESCE(cq.total_views_generated, 0)                   AS total_views_generated,
    ca.first_answer_date,
    ca.latest_answer_date,
    COALESCE(ca.active_months, 0)                           AS active_months
  FROM contributor_answer_stats ca
  FULL OUTER JOIN contributor_question_stats cq ON ca.user_id = cq.user_id
)

SELECT
  c.user_id,
  du.display_name,
  du.reputation,
  du.reputation_bucket,
  du.account_creation_date_date,
  du.account_age_days,
  du.is_active,
  c.total_answers,
  c.accepted_answers,
  c.avg_answer_score,
  c.total_answer_score,
  c.total_questions_asked,
  c.avg_question_score,
  c.total_views_generated,
  c.first_answer_date,
  c.latest_answer_date,
  c.active_months,

  -- ── Acceptance rate ───────────────────────────────────────────────────────
  ROUND(100.0 * c.accepted_answers / NULLIF(c.total_answers, 0), 2)
                                                            AS acceptance_rate_pct,

  -- ── Activity consistency score ────────────────────────────────────────────
  -- Months active / months on platform
  ROUND(
    100.0 * c.active_months / NULLIF(
      DATE_DIFF(CURRENT_DATE(), du.account_creation_date_date, MONTH), 0
    ),
    2
  )                                                         AS activity_consistency_pct,

  -- ── Overall contribution score ────────────────────────────────────────────
  -- Weighted: answers(3x), accepted answers(5x), reputation(0.01x)
  ROUND(
    (c.total_answers * 3.0)
    + (c.accepted_answers * 5.0)
    + (du.reputation * 0.01)
    + (c.total_answer_score * 0.5),
    2
  )                                                         AS contribution_score,

  -- ── Rankings ─────────────────────────────────────────────────────────────
  RANK() OVER (ORDER BY c.accepted_answers DESC)           AS rank_by_accepted_answers,
  RANK() OVER (ORDER BY c.total_answers DESC)              AS rank_by_total_answers,
  RANK() OVER (ORDER BY du.reputation DESC)                AS rank_by_reputation,
  DENSE_RANK() OVER (
    ORDER BY ROUND(
      (c.total_answers * 3.0)
      + (c.accepted_answers * 5.0)
      + (du.reputation * 0.01)
      + (c.total_answer_score * 0.5),
      2
    ) DESC
  )                                                         AS rank_by_contribution_score,

  -- ── NTILE: Contributor tier percentile ───────────────────────────────────
  NTILE(10) OVER (ORDER BY c.total_answers DESC)           AS contributor_decile,
  NTILE(100) OVER (ORDER BY c.total_answers DESC)          AS contributor_percentile_bucket,

  CURRENT_TIMESTAMP()                                       AS mart_refreshed_at

FROM combined_contributor c
JOIN `so-analytics-warehouse.warehouse.dim_user` du ON c.user_id = du.user_id
WHERE c.total_answers >= 1   -- Must have contributed at least one answer
ORDER BY contribution_score DESC;


-- ============================================================================
-- MART 5: marts.mart_contributor_retention
-- PURPOSE: Monthly cohort retention analysis for contributors
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.marts.mart_contributor_retention`
OPTIONS (
  description = 'Monthly contributor retention cohort analysis. Tracks new vs returning vs retained contributors. Uses LAG/LEAD window functions.',
  labels = [('layer', 'marts'), ('mart', 'contributor_retention')]
)
AS
WITH

-- ── Monthly active contributors ───────────────────────────────────────────────
monthly_activity AS (
  SELECT
    user_id,
    DATE_TRUNC(activity_date, MONTH)                        AS activity_month,
    SUM(questions_posted)                                   AS monthly_questions,
    SUM(answers_posted)                                     AS monthly_answers,
    SUM(total_posts)                                        AS total_monthly_posts,
    MAX(engagement_score)                                   AS peak_daily_engagement
  FROM `so-analytics-warehouse.warehouse.fact_user_activity`
  WHERE user_id > 0
  GROUP BY user_id, activity_month
),

-- ── For each contributor, flag their first active month ──────────────────────
first_active_month AS (
  SELECT
    user_id,
    MIN(activity_month)                                     AS cohort_month
  FROM monthly_activity
  GROUP BY user_id
),

-- ── Combine activity with cohort month ────────────────────────────────────────
contributor_cohorts AS (
  SELECT
    ma.user_id,
    ma.activity_month,
    fam.cohort_month,
    ma.monthly_questions,
    ma.monthly_answers,
    ma.total_monthly_posts,
    ma.peak_daily_engagement,
    -- Months since first activity (cohort age)
    DATE_DIFF(ma.activity_month, fam.cohort_month, MONTH)   AS months_since_first_activity
  FROM monthly_activity ma
  JOIN first_active_month fam ON ma.user_id = fam.user_id
),

-- ── Determine if contributor was active in prior month (LAG) ─────────────────
with_retention_flags AS (
  SELECT
    *,
    LAG(activity_month, 1) OVER (
      PARTITION BY user_id
      ORDER BY activity_month
    )                                                       AS prior_active_month,
    LEAD(activity_month, 1) OVER (
      PARTITION BY user_id
      ORDER BY activity_month
    )                                                       AS next_active_month
  FROM contributor_cohorts
),

-- ── Classify each activity record ────────────────────────────────────────────
classified_activity AS (
  SELECT
    *,
    CASE
      WHEN months_since_first_activity = 0                  THEN 'New'
      WHEN DATE_DIFF(activity_month, prior_active_month, MONTH) = 1 THEN 'Retained'
      ELSE 'Returned'
    END                                                     AS contributor_type
  FROM with_retention_flags
)

-- ── Aggregate to monthly summary ─────────────────────────────────────────────
SELECT
  activity_month,
  COUNT(DISTINCT user_id)                                   AS total_active_contributors,
  COUNTIF(contributor_type = 'New')                        AS new_contributors,
  COUNTIF(contributor_type = 'Retained')                   AS retained_contributors,
  COUNTIF(contributor_type = 'Returned')                   AS returned_contributors,
  ROUND(AVG(total_monthly_posts), 2)                        AS avg_posts_per_contributor,
  SUM(total_monthly_posts)                                  AS total_posts,

  -- ── Retention rate: Retained / prior month's total ────────────────────────
  ROUND(
    100.0 * COUNTIF(contributor_type = 'Retained') / NULLIF(
      LAG(COUNT(DISTINCT user_id), 1) OVER (ORDER BY activity_month), 0
    ),
    2
  )                                                         AS month_over_month_retention_pct,

  -- ── Net new contributors ──────────────────────────────────────────────────
  COUNTIF(contributor_type = 'New')
    - (LAG(COUNT(DISTINCT user_id), 1) OVER (ORDER BY activity_month)
       - COUNTIF(contributor_type = 'Retained'))            AS net_new_contributors,

  -- ── 3-month rolling average of total contributors ────────────────────────
  ROUND(AVG(COUNT(DISTINCT user_id)) OVER (
    ORDER BY activity_month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ), 0)                                                     AS rolling_3m_avg_contributors,

  CURRENT_TIMESTAMP()                                       AS mart_refreshed_at

FROM classified_activity
GROUP BY activity_month
ORDER BY activity_month;


-- ============================================================================
-- MART 6: marts.mart_answer_latency
-- PURPOSE: Measure response time from question posting to first/accepted answer
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.marts.mart_answer_latency`
OPTIONS (
  description = 'Answer response time metrics by technology tag. Average time to first answer and accepted answer. Powers Answer Latency Analysis dashboard page.',
  labels = [('layer', 'marts'), ('mart', 'answer_latency')]
)
AS
WITH

-- ── Find the first answer for each question ───────────────────────────────────
first_answers AS (
  SELECT
    question_id,
    MIN(creation_date)                                      AS first_answer_date,
    MIN(CASE WHEN accepted_answer_flag = 1
      THEN creation_date END)                               AS accepted_answer_date
  FROM `so-analytics-warehouse.warehouse.fact_answers`
  GROUP BY question_id
),

-- ── Join to questions to compute latency ──────────────────────────────────────
question_latency AS (
  SELECT
    fq.question_id,
    fq.creation_date                                        AS question_date,
    fq.tags_clean,
    fa.first_answer_date,
    fa.accepted_answer_date,
    -- Time to first answer in hours
    ROUND(
      TIMESTAMP_DIFF(
        TIMESTAMP(fa.first_answer_date),
        fq.creation_timestamp,
        MINUTE
      ) / 60.0,
      2
    )                                                       AS hours_to_first_answer,
    -- Time to accepted answer in hours
    ROUND(
      TIMESTAMP_DIFF(
        TIMESTAMP(fa.accepted_answer_date),
        fq.creation_timestamp,
        MINUTE
      ) / 60.0,
      2
    )                                                       AS hours_to_accepted_answer
  FROM `so-analytics-warehouse.warehouse.fact_questions` fq
  LEFT JOIN first_answers fa ON fq.question_id = fa.question_id
  WHERE
    fq.tags_clean IS NOT NULL
    AND fq.tags_clean != ''
),

-- ── Explode to tag level ──────────────────────────────────────────────────────
tag_latency AS (
  SELECT
    LOWER(TRIM(tag))                                        AS tag_name,
    hours_to_first_answer,
    hours_to_accepted_answer,
    CASE WHEN first_answer_date IS NOT NULL THEN 1 ELSE 0 END AS has_answer
  FROM question_latency,
  UNNEST(SPLIT(tags_clean, '|')) AS tag
  WHERE TRIM(tag) != ''
)

SELECT
  tag_name,
  COUNT(*)                                                  AS total_questions,
  COUNTIF(has_answer = 1)                                   AS questions_with_answers,
  ROUND(AVG(CASE WHEN has_answer = 1 THEN hours_to_first_answer END), 2)
                                                            AS avg_hours_to_first_answer,
  ROUND(APPROX_QUANTILES(
    CASE WHEN has_answer = 1 AND hours_to_first_answer >= 0
    THEN hours_to_first_answer END, 100)[OFFSET(50)], 2)   AS median_hours_to_first_answer,
  ROUND(AVG(CASE WHEN hours_to_accepted_answer IS NOT NULL
    THEN hours_to_accepted_answer END), 2)                  AS avg_hours_to_accepted_answer,
  ROUND(MIN(CASE WHEN hours_to_first_answer > 0
    THEN hours_to_first_answer END), 2)                     AS min_hours_to_first_answer,
  ROUND(MAX(CASE WHEN hours_to_first_answer < 10000
    THEN hours_to_first_answer END), 2)                     AS max_hours_to_first_answer,
  ROUND(100.0 * COUNTIF(has_answer = 1) / NULLIF(COUNT(*), 0), 2)
                                                            AS answer_rate_pct,
  RANK() OVER (
    ORDER BY AVG(CASE WHEN has_answer = 1 THEN hours_to_first_answer END) ASC
  )                                                         AS rank_fastest_community,
  CURRENT_TIMESTAMP()                                       AS mart_refreshed_at
FROM tag_latency
GROUP BY tag_name
HAVING total_questions >= 100
ORDER BY avg_hours_to_first_answer ASC;


-- ============================================================================
-- MART 7: marts.mart_community_health
-- PURPOSE: Composite health score per technology community
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.marts.mart_community_health`
OPTIONS (
  description = 'Composite community health score per technology. Combines response rate, acceptance rate, contributor growth, and engagement. Powers Community Health dashboard page.',
  labels = [('layer', 'marts'), ('mart', 'community_health')]
)
AS
WITH

-- ── Gather all underlying metrics ────────────────────────────────────────────
health_inputs AS (
  SELECT
    uq.tag_name,
    dt.tag_category,
    uq.total_questions,
    uq.answer_rate_pct,
    uq.acceptance_rate_pct,
    uq.unanswered_pct,
    uq.avg_answers_per_question,
    uq.distinct_askers,
    al.avg_hours_to_first_answer,
    al.median_hours_to_first_answer,
    tg.yoy_growth_rate_pct,
    tg.contributor_growth_rate_pct,
    tg.avg_engagement,
    tg.question_count AS annual_questions,
    tg.distinct_contributors,
    tg.rank_by_volume_in_year
  FROM `so-analytics-warehouse.marts.mart_unanswered_questions` uq
  LEFT JOIN `so-analytics-warehouse.marts.mart_answer_latency` al
    ON uq.tag_name = al.tag_name
  LEFT JOIN `so-analytics-warehouse.marts.mart_tag_growth` tg
    ON uq.tag_name = tg.tag_name
    AND tg.question_year = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
  LEFT JOIN `so-analytics-warehouse.warehouse.dim_tag` dt
    ON uq.tag_name = dt.tag_name
  WHERE uq.total_questions >= 100
),

-- ── Compute normalized component scores ──────────────────────────────────────
normalized_health AS (
  SELECT
    *,
    -- Response rate score (0-100): higher answer rate = better health
    LEAST(100, COALESCE(answer_rate_pct, 0))                AS response_rate_score,

    -- Acceptance rate score (0-100)
    LEAST(100, COALESCE(acceptance_rate_pct, 0))            AS acceptance_rate_score,

    -- Speed score (0-100): faster response = better health
    -- 100 = answered in <1 hour; 0 = answered in >72 hours
    GREATEST(0, 100 - LEAST(100,
      COALESCE(avg_hours_to_first_answer, 100) * (100.0/72.0)
    ))                                                      AS speed_score,

    -- Growth score (0-100): positive growth = healthy community
    LEAST(100, GREATEST(0,
      50 + COALESCE(yoy_growth_rate_pct, 0) / 2.0
    ))                                                      AS growth_score,

    -- Engagement score: normalize avg_engagement to 0-100
    LEAST(100, GREATEST(0,
      COALESCE(avg_engagement, 0) * 10
    ))                                                      AS engagement_norm_score

  FROM health_inputs
)

SELECT
  tag_name,
  tag_category,
  total_questions,
  answer_rate_pct,
  acceptance_rate_pct,
  unanswered_pct,
  avg_hours_to_first_answer,
  median_hours_to_first_answer,
  yoy_growth_rate_pct,
  contributor_growth_rate_pct,
  avg_engagement,
  distinct_contributors,
  annual_questions,
  rank_by_volume_in_year,

  -- ── Component scores (0-100) ──────────────────────────────────────────────
  ROUND(response_rate_score, 2)                             AS response_rate_score,
  ROUND(acceptance_rate_score, 2)                           AS acceptance_rate_score,
  ROUND(speed_score, 2)                                     AS speed_score,
  ROUND(growth_score, 2)                                    AS growth_score,
  ROUND(engagement_norm_score, 2)                           AS engagement_score,

  -- ── Composite health score (0-100) ───────────────────────────────────────
  -- Weighted: 25% response rate, 25% acceptance rate, 20% speed,
  --           20% growth, 10% engagement
  ROUND(
    (0.25 * response_rate_score)
    + (0.25 * acceptance_rate_score)
    + (0.20 * speed_score)
    + (0.20 * growth_score)
    + (0.10 * engagement_norm_score),
    2
  )                                                         AS community_health_score,

  -- ── Health classification ─────────────────────────────────────────────────
  CASE
    WHEN ROUND((0.25*response_rate_score)+(0.25*acceptance_rate_score)
      +(0.20*speed_score)+(0.20*growth_score)+(0.10*engagement_norm_score),2) >= 75
      THEN 'Thriving'
    WHEN ROUND((0.25*response_rate_score)+(0.25*acceptance_rate_score)
      +(0.20*speed_score)+(0.20*growth_score)+(0.10*engagement_norm_score),2) >= 50
      THEN 'Healthy'
    WHEN ROUND((0.25*response_rate_score)+(0.25*acceptance_rate_score)
      +(0.20*speed_score)+(0.20*growth_score)+(0.10*engagement_norm_score),2) >= 25
      THEN 'Developing'
    ELSE 'At Risk'
  END                                                       AS health_classification,

  -- ── Health rank ───────────────────────────────────────────────────────────
  RANK() OVER (ORDER BY
    ROUND((0.25*response_rate_score)+(0.25*acceptance_rate_score)
    +(0.20*speed_score)+(0.20*growth_score)+(0.10*engagement_norm_score),2)
    DESC
  )                                                         AS health_rank,

  CURRENT_TIMESTAMP()                                       AS mart_refreshed_at

FROM normalized_health
ORDER BY community_health_score DESC;
