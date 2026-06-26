/*
================================================================================
FILE: 03_staging_layer.sql
PROJECT: Developer Knowledge Platform Analytics Warehouse
LAYER: Staging (Silver)
PURPOSE: Clean, transform, and enrich raw data for dimensional modeling
================================================================================

EXECUTION ORDER: Run after 02_raw_layer.sql

TABLES CREATED:
  staging.stg_questions
  staging.stg_answers
  staging.stg_users

TRANSFORMATIONS APPLIED:
  1. NULL handling   — Replace NULLs with appropriate defaults
  2. Type casting    — Ensure consistent data types across all columns
  3. Deduplication   — Remove duplicate IDs using ROW_NUMBER()
  4. Derived fields  — question_year, question_month, answer_year,
                       answer_month, account_age_days
  5. Data validation — Exclude records with logically invalid values
  6. Tag parsing     — Split pipe-delimited tag strings into arrays

DEDUPLICATION STRATEGY:
  ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_activity_date DESC)
  Keeps the most recently active version of each record.
  This handles the case where source data contains re-ingested duplicates.

WHY TRANSFORM HERE (NOT IN WAREHOUSE)?
  Keeping transformations in staging means:
  - Dimension/fact tables contain only clean, validated data
  - Logic changes require re-running only staging (not rebuilding dims/facts)
  - Debugging is isolated to one layer
  - Warehouse SQL stays readable and focused on dimensional structure
================================================================================
*/


-- ============================================================================
-- TABLE: staging.stg_questions
-- SOURCE: raw.raw_questions
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.staging.stg_questions`
PARTITION BY DATE_TRUNC(question_date, MONTH)
CLUSTER BY owner_user_id, score
OPTIONS (
  description = 'Cleaned and enriched Stack Overflow questions. Deduplicated, null-handled, type-cast. Derived fields: question_year, question_month. Ready for dimensional model loading.',
  labels = [('layer', 'staging'), ('source', 'raw_questions')]
)
AS
WITH

-- ── Step 1: Deduplicate using ROW_NUMBER ─────────────────────────────────────
-- Some records may appear multiple times in raw due to re-ingestion.
-- We keep the record with the most recent last_activity_date.
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY last_activity_date DESC, _ingested_at DESC
    ) AS _row_num
  FROM `so-analytics-warehouse.raw.raw_questions`
),

-- ── Step 2: Keep only the canonical record per ID ────────────────────────────
deduplicated_canonical AS (
  SELECT *
  FROM deduplicated
  WHERE _row_num = 1
),

-- ── Step 3: Apply cleaning, casting, and derived fields ──────────────────────
cleaned AS (
  SELECT
    -- Primary key
    CAST(id AS INT64)                                   AS question_id,

    -- Title: trim whitespace, replace NULL with placeholder
    TRIM(COALESCE(title, 'Untitled Question'))           AS title,

    -- Owner/user: replace NULL user IDs with -1 (anonymous/deleted user)
    COALESCE(CAST(owner_user_id AS INT64), -1)          AS owner_user_id,

    -- Dates: cast to DATE and TIMESTAMP types
    CAST(creation_date AS TIMESTAMP)                    AS creation_date,
    DATE(creation_date)                                 AS question_date,
    CAST(last_activity_date AS TIMESTAMP)               AS last_activity_date,
    CAST(last_edit_date AS TIMESTAMP)                   AS last_edit_date,

    -- ── Derived date dimensions ─────────────────────────────────────────────
    -- Extracted for easier time-series analysis without date spine joins
    EXTRACT(YEAR  FROM creation_date)                   AS question_year,
    EXTRACT(MONTH FROM creation_date)                   AS question_month,
    EXTRACT(QUARTER FROM creation_date)                 AS question_quarter,

    -- Numeric metrics: COALESCE to 0 for NULL counts
    COALESCE(CAST(score         AS INT64), 0)           AS score,
    COALESCE(CAST(view_count    AS INT64), 0)           AS view_count,
    COALESCE(CAST(answer_count  AS INT64), 0)           AS answer_count,
    COALESCE(CAST(comment_count AS INT64), 0)           AS comment_count,
    COALESCE(CAST(favorite_count AS INT64), 0)          AS favorite_count,

    -- Accepted answer: cast to INT64, NULL means no accepted answer
    CAST(accepted_answer_id AS INT64)                   AS accepted_answer_id,

    -- Boolean derived: has the question been answered?
    CASE WHEN answer_count > 0 THEN TRUE ELSE FALSE END AS is_answered,

    -- Boolean derived: has the question been accepted?
    CASE
      WHEN accepted_answer_id IS NOT NULL THEN TRUE
      ELSE FALSE
    END                                                 AS has_accepted_answer,

    -- Tags: preserve raw string AND parse into ARRAY for downstream use
    -- Raw tags look like: <python><django><rest-api>
    -- We clean them into: python|django|rest-api
    REGEXP_REPLACE(
      REGEXP_REPLACE(COALESCE(tags, ''), r'><', '|'),
      r'[<>]', ''
    )                                                   AS tags_clean,

    -- Tag count: useful for questions that span multiple technologies
    ARRAY_LENGTH(
      SPLIT(
        REGEXP_REPLACE(
          REGEXP_REPLACE(COALESCE(tags, ''), r'><', '|'),
          r'[<>]', ''
        ), '|'
      )
    )                                                   AS tag_count,

    -- Pipeline metadata
    _ingested_at,
    _batch_id,
    CURRENT_TIMESTAMP()                                 AS _staged_at

  FROM deduplicated_canonical

  WHERE
    -- Exclude records where creation_date is impossible
    creation_date >= TIMESTAMP('2008-01-01')   -- Stack Overflow launched in 2008
    AND creation_date <= CURRENT_TIMESTAMP()
    -- Exclude records with clearly invalid view counts
    AND COALESCE(CAST(view_count AS INT64), 0) >= 0
    -- Exclude records with clearly invalid scores (huge negative values = corruption)
    AND COALESCE(CAST(score AS INT64), 0) > -10000
)

SELECT * FROM cleaned;


-- ============================================================================
-- TABLE: staging.stg_answers
-- SOURCE: raw.raw_answers
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.staging.stg_answers`
PARTITION BY DATE_TRUNC(answer_date, MONTH)
CLUSTER BY question_id, owner_user_id
OPTIONS (
  description = 'Cleaned and enriched Stack Overflow answers. Deduplicated, null-handled, type-cast. Derived fields: answer_year, answer_month, is_accepted.',
  labels = [('layer', 'staging'), ('source', 'raw_answers')]
)
AS
WITH

-- ── Step 1: Deduplicate ──────────────────────────────────────────────────────
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY last_activity_date DESC, _ingested_at DESC
    ) AS _row_num
  FROM `so-analytics-warehouse.raw.raw_answers`
),

deduplicated_canonical AS (
  SELECT * FROM deduplicated WHERE _row_num = 1
),

-- ── Step 2: Clean and enrich ─────────────────────────────────────────────────
cleaned AS (
  SELECT
    -- Primary key
    CAST(id AS INT64)                                   AS answer_id,

    -- Foreign key to questions (parent_id = question_id for answers)
    CAST(parent_id AS INT64)                            AS question_id,

    -- Owner: -1 for anonymous/deleted
    COALESCE(CAST(owner_user_id AS INT64), -1)          AS owner_user_id,

    -- Dates
    CAST(creation_date AS TIMESTAMP)                    AS creation_date,
    DATE(creation_date)                                 AS answer_date,
    CAST(last_activity_date AS TIMESTAMP)               AS last_activity_date,
    CAST(last_edit_date AS TIMESTAMP)                   AS last_edit_date,

    -- ── Derived date dimensions ─────────────────────────────────────────────
    EXTRACT(YEAR  FROM creation_date)                   AS answer_year,
    EXTRACT(MONTH FROM creation_date)                   AS answer_month,
    EXTRACT(QUARTER FROM creation_date)                 AS answer_quarter,

    -- Score metrics
    COALESCE(CAST(score         AS INT64), 0)           AS score,
    COALESCE(CAST(comment_count AS INT64), 0)           AS comment_count,

    -- ── Accepted answer flag ────────────────────────────────────────────────
    -- An answer is "accepted" if the question's accepted_answer_id = this answer's id
    -- We compute this by joining to questions; here we store a placeholder
    -- and compute the actual flag in the fact table join.
    -- For now, accepted_answer_id on the answer record is usually NULL in source.
    CASE
      WHEN accepted_answer_id IS NOT NULL THEN TRUE
      ELSE FALSE
    END                                                 AS is_accepted_flag,

    -- Answer quality indicators
    CASE
      WHEN COALESCE(CAST(score AS INT64), 0) > 0  THEN 'positive'
      WHEN COALESCE(CAST(score AS INT64), 0) = 0  THEN 'neutral'
      ELSE 'negative'
    END                                                 AS score_sentiment,

    -- Pipeline metadata
    _ingested_at,
    _batch_id,
    CURRENT_TIMESTAMP()                                 AS _staged_at

  FROM deduplicated_canonical

  WHERE
    creation_date >= TIMESTAMP('2008-01-01')
    AND creation_date <= CURRENT_TIMESTAMP()
    AND parent_id IS NOT NULL
    AND COALESCE(CAST(score AS INT64), 0) > -10000
)

SELECT * FROM cleaned;


-- ============================================================================
-- TABLE: staging.stg_users
-- SOURCE: raw.raw_users
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.staging.stg_users`
CLUSTER BY reputation
OPTIONS (
  description = 'Cleaned and enriched Stack Overflow users. Deduplicated, null-handled, type-cast. Derived fields: account_age_days, reputation_bucket.',
  labels = [('layer', 'staging'), ('source', 'raw_users')]
)
AS
WITH

-- ── Step 1: Deduplicate ──────────────────────────────────────────────────────
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY last_access_date DESC, _ingested_at DESC
    ) AS _row_num
  FROM `so-analytics-warehouse.raw.raw_users`
),

deduplicated_canonical AS (
  SELECT * FROM deduplicated WHERE _row_num = 1
),

-- ── Step 2: Clean, enrich, and compute derived fields ────────────────────────
cleaned AS (
  SELECT
    -- Primary key
    CAST(id AS INT64)                                   AS user_id,

    -- Name: trim and coalesce
    TRIM(COALESCE(display_name, 'Anonymous'))           AS display_name,

    -- Dates
    CAST(creation_date AS TIMESTAMP)                    AS account_creation_date,
    DATE(creation_date)                                 AS account_creation_date_date,
    CAST(last_access_date AS TIMESTAMP)                 AS last_access_date,

    -- ── Derived: Account age in days ────────────────────────────────────────
    -- Measures how long the user has been on the platform
    -- Using CURRENT_DATE as reference point for "age"
    DATE_DIFF(
      CURRENT_DATE(),
      DATE(creation_date),
      DAY
    )                                                   AS account_age_days,

    -- Reputation: coalesce to 1 (minimum valid Stack Overflow reputation)
    COALESCE(CAST(reputation AS INT64), 1)              AS reputation,

    -- Engagement metrics
    COALESCE(CAST(up_votes   AS INT64), 0)              AS up_votes,
    COALESCE(CAST(down_votes AS INT64), 0)              AS down_votes,
    COALESCE(CAST(views      AS INT64), 0)              AS profile_views,

    -- Location: trim, coalesce to 'Unknown'
    TRIM(COALESCE(location, 'Unknown'))                 AS location,

    -- ── Derived: Reputation bucket ──────────────────────────────────────────
    -- These thresholds roughly correspond to Stack Overflow privilege milestones
    CASE
      WHEN COALESCE(CAST(reputation AS INT64), 1) < 500        THEN 'Beginner'
      WHEN COALESCE(CAST(reputation AS INT64), 1) < 3000       THEN 'Intermediate'
      WHEN COALESCE(CAST(reputation AS INT64), 1) < 10000      THEN 'Advanced'
      WHEN COALESCE(CAST(reputation AS INT64), 1) < 50000      THEN 'Expert'
      ELSE 'Elite'
    END                                                 AS reputation_bucket,

    -- ── Derived: Reputation bucket ordinal ──────────────────────────────────
    -- Ordinal for sorting reputation buckets in visualizations
    CASE
      WHEN COALESCE(CAST(reputation AS INT64), 1) < 500        THEN 1
      WHEN COALESCE(CAST(reputation AS INT64), 1) < 3000       THEN 2
      WHEN COALESCE(CAST(reputation AS INT64), 1) < 10000      THEN 3
      WHEN COALESCE(CAST(reputation AS INT64), 1) < 50000      THEN 4
      ELSE 5
    END                                                 AS reputation_bucket_ordinal,

    -- ── Derived: Is active user ─────────────────────────────────────────────
    -- Active = accessed the platform in the last 365 days
    CASE
      WHEN DATE_DIFF(
        CURRENT_DATE(),
        DATE(last_access_date),
        DAY
      ) <= 365 THEN TRUE
      ELSE FALSE
    END                                                 AS is_active,

    -- Pipeline metadata
    _ingested_at,
    _batch_id,
    CURRENT_TIMESTAMP()                                 AS _staged_at

  FROM deduplicated_canonical

  WHERE
    id > 0                                   -- Exclude system accounts
    AND creation_date >= TIMESTAMP('2008-01-01')
    AND creation_date <= CURRENT_TIMESTAMP()
    -- Reputation must be >= 1 (minimum valid value on Stack Overflow)
    AND COALESCE(CAST(reputation AS INT64), 1) >= 1
    -- Reputation must be reasonable (max ever seen is ~1.5M)
    AND COALESCE(CAST(reputation AS INT64), 1) <= 2000000
)

SELECT * FROM cleaned;


-- ============================================================================
-- POST-STAGING VALIDATION
-- Compare row counts between raw and staging to catch data loss
-- ============================================================================
SELECT
  'stg_questions vs raw_questions'                        AS comparison,
  (SELECT COUNT(*) FROM `so-analytics-warehouse.staging.stg_questions`) AS staging_rows,
  (SELECT COUNT(*) FROM `so-analytics-warehouse.raw.raw_questions`)     AS raw_rows,
  ROUND(
    100.0 *
    (SELECT COUNT(*) FROM `so-analytics-warehouse.staging.stg_questions`) /
    NULLIF((SELECT COUNT(*) FROM `so-analytics-warehouse.raw.raw_questions`), 0),
    2
  )                                                               AS retention_pct

UNION ALL

SELECT
  'stg_answers vs raw_answers',
  (SELECT COUNT(*) FROM `so-analytics-warehouse.staging.stg_answers`),
  (SELECT COUNT(*) FROM `so-analytics-warehouse.raw.raw_answers`),
  ROUND(
    100.0 *
    (SELECT COUNT(*) FROM `so-analytics-warehouse.staging.stg_answers`) /
    NULLIF((SELECT COUNT(*) FROM `so-analytics-warehouse.raw.raw_answers`), 0),
    2
  )

UNION ALL

SELECT
  'stg_users vs raw_users',
  (SELECT COUNT(*) FROM `so-analytics-warehouse.staging.stg_users`),
  (SELECT COUNT(*) FROM `so-analytics-warehouse.raw.raw_users`),
  ROUND(
    100.0 *
    (SELECT COUNT(*) FROM `so-analytics-warehouse.staging.stg_users`) /
    NULLIF((SELECT COUNT(*) FROM `so-analytics-warehouse.raw.raw_users`), 0),
    2
  );

-- NOTE: Expect 95-100% retention. Values below 90% warrant investigation.
-- Common reasons for row loss:
--   - Deduplication removed true duplicates (expected, acceptable)
--   - Invalid date filtering removed corrupt records (expected, acceptable)
--   - Logic error in WHERE clause (investigate if >5% loss)
