/*
================================================================================
FILE: 07_incremental_etl.sql
PROJECT: Developer Knowledge Platform Analytics Warehouse
LAYER: Cross-cutting (ETL orchestration)
PURPOSE: Production incremental ETL using MERGE statements and watermarks
================================================================================

EXECUTION ORDER: Run on a schedule (daily) AFTER initial load (files 02-05)

INCREMENTAL STRATEGY:
  This file implements watermark-based incremental loading for all three
  pipeline layers. It replaces full-table rebuilds with targeted MERGE
  operations that only process new/changed records since the last run.

  Pattern:
    1. Read the watermark (MAX timestamp from target table)
    2. Select source records created after the watermark
    3. MERGE into target: UPDATE existing, INSERT new
    4. Advance the watermark implicitly (next run reads new MAX)

WHY MERGE OVER INSERT?
  Stack Overflow data can change retroactively:
  - Scores change as users upvote/downvote
  - Users can edit their profile (reputation, location)
  - Questions can gain new answers after initial ingestion
  
  INSERT-only would miss these updates. MERGE handles both new records
  (WHEN NOT MATCHED → INSERT) and changed records (WHEN MATCHED → UPDATE).

WHY NOT FULL REFRESH?
  At 50M+ questions and 90M+ answers, a full refresh would:
  - Cost $30-50 per run (scanning entire public dataset)
  - Take 45-90 minutes
  - Provide zero additional accuracy vs incremental
  
  Incremental MERGE on daily delta:
  - Costs <$1 per run
  - Completes in 5-10 minutes
  - Handles updates to existing records

SCHEDULING:
  Run via BigQuery Scheduled Queries daily at 02:00 UTC
  or orchestrate with Cloud Composer/Airflow as part of a DAG.
================================================================================
*/


-- ============================================================================
-- STEP 1: INCREMENTAL RAW LAYER — raw.raw_questions
-- Pulls new questions from source since last ingestion watermark
-- ============================================================================
DECLARE last_watermark_questions TIMESTAMP;

-- Read the current watermark from the raw table
SET last_watermark_questions = (
  SELECT COALESCE(
    MAX(_ingested_at),
    TIMESTAMP('2008-01-01 00:00:00 UTC')  -- Default: initial load from beginning
  )
  FROM `so-analytics-warehouse.raw.raw_questions`
);

-- ── Merge new/changed questions into raw layer ────────────────────────────────
MERGE `so-analytics-warehouse.raw.raw_questions` AS target
USING (
  SELECT
    id,
    title,
    body,
    accepted_answer_id,
    answer_count,
    comment_count,
    community_owned_date,
    creation_date,
    favorite_count,
    last_activity_date,
    last_edit_date,
    last_editor_display_name,
    last_editor_user_id,
    owner_display_name,
    owner_user_id,
    parent_id,
    post_type_id,
    score,
    tags,
    view_count,
    CURRENT_TIMESTAMP()                                     AS _ingested_at,
    'bigquery-public-data.stackoverflow.posts_questions'    AS _source_table,
    GENERATE_UUID()                                         AS _batch_id,
    'incremental_load'                                      AS _load_type
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE
    post_type_id = 1
    AND id IS NOT NULL
    AND creation_date <= CURRENT_TIMESTAMP()
    -- ── Delta filter: only process records active since last watermark ────────
    -- Using last_activity_date (not creation_date) catches edits/re-scores
    AND last_activity_date > last_watermark_questions
) AS source
ON target.id = source.id

-- ── Update existing records that have changed ─────────────────────────────────
WHEN MATCHED AND (
  target.score           != source.score
  OR target.answer_count != source.answer_count
  OR target.view_count   != source.view_count
  OR target.favorite_count != source.favorite_count
  OR target.accepted_answer_id IS DISTINCT FROM source.accepted_answer_id
  OR target.last_activity_date != source.last_activity_date
) THEN UPDATE SET
  score                = source.score,
  answer_count         = source.answer_count,
  view_count           = source.view_count,
  favorite_count       = source.favorite_count,
  accepted_answer_id   = source.accepted_answer_id,
  last_activity_date   = source.last_activity_date,
  last_edit_date       = source.last_edit_date,
  _ingested_at         = source._ingested_at,
  _batch_id            = source._batch_id,
  _load_type           = source._load_type

-- ── Insert brand new records ──────────────────────────────────────────────────
WHEN NOT MATCHED THEN INSERT (
  id, title, body, accepted_answer_id, answer_count, comment_count,
  community_owned_date, creation_date, favorite_count, last_activity_date,
  last_edit_date, last_editor_display_name, last_editor_user_id,
  owner_display_name, owner_user_id, parent_id, post_type_id, score,
  tags, view_count, _ingested_at, _source_table, _batch_id, _load_type
) VALUES (
  source.id, source.title, source.body, source.accepted_answer_id,
  source.answer_count, source.comment_count, source.community_owned_date,
  source.creation_date, source.favorite_count, source.last_activity_date,
  source.last_edit_date, source.last_editor_display_name, source.last_editor_user_id,
  source.owner_display_name, source.owner_user_id, source.parent_id,
  source.post_type_id, source.score, source.tags, source.view_count,
  source._ingested_at, source._source_table, source._batch_id, source._load_type
);


-- ============================================================================
-- STEP 2: INCREMENTAL RAW LAYER — raw.raw_answers
-- ============================================================================
DECLARE last_watermark_answers TIMESTAMP;

SET last_watermark_answers = (
  SELECT COALESCE(
    MAX(_ingested_at),
    TIMESTAMP('2008-01-01 00:00:00 UTC')
  )
  FROM `so-analytics-warehouse.raw.raw_answers`
);

MERGE `so-analytics-warehouse.raw.raw_answers` AS target
USING (
  SELECT
    id, title, body, accepted_answer_id, answer_count, comment_count,
    community_owned_date, creation_date, favorite_count, last_activity_date,
    last_edit_date, last_editor_display_name, last_editor_user_id,
    owner_display_name, owner_user_id, parent_id, post_type_id, score,
    tags, view_count,
    CURRENT_TIMESTAMP()                                     AS _ingested_at,
    'bigquery-public-data.stackoverflow.posts_answers'      AS _source_table,
    GENERATE_UUID()                                         AS _batch_id,
    'incremental_load'                                      AS _load_type
  FROM `bigquery-public-data.stackoverflow.posts_answers`
  WHERE
    post_type_id = 2
    AND id IS NOT NULL
    AND parent_id IS NOT NULL
    AND creation_date <= CURRENT_TIMESTAMP()
    AND last_activity_date > last_watermark_answers
) AS source
ON target.id = source.id

WHEN MATCHED AND (
  target.score != source.score
  OR target.last_activity_date != source.last_activity_date
) THEN UPDATE SET
  score              = source.score,
  last_activity_date = source.last_activity_date,
  last_edit_date     = source.last_edit_date,
  _ingested_at       = source._ingested_at,
  _batch_id          = source._batch_id,
  _load_type         = source._load_type

WHEN NOT MATCHED THEN INSERT (
  id, title, body, accepted_answer_id, answer_count, comment_count,
  community_owned_date, creation_date, favorite_count, last_activity_date,
  last_edit_date, last_editor_display_name, last_editor_user_id,
  owner_display_name, owner_user_id, parent_id, post_type_id, score,
  tags, view_count, _ingested_at, _source_table, _batch_id, _load_type
) VALUES (
  source.id, source.title, source.body, source.accepted_answer_id,
  source.answer_count, source.comment_count, source.community_owned_date,
  source.creation_date, source.favorite_count, source.last_activity_date,
  source.last_edit_date, source.last_editor_display_name, source.last_editor_user_id,
  source.owner_display_name, source.owner_user_id, source.parent_id,
  source.post_type_id, source.score, source.tags, source.view_count,
  source._ingested_at, source._source_table, source._batch_id, source._load_type
);


-- ============================================================================
-- STEP 3: INCREMENTAL RAW LAYER — raw.raw_users
-- ============================================================================
DECLARE last_watermark_users TIMESTAMP;

SET last_watermark_users = (
  SELECT COALESCE(
    MAX(_ingested_at),
    TIMESTAMP('2008-01-01 00:00:00 UTC')
  )
  FROM `so-analytics-warehouse.raw.raw_users`
);

MERGE `so-analytics-warehouse.raw.raw_users` AS target
USING (
  SELECT
    id, display_name, about_me, age, creation_date, last_access_date,
    location, reputation, up_votes, down_votes, views, website_url, account_id,
    CURRENT_TIMESTAMP()                                     AS _ingested_at,
    'bigquery-public-data.stackoverflow.users'              AS _source_table,
    GENERATE_UUID()                                         AS _batch_id,
    'incremental_load'                                      AS _load_type
  FROM `bigquery-public-data.stackoverflow.users`
  WHERE
    id > 0
    AND id IS NOT NULL
    AND creation_date <= CURRENT_TIMESTAMP()
    AND last_access_date > last_watermark_users
) AS source
ON target.id = source.id

WHEN MATCHED AND (
  target.reputation    != source.reputation
  OR target.up_votes   != source.up_votes
  OR target.down_votes != source.down_votes
  OR target.last_access_date != source.last_access_date
) THEN UPDATE SET
  reputation       = source.reputation,
  up_votes         = source.up_votes,
  down_votes       = source.down_votes,
  last_access_date = source.last_access_date,
  location         = source.location,
  _ingested_at     = source._ingested_at,
  _batch_id        = source._batch_id,
  _load_type       = source._load_type

WHEN NOT MATCHED THEN INSERT (
  id, display_name, about_me, age, creation_date, last_access_date,
  location, reputation, up_votes, down_votes, views, website_url, account_id,
  _ingested_at, _source_table, _batch_id, _load_type
) VALUES (
  source.id, source.display_name, source.about_me, source.age,
  source.creation_date, source.last_access_date, source.location,
  source.reputation, source.up_votes, source.down_votes, source.views,
  source.website_url, source.account_id,
  source._ingested_at, source._source_table, source._batch_id, source._load_type
);


-- ============================================================================
-- STEP 4: INCREMENTAL STAGING — stg_questions
-- Processes only raw records staged since the last staging watermark
-- ============================================================================
DECLARE last_watermark_stg_questions TIMESTAMP;

SET last_watermark_stg_questions = (
  SELECT COALESCE(
    MAX(_staged_at),
    TIMESTAMP('2008-01-01 00:00:00 UTC')
  )
  FROM `so-analytics-warehouse.staging.stg_questions`
);

MERGE `so-analytics-warehouse.staging.stg_questions` AS target
USING (
  -- Re-apply the full cleaning/transformation logic from 03_staging_layer.sql
  -- but only for records ingested since the last staging watermark
  SELECT
    CAST(id AS INT64)                                   AS question_id,
    TRIM(COALESCE(title, 'Untitled Question'))           AS title,
    COALESCE(CAST(owner_user_id AS INT64), -1)          AS owner_user_id,
    CAST(creation_date AS TIMESTAMP)                    AS creation_date,
    DATE(creation_date)                                 AS question_date,
    CAST(last_activity_date AS TIMESTAMP)               AS last_activity_date,
    CAST(last_edit_date AS TIMESTAMP)                   AS last_edit_date,
    EXTRACT(YEAR FROM creation_date)                    AS question_year,
    EXTRACT(MONTH FROM creation_date)                   AS question_month,
    EXTRACT(QUARTER FROM creation_date)                 AS question_quarter,
    COALESCE(CAST(score AS INT64), 0)                   AS score,
    COALESCE(CAST(view_count AS INT64), 0)              AS view_count,
    COALESCE(CAST(answer_count AS INT64), 0)            AS answer_count,
    COALESCE(CAST(comment_count AS INT64), 0)           AS comment_count,
    COALESCE(CAST(favorite_count AS INT64), 0)          AS favorite_count,
    CAST(accepted_answer_id AS INT64)                   AS accepted_answer_id,
    CASE WHEN COALESCE(CAST(answer_count AS INT64),0) > 0 THEN TRUE ELSE FALSE END AS is_answered,
    CASE WHEN accepted_answer_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_accepted_answer,
    REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(tags,''),r'><','|'),r'[<>]','') AS tags_clean,
    ARRAY_LENGTH(SPLIT(
      REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(tags,''),r'><','|'),r'[<>]',''),'|'
    ))                                                  AS tag_count,
    _ingested_at,
    _batch_id,
    CURRENT_TIMESTAMP()                                 AS _staged_at
  FROM `so-analytics-warehouse.raw.raw_questions`
  WHERE
    _ingested_at > last_watermark_stg_questions
    AND creation_date >= TIMESTAMP('2008-01-01')
    AND creation_date <= CURRENT_TIMESTAMP()
    AND COALESCE(CAST(view_count AS INT64), 0) >= 0
) AS source
ON target.question_id = source.question_id

WHEN MATCHED AND (
  target.score != source.score
  OR target.answer_count != source.answer_count
  OR target.view_count != source.view_count
  OR target.accepted_answer_id IS DISTINCT FROM source.accepted_answer_id
) THEN UPDATE SET
  score              = source.score,
  answer_count       = source.answer_count,
  view_count         = source.view_count,
  favorite_count     = source.favorite_count,
  accepted_answer_id = source.accepted_answer_id,
  is_answered        = source.is_answered,
  has_accepted_answer = source.has_accepted_answer,
  last_activity_date = source.last_activity_date,
  _staged_at         = source._staged_at

WHEN NOT MATCHED THEN INSERT ROW;


-- ============================================================================
-- STEP 5: INCREMENTAL DIMENSION — dim_user (SCD Type 1)
-- Updates reputation and other mutable attributes for existing users.
-- Inserts new users.
-- ============================================================================
MERGE `so-analytics-warehouse.warehouse.dim_user` AS target
USING (
  SELECT
    user_id,
    display_name,
    account_creation_date,
    account_creation_date_date,
    last_access_date,
    account_age_days,
    CASE
      WHEN account_age_days < 365   THEN 'New (< 1 year)'
      WHEN account_age_days < 1095  THEN 'Established (1-3 years)'
      WHEN account_age_days < 2555  THEN 'Veteran (3-7 years)'
      ELSE 'Long-term (7+ years)'
    END                                                   AS account_age_bucket,
    reputation,
    reputation_bucket,
    reputation_bucket_ordinal,
    ROUND(PERCENT_RANK() OVER (ORDER BY reputation) * 100, 2) AS reputation_percentile_pct,
    NTILE(4) OVER (ORDER BY reputation)                   AS reputation_quartile,
    ROW_NUMBER() OVER (
      PARTITION BY reputation_bucket
      ORDER BY reputation DESC
    )                                                     AS rank_within_bucket,
    up_votes,
    down_votes,
    profile_views,
    location,
    is_active,
    _staged_at                                            AS dim_updated_at
  FROM `so-analytics-warehouse.staging.stg_users`
) AS source
ON target.user_id = source.user_id

-- SCD Type 1: overwrite mutable attributes
WHEN MATCHED AND (
  target.reputation    != source.reputation
  OR target.is_active  != source.is_active
  OR target.location   != source.location
) THEN UPDATE SET
  reputation              = source.reputation,
  reputation_bucket       = source.reputation_bucket,
  reputation_bucket_ordinal = source.reputation_bucket_ordinal,
  reputation_percentile_pct = source.reputation_percentile_pct,
  account_age_days        = source.account_age_days,
  account_age_bucket      = source.account_age_bucket,
  is_active               = source.is_active,
  location                = source.location,
  last_access_date        = source.last_access_date,
  dim_updated_at          = source.dim_updated_at

WHEN NOT MATCHED THEN INSERT ROW;


-- ============================================================================
-- STEP 6: INCREMENTAL FACT — fact_questions (partition-aligned)
-- Processes only the partitions affected by new/changed data
-- ============================================================================
DECLARE incremental_start_date DATE;
DECLARE incremental_end_date DATE;

-- Process the last 2 days to catch any late-arriving records
SET incremental_start_date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY);
SET incremental_end_date   = CURRENT_DATE();

MERGE `so-analytics-warehouse.warehouse.fact_questions` AS target
USING (
  SELECT
    q.question_id,
    CAST(FORMAT_DATE('%Y%m%d', q.question_date) AS INT64)   AS date_key,
    COALESCE(q.owner_user_id, -1)                           AS user_id,
    q.question_date                                         AS creation_date,
    q.creation_date                                         AS creation_timestamp,
    q.last_activity_date,
    q.score,
    q.view_count,
    q.answer_count,
    q.comment_count,
    q.favorite_count,
    q.is_answered,
    q.has_accepted_answer,
    q.accepted_answer_id,
    q.tags_clean,
    q.tag_count,
    SPLIT(q.tags_clean, '|')[SAFE_OFFSET(0)]               AS primary_tag,
    CASE
      WHEN q.score >= 10  THEN 'High Quality'
      WHEN q.score >= 1   THEN 'Good'
      WHEN q.score = 0    THEN 'Neutral'
      WHEN q.score < 0    THEN 'Low Quality'
    END                                                     AS quality_tier,
    ROUND(
      (COALESCE(q.score, 0) * 2.0)
      + LOG(COALESCE(q.view_count, 0) + 1)
      + (COALESCE(q.answer_count, 0) * 1.5)
      + (COALESCE(q.favorite_count, 0) * 2.0),
      4
    )                                                       AS engagement_score,
    q._ingested_at,
    q._batch_id
  FROM `so-analytics-warehouse.staging.stg_questions` q
  WHERE
    -- ── Partition-aligned filter: only target recent partitions ───────────────
    q.question_date BETWEEN incremental_start_date AND incremental_end_date
) AS source
ON target.question_id = source.question_id
   -- ── Partition pruning hint: tell BigQuery which partitions to check ────────
   AND target.creation_date BETWEEN incremental_start_date AND incremental_end_date

WHEN MATCHED AND (
  target.score != source.score
  OR target.answer_count != source.answer_count
  OR target.view_count   != source.view_count
) THEN UPDATE SET
  score              = source.score,
  answer_count       = source.answer_count,
  view_count         = source.view_count,
  favorite_count     = source.favorite_count,
  has_accepted_answer = source.has_accepted_answer,
  accepted_answer_id = source.accepted_answer_id,
  is_answered        = source.is_answered,
  engagement_score   = source.engagement_score,
  quality_tier       = source.quality_tier

WHEN NOT MATCHED THEN INSERT ROW;


-- ============================================================================
-- STEP 7: REFRESH ANALYTICS MARTS
-- Marts are fully refreshed daily (they are small aggregated tables)
-- This triggers the mart rebuild queries from 08_analytics_marts.sql
-- In production: call this as a separate scheduled query after steps 1-6
-- ============================================================================
-- NOTE: Mart refresh is handled by 08_analytics_marts.sql
-- Schedule that file to run after this incremental ETL completes.
-- In Airflow: use a TriggerDagRunOperator or downstream task dependency.


-- ============================================================================
-- STEP 8: LOG ETL RUN METADATA
-- Create a simple pipeline audit log table to track run history
-- ============================================================================
CREATE TABLE IF NOT EXISTS `so-analytics-warehouse.raw.pipeline_audit_log` (
  run_id            STRING,
  run_timestamp     TIMESTAMP,
  step_name         STRING,
  rows_processed    INT64,
  rows_inserted     INT64,
  rows_updated      INT64,
  watermark_used    TIMESTAMP,
  status            STRING,
  error_message     STRING
);

INSERT INTO `so-analytics-warehouse.raw.pipeline_audit_log` (
  run_id, run_timestamp, step_name, rows_processed,
  rows_inserted, rows_updated, watermark_used, status, error_message
) VALUES (
  GENERATE_UUID(),
  CURRENT_TIMESTAMP(),
  'incremental_etl_complete',
  NULL,   -- Populate from @@row_count in production
  NULL,
  NULL,
  last_watermark_questions,
  'SUCCESS',
  NULL
);
