/*
================================================================================
FILE: 02_raw_layer.sql
PROJECT: Developer Knowledge Platform Analytics Warehouse
LAYER: Raw (Bronze)
PURPOSE: Ingest Stack Overflow source data into the raw layer
================================================================================

EXECUTION ORDER: Run after 01_create_datasets.sql

SOURCE:
  bigquery-public-data.stackoverflow.posts_questions
  bigquery-public-data.stackoverflow.posts_answers
  bigquery-public-data.stackoverflow.users

TABLES CREATED:
  raw.raw_questions
  raw.raw_answers
  raw.raw_users

DESIGN PRINCIPLES:
  1. Preserve source schema exactly — no business logic applied
  2. Add metadata columns (_ingested_at, _source_table, _batch_id)
  3. Partition by _ingested_at for efficient incremental delta queries
  4. No filtering — ingest complete source (initial load)
  5. Initial load uses CREATE OR REPLACE TABLE AS SELECT (CTAS)
     Subsequent loads use the MERGE pattern in 07_incremental_etl.sql

PARTITIONING RATIONALE:
  All raw tables are partitioned by _ingested_at (ingestion date).
  This allows the incremental ETL to efficiently query:
    WHERE _ingested_at > last_watermark
  Without this partition, every incremental run would scan the full table.

COST NOTE:
  Initial load scans the full public dataset (~60GB total).
  Subsequent incremental loads scan only new partitions (<1GB per day).
================================================================================
*/


-- ============================================================================
-- TABLE: raw.raw_questions
-- SOURCE: bigquery-public-data.stackoverflow.posts_questions
-- ROWS: ~23M (as of 2024)
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.raw.raw_questions`
PARTITION BY DATE(_ingested_at)
CLUSTER BY id
OPTIONS (
  description         = 'Raw ingestion of Stack Overflow questions. Source-faithful copy with metadata columns. Partitioned by ingestion date.',
  require_partition_filter = FALSE,
  labels = [('layer', 'raw'), ('source', 'stackoverflow_questions')]
)
AS
SELECT
  -- ── Source columns (preserved exactly) ────────────────────────────────────
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

  -- ── Pipeline metadata columns ──────────────────────────────────────────────
  CURRENT_TIMESTAMP()                             AS _ingested_at,
  'bigquery-public-data.stackoverflow.posts_questions' AS _source_table,
  GENERATE_UUID()                                 AS _batch_id,
  'initial_load'                                  AS _load_type

FROM
  `bigquery-public-data.stackoverflow.posts_questions`

WHERE
  -- Filter to valid question records only
  -- post_type_id = 1 means "Question" in Stack Overflow schema
  post_type_id = 1
  -- Exclude records with null IDs (data quality guard)
  AND id IS NOT NULL
  -- Exclude far-future dates (data corruption guard)
  AND creation_date <= CURRENT_TIMESTAMP();


-- ============================================================================
-- TABLE: raw.raw_answers
-- SOURCE: bigquery-public-data.stackoverflow.posts_answers
-- ROWS: ~52M (as of 2024)
-- NOTE: In the public dataset, answers are a separate table. If your
--       BigQuery project only has posts_questions, use:
--       bigquery-public-data.stackoverflow.posts_answers
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.raw.raw_answers`
PARTITION BY DATE(_ingested_at)
CLUSTER BY id, parent_id
OPTIONS (
  description         = 'Raw ingestion of Stack Overflow answers. Source-faithful copy with metadata columns. Partitioned by ingestion date, clustered by id and parent_id (question_id).',
  require_partition_filter = FALSE,
  labels = [('layer', 'raw'), ('source', 'stackoverflow_answers')]
)
AS
SELECT
  -- ── Source columns ─────────────────────────────────────────────────────────
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
  parent_id,              -- This is the question_id for answers
  post_type_id,
  score,
  tags,
  view_count,

  -- ── Pipeline metadata ──────────────────────────────────────────────────────
  CURRENT_TIMESTAMP()                             AS _ingested_at,
  'bigquery-public-data.stackoverflow.posts_answers' AS _source_table,
  GENERATE_UUID()                                 AS _batch_id,
  'initial_load'                                  AS _load_type

FROM
  `bigquery-public-data.stackoverflow.posts_answers`

WHERE
  -- post_type_id = 2 means "Answer"
  post_type_id = 2
  AND id IS NOT NULL
  AND parent_id IS NOT NULL        -- Orphaned answers are useless; exclude them
  AND creation_date <= CURRENT_TIMESTAMP();


-- ============================================================================
-- TABLE: raw.raw_users
-- SOURCE: bigquery-public-data.stackoverflow.users
-- ROWS: ~15M (as of 2024)
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.raw.raw_users`
PARTITION BY DATE(_ingested_at)
CLUSTER BY id
OPTIONS (
  description         = 'Raw ingestion of Stack Overflow users. Source-faithful copy with metadata columns.',
  require_partition_filter = FALSE,
  labels = [('layer', 'raw'), ('source', 'stackoverflow_users')]
)
AS
SELECT
  -- ── Source columns ─────────────────────────────────────────────────────────
  id,
  display_name,
  about_me,
  age,
  creation_date,
  last_access_date,
  location,
  reputation,
  up_votes,
  down_votes,
  views,
  website_url,

  -- ── Pipeline metadata ──────────────────────────────────────────────────────
  CURRENT_TIMESTAMP()                             AS _ingested_at,
  'bigquery-public-data.stackoverflow.users'      AS _source_table,
  GENERATE_UUID()                                 AS _batch_id,
  'initial_load'                                  AS _load_type

FROM
  `bigquery-public-data.stackoverflow.users`

WHERE
  id IS NOT NULL
  -- Exclude system/bot accounts (negative IDs are special Stack Overflow accounts)
  AND id > 0
  AND creation_date <= CURRENT_TIMESTAMP();


-- ============================================================================
-- POST-LOAD VALIDATION: Row counts and basic sanity checks
-- Run after CTAS completes to confirm successful ingestion
-- ============================================================================
SELECT
  'raw_questions'                 AS table_name,
  COUNT(*)                        AS row_count,
  MIN(creation_date)              AS earliest_record,
  MAX(creation_date)              AS latest_record,
  COUNTIF(id IS NULL)             AS null_ids,
  COUNTIF(owner_user_id IS NULL)  AS null_user_ids,
  MAX(_ingested_at)               AS last_ingested_at
FROM `so-analytics-warehouse.raw.raw_questions`

UNION ALL

SELECT
  'raw_answers',
  COUNT(*),
  MIN(creation_date),
  MAX(creation_date),
  COUNTIF(id IS NULL),
  COUNTIF(owner_user_id IS NULL),
  MAX(_ingested_at)
FROM `so-analytics-warehouse.raw.raw_answers`

UNION ALL

SELECT
  'raw_users',
  COUNT(*),
  MIN(creation_date),
  MAX(creation_date),
  COUNTIF(id IS NULL),
  COUNTIF(id IS NULL),    -- users don't have owner_user_id; reuse id check
  MAX(_ingested_at)
FROM `so-analytics-warehouse.raw.raw_users`

ORDER BY table_name;
