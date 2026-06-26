-- ============================================================================
-- TABLE: warehouse.dim_tag
-- PURPOSE: Technology tag dimension with surrogate keys
-- SOURCE: staging.stg_questions (tags parsed from tag string)
-- NOTE: Tags in Stack Overflow are stored as '<python><django><rest-api>'
--       We explode this into individual tag rows with surrogate keys.
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.warehouse.dim_tag`
CLUSTER BY tag_name
OPTIONS (
  description = 'Technology tag dimension. Exploded from question tag strings. Surrogate key generated via FARM_FINGERPRINT for stable cross-run joins.',
  labels = [('layer', 'warehouse'), ('type', 'dimension')]
)
AS
WITH

-- ── Step 1: Extract all distinct tags from question tag strings ───────────────
-- tags_clean looks like: python|django|rest-api
all_tags AS (
  SELECT DISTINCT
    LOWER(TRIM(tag))  AS tag_name
  FROM
    `so-analytics-warehouse.staging.stg_questions`,
    UNNEST(SPLIT(tags_clean, '|')) AS tag
  WHERE
    tags_clean IS NOT NULL
    AND tags_clean != ''
    AND TRIM(tag) != ''
),

-- ── Step 2: Add tag metadata ─────────────────────────────────────────────────
tags_with_metadata AS (
  SELECT
    tag_name,

    -- ── Surrogate key using FARM_FINGERPRINT ─────────────────────────────────
    -- FARM_FINGERPRINT produces a stable 64-bit integer hash from a string.
    -- Using ABS() to avoid negative values for readability.
    -- This key is STABLE across pipeline runs — same tag always gets same key.
    ABS(FARM_FINGERPRINT(tag_name))                     AS tag_key,

    -- ── Tag category (broad classification) ──────────────────────────────────
    -- Heuristic classification for dashboard filtering
    CASE
      WHEN tag_name IN ('python','r','julia','matlab','scala','haskell','erlang')
        THEN 'Data/Scientific'
      WHEN tag_name IN ('javascript','typescript','coffeescript','elm','purescript')
        THEN 'Web Frontend'
      WHEN tag_name IN ('java','c#','kotlin','swift','objective-c','dart')
        THEN 'Application'
      WHEN tag_name IN ('c','c++','rust','go','assembly','fortran')
        THEN 'Systems'
      WHEN tag_name IN ('sql','mysql','postgresql','oracle','sqlite','bigquery',
                        'mongodb','redis','cassandra','elasticsearch')
        THEN 'Data/Database'
      WHEN tag_name IN ('docker','kubernetes','terraform','ansible','jenkins',
                        'github-actions','aws','gcp','azure')
        THEN 'DevOps/Cloud'
      WHEN tag_name IN ('react','angular','vue.js','svelte','next.js','gatsby')
        THEN 'Web Framework'
      WHEN tag_name IN ('django','flask','fastapi','spring','rails','laravel',
                        'express','nestjs')
        THEN 'Backend Framework'
      WHEN tag_name IN ('machine-learning','deep-learning','tensorflow','pytorch',
                        'scikit-learn','keras','nlp','computer-vision')
        THEN 'AI/ML'
      WHEN tag_name IN ('android','ios','react-native','flutter','xamarin')
        THEN 'Mobile'
      ELSE 'Other'
    END                                                 AS tag_category,

    CURRENT_TIMESTAMP()                                 AS dim_created_at

  FROM all_tags
)

SELECT
  tag_key,
  tag_name,
  tag_category,
  dim_created_at
FROM tags_with_metadata
;


-- ============================================================================
-- VALIDATION: Confirm dimension integrity
-- ============================================================================
SELECT
  'dim_date'    AS dimension,
  COUNT(*)      AS row_count,
  MIN(full_date) AS min_date,
  MAX(full_date) AS max_date,
  COUNTIF(date_key IS NULL) AS null_keys
FROM `so-analytics-warehouse.warehouse.dim_date`

UNION ALL

SELECT
  'dim_user',
  COUNT(*),
  NULL,
  NULL,
  COUNTIF(user_id IS NULL)
FROM `so-analytics-warehouse.warehouse.dim_user`

UNION ALL

SELECT
  'dim_tag',
  COUNT(*),
  NULL,
  NULL,
  COUNTIF(tag_key IS NULL)
FROM `so-analytics-warehouse.warehouse.dim_tag`;
