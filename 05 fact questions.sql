CREATE OR REPLACE TABLE `so-analytics-warehouse.warehouse.fact_questions`
CLUSTER BY user_id, score
OPTIONS (
  description = 'Fact table for Stack Overflow questions.',
  labels = [('layer', 'warehouse'), ('type', 'fact')]
)
AS
WITH
questions_with_keys AS (
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
)
SELECT * FROM questions_with_keys;