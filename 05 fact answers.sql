CREATE OR REPLACE TABLE `so-analytics-warehouse.warehouse.fact_answers`
CLUSTER BY question_id, user_id
OPTIONS (
  description = 'Fact table for Stack Overflow answers.',
  labels = [('layer', 'warehouse'), ('type', 'fact')]
)
AS
WITH
answers_enriched AS (
  SELECT
    a.answer_id,
    a.question_id,
    COALESCE(a.owner_user_id, -1)                           AS user_id,
    CAST(FORMAT_DATE('%Y%m%d', a.answer_date) AS INT64)     AS date_key,
    a.answer_date                                           AS creation_date,
    a.creation_date                                         AS creation_timestamp,
    a.last_activity_date,
    a.score,
    a.comment_count,
    CASE
      WHEN q.accepted_answer_id = a.answer_id THEN 1
      ELSE 0
    END                                                     AS accepted_answer_flag,
    CASE
      WHEN a.score >= 10  THEN 'High Quality'
      WHEN a.score >= 1   THEN 'Good'
      WHEN a.score = 0    THEN 'Neutral'
      WHEN a.score < 0    THEN 'Low Quality'
    END                                                     AS quality_tier,
    a.score_sentiment,
    ROUND(
      (COALESCE(a.score, 0) * 3.0)
      + (COALESCE(a.comment_count, 0) * 0.5),
      4
    )                                                       AS engagement_score,
    a._ingested_at,
    a._batch_id
  FROM `so-analytics-warehouse.staging.stg_answers` a
  LEFT JOIN `so-analytics-warehouse.staging.stg_questions` q
    ON a.question_id = q.question_id
)
SELECT * FROM answers_enriched;