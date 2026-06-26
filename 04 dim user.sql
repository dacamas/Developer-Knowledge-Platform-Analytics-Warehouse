CREATE OR REPLACE TABLE `so-analytics-warehouse.warehouse.dim_user`
CLUSTER BY reputation, reputation_bucket
OPTIONS (
  description = 'User dimension with reputation bucketing and account age.',
  labels = [('layer', 'warehouse'), ('type', 'dimension')]
)
AS
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
  END                             AS account_age_bucket,
  reputation,
  reputation_bucket,
  reputation_bucket_ordinal,
  up_votes,
  down_votes,
  profile_views,
  location,
  is_active,
  _staged_at                      AS dim_updated_at
FROM `so-analytics-warehouse.staging.stg_users`;

INSERT INTO `so-analytics-warehouse.warehouse.dim_user` (
  user_id, display_name, account_creation_date, account_creation_date_date,
  last_access_date, account_age_days, account_age_bucket,
  reputation, reputation_bucket, reputation_bucket_ordinal,
  up_votes, down_votes, profile_views, location, is_active, dim_updated_at
)
VALUES (
  -1, 'Anonymous/Deleted User', TIMESTAMP('2008-01-01'), DATE('2008-01-01'),
  TIMESTAMP('2008-01-01'), 0, 'New (< 1 year)',
  1, 'Beginner', 1,
  0, 0, 0, 'Unknown', FALSE, CURRENT_TIMESTAMP()
);