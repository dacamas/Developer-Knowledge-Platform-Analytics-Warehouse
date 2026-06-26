/*
================================================================================
FILE: 04_dimensions.sql
PROJECT: Developer Knowledge Platform Analytics Warehouse
LAYER: Warehouse (Gold) — Dimensions
PURPOSE: Create dimension tables for the star schema
================================================================================

EXECUTION ORDER: Run after 03_staging_layer.sql

TABLES CREATED:
  warehouse.dim_date   — Date spine dimension
  warehouse.dim_user   — User/contributor dimension
  warehouse.dim_tag    — Technology tag dimension

DIMENSION TABLE DESIGN PRINCIPLES:
  1. Surrogate keys used for tag dimension (natural key for user/date)
  2. All dimensions are SCD Type 1 (overwrite on change)
  3. dim_date is static — generated once, covers 2008–2035
  4. dim_user and dim_tag refresh via MERGE in the incremental ETL
  5. No partitioning on dimension tables (they are small enough to full-scan)
  6. Clustering on most-queried filter columns

SCD TYPE 1 RATIONALE:
  For this use case, historical user reputation values are not needed.
  We track "current" reputation only. If historical tracking is required,
  this can be upgraded to SCD Type 2 with effective_from/effective_to dates.
================================================================================
*/


-- ============================================================================
-- TABLE: warehouse.dim_date
-- PURPOSE: Date spine for time-based analysis
-- GENERATION: Static table covering 2008-01-01 to 2035-12-31
-- NOTE: Stack Overflow launched July 31, 2008. Starting from Jan 1 2008
--       ensures no questions fall outside the date spine.
-- ============================================================================
CREATE OR REPLACE TABLE `so-analytics-warehouse.warehouse.dim_date`
CLUSTER BY year, month
OPTIONS (
  description = 'Date dimension covering 2008-01-01 through 2035-12-31. Includes year, quarter, month, week, day-of-week attributes for flexible time-series analysis.',
  labels = [('layer', 'warehouse'), ('type', 'dimension')]
)
AS
WITH

-- ── Generate a date spine using UNNEST + GENERATE_ARRAY ──────────────────────
-- GENERATE_ARRAY creates integers 0..N, which we offset from a base date
-- This is the idiomatic BigQuery approach to generating date spines
date_spine AS (
  SELECT
    DATE_ADD(DATE '2008-01-01', INTERVAL day_offset DAY) AS full_date
  FROM
    UNNEST(
      GENERATE_ARRAY(
        0,
        DATE_DIFF(DATE '2035-12-31', DATE '2008-01-01', DAY)
      )
    ) AS day_offset
)

SELECT
  -- ── Surrogate key: YYYYMMDD integer format ───────────────────────────────
  -- Using integer format for compact storage and fast joins
  CAST(FORMAT_DATE('%Y%m%d', full_date) AS INT64)         AS date_key,

  full_date,

  -- ── Year ─────────────────────────────────────────────────────────────────
  EXTRACT(YEAR FROM full_date)                            AS year,

  -- ── Quarter ──────────────────────────────────────────────────────────────
  EXTRACT(QUARTER FROM full_date)                         AS quarter,
  CONCAT('Q', CAST(EXTRACT(QUARTER FROM full_date) AS STRING)) AS quarter_name,

  -- ── Month ─────────────────────────────────────────────────────────────────
  EXTRACT(MONTH FROM full_date)                           AS month,
  FORMAT_DATE('%B', full_date)                            AS month_name,
  FORMAT_DATE('%b', full_date)                            AS month_short_name,

  -- ── Week ──────────────────────────────────────────────────────────────────
  EXTRACT(WEEK FROM full_date)                            AS week_of_year,
  EXTRACT(ISOWEEK FROM full_date)                         AS iso_week,

  -- ── Day ───────────────────────────────────────────────────────────────────
  EXTRACT(DAY FROM full_date)                             AS day_of_month,
  EXTRACT(DAYOFWEEK FROM full_date)                       AS day_of_week,        -- 1=Sunday
  FORMAT_DATE('%A', full_date)                            AS day_name,
  FORMAT_DATE('%a', full_date)                            AS day_short_name,

  -- ── Boolean helpers ───────────────────────────────────────────────────────
  CASE WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) THEN TRUE ELSE FALSE END
                                                          AS is_weekend,

  -- ── Fiscal year (assuming Jan 1 fiscal year start) ───────────────────────
  EXTRACT(YEAR FROM full_date)                            AS fiscal_year,

  -- ── Period labels for charts ──────────────────────────────────────────────
  FORMAT_DATE('%Y-%m', full_date)                         AS year_month,
  FORMAT_DATE('%Y-Q%Q', full_date)                        AS year_quarter,

  -- ── Days since Stack Overflow launch ─────────────────────────────────────
  DATE_DIFF(full_date, DATE '2008-07-31', DAY)            AS days_since_so_launch

FROM date_spine

