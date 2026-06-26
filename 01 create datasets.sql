/*
================================================================================
FILE: 01_create_datasets.sql
PROJECT: Developer Knowledge Platform Analytics Warehouse
LAYER: Infrastructure
PURPOSE: Create all BigQuery datasets for the Medallion Architecture
================================================================================

EXECUTION ORDER: Run first, before any other SQL files.

DATASETS CREATED:
  - raw       : Source-faithful ingestion layer (Bronze)
  - staging   : Cleaned and transformed layer (Silver)
  - warehouse : Dimensional model layer (Gold)
  - marts     : Pre-aggregated analytics layer (Platinum)

NOTES:
  - Replace `so-analytics-warehouse` with your actual GCP project ID
  - Dataset location set to US multi-region for public dataset compatibility
  - Labels applied for cost attribution and governance
  - Default table expiration NOT set on warehouse/marts (permanent tables)
  - Raw layer has 90-day default partition expiry to control storage costs
================================================================================
*/


-- ============================================================================
-- RAW DATASET
-- Purpose: Stores source-faithful copies of Stack Overflow data
-- Retention: Partitions expire after 60 days (configurable)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS `so-analytics-warehouse.raw`
OPTIONS (
  location                     = 'US',
  description                  = 'Raw ingestion layer - source faithful copies of Stack Overflow public data. No transformations applied. Partitions expire after 60 days.',
  default_partition_expiration_days = 60,
  labels = [
    ('environment', 'production'),
    ('layer', 'raw'),
    ('project', 'developer-knowledge-platform'),
    ('managed_by', 'analytics-engineering')
  ]
);


-- ============================================================================
-- STAGING DATASET
-- Purpose: Cleaned, typed, and enriched data ready for dimensional modeling
-- Retention: No expiry (tables rebuilt on each pipeline run via MERGE)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS `so-analytics-warehouse.staging`
OPTIONS (
  location    = 'US',
  description = 'Staging layer - cleaned, type-cast, deduplicated, and enriched Stack Overflow data. Derived fields computed here. Source for warehouse dimensional model.',
  labels = [
    ('environment', 'production'),
    ('layer', 'staging'),
    ('project', 'developer-knowledge-platform'),
    ('managed_by', 'analytics-engineering')
  ]
);


-- ============================================================================
-- WAREHOUSE DATASET
-- Purpose: Star schema dimensional model (dimensions + fact tables)
-- Retention: Permanent (no expiry)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS `so-analytics-warehouse.warehouse`
OPTIONS (
  location    = 'US',
  description = 'Warehouse layer - star schema dimensional model. Contains dim_date, dim_user, dim_tag, fact_questions, fact_answers, fact_user_activity. Partitioned and clustered for BI performance.',
  labels = [
    ('environment', 'production'),
    ('layer', 'warehouse'),
    ('project', 'developer-knowledge-platform'),
    ('managed_by', 'analytics-engineering')
  ]
);


-- ============================================================================
-- MARTS DATASET
-- Purpose: Pre-aggregated analytics tables optimized for Tableau consumption
-- Retention: Permanent (tables refreshed daily via scheduled queries)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS `so-analytics-warehouse.marts`
OPTIONS (
  location    = 'US',
  description = 'Analytics marts layer - pre-aggregated business metrics for Tableau dashboards. Tables refresh daily. Executive KPIs, technology trends, community health, contributor analytics.',
  labels = [
    ('environment', 'production'),
    ('layer', 'marts'),
    ('project', 'developer-knowledge-platform'),
    ('managed_by', 'analytics-engineering')
  ]
);
