# Developer-Knowledge-Platform-Analytics-Warehouse
A production-grade analytics warehouse built on the Stack Overflow public dataset in BigQuery, featuring a full Medallion Architecture, dimensional modeling, incremental ETL, data quality validation, and a 5-page Tableau Public dashboard.

Tableau Dashboard: https://public.tableau.com/app/profile/dylan.camas/viz/DeveloperKnowledgePlatformAnalyticsWarehouse/Dashboard5

This project transforms 90GB of raw Stack Overflow data into a fully functional analytics warehouse that answers executive-level business questions about technology adoption, community health, and contributor engagement.

---

## Business Questions Answered

| Question | Mart Table | Dashboard Page |
|---|---|---|
| Which technologies are growing fastest? | `mart_tag_growth` | Technology Trends |
| Which technologies are declining? | `mart_tag_growth` | Technology Trends |
| Which communities are healthiest? | `mart_community_health` | Community Health |
| Which tags have the highest unanswered rates? | `mart_unanswered_questions` | Community Health |
| Who are the top contributors? | `mart_top_contributors` | Contributor Analytics |
| How has contributor retention changed? | `mart_contributor_retention` | Contributor Analytics |
| How fast do questions get answered? | `mart_answer_latency` | Answer Latency |
| What technologies are emerging? | `mart_tag_growth` | Technology Trends |

---

## Architecture

```
bigquery-public-data.stackoverflow
            │
            ▼
    ┌───────────────┐
    │   RAW LAYER   │  Source-faithful copy with metadata columns
    │   (Bronze)    │  raw_questions, raw_answers, raw_users
    └───────┬───────┘
            │
            ▼
    ┌───────────────┐
    │ STAGING LAYER │  Cleaned, typed, deduplicated, enriched
    │   (Silver)    │  stg_questions, stg_answers, stg_users
    └───────┬───────┘
            │
            ▼
    ┌───────────────┐
    │  WAREHOUSE    │  Star schema dimensional model
    │   (Gold)      │  3 dimensions + 3 fact tables
    └───────┬───────┘
            │
            ▼
    ┌───────────────┐
    │    MARTS      │  Pre-aggregated business metrics
    │  (Platinum)   │  7 mart tables
    └───────┬───────┘
            │
            ▼
    ┌───────────────┐
    │    TABLEAU    │  5-page executive dashboard
    │   DASHBOARD   │
    └───────────────┘
```

---

## Data Model

### Dimensions
| Table | Description | Rows |
|---|---|---|
| `dim_date` | Date spine 2008–2035 | ~10,000 |
| `dim_user` | User profiles with reputation bucketing | ~18M |
| `dim_tag` | Technology tags with category classification | ~63,000 |

### Facts
| Table | Description | Rows |
|---|---|---|
| `fact_questions` | One row per question | ~23M |
| `fact_answers` | One row per answer | ~34M |
| `fact_user_activity` | One row per user per active day | ~40M |

### Analytics Marts
| Table | Business Purpose |
|---|---|
| `mart_tag_growth` | YoY technology growth rates using LAG() |
| `mart_trending_technologies` | Composite popularity score (weighted formula) |
| `mart_unanswered_questions` | Knowledge gap analysis by technology |
| `mart_top_contributors` | Contributor leaderboard with acceptance rates |
| `mart_contributor_retention` | Monthly cohort retention analysis |
| `mart_answer_latency` | Response time metrics by technology |
| `mart_community_health` | Composite health score per technology |

---

## Tech Stack

| Component | Technology |
|---|---|
| Cloud Platform | Google Cloud Platform |
| Data Warehouse | BigQuery |
| Source Data | Stack Overflow Public Dataset |
| Architecture Pattern | Medallion (Raw → Staging → Warehouse → Marts) |
| Incremental Loading | BigQuery MERGE statements + watermark strategy |
| Data Quality | SQL-based validation framework (12 checks) |
| Visualization | Tableau Public |

---

## Key SQL Techniques Demonstrated

- `MERGE` statements for incremental upsert loading
- `ROW_NUMBER()` for deduplication
- `LAG()` and `LEAD()` for year-over-year growth rates
- `RANK()`, `DENSE_RANK()`, `NTILE()` for contributor ranking
- `PERCENT_RANK()` for reputation percentile scoring
- `FARM_FINGERPRINT()` for stable surrogate key generation
- `UNNEST(SPLIT())` for tag explosion
- Window functions for rolling averages and cohort analysis
- `GENERATE_ARRAY` for date spine generation
- Partitioning and clustering for query optimization

---

## Data Quality Framework

12 automated validation checks covering:

1. Duplicate primary keys in fact tables
2. Null foreign keys
3. Invalid dates (pre-2008 or future dates)
4. Orphaned answers (no matching question)
5. Missing dimension records
6. Date key referential integrity
7. Out-of-range reputation values
8. Invalid score values
9. Data freshness check

---

## Repository Structure

```
developer-knowledge-platform-analytics-warehouse/
│
├── README.md
│
├── architecture/
│   └── architecture.md           # Full architecture documentation
│
├── sql/
│   ├── 01_create_datasets.sql    # BigQuery dataset creation
│   ├── 02_raw_layer.sql          # Source ingestion
│   ├── 03_staging_layer.sql      # Cleaning + transformation
│   ├── 04_dimensions.sql         # Dimension table creation
│   ├── 05_fact_tables.sql        # Fact table creation
│   ├── 06_data_quality_checks.sql# Validation framework
│   ├── 07_incremental_etl.sql    # MERGE-based incremental load
│   ├── 08_analytics_marts.sql    # Pre-aggregated mart tables
│   └── 09_kpi_queries.sql        # Executive KPI queries
│
├── tableau/
│   └── dashboard_specification.md# Full Tableau spec
│
└── docs/
    ├── business_questions.md     # Business requirements mapping
    └── data_dictionary.md        # Field-level documentation
```

---

## Dashboard Pages

| Page | Description |
|---|---|
| Executive Overview | Technology trends, retention, and community health at a glance |
| Technology Trends | Top growing/declining technologies, YoY growth rates |
| Community Health | Health scores, unanswered rates, answer vs acceptance scatter |
| Contributor Analytics | Top contributors, acceptance rates, retention trends |
| Answer Latency Analysis | Fastest/slowest responding communities |

---

## Getting Started

### Prerequisites
- Google Cloud project with BigQuery enabled
- Billing account linked to GCP project
- Access to `bigquery-public-data.stackoverflow` (free, public)

### Setup

Run SQL files in order:

```bash
# 1. Create datasets
bq query --use_legacy_sql=false < sql/01_create_datasets.sql

# 2. Ingest raw data (~90GB, runs in ~1 minute)
bq query --use_legacy_sql=false < sql/02_raw_layer.sql

# 3. Clean and transform
bq query --use_legacy_sql=false < sql/03_staging_layer.sql

# 4. Build dimensions
bq query --use_legacy_sql=false < sql/04_dimensions.sql

# 5. Build fact tables
bq query --use_legacy_sql=false < sql/05_fact_tables.sql

# 6. Run data quality checks
bq query --use_legacy_sql=false < sql/06_data_quality_checks.sql

# 7. Build analytics marts
bq query --use_legacy_sql=false < sql/08_analytics_marts.sql

# 8. Run KPI queries
bq query --use_legacy_sql=false < sql/09_kpi_queries.sql
```

> **Note:** Each SQL file must be run as individual statements in the BigQuery console. Files with multiple CREATE statements should be split into separate query tabs.

### Cost Estimate
- Initial load: ~$5–10 (covered by free credits)
- Storage: ~$1–2/month
- Daily queries: <$1/month

---

## Key Results

| Metric | Value |
|---|---|
| Total Questions Loaded | 23,020,127 |
| Total Answers Loaded | 34,024,119 |
| Registered Users | 18,712,203 |
| Distinct Technologies | 63,591 |
| Platform Answer Rate | 85.59% |
| Platform Acceptance Rate | 51.07% |
| Top Technology (2022) | Python |
| Top Contributor | VonC |

---

## Resume Bullets

- Architected a production Medallion Analytics Warehouse on BigQuery ingesting 90GB of Stack Overflow data across raw, staging, warehouse, and mart layers with incremental ETL using MERGE statements and watermark-based delta processing.

- Designed and implemented a star schema dimensional model (3 dimensions, 3 fact tables) with BigQuery clustering, achieving fast query response times on 100M+ row datasets for executive-facing Tableau dashboards.

- Built a 7-mart analytics engineering layer with advanced window functions (LAG, LEAD, NTILE, PERCENT_RANK, DENSE_RANK) computing technology growth rates, contributor retention cohorts, and composite community health scores.

- Delivered a SQL-based data quality validation framework with 12 automated checks (duplicate keys, null FKs, referential integrity, date validation, range checks) ensuring warehouse reliability across 15+ tables.

- Designed and published a 5-page Tableau Public executive dashboard (Technology Trends, Community Health, Contributor Analytics, Answer Latency, Executive Overview) with 14 visualizations and interactive cross-filtering.

---

## Future Improvements

- [ ] dbt integration for lineage, testing, and documentation
- [ ] Airflow orchestration for pipeline scheduling and alerting
- [ ] SCD Type 2 for dim_user to track historical reputation changes
- [ ] Terraform for infrastructure as code
- [ ] CI/CD pipeline with SQL linting and data quality gates
- [ ] Real-time streaming layer via Pub/Sub and Dataflow

---

*Data source: Stack Overflow public dataset — `bigquery-public-data.stackoverflow`*  
*Built on Google BigQuery | Visualized with Tableau Public*
