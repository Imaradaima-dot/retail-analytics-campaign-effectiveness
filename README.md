# Real World Data: Retail Analytics Platform — Customer 360 & Campaign Intelligence

> End-to-end retail analytics platform built on a dual-database persistence architecture.  
> Transforms raw transaction data into a Customer 360 intelligence layer with RFM segmentation, campaign attribution, churn prediction, and CLV modelling — enriched with macroeconomic and weather signals.

---

## Table of Contents

1. [Overview](#overview)
2. [Business Problem](#business-problem)
3. [Architecture](#architecture)
4. [Dataset](#dataset)
5. [Project Phases](#project-phases)
   - [Phase 1 — Exploratory Data Analysis](#phase-1--exploratory-data-analysis)
   - [Phase 2 — Dual Persistence & Database Design](#phase-2--dual-persistence--database-design)
   - [Phase 2 — External Data Enrichment](#phase-2--external-data-enrichment)
   - [Phase 2 — Modelling & SHAP Analysis](#phase-2--modelling--shap-analysis)
   - [Phase 2 — SQL & MongoDB Business Insights](#phase-2--sql--mongodb-business-insights)
6. [Key Findings](#key-findings)
7. [Repository Structure](#repository-structure)
8. [Tech Stack](#tech-stack)
9. [Setup & Installation](#setup--installation)
10. [Results at a Glance](#results-at-a-glance)

---

## Overview

This project is a full-stack retail analytics case study built on the [Dunnhumby — The Complete Journey](https://www.dunnhumby.com/source-files) dataset. It demonstrates how real world raw retail transaction data can be transformed into a production-grade Customer 360 analytics foundation using a **dual-database persistence** strategy — combining a PostgreSQL star schema with a MongoDB Customer 360 document store — enriched with real-world macroeconomic and weather data.

The platform answers five core business questions through direct database queries (no ML required for analytics), while also providing an optional modelling layer that uses XGBoost, SHAP explainability, K-Means clustering, Difference-in-Differences causal inference, and OLS regression for deeper predictive and causal analysis.

---

## Business Problem

Retail organisations need a unified view of customer behaviour, campaign performance, product trends, price sensitivity, and churn risk. Raw transaction data alone is fragmented, context-free, and difficult to operationalise. This platform addresses four interconnected challenges:

| Challenge | Solution |
|-----------|----------|
| Relational schema alone can't serve flexible customer profiles | Dual persistence — PostgreSQL for transactions, MongoDB for Customer 360 |
| Customer profiles scattered across normalised tables | MongoDB Customer 360 — one enriched document per household |
| Campaigns evaluated without baselines | Difference-in-Differences with 28-day pre-window |
| Churn signals hidden in internal data | XGBoost + external weather enrichment + SHAP attribution |
| Macro conditions ignored in analytics | FRED PPD inflation index + Open-Meteo weather integration |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│   Dunnhumby CSV files  │  FRED API (CPI/PPD)  │  Open-Meteo API  │
└───────────┬────────────────────┬──────────────────┬─────────────┘
            │                    │                  │
            ▼                    ▼                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                  PYTHON ETL LAYER  (Docker Compose)              │
│          mongo_ingest.py  │  postgres_campaign_ingest.py         │
└───────────┬──────────────────────────┬───────────────────────────┘
            │                          │
            ▼                          ▼
┌───────────────────────┐   ┌──────────────────────────────────────┐
│      PostgreSQL       │   │              MongoDB                  │
│                       │   │                                      │
│  Star Schema          │   │  Customer 360 Document Store         │
│  1,427,303 rows       │   │  924 household documents             │
│  Dim / Fact tables    │   │  RFM · ML scores · Geo · Exog        │
│  Ext: calendar,       │   │  Real-time aggregation queries       │
│  macro, weather       │   │  avg 4.87ms per segment query        │
└───────────┬───────────┘   └──────────────┬───────────────────────┘
            │                              │
            └──────────────┬───────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                  ANALYTICS & MODELLING LAYER                     │
│   SQL Insights  │  MongoDB Aggregations  │  XGBoost + SHAP       │
│   DiD Uplift    │  K-Means CLV           │  OLS Trade-Down        │
└──────────────────────────────────────────────────────────────────┘
```

**Design rationale — two databases, two jobs:**  
PostgreSQL handles what relational databases do best: enforcing transactional integrity across 1.4M rows, supporting complex multi-table analytical queries, and storing external enrichment data in structured form. MongoDB handles what document stores do best: representing each household as a single, flexible, fully-enriched object that can be queried in real time without joins. The two stores are complementary — PostgreSQL is the analytical backbone, MongoDB is the operational Customer 360 layer.

---

## Dataset

**Source:** [Dunnhumby — The Complete Journey](https://www.dunnhumby.com/source-files)

| Table | Description | Rows |
|-------|-------------|------|
| `transaction_data` | Household-level basket transactions | 2.5M+ |
| `hh_demographic` | Household demographic classifications (7 variables) | 801 |
| `product` | Product hierarchy: department → commodity → sub-commodity | 92K |
| `campaign_table` | Household × campaign assignments | ~7K |
| `campaign_desc` | Campaign type (A/B/C) and duration | 30 |
| `coupon` | Coupon → product mappings | ~1.7K |
| `coupon_redempt` | Coupon redemption events | ~2.3K |
| `causal_data` | Store-level display/mailer flags | 500K+ |

**Date anchor:** Day 1 = January 1 2023 → Day 711 = December 11 2024  
*(Weeks 1–16 are a panel recruitment ramp-up artefact; excluded from steady-state analysis)*

---

## Project Phases

### Phase 1 — Exploratory Data Analysis

**Notebook:** `01_eda_customer360.ipynb`

Establishes baseline findings across three research questions that carry through all subsequent phases:

**Q1 — Spending Trends**
- Identified a 16-week ramp-up artefact (panel recruitment) and a Week 102 truncation
- Steady-state weekly spend: **$38K–$58K** (avg $49K) across 801 households
- Year-on-year growth: **+33.5%** (2023 → 2024)

**Q2 — Demographic Influences**
- Tested all 7 classification variables for spend predictive power
- `classification_3` is the strongest signal with a **$6,693 spend spread** across 12 levels (3× the next-best variable)
- `classification_2` Group X represents **46% of total revenue** despite being 43% of households

**Q3 — Category Concentration**
- GROCERY dominates at **50.2% of total revenue** ($2.26M)
- Top 5 departments account for **84.7% of all revenue**
- All top-15 departments showed positive YoY growth; KIOSK-GAS fastest at **+60.9%**

---

### Phase 2 — Polyglot Persistence & Database Design

#### PostgreSQL — Star Schema

**Scripts:** `postgres/init/01-create-databases.sh`, `02-create-schema.sql`, `sql/03-add-campaign-tables.sql`  
**Ingest:** `src/ingest/postgres_campaign_ingest.py`

Designed and hydrated a full star schema optimised for analytical queries:

```
dim_households        dim_demographics      dim_products
dim_stores            dim_departments       dim_date
dim_commodities       dim_sub_commodities   dim_campaigns
fact_transactions     fact_coupon_redemptions
campaign_table        coupon                coupon_redempt
ext_calendar          ext_macro_weekly      ext_weather_daily
mart_household_metrics  mart_churn_scores   mart_household_segments
```

Key design decisions:
- `dim_demographics` is a separate table from `dim_households` — classification columns join via `household_key`
- `ext_calendar` provides `is_rampup` and `is_truncation` flags for clean time-windowing
- Campaign tables loaded via Python ingest script (not notebook CTEs) for reproducibility
- `mart_*` tables store pre-computed aggregates for downstream ML feature engineering

#### MongoDB — Customer 360

**Notebook:** `02_mongodb_design_hydration.ipynb`, `03_mongodb_customer360_enrichment.ipynb`  
**Ingest:** `src/ingest/mongo_ingest.py`

Each of the 924 households is represented as a single enriched document:

```json
{
  "_id": "HH_232",
  "household_key": 232,
  "demographics": { "classification_1": "A", ..., "classification_7": "D" },
  "financial_metrics": {
    "lifetime_spend": 12695.57,
    "avg_basket_value": 20.12,
    "total_trips": 631,
    "avg_inter_purchase_days": 1.2,
    "days_since_last_purchase": 39,
    "coupon_redemption_rate": 0.08
  },
  "rfm_segment": "At Risk",
  "rfm_scores": { "R": 2, "F": 5, "M": 5, "rfm_code": "255" },
  "ml_scores": {
    "churn_risk_score": 0.74,
    "churn_label": "At Risk",
    "clv_segment": "High Value",
    "model_version": "xgb_v3_clean_weather",
    "scored_at": "2024-03-11T00:00:00Z"
  },
  "exog_context": {
    "ppd_at_last_purchase": 4.2,
    "food_inflation_at_last": 3.8,
    "avg_temp_last90d": 78.4,
    "rain_days_last90d": 12,
    "heat_days_last90d": 45,
    "cold_days_last90d": 2
  },
  "segments": ["High Value", "At Risk", "Champions"],
  "preferred_categories": ["GROCERY", "PRODUCE", "DRUG GM"],
  "recent_transactions": [...],
  "active_campaigns": [...],
  "nearest_store": {
    "store_id": 367,
    "location": { "type": "Point", "coordinates": [-96.65, 33.10] }
  }
}
```

**6 indexes:** 2dsphere (geo), churn+CLV compound, segments, recency, lifetime spend, RFM segment  
**Query benchmarks (50 runs, local Docker):**
| Query | Avg | P95 |
|-------|-----|-----|
| Single household lookup | 1.25ms | — |
| RFM segment aggregation | 4.87ms | 5.08ms |
| Spend leaderboard | 7.20ms | — |

---

### Phase 2 — External Data Enrichment

**Notebook:** `04_modeling_campaign_effectiveness.ipynb` (Sections 3–4)

Two external data sources were integrated to provide economic and environmental context:

#### Macroeconomic — FRED PPD Index
- **Source:** Federal Reserve Economic Data (FRED) — Food-at-home CPI
- **Metric:** PPD (Purchasing Power Decay) index anchored to Jan 2023 baseline
- **Range observed:** 1.40 → 5.50 over the 2-year window
- **Stored in:** `ext_macro_weekly` (102 rows) — joined to transactions via `week_num`
- **Use:** Controls for inflationary environment in campaign uplift OLS and trade-down analysis

#### Weather — Open-Meteo / NOAA
- **Location:** Allen, TX (retailer's primary market)
- **Variables:** Daily max temp, precipitation, heat-day flag (≥95°F), cold-day flag (≤32°F)
- **Stored in:** `ext_weather_daily` (711 rows) — joined via `day_int`
- **Aggregated to:** 90-day rolling windows per household at last purchase date
- **Use:** SHAP analysis revealed weather as the dominant churn signal (see below)

---

### Phase 2 — Modelling & SHAP Analysis

**Notebook:** `04_modeling_campaign_effectiveness.ipynb`  
**Service:** `src/serving/model_scoring_service.py`

#### Section 6 — Churn Prediction (XGBoost + SHAP)

**Target:** Binary churn label — households with `days_since_last_purchase ≥ 30` at dataset end

**Feature engineering:** Two model variants were tested to isolate the value of exogenous data:

| Model | Features | ROC-AUC | PR-AUC |
|-------|----------|---------|--------|
| Model A — Internal only | Transaction behaviour, RFM (F+M only), campaign exposure, demographics | 0.8275 | 0.1607 |
| **Model B — + Weather (selected)** | **Model A features + rain/heat/cold 90-day windows** | **0.9109** | **0.6296** |

> Adding weather data improved PR-AUC by **4×** (0.16 → 0.63) — the most meaningful improvement on the minority churn class.

**Internal features used (leakage-cleaned):**

```python
BASE_FEATURES_CLEAN = [
    # Transaction behaviour
    'total_trips', 'lifetime_spend', 'avg_basket_value',
    'avg_inter_purchase_days', 'distinct_days',
    # RFM (recency removed — leakage)
    'F', 'M',
    # Campaign exposure
    'n_campaigns', 'n_campaign_types', 'has_typeA', 'has_typeB', 'has_typeC',
    # Coupon behaviour
    'n_redemptions', 'has_redeemed',
    # Demographics (ordinal-encoded)
    'classification_1', ..., 'classification_7'
]

EXOG_FEATURES_CLEAN = [
    'rain_days_last90d', 'heat_days_last90d', 'cold_days_last90d'
]

# Removed as leakage:
# days_since_last_purchase, R, ppd_at_last_purchase,
# avg_ppd_last90d, food_inflation_at_last, apparel_inflation_at_last, avg_temp_last90d
```

**SHAP Feature Importance (Model B — selected model):**

| Rank | Feature | SHAP Value | Interpretation |
|------|---------|-----------|----------------|
| 1 | `rain_days_last90d` | 1.5756 | Rainy periods suppress shopping trips → strongest churn predictor |
| 2 | `heat_days_last90d` | 1.5710 | Extreme heat disrupts routine purchasing patterns |
| 3 | `avg_inter_purchase_days` | 0.3535 | Longer gaps between purchases → higher churn risk |
| 4 | `total_trips` | — | Higher engagement = lower churn |
| 5 | `has_typeC` | — | TypeC campaign exposure = protective signal |

**Key SHAP insight:** Weather-driven routine disruption is a stronger churn signal than purchase frequency or spend behaviour alone. This was not detectable from internal transaction data. The 4× PR-AUC improvement is entirely attributable to the 90-day weather window features.

**Final scoring output:** 763 Active households / 38 At Risk  
**Scores written to:** `mart_churn_scores` (PostgreSQL) and `ml_scores.churn_risk_score` (MongoDB)

---

#### Section 7 — CLV Segmentation (K-Means, k=3)

**Method:** K-Means clustering on standardised `lifetime_spend`, `total_trips`, `avg_basket_value`  
**Optimal k=3** selected via elbow method and silhouette score

| Segment | Households | Avg Lifetime Spend | Avg Trips |
|---------|-----------|-------------------|-----------|
| High Value | 267 | $9,750 | ~260 |
| Mid Value | 267 | $4,600 | ~150 |
| Low Value | 267 | $2,350 | ~110 |

**Cross-tabulation with churn scores:**

| CLV Tier | At Risk | Interpretation |
|----------|---------|---------------|
| High Value | **8** | Highest-priority retention — avg spend $9,013 |
| Mid Value | **14** | Secondary retention priority — avg spend $4,142 |
| Low Value | 15 | Lower recovery ROI |

**Scores written to:** `mart_household_segments` (PostgreSQL) and `ml_scores.clv_segment` (MongoDB)

---

#### Section 9 — Campaign Uplift (Difference-in-Differences + OLS)

**Method:** DiD causal inference — compares household spend *during* each campaign window vs a 28-day pre-campaign baseline, then controls for macro and weather conditions via OLS.

**Raw DiD results (4,165 campaign events):**

| Campaign Type | Events | Avg Pre Spend | Avg Lift | % Lift | Trip Lift |
|--------------|--------|--------------|----------|--------|-----------|
| TypeC | 355 | $379 | **+$515** | **+288%** | +13.2 |
| TypeA | 2,154 | $272 | +$222 | +173% | +6.6 |
| TypeB | 1,656 | $363 | +$110 | +70% | +2.9 |

**OLS Controls (macro + weather):**
- `avg_ppd_during_campaign`: −$17.59 per 1-point PPD rise (p=0.010) — inflationary periods suppress campaign lift
- `avg_temp_campaign`: +$1.92 per 1°F rise (p=0.017) — mild weather amplifies campaign response
- **TypeC raw lift advantage is not statistically significant once macro conditions are controlled** — campaign timing relative to economic conditions matters

**Campaign targeting gap (selection bias check):**

| Spend Quintile | Exposure Rate |
|---------------|--------------|
| Bottom (Q1) | 82.0% |
| Q2–Q4 | 95–99% |
| Top (Q5) | 98.8% |

> Top/bottom exposure ratio: **1.2× (vs 14× proxy in Phase 1)** — corrected using properly deduplicated `campaign_table` denominators.

---

#### Section 10 — Trade-Down Analysis (OLS)

**Question:** As cumulative price pressure (PPD index) rose from 2023 to 2024, did households shift spend toward cheaper commodity tiers?

**Method:** Classify 271 commodities into Budget/Low-Mid/High-Mid/Premium tiers by average unit price (NTILE 4). Track budget-tier spend share weekly against the PPD index.

**Result:**
- PPD ↔ Budget share correlation: **+0.036** (near zero)
- PPD coefficient in OLS panel regression: **+0.0027, p=0.286** — not statistically significant
- High vs Mid premium gap: **significant (p=0.030)** — a structural pricing effect, not an inflation response

**Conclusion:** No broad trade-down is confirmed. Low-income demographic groups (low `classification_3` levels) show consistently higher budget-tier share across *all* PPD environments — this is a demographic structural pattern, not an inflation-driven response. The inflation environment did not measurably alter basket composition during the 2023–2024 observation window.

---

### Phase 2 — SQL & MongoDB Business Insights

**Notebook:** `05_campaign_effectiveness_sql_mongodb_insights.ipynb`

Pure database query notebook — no ML, no scikit-learn. All five business questions answered directly from PostgreSQL and MongoDB.

**Query performance benchmarks (5 runs each):**

| Query | Engine | Avg | Median | P95 |
|-------|--------|-----|--------|-----|
| Steady-state weekly summary | SQL | 211ms | 206ms | 229ms |
| Department revenue share | SQL | 306ms | 301ms | 340ms |
| YoY growth by department | SQL | 399ms | 399ms | 400ms |
| RFM segment aggregation | MongoDB | 4.87ms | 4.82ms | 5.08ms |

Notable design: RFM segments queried live from MongoDB and joined to the SQL DiD output in-memory via Pandas — demonstrating a polyglot cross-database analytics pattern without ETL duplication.

---

## Key Findings

| # | Finding | Metric |
|---|---------|--------|
| 1 | Revenue grew strongly year-on-year | +33.5% avg weekly spend growth (2023→2024) |
| 2 | `classification_3` is the dominant demographic signal | $6,693 spend spread — 3× any other variable |
| 3 | GROCERY dominates revenue with strong growth | 50.2% share · +45.8% YoY |
| 4 | TypeC campaigns generate the highest lift | +$515 avg (+288%) vs 28-day baseline |
| 5 | Weather is the strongest churn predictor | Rain/heat 90-day windows: SHAP 1.57 each |
| 6 | No trade-down under inflation | PPD–budget correlation: +0.036 (near zero) |
| 7 | 22 high-value households at immediate churn risk | 8 High Value + 14 Mid Value, absent ≥30 days |
| 8 | 'Other' RFM segment is critically under-targeted | 3.5 avg campaigns vs 7.6 for Champions |

---

## Repository Structure

```text
retail-analytics-platform/
├── notebooks/
│   ├── 01_eda_customer360.ipynb                  # Phase 1 EDA — spending, demographics, categories
│   ├── 02_mongodb_design_hydration.ipynb          # MongoDB schema design + initial hydration
│   ├── 03_mongodb_customer360_enrichment.ipynb    # RFM scoring, external enrichment, geo, ML write-back
│   ├── 04_modeling_campaign_effectiveness.ipynb   # XGBoost churn, SHAP, K-Means CLV, DiD, OLS
│   └── 05_campaign_effectiveness_sql_mongodb_insights.ipynb  # Pure SQL + MongoDB analytics
│
├── postgres/
│   └── init/
│       ├── 01-create-databases.sh                # Database + user initialisation
│       └── 02-create-schema.sql                  # Full star schema DDL
│
├── sql/
│   └── 03-add-campaign-tables.sql                # Campaign, coupon, ext tables DDL
│
├── src/
│   ├── ingest/
│   │   ├── mongo_ingest.py                       # Customer 360 hydration pipeline
│   │   └── postgres_campaign_ingest.py           # Campaign CSV → PostgreSQL loader
│   └── serving/
│       └── model_scoring_service.py              # Reusable churn + CLV scoring service
│
├── data/
│   ├── README.md                                 # Data source and licensing notes
│   └── TRN00-InStoreSales-Sample.xlsx            # Sample transaction data (not full dataset)
│
├── images/
│   ├── postgres_architecture.png                 # Star schema ERD
│   ├── mongodb_architecture.png                  # MongoDB document structure diagram
│   ├── mongodb_database_design.png               # Collection design overview
│   └── mongodb_enrichment_pipeline.png           # ETL + enrichment flow
│
├── docs/
│   └── phase2_retail_analytics_report.pdf        # Full technical report
│
├── docker-compose.yml                            # PostgreSQL + MongoDB services
├── requirements.txt                              # Python dependencies
├── .gitignore
└── README.md
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Relational DB** | PostgreSQL 16 | Star schema, fact/dim tables, mart aggregations |
| **Document DB** | MongoDB 7 | Customer 360 profiles, RFM, ML scores, geo |
| **Orchestration** | Docker Compose | Single-command environment spin-up |
| **ETL** | Python (pandas, pymongo, psycopg2, SQLAlchemy) | Ingest pipelines |
| **Analytics** | SQL, MongoDB aggregation pipelines | Business question answering |
| **ML — Gradient Boosting** | XGBoost | Churn prediction |
| **ML — Explainability** | SHAP | Feature attribution, model interpretability |
| **ML — Clustering** | scikit-learn (K-Means) | CLV segmentation |
| **Causal Inference** | statsmodels (OLS) | Campaign uplift, trade-down analysis |
| **External APIs** | FRED (macro), Open-Meteo (weather) | Economic + environmental enrichment |
| **Visualisation** | matplotlib, seaborn | EDA and model diagnostics |
| **Notebook** | Jupyter | Analysis and reporting |

---

## Setup & Installation

### Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- Python 3.10+
- 4GB RAM recommended

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/retail-analytics-platform.git
cd retail-analytics-platform
```

### 2. Download the dataset

Download the Dunnhumby Complete Journey dataset from [dunnhumby.com/source-files](https://www.dunnhumby.com/source-files) and place the CSV files in the `data/` directory.

### 3. Start the database stack

```bash
docker-compose up -d
```

This starts PostgreSQL (port 5434) and MongoDB (port 27017).

### 4. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 5. Run notebooks in order

```bash
jupyter lab
```

Open and run notebooks `01` → `05` sequentially. Notebooks `02` and `03` depend on the MongoDB connection; notebook `05` depends on `mart_*` tables populated by notebook `04`.

### 6. (Optional) Register API keys for live external data

```bash
# FRED API key — https://fred.stlouisfed.org/docs/api/api_key.html
export FRED_API_KEY=your_key_here

# Open-Meteo requires no API key for standard use
```

---

## Results at a Glance

### Churn Model Performance (XGBoost)

```
Model A — Internal features only:   ROC-AUC = 0.8275  |  PR-AUC = 0.1607
Model B — + Weather enrichment:     ROC-AUC = 0.9109  |  PR-AUC = 0.6296  ✓ selected

PR-AUC improvement from weather features: +292% (0.16 → 0.63)
Final scoring: 763 Active, 38 At Risk (threshold = 0.50)
```

### Top SHAP Values (Model B)

```
rain_days_last90d      ████████████████████  1.5756
heat_days_last90d      ████████████████████  1.5710
avg_inter_purchase_days █████                0.3535
total_trips            ████                  (protective)
has_typeC              ███                   (protective)
```

### Campaign Lift Summary (DiD — 28-day pre-window)

```
TypeC:  +$515  (+288%)  |  13.2 additional trips  |  355 events
TypeA:  +$222  (+173%)  |   6.6 additional trips  |  2,154 events
TypeB:  +$110   (+70%)  |   2.9 additional trips  |  1,656 events
```

### RFM Segment Distribution (n=801)

```
Other       ██████████  28.4%  (under-targeted: 3.5 campaigns avg)
At Risk     ██████      15.0%  (avg spend $6,105)
Champions   █████       14.1%  (avg spend $10,408 · 326 trips)
Loyal       █████       13.2%
Potential   ████         9.0%
New         ███          7.0%
```

---

## Notes & Limitations

- **Date simulation:** The dataset uses relative day integers (1–711). Day 1 is anchored to January 1 2023 for external data alignment. This is a methodological disclosure — it enables the 2023–2024 inflationary and weather enrichment but should be treated as a simulation rather than confirmed historical record.
- **Ramp-up exclusion:** Weeks 1–16 reflect panel recruitment, not organic behaviour. All trend analyses use steady-state weeks (17–101) only.
- **Household count:** 924 total households in `dim_households` (801 original + 123 campaign-only households added during campaign ingest). ML models use n=801; campaign coverage uses n=924.
- **Trade-down finding:** The OLS result (p=0.286) is directionally consistent with the hypothesis but statistically underpowered at n=801 panel size. A larger cross-retailer dataset would be needed to confirm.
- **FRED fallback:** If no API key is provided, the pipeline uses pre-computed PPD index values stored in `ext_macro_weekly`.

---


## Skills Demonstrated
### Database Engineering
- Relational database design — star schema with fact/dimension tables, surrogate keys, and referential integrity
- PostgreSQL DDL — multi-script initialisation, schema versioning, and reproducible database setup via Docker
- MongoDB document modelling — schema design for a Customer 360 document store with nested subdocuments, arrays, and embedded metrics
- MongoDB index design — compound indexes, 2dsphere geospatial index, and query performance optimisation
- SQL querying — window functions (`NTILE`, `RANK`, `PERCENTILE_CONT`), CTEs, lateral joins, and aggregation pipelines
- MongoDB aggregation pipelines — `$group`, `$match`, `$project`, `$sort`, `$lookup` for real-time analytics
- Cross-database analytics — joining MongoDB aggregation outputs with PostgreSQL SQL results via in-memory pandas merges

### Data Engineering & ETL
- Python ETL pipeline development — end-to-end ingest scripts for both PostgreSQL and MongoDB
- External API integration — FRED (Federal Reserve Economic Data) for macroeconomic CPI/PPD series
- Weather data ingestion — Open-Meteo API for daily temperature, precipitation, and extreme weather flags
- Feature engineering — 90-day rolling weather windows, inter-purchase intervals, RFM quintile scoring, coupon redemption rates
- Data quality handling — panel ramp-up artefact identification, truncation detection, leakage-prone variable removal
- Docker Compose orchestration — multi-service containerised environment for reproducible local deployment

### Analytics & Business Intelligence
- Exploratory data analysis — spending trend decomposition, demographic signal ranking, category concentration analysis
- RFM segmentation — Recency / Frequency / Monetary quintile scoring with named segment classification
- Customer lifetime value (CLV) segmentation — spend-tier analysis using SQL `NTILE` and K-Means clustering
- Year-on-year comparative analysis — steady-state windowing, artefact exclusion, like-for-like comparisons
- Campaign selection bias identification — exposure rate analysis across spend quintiles
- Revenue concentration analysis — cumulative share (Pareto-style) across product departments

### Causal Inference & Statistical Modelling
- Difference-in-Differences (DiD) — SQL-based causal campaign lift estimation with 28-day pre-campaign baseline
- OLS regression — campaign uplift modelling with macroeconomic and weather controls (statsmodels)
- Trade-down analysis — commodity price-tier classification, budget-share trend regression, heterogeneous treatment effects by demographic group
- Hypothesis testing — p-value interpretation, statistical significance assessment, distinguishing structural from cyclical effects

### Machine Learning
- Binary classification — XGBoost churn prediction with class imbalance handling
- Model evaluation — ROC-AUC and PR-AUC metrics, with emphasis on PR-AUC for imbalanced minority-class problems
- Feature leakage detection and removal — identifying and excluding target-proximate variables before model training
- Hyperparameter tuning — XGBoost model iteration across feature sets
- Unsupervised learning — K-Means clustering with elbow method and silhouette score for optimal k selection
- Model scoring service — reusable Python scoring pipeline writing outputs to both PostgreSQL and MongoDB

### Explainability & Model Interpretability
- SHAP (SHapley Additive exPlanations) — feature attribution for XGBoost churn model
- Exogenous feature discovery — identifying weather-driven routine disruption as the dominant churn signal over internal transaction behaviour
- Comparative model analysis — quantifying the marginal lift of external enrichment features (PR-AUC +292%)

### External Data Enrichment
- Macroeconomic enrichment — FRED CPI series integration, PPD (Purchasing Power Decay) index construction, weekly join to transaction data
- Weather enrichment — daily temperature and precipitation data anchored to a specific retail market (Allen, TX), aggregated to household-level 90-day windows
- Temporal data alignment — anchoring a relative-day dataset to a real-world calendar for external join compatibility

### Software Engineering Practices
- Modular code organisation — separation of ingest, serving, and notebook layers
- Reproducibility — terminal-executed SQL initialisation scripts over notebook-based setup to avoid long-running execution issues
- Environment management — Docker Compose for consistent, portable multi-service environments
- Version-controlled notebook structure — sequentially numbered notebooks with clear input/output dependencies
### Overcoming Real-World Data Challenges
- Handling panel recruitment artefacts — identifying and excluding a 16-week ramp-up period from a real retail loyalty panel before any trend analysis
- Partial-period detection — flagging and excluding a truncated observation week to prevent distorted aggregations
- Dual population denominators — managing two valid household counts (n=801 transacting households vs n=924 total including campaign-only records) and applying the correct denominator per analysis context
- Sparse demographic coverage — working with classification variables that are present for some households but not others, without imputing or distorting segment distributions
- Campaign assignment imbalance — correcting a 14× exposure gap between spend quintiles identified in Phase 1, tracing it to a denominator error, and producing the accurate 1.2× ratio from properly deduplicated source tables
- Relative-date datasets — anchoring a day-integer time series (Day 1–711) to a real-world calendar to enable external data joins while disclosing the methodological assumptions this requires
- External data fallback patterns — designing pipelines that degrade gracefully when a live API key (FRED) is unavailable, falling back to pre-computed stored values without breaking downstream analysis
- Retail data skew — working with a highly imbalanced churn target (38 at-risk vs 763 active households) and selecting evaluation metrics (PR-AUC over ROC-AUC) appropriate for rare-event detection in real operational data
## Acknowledgements

- Dataset: [Dunnhumby — The Complete Journey](https://www.dunnhumby.com/source-files)
- Macroeconomic data: [Federal Reserve Economic Data (FRED)](https://fred.stlouisfed.org/)
- Weather data: [Open-Meteo](https://open-meteo.com/) — free, open-source weather API
- Grace Burns — Retail Analytics & Campaign Effectiveness, Database & Analytics Systems course- NorthWestern University Masters Data Science Program
