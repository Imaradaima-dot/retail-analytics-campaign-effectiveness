-- postgres/init/02-create-schema.sql
-- Snowflake schema for the Dunnhumby Complete Journey dataset.
-- Runs automatically on first container start via docker-entrypoint-initdb.d/

\c retail_analytics;

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_households (
    household_key   INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_demographics (
    household_key     INTEGER PRIMARY KEY REFERENCES dim_households(household_key),
    classification_1  VARCHAR(50),
    classification_2  VARCHAR(50),
    classification_3  VARCHAR(50),
    classification_4  VARCHAR(50),
    classification_5  VARCHAR(50),
    classification_6  VARCHAR(50),
    classification_7  VARCHAR(50)
);

-- Normalised product hierarchy (Snowflake pattern)
CREATE TABLE IF NOT EXISTS dim_departments (
    department_id   SERIAL PRIMARY KEY,
    department      VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_commodities (
    commodity_id    SERIAL PRIMARY KEY,
    department_id   INTEGER REFERENCES dim_departments(department_id),
    commodity       VARCHAR(150) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_sub_commodities (
    sub_commodity_id  SERIAL PRIMARY KEY,
    commodity_id      INTEGER REFERENCES dim_commodities(commodity_id),
    sub_commodity     VARCHAR(150) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id        INTEGER PRIMARY KEY,
    sub_commodity_id  INTEGER REFERENCES dim_sub_commodities(sub_commodity_id),
    manufacturer      INTEGER,
    department        VARCHAR(100),
    brand             VARCHAR(50),
    commodity         VARCHAR(150),
    sub_commodity     VARCHAR(150),
    curr_size_of_product VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS dim_stores (
    store_id    INTEGER PRIMARY KEY,
    region      VARCHAR(100),
    zip_code    VARCHAR(10),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dim_date (
    day_id              INTEGER PRIMARY KEY,   -- dataset's DAY value (1–711)
    calendar_date       DATE,
    week_num            INTEGER,
    month_num           INTEGER,
    quarter             INTEGER,
    year                INTEGER,
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN,
    -- Exogenous variables (joined from FRED/BLS/NOAA APIs)
    purchasing_power_idx  DOUBLE PRECISION,    -- Tariff-adjusted purchasing power
    cpi_u               DOUBLE PRECISION,      -- CPI-U from BLS
    max_temp_f          DOUBLE PRECISION,      -- NOAA daily max temperature (°F)
    precipitation_in    DOUBLE PRECISION,      -- NOAA daily precipitation (inches)
    is_extreme_weather  BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_campaigns (
    campaign_id    INTEGER PRIMARY KEY,
    campaign_type  VARCHAR(50),
    start_day      INTEGER,
    end_day        INTEGER
);

-- =============================================================================
-- FACT TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id  BIGSERIAL PRIMARY KEY,
    household_key   INTEGER   REFERENCES dim_households(household_key),
    basket_id       BIGINT    NOT NULL,
    product_id      INTEGER   REFERENCES dim_products(product_id),
    store_id        INTEGER   REFERENCES dim_stores(store_id),
    day             INTEGER   REFERENCES dim_date(day_id),
    quantity        INTEGER,
    sales_value     NUMERIC(10, 2),
    retail_disc     NUMERIC(10, 2),
    coupon_disc     NUMERIC(10, 2),
    coupon_match_disc NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS fact_coupon_redemptions (
    redemption_id   BIGSERIAL PRIMARY KEY,
    household_key   INTEGER  REFERENCES dim_households(household_key),
    campaign_id     INTEGER  REFERENCES dim_campaigns(campaign_id),
    product_id      INTEGER  REFERENCES dim_products(product_id),
    day             INTEGER  REFERENCES dim_date(day_id),
    coupon_disc     NUMERIC(10, 2)
);

-- =============================================================================
-- MART TABLES (pre-computed aggregates — written by postgres_ingest ETL)
-- =============================================================================

CREATE TABLE IF NOT EXISTS mart_household_metrics (
    household_key             INTEGER PRIMARY KEY REFERENCES dim_households(household_key),
    lifetime_spend            NUMERIC(12, 2),
    avg_basket_value          NUMERIC(10, 2),
    total_trips               INTEGER,
    avg_inter_purchase_days   NUMERIC(8, 2),
    days_since_last_purchase  INTEGER,
    coupon_redemption_rate    NUMERIC(5, 4)
);

CREATE TABLE IF NOT EXISTS mart_churn_scores (
    household_key   INTEGER PRIMARY KEY REFERENCES dim_households(household_key),
    churn_risk_score NUMERIC(5, 4),
    churn_label     VARCHAR(20),       -- 'Loyal' | 'At Risk' | 'Churned'
    clv_segment     VARCHAR(30),       -- 'High Value' | 'Mid Value' | 'Low Value'
    model_version   VARCHAR(30),
    scored_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mart_household_segments (
    household_key   INTEGER REFERENCES dim_households(household_key),
    segment_label   VARCHAR(50),       -- 'Price Sensitive', 'VIP Family Shopper', etc.
    PRIMARY KEY (household_key, segment_label)
);

-- =============================================================================
-- INDEXES (on the fact table — high query volume)
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_fact_txn_household  ON fact_transactions(household_key);
CREATE INDEX IF NOT EXISTS idx_fact_txn_basket     ON fact_transactions(basket_id);
CREATE INDEX IF NOT EXISTS idx_fact_txn_product    ON fact_transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_txn_day        ON fact_transactions(day);
