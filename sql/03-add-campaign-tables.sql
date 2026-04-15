-- =============================================================================
-- 03-add-campaign-tables.sql
-- Adds the four campaign-related tables missing from the initial schema load.
--
-- Run order: AFTER 02-create-schema.sql (dim_households, dim_products,
--            dim_date, dim_campaigns must already exist).
--
-- Safe to re-run — all statements use IF NOT EXISTS / ON CONFLICT DO NOTHING.
--
-- Dunnhumby table mapping
-- ─────────────────────────────────────────────────────────────────────────────
-- CSV file            → Postgres table         Notes
-- campaign_desc.csv   → dim_campaigns          POPULATES existing empty table
-- campaign_table.csv  → campaign_table         NEW — household ↔ campaign map
-- coupon.csv          → coupon                 NEW — coupon pool per campaign
-- coupon_redempt.csv  → coupon_redempt         NEW — raw redemptions w/ UPC
--                     → fact_coupon_redemptions ALSO written (star schema)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- campaign_table
-- "Lists the campaigns received by each household."
-- PK is composite: one row per (household, campaign).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS campaign_table (
    household_key  INTEGER NOT NULL REFERENCES dim_households(household_key),
    campaign_id    INTEGER NOT NULL REFERENCES dim_campaigns(campaign_id),
    PRIMARY KEY (household_key, campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_campaign_table_hh
    ON campaign_table(household_key);

CREATE INDEX IF NOT EXISTS idx_campaign_table_campaign
    ON campaign_table(campaign_id);

-- -----------------------------------------------------------------------------
-- coupon
-- "Lists all coupons sent as part of a campaign and the products redeemable."
-- One coupon_upc can apply to many products — one row per (campaign, coupon, product).
--
-- TypeA  : table holds the full *pool*; individual 16-coupon allocation per
--          household is NOT in the dataset (outside scope per Dunnhumby guide).
-- TypeB/C: all participating households receive every coupon in this table.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS coupon (
    campaign_id   INTEGER NOT NULL REFERENCES dim_campaigns(campaign_id),
    coupon_upc    BIGINT  NOT NULL,
    product_id    INTEGER NOT NULL REFERENCES dim_products(product_id),
    PRIMARY KEY (campaign_id, coupon_upc, product_id)
);

CREATE INDEX IF NOT EXISTS idx_coupon_campaign
    ON coupon(campaign_id);

CREATE INDEX IF NOT EXISTS idx_coupon_upc
    ON coupon(coupon_upc);

CREATE INDEX IF NOT EXISTS idx_coupon_product
    ON coupon(product_id);

-- -----------------------------------------------------------------------------
-- coupon_redempt  (raw staging table — preserves coupon_upc for analysis)
-- "Identifies the coupons that each household redeemed."
-- Kept separate from fact_coupon_redemptions so coupon_upc is queryable.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS coupon_redempt (
    household_key  INTEGER NOT NULL REFERENCES dim_households(household_key),
    day            INTEGER NOT NULL,
    coupon_upc     BIGINT  NOT NULL,
    campaign_id    INTEGER NOT NULL REFERENCES dim_campaigns(campaign_id),
    PRIMARY KEY (household_key, day, coupon_upc, campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_coupon_redempt_hh
    ON coupon_redempt(household_key);

CREATE INDEX IF NOT EXISTS idx_coupon_redempt_campaign
    ON coupon_redempt(campaign_id);

CREATE INDEX IF NOT EXISTS idx_coupon_redempt_day
    ON coupon_redempt(day);

-- -----------------------------------------------------------------------------
-- Add coupon_upc to fact_coupon_redemptions if the column is missing.
-- (Original schema omitted it — this is non-destructive.)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'fact_coupon_redemptions'
          AND column_name = 'coupon_upc'
    ) THEN
        ALTER TABLE fact_coupon_redemptions ADD COLUMN coupon_upc BIGINT;
        CREATE INDEX IF NOT EXISTS idx_fact_cr_coupon_upc
            ON fact_coupon_redemptions(coupon_upc);
    END IF;
END
$$;

-- Helpful view: campaign participation with type and window — used by
-- Mongo ingestion SQL_CAMPAIGNS query and RFM analysis.
CREATE OR REPLACE VIEW v_campaign_participation AS
SELECT
    ct.household_key,
    ct.campaign_id,
    cd.campaign_type,
    cd.start_day,
    cd.end_day,
    (cd.end_day - cd.start_day + 1) AS campaign_duration_days
FROM campaign_table  ct
JOIN dim_campaigns   cd ON ct.campaign_id = cd.campaign_id;

-- Helpful view: coupon redemption enriched with campaign type and product info.
-- TypeA note: redeemed coupons are observable; issued-but-unredeemed are not.
CREATE OR REPLACE VIEW v_coupon_redemptions_enriched AS
SELECT
    cr.household_key,
    cr.day,
    cr.coupon_upc,
    cr.campaign_id,
    cd.campaign_type,
    cd.start_day           AS campaign_start,
    cd.end_day             AS campaign_end,
    c.product_id,
    p.department,
    p.commodity
FROM coupon_redempt  cr
JOIN dim_campaigns   cd ON cr.campaign_id  = cd.campaign_id
LEFT JOIN coupon      c  ON cr.coupon_upc   = c.coupon_upc
                         AND cr.campaign_id = c.campaign_id
LEFT JOIN dim_products p  ON c.product_id   = p.product_id;

-- Quick row-count sanity check (run after ingestion):
-- SELECT 'dim_campaigns'          AS tbl, COUNT(*) FROM dim_campaigns
-- UNION ALL
-- SELECT 'campaign_table',                 COUNT(*) FROM campaign_table
-- UNION ALL
-- SELECT 'coupon',                         COUNT(*) FROM coupon
-- UNION ALL
-- SELECT 'coupon_redempt',                 COUNT(*) FROM coupon_redempt
-- UNION ALL
-- SELECT 'fact_coupon_redemptions',        COUNT(*) FROM fact_coupon_redemptions;
