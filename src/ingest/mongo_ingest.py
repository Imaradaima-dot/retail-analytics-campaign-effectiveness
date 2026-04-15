"""
mongo_ingest.py
───────────────
Hydrates MongoDB Customer 360 collection from PostgreSQL.
Run from terminal:  python mongo_ingest.py

Optimised for low-memory machines:
  - Streams fact_transactions in chunks (never loads the full table)
  - Builds category / txn / campaign maps household-by-household via SQL
  - Upserts to MongoDB in small batches
  - Prints live progress so you can see it working
"""

import os
import math
import json
import time
import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from sqlalchemy import create_engine, text, bindparam

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION  — edit these or set as environment variables
# =============================================================================

PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = os.getenv("PG_PORT",     "5434")
PG_DB       = os.getenv("PG_DB",       "retail_analytics")
PG_USER     = os.getenv("PG_USER",     "retail_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "retail_pass")

MONGO_URI   = os.getenv("MONGO_URI",   "mongodb://localhost:27017/")
MONGO_DB    = os.getenv("MONGO_DB",    "retail_customer360")
MONGO_COLL  = "households"

BATCH_SIZE      = 100   # households written to MongoDB per bulk_write call
MAX_RECENT_TXNS = 10    # Subset Pattern — max embedded transactions per household

PG_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
    f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# =============================================================================
# SIMULATED STORE LOCATIONS  (Allen, TX metro)
# =============================================================================

STORES = [
    {"store_id": 367, "name": "Allen Town Center",  "coordinates": [-96.6698, 33.1032]},
    {"store_id": 368, "name": "Plano East",         "coordinates": [-96.6462, 33.0551]},
    {"store_id": 369, "name": "McKinney Westside",  "coordinates": [-96.7540, 33.1973]},
    {"store_id": 370, "name": "Frisco Central",     "coordinates": [-96.8230, 33.1507]},
    {"store_id": 371, "name": "Richardson North",   "coordinates": [-96.7298, 32.9483]},
]

STORE_MAP = {s["store_id"]: s for s in STORES}

def pick_store(store_id):
    """Return GeoJSON Point for a store_id, falling back to store 367."""
    s = STORE_MAP.get(store_id, STORES[0])
    return {
        "store_id": s["store_id"],
        "location": {"type": "Point", "coordinates": s["coordinates"]}
    }

# =============================================================================
# SQL — pure PostgreSQL, no QUALIFY
# =============================================================================

# All households with pre-computed metrics via CTEs.
# Works even when mart tables are empty (COALESCE falls back to live CTEs).
SQL_HOUSEHOLDS = """
SELECT
  h.household_key,
  d.classification_1,
  d.classification_2,
  d.classification_3,
  d.classification_4,
  d.classification_5,
  d.classification_6,
  d.classification_7,
  NULL::numeric AS lifetime_spend,
  NULL::numeric AS avg_basket_value,
  NULL::int     AS total_trips,
  NULL::numeric AS avg_inter_purchase_days,
  NULL::int     AS days_since_last_purchase,
  NULL::numeric AS coupon_redemption_rate,
  NULL::numeric AS churn_risk_score,
  NULL::text    AS churn_label,
  NULL::text    AS clv_segment,
  NULL::text    AS model_version,
  NULL::timestamp AS scored_at
FROM dim_households h
LEFT JOIN dim_demographics d ON h.household_key = d.household_key
ORDER BY h.household_key;
"""

# Top-5 categories per household — ROW_NUMBER subquery, no QUALIFY
SQL_CATEGORIES = """
SELECT household_key, department, commodity, spend_share
FROM (
    SELECT
        t.household_key,
        p.department,
        p.commodity,
        ROUND(
            SUM(t.sales_value) /
            SUM(SUM(t.sales_value)) OVER (PARTITION BY t.household_key),
            4
        ) AS spend_share,
        ROW_NUMBER() OVER (
            PARTITION BY t.household_key
            ORDER BY SUM(t.sales_value) DESC
        ) AS rn
    FROM fact_transactions t
    JOIN dim_products p ON t.product_id = p.product_id
    GROUP BY t.household_key, p.department, p.commodity
) ranked
WHERE rn <= 5
ORDER BY household_key, rn
"""

# Last N transactions per household — ROW_NUMBER subquery, no QUALIFY
SQL_RECENT_TXNS = """
SELECT household_key, basket_id, day,
       ROUND(sales_value::numeric, 2) AS sales_value,
       quantity,
       ROUND(retail_disc::numeric, 2) AS retail_disc,
       ROUND(coupon_disc::numeric, 2) AS coupon_disc,
       store_id, rn
FROM (
    SELECT
        household_key, basket_id, day,
        sales_value, quantity, retail_disc, coupon_disc, store_id,
        ROW_NUMBER() OVER (
            PARTITION BY household_key
            ORDER BY day DESC, basket_id DESC
        ) AS rn
    FROM fact_transactions
) ranked
WHERE rn <= :max_txns
ORDER BY household_key, rn
"""

SQL_CAMPAIGNS = """
SELECT ct.household_key,
       ct.campaign_id,
       dc.campaign_type,
       dc.start_day,
       dc.end_day
FROM campaign_table ct
JOIN dim_campaigns dc
  ON ct.campaign_id = dc.campaign_id
ORDER BY ct.household_key
"""

SQL_SEGMENTS = """
SELECT household_key, segment_label
FROM mart_household_segments
ORDER BY household_key
"""

SQL_MAX_DAY = "SELECT MAX(day) AS max_day FROM fact_transactions;"

SQL_METRICS_BATCH = """
WITH txn AS (
  SELECT
    household_key,
    SUM(sales_value) AS lifetime_spend,
    COUNT(DISTINCT basket_id) AS total_trips,
    MAX(day) AS last_day,
    MIN(day) AS first_day,
    COUNT(DISTINCT day) AS distinct_days
  FROM fact_transactions
  WHERE household_key IN :hh_keys
  GROUP BY household_key
),
ip AS (
  SELECT
    household_key,
    CASE
      WHEN distinct_days > 1 THEN ROUND(((last_day - first_day)::numeric / (distinct_days - 1)), 1)
      ELSE 0
    END AS avg_inter_purchase_days
  FROM txn
),
cr AS (
  SELECT
    t.household_key,
    ROUND(
      COUNT(DISTINCT r.redemption_id)::numeric / NULLIF(COUNT(DISTINCT t.basket_id), 0),
      4
    ) AS coupon_redemption_rate
  FROM fact_transactions t
  LEFT JOIN fact_coupon_redemptions r
    ON r.household_key = t.household_key
  WHERE t.household_key IN :hh_keys
  GROUP BY t.household_key
)
SELECT
  tx.household_key,
  ROUND(tx.lifetime_spend::numeric, 2) AS lifetime_spend,
  ROUND((tx.lifetime_spend / NULLIF(tx.total_trips, 0))::numeric, 2) AS avg_basket_value,
  tx.total_trips,
  ip.avg_inter_purchase_days,
  tx.last_day,
  cr.coupon_redemption_rate
FROM txn tx
LEFT JOIN ip ON ip.household_key = tx.household_key
LEFT JOIN cr ON cr.household_key = tx.household_key;
"""

# =============================================================================
# HELPERS
# =============================================================================

def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def safe_int(val, default=0):
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def build_doc(row, cat_map, txn_map, camp_map, seg_map):
    hh  = int(row["household_key"])
    now = datetime.now(timezone.utc).isoformat()

    recent = txn_map.get(hh, [])
    store_id = int(recent[0]["store_id"]) if recent else None

    return {
        "_id":           f"HH_{hh}",
        "household_key": hh,
        "demographics": {
            f"classification_{i}": row.get(f"classification_{i}")
            for i in range(1, 8)
        },
        "financial_metrics": {
            "lifetime_spend":           safe_float(row.get("lifetime_spend")),
            "avg_basket_value":         safe_float(row.get("avg_basket_value")),
            "total_trips":              safe_int(row.get("total_trips")),
            "avg_inter_purchase_days":  safe_float(row.get("avg_inter_purchase_days")),
            "days_since_last_purchase": safe_int(row.get("days_since_last_purchase")),
            "coupon_redemption_rate":   safe_float(row.get("coupon_redemption_rate")),
        },
        "ml_scores": {
            "churn_risk_score": safe_float(row.get("churn_risk_score")),
            "churn_label":      row.get("churn_label"),
            "clv_segment":      row.get("clv_segment"),
            "model_version":    row.get("model_version"),
            "scored_at":        str(row.get("scored_at") or ""),
        },
        "segments":             seg_map.get(hh, []),
        "preferred_categories": cat_map.get(hh, []),
        "recent_transactions":  recent,
        "active_campaigns":     camp_map.get(hh, []),
        "nearest_store":        pick_store(store_id),
        "created_at":           now,
        "updated_at":           now,
        "etl_version":          "phase2_v1.0",
    }

def enrich_financial_metrics(engine, coll, batch_size=200):
    log.info("-" * 60)
    log.info("Enrichment pass: updating financial_metrics from Postgres aggregates ...")

    # Get dataset max day once
    with engine.connect() as conn:
        max_day = conn.execute(text(SQL_MAX_DAY)).scalar() or 0
    log.info("  dataset max day: %s", max_day)

    # Household keys (consistent with Postgres SSoT)
    with engine.connect() as conn:
        hh_keys_all = [
            r[0] for r in conn.execute(text("SELECT household_key FROM dim_households ORDER BY household_key")).fetchall()
        ]

    stmt = text(SQL_METRICS_BATCH).bindparams(bindparam("hh_keys", expanding=True))

    total = len(hh_keys_all)
    updated = 0
    chunk_num = 0

    for i in range(0, total, batch_size):
        chunk_num += 1
        hh_keys = hh_keys_all[i:i + batch_size]

        df = pd.read_sql(stmt, engine, params={"hh_keys": hh_keys})
        if df is None or len(df) == 0:
            continue

        ops = []
        for _, row in df.iterrows():
            hk = int(row["household_key"])
            last_day = int(row["last_day"]) if row["last_day"] is not None else 0

            metrics = {
                "lifetime_spend": float(row["lifetime_spend"] or 0),
                "avg_basket_value": float(row["avg_basket_value"] or 0),
                "total_trips": int(row["total_trips"] or 0),
                "avg_inter_purchase_days": float(row["avg_inter_purchase_days"] or 0),
                "days_since_last_purchase": int(max_day - last_day) if max_day and last_day else 0,
                "coupon_redemption_rate": float(row["coupon_redemption_rate"] or 0),
            }

            ops.append(
                UpdateOne(
                    {"_id": f"HH_{hk}"},
                    {"$set": {"financial_metrics": metrics, "updated_at": datetime.now(timezone.utc).isoformat()}},
                    upsert=False,
                )
            )

        if ops:
            result = coll.bulk_write(ops, ordered=False)
            updated += (result.modified_count + result.matched_count)

        log.info("  Metrics chunk %3d | households %4d | processed %4d/%4d",
                 chunk_num, len(hh_keys), min(i + batch_size, total), total)

    log.info("Enrichment complete. Households processed: %d", total)

def groupby_map(df, key_col, value_cols=None, drop_cols=None):
    """
    Convert a DataFrame into a dict: {key -> [list of row dicts]}.
    Handles empty DataFrames gracefully.
    Safe across pandas versions and does not assume include_groups support.
    """
    if df is None or len(df) == 0:
        return {}

    # Never drop the grouping key
    if drop_cols:
        drop_cols = [c for c in drop_cols if c in df.columns and c != key_col]
        if drop_cols:
            df = df.drop(columns=drop_cols)

    if value_cols:
        keep = [key_col] + [c for c in value_cols if c in df.columns]
        df = df[keep]

    # group_keys=False prevents pandas from adding the key into the index
    grouped = df.groupby(key_col, group_keys=False)

    return (
        grouped
        .apply(lambda g: g.drop(columns=[key_col], errors="ignore").to_dict("records"))
        .to_dict()
    )
# =============================================================================
# CONNECTION CHECKS
# =============================================================================

def check_postgres(engine):
    log.info("Checking PostgreSQL connection ...")
    with engine.connect() as conn:
        ver = conn.execute(text("SELECT version()")).scalar()
        log.info("  PostgreSQL OK — %s", ver.split(",")[0])

        count = conn.execute(
            text("SELECT COUNT(*) FROM fact_transactions")
        ).scalar()
        log.info("  fact_transactions rows: %s", f"{count:,}")

        if count == 0:
            raise RuntimeError(
                "fact_transactions is empty — run postgres_ingest first."
            )
    return count


def check_mongo():
    log.info("Checking MongoDB connection ...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    ver = client.server_info().get("version", "unknown")
    log.info("  MongoDB OK — version %s", ver)
    client.close()

# =============================================================================
# LOAD LOOKUP TABLES
# Loaded once into memory — these are small tables (categories, campaigns, etc.)
# =============================================================================

def load_lookup_tables(engine):
    log.info("Loading lookup tables ...")

    log.info("  categories ...")
    df_cat = pd.read_sql(text(SQL_CATEGORIES), engine)
    log.info("    %s rows", f"{len(df_cat):,}")
    cat_map = groupby_map(df_cat, "household_key",
                          value_cols=["department", "commodity", "spend_share"])

    log.info("  recent transactions (top %d per household) ...", MAX_RECENT_TXNS)
    df_txn = pd.read_sql(
        text(SQL_RECENT_TXNS).bindparams(max_txns=MAX_RECENT_TXNS),
        engine
    )
    log.info("    %s rows", f"{len(df_txn):,}")
    txn_map = groupby_map(df_txn, "household_key", drop_cols=["rn"])

    log.info("  campaigns ...")
    try:
        df_camp = pd.read_sql(text(SQL_CAMPAIGNS), engine)
        log.info("    %s rows", f"{len(df_camp):,}")
    except Exception as e:
        log.warning("  campaigns skipped: %s", e)
        df_camp = pd.DataFrame()
    camp_map = groupby_map(df_camp, "household_key")

    log.info("  segments ...")
    try:
        df_seg = pd.read_sql(text(SQL_SEGMENTS), engine)
        log.info("    %s rows", f"{len(df_seg):,}")
        seg_map = (
            df_seg.groupby("household_key")["segment_label"]
            .apply(list)
            .to_dict()
        ) if len(df_seg) > 0 else {}
    except Exception as e:
        log.warning("  segments skipped: %s", e)
        seg_map = {}

    return cat_map, txn_map, camp_map, seg_map

# =============================================================================
# MAIN INGESTION
# =============================================================================

def run():
    start_time = time.time()
    log.info("=" * 60)
    log.info("MongoDB Customer 360 Ingestion — Phase 2")
    log.info("=" * 60)
    log.info("PostgreSQL : %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    log.info("MongoDB    : %s  db=%s  coll=%s", MONGO_URI, MONGO_DB, MONGO_COLL)
    log.info("Batch size : %d  |  Max recent txns : %d", BATCH_SIZE, MAX_RECENT_TXNS)
    log.info("-" * 60)

    # ── Connections ───────────────────────────────────────────────────────────
    engine = create_engine(PG_URL, pool_pre_ping=True)
    check_postgres(engine)
    check_mongo()

    # ── Lookup tables (small — load once) ────────────────────────────────────
    cat_map, txn_map, camp_map, seg_map = load_lookup_tables(engine)

    # ── Stream households in chunks ───────────────────────────────────────────
    log.info("-" * 60)
    log.info("Streaming households from PostgreSQL and upserting to MongoDB ...")

    client     = MongoClient(MONGO_URI)
    coll       = client[MONGO_DB][MONGO_COLL]
    total_hh   = 0
    upserted   = 0
    errors     = 0
    chunk_num  = 0

    # Stream the household base query in chunks to avoid loading 2,500 rows
    # with all their CTE results into memory at once.
    for chunk in pd.read_sql(text(SQL_HOUSEHOLDS), engine, chunksize=BATCH_SIZE):
        chunk_num += 1
        chunk_size = len(chunk)
        total_hh  += chunk_size

        # Build BSON documents for this chunk
        docs = [
            build_doc(row, cat_map, txn_map, camp_map, seg_map)
            for _, row in chunk.iterrows()
        ]

        # Upsert — safe to re-run (overwrites existing docs by _id)
        ops = [
            UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
            for doc in docs
        ]

        try:
            result    = coll.bulk_write(ops, ordered=False)
            upserted += result.upserted_count + result.modified_count
        except BulkWriteError as bwe:
            batch_errors = len(bwe.details.get("writeErrors", []))
            errors += batch_errors
            log.warning(
                "  BulkWriteError in chunk %d: %d write error(s)",
                chunk_num, batch_errors
            )

        # Progress line — printed after every chunk so you can see it's running
        elapsed = time.time() - start_time
        log.info(
            "  Chunk %3d | households %5d | upserted so far %5d | %.1fs",
            chunk_num, total_hh, upserted, elapsed
        )
    enrich_financial_metrics(engine, coll, batch_size=200)
    # ── Final summary ─────────────────────────────────────────────────────────
    final_count = coll.count_documents({})
    elapsed     = time.time() - start_time

    log.info("-" * 60)
    log.info("COMPLETE")
    log.info("  Households processed : %s", f"{total_hh:,}")
    log.info("  Documents upserted   : %s", f"{upserted:,}")
    log.info("  Errors               : %s", f"{errors:,}")
    log.info("  Collection total     : %s", f"{final_count:,}")
    log.info("  Time elapsed         : %.1fs", elapsed)
    log.info("=" * 60)

    client.close()
    engine.dispose()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    run()
