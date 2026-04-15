"""
campaign_ingest.py
──────────────────
Loads the four Dunnhumby campaign CSV files into PostgreSQL.

Run order (must complete before this script):
  1. postgres_ingest.py  — populates dim_households, dim_products, dim_date
  2. psql -f 03-add-campaign-tables.sql  — creates the missing tables

Then run:
  python campaign_ingest.py

CSV files expected in DATA_DIR (default: ./data/raw/):
  campaign_desc.csv    →  dim_campaigns         (campaign metadata)
  campaign_table.csv   →  campaign_table        (household participation)
  coupon.csv           →  coupon                (coupon pool per campaign)
  coupon_redempt.csv   →  coupon_redempt        (raw redemptions)
                       →  fact_coupon_redemptions (star schema fact)

TypeA dataset limitation (per Dunnhumby guide, p.4):
  For TypeA campaigns, only the coupon *pool* is recorded.
  Which 16 coupons each household received is NOT in the dataset.
  For TypeB / TypeC, all households receive every coupon in the pool.
  This script loads what is available and flags TypeA rows in logs.
"""

import os
import logging
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = os.getenv("PG_PORT",     "5434")
PG_DB       = os.getenv("PG_DB",       "retail_analytics")
PG_USER     = os.getenv("PG_USER",     "retail_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "retail_pass")

DATA_DIR = Path(os.getenv("DATA_DIR", "./data/raw"))

PG_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
    f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# =============================================================================
# HELPERS
# =============================================================================

def load_csv(filename: str, required_cols: list[str]) -> pd.DataFrame | None:
    """Load a CSV, normalise column names to lowercase, validate required cols."""
    path = DATA_DIR / filename
    if not path.exists():
        log.error("CSV not found: %s  (expected at %s)", filename, path)
        return None

    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]

    # Check required columns are present
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        log.error("  %s is missing columns: %s  (found: %s)",
                  filename, missing, list(df.columns))
        return None

    log.info("  Loaded %-30s  %s rows", filename, f"{len(df):,}")
    return df


def upsert_df(df: pd.DataFrame, table: str, engine,
              pk_cols: list[str], chunk: int = 5000):
    """
    Insert DataFrame rows, skipping duplicates on the primary key.
    Uses INSERT … ON CONFLICT DO NOTHING for idempotency.
    """
    cols      = list(df.columns)
    col_list  = ", ".join(cols)
    val_list  = ", ".join(f":{c}" for c in cols)
    pk_clause = ", ".join(pk_cols)

    sql = text(
        f"INSERT INTO {table} ({col_list}) VALUES ({val_list}) "
        f"ON CONFLICT ({pk_clause}) DO NOTHING"
    )

    total   = len(df)
    written = 0
    with engine.begin() as conn:
        for start in range(0, total, chunk):
            rows = df.iloc[start : start + chunk].to_dict("records")
            conn.execute(sql, rows)
            written += len(rows)

    return written


# =============================================================================
# STEP 1 — dim_campaigns  (from campaign_desc.csv)
# =============================================================================

def load_dim_campaigns(engine) -> bool:
    log.info("Step 1/4 — Loading dim_campaigns from campaign_desc.csv ...")

    df = load_csv("campaign_desc.csv",
                  required_cols=["campaign", "description", "start_day", "end_day"])
    if df is None:
        return False

    # Rename to match existing schema columns
    df = df.rename(columns={
        "campaign":    "campaign_id",
        "description": "campaign_type",
    })

    # Validate: campaign type must be TypeA, TypeB, or TypeC
    valid_types = {"TypeA", "TypeB", "TypeC"}
    unexpected  = set(df["campaign_type"].unique()) - valid_types
    if unexpected:
        log.warning("  Unexpected campaign_type values: %s", unexpected)

    type_counts = df["campaign_type"].value_counts().to_dict()
    log.info("  Campaign types: %s", type_counts)
    log.info("  Day range: start %d – %d  |  end %d – %d",
             df.start_day.min(), df.start_day.max(),
             df.end_day.min(), df.end_day.max())

    written = upsert_df(df, "dim_campaigns", engine, pk_cols=["campaign_id"])
    log.info("  dim_campaigns rows written: %d", written)
    return True


# =============================================================================
# STEP 2 — campaign_table  (from campaign_table.csv)
# =============================================================================

def load_campaign_table(engine) -> bool:
    log.info("Step 2/4 — Loading campaign_table from campaign_table.csv ...")

    df = load_csv("campaign_table.csv",
                  required_cols=["household_key", "campaign"])
    if df is None:
        return False

    df = df.rename(columns={"campaign": "campaign_id"})
    df = df[["household_key", "campaign_id"]].drop_duplicates()

    # Check all campaign_ids exist in dim_campaigns (FK safety)
    with engine.connect() as conn:
        known_campaigns = {
            r[0] for r in conn.execute(
                text("SELECT campaign_id FROM dim_campaigns")
            ).fetchall()
        }
    unknown = set(df["campaign_id"].unique()) - known_campaigns
    if unknown:
        log.warning("  Dropping %d rows with unknown campaign_ids: %s",
                    len(df[df.campaign_id.isin(unknown)]), sorted(unknown))
        df = df[~df.campaign_id.isin(unknown)]

    # Check all household_keys exist (FK safety)
    with engine.connect() as conn:
        known_hh = {
            r[0] for r in conn.execute(
                text("SELECT household_key FROM dim_households")
            ).fetchall()
        }
    unknown_hh = set(df["household_key"].unique()) - known_hh
    if unknown_hh:
        log.warning("  Dropping %d rows with unknown household_keys (first 5: %s)",
                    len(df[df.household_key.isin(unknown_hh)]),
                    sorted(unknown_hh)[:5])
        df = df[~df.household_key.isin(unknown_hh)]

    # Participation breakdown by campaign type
    with engine.connect() as conn:
        camp_types = pd.read_sql(
            text("SELECT campaign_id, campaign_type FROM dim_campaigns"), conn
        )
    merged = df.merge(camp_types, on="campaign_id", how="left")
    type_breakdown = merged.groupby("campaign_type")["household_key"].nunique()
    log.info("  Unique households per campaign type: %s",
             type_breakdown.to_dict())
    log.info("  TypeA note: household received campaign, but specific 16 coupons "
             "per household are NOT in the dataset (Dunnhumby guide limitation).")

    written = upsert_df(df, "campaign_table", engine,
                        pk_cols=["household_key", "campaign_id"])
    log.info("  campaign_table rows written: %d", written)
    return True


# =============================================================================
# STEP 3 — coupon  (from coupon.csv)
# =============================================================================

def load_coupon(engine) -> bool:
    log.info("Step 3/4 — Loading coupon from coupon.csv ...")

    df = load_csv("coupon.csv",
                  required_cols=["campaign", "coupon_upc", "product_id"])
    if df is None:
        return False

    df = df.rename(columns={"campaign": "campaign_id"})
    df["coupon_upc"]  = pd.to_numeric(df["coupon_upc"],  errors="coerce")
    df["product_id"]  = pd.to_numeric(df["product_id"],  errors="coerce")
    df["campaign_id"] = pd.to_numeric(df["campaign_id"], errors="coerce")
    df = df.dropna(subset=["campaign_id", "coupon_upc", "product_id"])
    df = df.astype({"campaign_id": int, "coupon_upc": int, "product_id": int})
    df = df.drop_duplicates()

    # Drop product_ids not in dim_products (FK safety — coupon table references products)
    with engine.connect() as conn:
        known_products = {
            r[0] for r in conn.execute(
                text("SELECT product_id FROM dim_products")
            ).fetchall()
        }
    unknown_p = set(df["product_id"].unique()) - known_products
    if unknown_p:
        log.warning("  Dropping %d rows with unknown product_ids (not in dim_products)",
                    len(df[df.product_id.isin(unknown_p)]))
        df = df[~df.product_id.isin(unknown_p)]

    # TypeA vs B/C pool sizes
    with engine.connect() as conn:
        camp_types = pd.read_sql(
            text("SELECT campaign_id, campaign_type FROM dim_campaigns"), conn
        )
    merged = df.merge(camp_types, on="campaign_id", how="left")
    pool_by_type = merged.groupby("campaign_type")["coupon_upc"].nunique()
    log.info("  Unique coupons in pool by campaign type: %s",
             pool_by_type.to_dict())
    log.info("  TypeA note: this is the full pool — "
             "individual 16-coupon allocations per household are not in the data.")

    written = upsert_df(df, "coupon", engine,
                        pk_cols=["campaign_id", "coupon_upc", "product_id"])
    log.info("  coupon rows written: %d", written)
    return True


# =============================================================================
# STEP 4 — coupon_redempt + fact_coupon_redemptions  (from coupon_redempt.csv)
# =============================================================================

def load_coupon_redempt(engine) -> bool:
    log.info("Step 4/4 — Loading coupon_redempt from coupon_redempt.csv ...")

    df = load_csv("coupon_redempt.csv",
                  required_cols=["household_key", "day", "coupon_upc", "campaign"])
    if df is None:
        return False

    df = df.rename(columns={"campaign": "campaign_id"})
    df["coupon_upc"]  = pd.to_numeric(df["coupon_upc"],  errors="coerce")
    df["campaign_id"] = pd.to_numeric(df["campaign_id"], errors="coerce")
    df = df.dropna(subset=["household_key", "day", "coupon_upc", "campaign_id"])
    df = df.astype({
        "household_key": int,
        "day": int,
        "coupon_upc": int,
        "campaign_id": int,
    })
    df = df.drop_duplicates()

    # ── 4a: Write raw coupon_redempt table ───────────────────────────────────
    written_raw = upsert_df(
        df, "coupon_redempt", engine,
        pk_cols=["household_key", "day", "coupon_upc", "campaign_id"]
    )
    log.info("  coupon_redempt rows written: %d", written_raw)

    # ── 4b: Populate fact_coupon_redemptions ─────────────────────────────────
    # Join to coupon table to recover product_id, then insert into fact table.
    # coupon_upc added to fact table by 03-add-campaign-tables.sql.
    log.info("  Populating fact_coupon_redemptions from coupon_redempt ...")

    insert_fact_sql = text("""
        INSERT INTO fact_coupon_redemptions
            (household_key, campaign_id, product_id, day, coupon_upc, coupon_disc)
        SELECT
            cr.household_key,
            cr.campaign_id,
            c.product_id,
            cr.day,
            cr.coupon_upc,
            NULL::numeric   AS coupon_disc   -- disc is on the transaction, not here
        FROM coupon_redempt cr
        LEFT JOIN coupon c
            ON  c.coupon_upc   = cr.coupon_upc
            AND c.campaign_id  = cr.campaign_id
        ON CONFLICT DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(insert_fact_sql)

    with engine.connect() as conn:
        fact_count = conn.execute(
            text("SELECT COUNT(*) FROM fact_coupon_redemptions")
        ).scalar()
    log.info("  fact_coupon_redemptions total rows: %d", fact_count)

    # Campaign type breakdown for redemptions
    with engine.connect() as conn:
        breakdown = pd.read_sql(text("""
            SELECT cd.campaign_type,
                   COUNT(DISTINCT cr.household_key)  AS hh_count,
                   COUNT(*)                          AS redemption_count
            FROM coupon_redempt cr
            JOIN dim_campaigns  cd ON cr.campaign_id = cd.campaign_id
            GROUP BY cd.campaign_type
            ORDER BY cd.campaign_type
        """), conn)

    log.info("  Redemption breakdown by campaign type:")
    for _, row in breakdown.iterrows():
        log.info("    %-8s  households: %4d  redemptions: %6d",
                 row.campaign_type, int(row.hh_count), int(row.redemption_count))

    log.info("  TypeA note: redeemed coupons above are observable. "
             "TypeA coupons received-but-not-redeemed cannot be recovered.")
    return True


# =============================================================================
# VERIFICATION SUMMARY
# =============================================================================

def verify(engine):
    log.info("=" * 60)
    log.info("Verification — row counts")
    log.info("=" * 60)

    checks = [
        ("dim_campaigns",           "campaign_id"),
        ("campaign_table",          "household_key"),
        ("coupon",                  "coupon_upc"),
        ("coupon_redempt",          "household_key"),
        ("fact_coupon_redemptions",  "redemption_id"),
    ]

    with engine.connect() as conn:
        for table, pk in checks:
            try:
                n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                log.info("  %-35s %8s rows", table, f"{n:,}")
            except Exception as e:
                log.error("  %-35s ERROR: %s", table, e)

        # Campaign participation summary
        log.info("")
        log.info("Campaign participation by type:")
        rows = conn.execute(text("""
            SELECT
                cd.campaign_type,
                COUNT(DISTINCT ct.campaign_id)    AS n_campaigns,
                COUNT(DISTINCT ct.household_key)  AS n_households
            FROM campaign_table ct
            JOIN dim_campaigns  cd ON ct.campaign_id = cd.campaign_id
            GROUP BY cd.campaign_type
            ORDER BY cd.campaign_type
        """)).fetchall()

        if rows:
            for r in rows:
                log.info("  %-8s  %2d campaigns  %4d households",
                         r[0], int(r[1]), int(r[2]))
        else:
            log.warning("  campaign_table appears empty — check CSV load above.")

        # Coupon pool breakdown
        log.info("")
        log.info("Coupon pool by campaign type:")
        pool_rows = conn.execute(text("""
            SELECT cd.campaign_type,
                   COUNT(DISTINCT c.coupon_upc) AS unique_coupons,
                   COUNT(DISTINCT c.product_id) AS redeemable_products
            FROM coupon c
            JOIN dim_campaigns cd ON c.campaign_id = cd.campaign_id
            GROUP BY cd.campaign_type
            ORDER BY cd.campaign_type
        """)).fetchall()

        if pool_rows:
            for r in pool_rows:
                log.info("  %-8s  %4d unique coupons  %5d redeemable products",
                         r[0], int(r[1]), int(r[2]))

        # Exposure rate (now that campaign_table exists)
        log.info("")
        log.info("Exposure rate check (households with >= 1 campaign):")
        exp = conn.execute(text("""
            SELECT
                COUNT(DISTINCT household_key)::numeric /
                (SELECT COUNT(*) FROM dim_households) * 100
            AS exposure_pct
            FROM campaign_table
        """)).scalar() or 0
        log.info("  Overall household campaign exposure: %.1f%%", exp)

    log.info("=" * 60)


# =============================================================================
# ENTRY POINT
# =============================================================================

def run():
    start = time.time()
    log.info("=" * 60)
    log.info("Campaign Tables Ingestion — Phase 2")
    log.info("=" * 60)
    log.info("PostgreSQL  : %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
    log.info("Data dir    : %s", DATA_DIR.resolve())
    log.info("-" * 60)

    engine = create_engine(PG_URL, pool_pre_ping=True)

    # Confirm prerequisite tables exist
    with engine.connect() as conn:
        for prereq in ["dim_households", "dim_products", "dim_campaigns",
                       "campaign_table", "coupon", "coupon_redempt"]:
            exists = conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_name = :t)"
            ), {"t": prereq}).scalar()
            if not exists:
                log.error(
                    "Table '%s' does not exist. "
                    "Run 03-add-campaign-tables.sql first.", prereq
                )
                return

    ok1 = load_dim_campaigns(engine)
    ok2 = load_campaign_table(engine)  if ok1 else False
    ok3 = load_coupon(engine)          if ok1 else False
    ok4 = load_coupon_redempt(engine)  if ok1 else False

    if ok1 and ok2 and ok3 and ok4:
        verify(engine)
    else:
        log.error("One or more steps failed — see errors above.")

    log.info("Total time: %.1fs", time.time() - start)


if __name__ == "__main__":
    run()
