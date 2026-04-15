# Retail Analytics — Phase 2 Setup Guide

## Directory Structure

Place files in this layout before running Docker Compose:

```
project-root/
│
├── docker-compose.yml                  ← Unified orchestration file
│
├── data/
│   └── raw/                            ← Dunnhumby CSV files go here
│       ├── transaction_data.csv
│       ├── hh_demographic.csv
│       ├── campaign_table.csv
│       ├── campaign_desc.csv
│       ├── product.csv
│       ├── coupon.csv
│       ├── coupon_redempt.csv
│       └── causal_data.csv
│
├── postgres/
│   └── init/
│       ├── 01-create-databases.sh      ← Creates retail_staging + retail_analytics
│       └── 02-create-schema.sql        ← Snowflake schema (fact + dim + mart tables)
│
├── mongo/
│   └── init-rs.js                      ← Replica set initialisation
│
│
└── ingestion/
    ├── Dockerfile.postgres             ← PostgreSQL ETL container
    ├── Dockerfile.mongo                ← MongoDB Customer 360 container
    ├── requirements-postgres.txt
    ├── requirements-mongo.txt
    ├── postgres_ingest.py
    ├── mongo_ingest.py
    
    
```

---

## Startup Sequence

The `depends_on` + health check conditions enforce this order automatically:

```
Phase 1 (parallel):   postgres  ──┐
                      mongo     ──┤──→ postgres_ingest (once all healthy)
                      
                    

Phase 2 (parallel):   postgres_ingest completes ──→ mongo_ingest
                                                
                                                
```

---

## Commands

```bash
# Start all four databases (background)
docker compose up -d postgres mongo 

# Watch health checks
docker compose ps

# Run full pipeline once databases are healthy
docker compose up postgres_ingest
docker compose up mongo_ingest 

# Or bring everything up at once (compose handles ordering automatically)
docker compose up

# Tear down (keeps volumes — data survives)
docker compose down

# Tear down and wipe all data
docker compose down -v
```

---

## Port Reference

| Service       | Port  | Tool                          |
|---------------|-------|-------------------------------|
| PostgreSQL    | 5432  | pgAdmin, DBeaver, psql        |
| MongoDB       | 27017 | MongoDB Compass               |
  |

---

## Team Responsibilities

| Member           | Service            | Ingestion Script              |
|------------------|--------------------|-------------------------------|
| PostgreSQL owner | `postgres_ingest`  | `postgres_ingest.py`          |
| MongoDB owner    | `mongo_ingest`     | `mongo_ingest.py`             |
