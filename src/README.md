# Source Code

This folder contains reusable scripts that support the retail analytics workflow.

## ingest
- `mongo_ingest.py`: loads and hydrates MongoDB collections.
- `postgres_campaign_ingest.py`: loads campaign and transaction data into PostgreSQL.

## serving
- `model_scoring_service.py`: scoring service logic for applying the trained model outside the notebook environment.
