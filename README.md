# Michigan Elections Data Platform

This repository is being migrated from a notebook plus SQLite prototype into a modular Python application backed by PostgreSQL.

The current scaffold includes:

- SQLAlchemy ORM models for the core election data schema
- Reusable ETL modules for CSV parsing, validation, lookup resolution, and transactional upserts
- A Streamlit admin uploader app for local use against the shared PostgreSQL database
- A client query app for safe browsing and export
- A small schema bootstrap CLI that creates or rebuilds the ORM-managed tables without Alembic

## Proposed project structure

```text
.
|-- apps/
|   |-- admin_app.py
|   `-- query_app.py
|-- src/
|   `-- mielections/
|       |-- config/
|       |-- db/
|       |   `-- bootstrap.py
|       `-- etl/
|-- .env.example
|-- requirements.txt
`-- streamlit.py
```

## Local setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Set `DATABASE_URL` to your hosted PostgreSQL instance, then run:

```powershell
$env:PYTHONPATH = "src"
python -m mielections.db.bootstrap --rebuild
streamlit run apps/admin_app.py
```

Recommended load order:

1. `counties`
2. `locations`
3. `elections`
4. `election_usage`

Current table layout:

1. `counties`
2. `locations`
   Columns: `county_id`, `location_name`, `address`, `city`, `zip_code`, `jurisdiction_name`, `precinct`, `latitude`, `longitude`, `handicap_accessible`, `access_notes`, `location_description`
3. `elections`
4. `election_usage`
   Columns: `election_id`, `location_id`, `location_function`, `day`, `hour`

`election_date` remains a PostgreSQL `DATE`.

Deployment instructions are intentionally deferred until the separate client query app is implemented.
