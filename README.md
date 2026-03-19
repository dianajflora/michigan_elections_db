# Michigan Elections Data Platform

This repository is being migrated from a notebook plus SQLite prototype into a modular Python application backed by PostgreSQL.

The current scaffold includes:

- SQLAlchemy ORM models for the core election data schema
- Alembic migration setup with an initial schema migration
- Reusable ETL modules for CSV parsing, validation, lookup resolution, and transactional upserts
- A Streamlit admin uploader app for local use against the shared PostgreSQL database
- A placeholder for the separate client query app, which is the next build step

## Proposed project structure

```text
.
|-- alembic.ini
|-- alembic/
|   |-- env.py
|   |-- script.py.mako
|   `-- versions/
|       `-- 20260203_01_create_core_tables.py
|-- apps/
|   |-- admin_app.py
|   `-- query_app.py
|-- src/
|   `-- mielections/
|       |-- config/
|       |-- db/
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
alembic upgrade head
streamlit run apps/admin_app.py
```

Recommended load order:

1. `counties`
2. `jurisdictions`
3. `locations`
4. `elections`
5. `election_usage`

`election_date`, `open_date`, and `close_date` are migrated to PostgreSQL `DATE`.

Deployment instructions are intentionally deferred until the separate client query app is implemented.
