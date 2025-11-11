# Finance Platform

This project is a finance dashboard that simulates the clients of a bank and their transactions.
The application is built using an architecture that separates APIs, services, models, and data
access. 

## Tech Stack
- Backend: Python 3.11, FastAPI, Uvicorn
- Persistence: MariaDB (Docker Compose), SQLAlchemy 2.x, Alembic, PyMySQL
- Validation & Auth: Pydantic v2, python-multipart, PyJWT
- Presentation: Jinja2 templates, static assets served by FastAPI
- Tooling & Data: pytest, httpx, rich, Faker, yfinance, python-dotenv

## Project Layout
```
app/
├── README.md               # Overview of the application scaffold
├── __init__.py             # Re-exports create_app/get_logger helpers
├── config/                 # Backwards-compatible configuration entrypoint
├── core/
│   ├── config.py           # Environment-driven settings loader
│   ├── logger.py           # Wrapper around the shared logging utilities
│   ├── log/                # Logging implementation (init, progress, timing)
│   └── security.py         # Stub security helpers
├── db/
│   ├── __init__.py         # Convenience exports for database helpers
│   ├── database.py         # Engine and session factory helpers
│   ├── engine.py           # Legacy-compatible engine constructors
│   ├── migrations/         # Placeholder for Alembic migrations
│   └── session.py          # Context-managed SQLAlchemy sessions
├── log.py                  # Legacy import path for logging helpers
├── main.py                 # FastAPI application factory (includes dashboard router)
├── middleware/             # Placeholder package for custom middleware
├── models/                 # Stub SQLAlchemy models
├── routers/                # FastAPI routers (admin dashboard + future modules)
├── schemas/                # Stub Pydantic schemas
├── services/               # Stub service layer modules
├── static/                 # Static asset directories (e.g. css/admin.css theme)
└── templates/              # Template directory (e.g. admin/dashboard.html)
```

Additional top-level directories provide SQL schema, scripts for generating seed
CSV data, Docker Compose definitions, and automated tests.

## Admin Dashboard

The `/dashboard/` route now renders a server-side Jinja2 template populated with
metrics from the `AdminService`. A pastel SaaS-inspired theme lives in
`app/static/css/admin.css`, making it easy to re-skin the layout by tweaking a
handful of CSS custom properties. FastAPI serves these assets via the `/static`
mount configured in `app/main.py`.

## Getting Started

### Quickstart (recommended)
The quickest way to replicate the CI workflow locally is:

```bash
make quickstart
```

The script will:
1. Create a Python virtual environment (if missing).
2. Install the dependencies listed in `requirements.txt`.
3. Launch MariaDB using Docker Compose.
4. Wait for the database to become available.
5. Apply `sql/schema.sql`.
6. Run the database smoketest and generate seed CSV files.
7. Load the seed data into MariaDB.
8. Start the FastAPI development server with `uvicorn` (running until you stop it).

Set `QUICKSTART_SKIP_DOCKER=1` to reuse an existing MariaDB instance (as the CI
workflow does) or `QUICKSTART_START_SERVER=0` if you only need to prepare the
database without launching the API. The default behaviour assumes a fresh
developer workstation and launches everything automatically.

### Running Tests

```bash
pytest
```

The test suite now exercises the admin dashboard template rendering (ensuring
Jinja2 and the dependency overrides function correctly) alongside the existing
application structure checks.

### Manual Setup (optional)
If you prefer to perform the steps yourself:
1. Create a `.env` file (or copy `.env.example`) with the database credentials
   used by Docker Compose.
2. Start MariaDB: `docker compose -f docker/docker-compose.yaml up -d`.
3. Create & activate a virtual environment, then install dependencies.
4. Apply the schema: `mysql -h 127.0.0.1 -uroot -proot finance < sql/schema.sql`
   (or run the statements in your SQL client).
5. Smoke test connectivity: `python scripts/db_smoketest.py`.
6. Generate and load seed data using `python scripts/gen_seed_data.py` followed by
   `python scripts/load_csvs.py`.

When you are ready to run the application manually, execute
`uvicorn app.main:app --reload` (the quickstart already installs `uvicorn` and
will launch the server for you unless disabled via environment variables).

## Synthetic Data & Tooling
The data generation scripts provide a comprehensive dataset covering individuals,
companies, a double-entry ledger, and stock trades with real historical price data.

### Data Sources
- **Stock Prices**: Real historical daily prices from 2021-present via Yahoo Finance
- **FX Rates**: Real USD/EUR exchange rates from 2021-present
- **Journal Ledger**: Synthetic entries and lines that balance to zero per entry
- **Stock Trades**: Portfolio simulation with 70% of users having varying investment levels

### Data Generation Workflow
1. `scripts/fetch_stock_prices.py` - Downloads real historical stock prices and FX rates
2. `scripts/gen_seed_data.py` - Generates synthetic users, companies, journal entries/lines, and trades; it validates that every entry balances before writing CSVs
3. `scripts/load_csvs.py` - Loads all data into the database

Adjust parameters in `scripts/gen_seed_data.py` to create lighter or heavier workloads.

> **After updating**: rerun `python scripts/gen_seed_data.py --seed 42` (or your preferred settings) followed by `python scripts/load_csvs.py` to regenerate journal entries/lines. The loader now expects the journal CSVs and will ignore the retired `transactions.csv` artifact.

## Next Steps
- Flesh out domain models in `