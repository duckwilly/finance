# Finance Platform (Rebuild)

This project is a finance dashboard that simulates the clients of a bank and their transactions.
The application is built using an architecture that separates APIs, services, models, and data
access. 

## Tech Stack
- Python 3.11+
- FastAPI (application framework)
- SQLAlchemy (database access)
- MariaDB via Docker Compose
- Alembic (migrations placeholder)
- Pytest (tests)
- Faker & CSV scripts for synthetic data generation

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
├── main.py                 # FastAPI application factory (includes stub router)
├── middleware/             # Placeholder package for custom middleware
├── models/                 # Stub SQLAlchemy models
├── routers/                # FastAPI routers (currently a dashboard placeholder)
├── schemas/                # Stub Pydantic schemas
├── services/               # Stub service layer modules
├── static/                 # Static asset directories (.gitkeep placeholders)
└── templates/              # Template directory (.gitkeep placeholder)
```

Additional top-level directories provide SQL schema, scripts for generating seed
CSV data, Docker Compose definitions, and automated tests.

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

Because the web frontend is being rebuilt, the script stops after preparing the
infrastructure and prints a reminder about running `uvicorn app.main:app`
manually once routes are implemented. Install `uvicorn` separately when you are
ready to iterate on the API.

### Running Tests

```bash
pytest
```

The new tests validate that the FastAPI application factory wires the placeholder
router and that configuration defaults are loaded from the environment.

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

Once API endpoints exist, install `uvicorn` and launch the application with
`uvicorn app.main:app` (the Makefile no longer starts a server automatically).

## Synthetic Data & Tooling
The data generation scripts continue to provide a comprehensive dataset covering
individuals, companies, transactions, and stock trades. Adjust the parameters in
`scripts/gen_seed_data.py` to create lighter or heavier workloads, then reload
with `scripts/load_csvs.py`. These scripts rely on the shared configuration and
logging modules preserved during the rebuild.

## Next Steps
- Flesh out domain models in `app/models/` and their accompanying schemas.
- Implement service-layer logic that aggregates financial metrics.
- Introduce FastAPI routers for admin, company, and individual dashboards.
- Add Alembic migrations to replace the raw SQL schema.
- Expand automated tests alongside new features.
