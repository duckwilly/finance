# Project overview
This project is a FastAPI-based financial dashboard and backend that models companies, individuals, stocks, and transactions. It provides APIs and server-side pages to view, search, and analyze financial data, with a service layer that encapsulates business logic and a database layer for persistent storage and migrations.

Key goals
- Present clear dashboards for companies and individuals showing net worth, cash, holdings, income/expenses, and cash flow for a selected period.
- Provide admin tools for browsing, searching, and drilling into companies, individuals, and transactions.
- Aggregate and analyze stock holdings across clients, including profit/loss stats and leaderboards.
- Keep business logic separate from persistence and routing for maintainability and testability. 
- Bloat should be avoided. Code should be compact and readable. 
- The project should be buildable in one go using ``make quickstart``, and care should be taken to keep README.md up to date.

Architecture and components
- FastAPI application (app/main.py) exposing routers for dashboards and APIs.
- Core utilities for config, logging, and security.
- SQLAlchemy models for persistent data.
- Pydantic schemas for request/response validation.
- Services layer implementing calculations, reconciliation, and domain rules.
- Templates/static for any server-rendered UI; tests and CI-ready structure.
- A logger is implemented and should be used where needed for debugging and monitoring purposes.

Tech stack
- Python, MariaDB, FastAPI, SQLAlchemy, Pydantic, Alembic, Jinja2, pytest.

Developer notes
- Directory layout separates APIs, models, schemas, services, and DB for clear ownership.
- Add auth and role-based access controls in core/security for admin vs. user views.
- Implement pagination, filtering, and indexing for transaction and listing endpoints to ensure scalability.
- Keep models modular so shared functionality can be factored into reusable components.

Intended users: admin, business owners (company views), and individual account holders who need interactive dashboards and admin oversight.

# /app directory scaffold

## Directory structure

finance/
├── app/
│   ├── README.md            # This readme
│   ├── __init__.py
│   ├── main.py              # FastAPI application instance and startup
│   ├── api/ or routers/     # API routes (FastAPI routers for different endpoints)
│   │   ├── __init__.py
│   │   ├── dashboard.py     # e.g., routes for dashboard pages or data APIs
│   │   └── ... (other route files as needed)
│   ├── core/                # Core config, logging, auth, and dependency utilities
│   │   ├── __init__.py
│   │   ├── config.py        # Settings and configuration (env variables)
│   │   └── security.py      # Auth/security utilities (to be decided how to implement this)
│   ├── models/              # SQLAlchemy models (database tables)
│   │   ├── __init__.py
│   │   ├── companies.py     # Company-related models 
│   │   ├── individuals.py   # Individual/user models
│   │   ├── stocks.py        # Stock/equity models 
│   │   ├── transactions.py  # Transaction records
│   │   └── admin.py         # Admin-related models 
│   ├── schemas/             # Pydantic schemas (data shapes for input/output)
│   │   ├── __init__.py
│   │   ├── companies.py     # Pydantic models for companies data
│   │   ├── individuals.py   # Pydantic models for individual/user data
│   │   ├── stocks.py        # Pydantic models for stock data
│   │   ├── transactions.py  # Pydantic models for transactions and requests
│   │   └── admin.py         # Pydantic models for admin interfaces and responses
│   ├── services/            # Business logic layer (processing, calculations)
│   │   ├── __init__.py
│   │   ├── companies_service.py   # Company-related business logic
│   │   ├── individuals_service.py # Individuals-related business logic
│   │   ├── stocks_service.py      # Market data processing, reconciliation
│   │   ├── transactions_service.py# Transaction processing, settlement, validation
│   │   └── admin_service.py       # Admin operations, audits, and management utilities
│   │   └── ...                    # other service modules as needed
│   ├── db/                  # Database related modules
│   │   ├── __init__.py
│   │   ├── database.py      # SQLAlchemy session setup (engine, SessionLocal)
│   │   └── migrations/      # Alembic migrations (versioned change scripts)
│   ├── templates/           # Jinja2 templates for HTML pages (if using server-side rendering)
│   │   └── *.html           # e.g., dashboard.html, layout.html, etc.
│   ├── static/              # Static files (CSS, JS, images) for the frontend
│   │   ├── css/
│   │   └── js/
│   └── middleware/ (optional) # Custom Starlette/FastAPI middleware, if needed
│
├── tests/                   # Test cases
│   ├── __init__.py
│   ├── test_api.py          # tests for API routes
│   └── test_services.py     # tests for service logic
├── requirements.txt         # Python dependencies 
├── README.md                # Documentation to explain the project
└── .env                     # Environment variable definitions (not committed to VCS)

## Models
This section will go over the required functionality of the different models. Functionality that is shared between different models should be separated out into new models. 

### Companies
Companies should have a main dashboard with an overview of its accounts and transactions in a selected period. The dashboard should show net worth, cash holdings, income and expenses in a selected period and net cash flow. There should be an overview of the payroll of the company and an overview of income and expenses that can be expanded for more details on a transaction level. 

### Individuals
The individual should have a main dashboard with an overview of the user's accounts and transactions in a selected period. The dashboard should show the user's net worth, cash accounts and brokerage holdings, their income and expenses in the selected period and the net cash flow. There should be an overview of their accounts (i.e. checking, brokerage, savings), a breakdown of income, brokerage holdings (with stats like profit), and an overview of expenses in different categories (that can be expanded for more details on a transaction level). 

### Admin
The admin should be able to browse all other views. The admin should be able to view lists of companies and individuals (paged and searchable), and click through to every company/individual view. 

### Stocks 
An overview of the stock holdings of all clients, which includes statistics about profits/losses for different users and a leaderboard for biggest profits/losses.

### Transactions
A searchable overview for the admin of all transactions, that allows the admin to click through to the different users involved.

# Common Development Workflows

## Environment Setup
- Use ``python3`` to run python commands.
- **Virtual Environment**: Located at `.venv/` in the project root
- **Environment Variables**: Database credentials and configuration are stored in `.env` file (not committed to VCS)
- **Database**: MariaDB running via Docker Compose with connection details in `.env`
- **Quick Setup**: Use `make quickstart` for automated setup (see README.md for details). note that in most cases, everything will already be running, so test whether you can connect to the database before trying to build from scratch.
- **Requirements**: See requirements.txt for python requirements. If you add any new requirements, make sure to update it. 

## Development Commands
- `make quickstart` - Complete automated setup (venv, deps, database, seed data, server)

## Key Files for Development
- **Main App**: `app/main.py` - FastAPI application entry point
- **Database Config**: `app/db/database.py` - SQLAlchemy session management
- **Environment**: `.env` - Database credentials and configuration
- **Schema**: `sql/schema.sql` - Database structure
- **Seed Data**: `data/seed/` - CSV files with synthetic data
- **Scripts**: `scripts/` - Data generation and loading utilities

## Testing
- Run tests: `pytest`
- Test database connectivity: `python scripts/db_smoketest.py`
- Generate fresh data: `python scripts/gen_seed_data.py`

## Server Development
- Start development server: `uvicorn app.main:app --reload`
- Admin dashboard: Available at `/dashboard/` route
- Static assets: Served from `app/static/` directory

For more detailed setup instructions and project information, see `README.md`. 
